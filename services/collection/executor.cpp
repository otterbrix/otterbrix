#include "executor.hpp"

#include <array>
#include <atomic>
#include <chrono>

#include <components/catalog/catalog_codes.hpp>
#include <components/context/execution_context.hpp>
#include <components/planner/planner.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_allocate_oids.hpp>
#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_set_timezone.hpp>
#include <components/logical_plan/param_storage.hpp>
// The executor only sees the base operator_t: each operator's DML I/O
// intercept lives in its own await_async_and_resume, not here. The commit
// pipeline's commit_id comes back via pipeline::context_t::committed_id.
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/executor.hpp>
// catalog-resolve helpers (services::catalog_resolve) let the executor drive
// resolve without a dispatcher dependency. Defined in
// services/dispatcher/enrich_logical_plan.cpp, already linked into the library.
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_transaction.hpp>
#include <components/planner/optimizer.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/enrich_logical_plan.hpp>
#include <services/dispatcher/plan_resolve_index.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

using namespace components::cursor;

namespace services::collection::executor {

    // Test-observable streaming-path counter (see executor.hpp). Bumped once per
    // execute_pipeline() entry; relaxed because it is an instrumentation read, not a
    // happens-before edge. DEV_MODE-only: production binaries carry nothing (the
    // integration test target compiles with -DDEV_MODE).
#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_streaming_pipeline_runs{0};
        std::atomic<uint64_t> g_dml_appends_reverted{0};
        std::atomic<uint64_t> g_dml_flush_count{0};
    } // namespace

    uint64_t streaming_pipeline_runs() noexcept { return g_streaming_pipeline_runs.load(std::memory_order_relaxed); }
    uint64_t dml_appends_reverted() noexcept { return g_dml_appends_reverted.load(std::memory_order_relaxed); }
    uint64_t dml_flush_count() noexcept { return g_dml_flush_count.load(std::memory_order_relaxed); }
#endif

    // ---- behavior/dispatch_traits sync check ----
    // Ensures behavior() handles every method registered in dispatch_traits
    // (positional msg_id: a missed case = silent message loss). When adding a
    // method: dispatch_traits entry + behavior() case + kBehaviorHandledIds.
    namespace {
        template<typename MethodList>
        struct behavior_expected_ids_t;

        template<auto... Ptrs>
        struct behavior_expected_ids_t<actor_zeta::type_traits::type_list<actor_zeta::method_map_entry<Ptrs>...>> {
            static constexpr std::array<actor_zeta::mailbox::message_id, sizeof...(Ptrs)> value{
                actor_zeta::msg_id<executor_t, Ptrs>...};
        };

        constexpr auto kImplementedIds = behavior_expected_ids_t<executor_t::dispatch_traits::methods>::value;

        constexpr std::array kBehaviorHandledIds{
            actor_zeta::msg_id<executor_t, &executor_t::execute_plan_full>,
            actor_zeta::msg_id<executor_t, &executor_t::register_udf>,
            actor_zeta::msg_id<executor_t, &executor_t::set_explain_renderer>,
            actor_zeta::msg_id<executor_t, &executor_t::poke_msg>,
        };

        constexpr bool behavior_covers_all_implements() noexcept {
            if (kImplementedIds.size() != kBehaviorHandledIds.size())
                return false;
            for (auto id : kImplementedIds) {
                bool found = false;
                for (auto hid : kBehaviorHandledIds) {
                    if (id == hid) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return false;
            }
            return true;
        }

        static_assert(behavior_covers_all_implements(),
                      "behavior() is out of sync with dispatch_traits: "
                      "add a case to behavior() AND an entry to kBehaviorHandledIds");
    } // namespace

    namespace {
        // EXPLAIN ANALYZE row tally: sum row counts across a chunk vector.
        inline uint64_t count_rows(const components::operators::chunks_vector_t& chunks) noexcept {
            uint64_t rows = 0;
            for (const auto& c : chunks) {
                rows += c.size();
            }
            return rows;
        }

        // EXPLAIN ANALYZE stopwatch: samples steady_clock only when `on` (zero cost off the ANALYZE
        // path). elapsed() is read only under an `if (analyze)` guard at each call site, which also
        // keeps count_rows off the normal (non-analyze) hot path.
        struct analyze_scope {
            std::chrono::steady_clock::time_point t0;
            explicit analyze_scope(bool on) noexcept
                : t0(on ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{}) {}
            [[nodiscard]] std::chrono::nanoseconds elapsed() const noexcept {
                return std::chrono::steady_clock::now() - t0;
            }
        };

        // Build-side selection: collect the resolved child table oids of
        // every INNER hash join in the optimized plan tree. execute_plan_full
        // fetches each oid's live row count so create_plan_join can put the
        // smaller table on the hash build side. Recurses the whole tree because
        // q2-q4 lower to several stacked joins. A child that is not a single base
        // table (nested join / subquery -> INVALID_OID) is dropped here, so it is
        // never sent to disk. Duplicate oids collapse in the set.
        void collect_inner_hash_join_oids(const components::logical_plan::node_ptr& node,
                                          std::pmr::set<components::catalog::oid_t>& out) {
            if (!node) {
                return;
            }
            if (node->type() == components::logical_plan::node_type::join_t) {
                const auto* join = static_cast<const components::logical_plan::node_join_t*>(node.get());
                if (join->type() == components::logical_plan::join_type::inner &&
                    join->algo() == components::logical_plan::node_join_t::join_algo::hash &&
                    !node->children().empty()) {
                    const auto left_oid = node->children().front()->table_oid();
                    const auto right_oid = node->children().back()->table_oid();
                    if (left_oid != components::catalog::INVALID_OID) {
                        out.insert(left_oid);
                    }
                    if (right_oid != components::catalog::INVALID_OID) {
                        out.insert(right_oid);
                    }
                }
            }
            for (const auto& child : node->children()) {
                collect_inner_hash_join_oids(child, out);
            }
        }
    } // namespace

    plan_t::plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                   const components::logical_plan::storage_parameters* parameters,
                   services::context_storage_t&& context_storage)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage) {}

    executor_t::executor_t(std::pmr::memory_resource* resource,
                           actor_zeta::address_t parent_address,
                           actor_zeta::address_t wal_address,
                           actor_zeta::address_t disk_address,
                           actor_zeta::address_t index_address,
                           log_t&& log,
                           uint64_t dml_flush_row_threshold)
        : actor_zeta::basic_actor<executor_t>{resource}
        , parent_address_(std::move(parent_address))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
        , log_(log)
        , function_registry_(resource)
        , dml_flush_row_threshold_(dml_flush_row_threshold)
        , explain_renderers_(resource) {
        register_default_functions(function_registry_);
        explain_renderers_.push_back(&render_postgres); // slot 0 = built-in default renderer
    }

    actor_zeta::behavior_t executor_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<executor_t, &executor_t::execute_plan_full>: {
                co_await actor_zeta::dispatch(this, &executor_t::execute_plan_full, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::register_udf>: {
                co_await actor_zeta::dispatch(this, &executor_t::register_udf, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::set_explain_renderer>: {
                co_await actor_zeta::dispatch(this, &executor_t::set_explain_renderer, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::poke_msg>: {
                co_await actor_zeta::dispatch(this, &executor_t::poke_msg, msg);
                break;
            }
            default:
                break;
        }
    }

    // No-op mailbox handler whose ONLY purpose is to exist as a poke target:
    // the dispatcher's lost-wakeup watchdog PUSHes it into a stale executor's
    // mailbox to unblock a reader_blocked actor whose awaited future is ready
    // (docs/actor-zeta-lost-wakeup.md). Remove together with the watchdog once
    // the framework fix lands.
    executor_t::unique_future<void> executor_t::poke_msg() { co_return; }

    auto executor_t::make_type() const noexcept -> const char* { return "executor"; }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan(components::session::session_id_t session,
                             components::logical_plan::execution_plan_t plan,
                             services::context_storage_t context_storage,
                             components::table::transaction_data txn,
                             uint64_t lowest_active_start_time,
                             std::pmr::vector<explain_plan_node> captured_subplans) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        using namespace components::logical_plan;

        // Pure operator-pipeline run. The txn lifecycle (begin / commit /
        // abort / accumulate) is owned entirely by execute_plan_full's tail;
        // this function lowers the plan, drives the operators, and returns the
        // cursor plus the raw range accumulators.
        components::table::transaction_data txn_data = txn;

        // With the transformer wrap, logical_plan may be
        // sequence_t(catalog_resolve_*, ..., <consumer>). The limit_t node
        // lives as a child of the consumer, not on the wrapping sequence_t.
        // Skip the catalog_resolve_* prefix first (it never carries a limit),
        // then fall back to iterating the raw children for non-DML plans.
        auto limit = components::logical_plan::limit_t::unlimit();
        auto* limit_lookup_node = plan.sub_queries.back().get();
        if (limit_lookup_node && limit_lookup_node->type() == components::logical_plan::node_type::sequence_t) {
            // Find first non-resolve child as the limit-carrying consumer.
            auto is_catalog_resolve = [](components::logical_plan::node_type t) {
                return t == components::logical_plan::node_type::catalog_resolve_t;
            };
            for (const auto& c : limit_lookup_node->children()) {
                if (c && !is_catalog_resolve(c->type())) {
                    limit_lookup_node = c.get();
                    break;
                }
            }
        }
        for (const auto& child : limit_lookup_node->children()) {
            if (child->type() == components::logical_plan::node_type::limit_t) {
                limit = static_cast<components::logical_plan::node_limit_t*>(child.get())->limit();
            }
        }

        // Plan generators read parameter values through context_storage (e.g.
        // create_plan_match probes context.parameters to gate parameterised
        // index scans). Point it at THIS frame's `parameters` local: the
        // pointer is consumed only at plan-build time (inside create_plan),
        // before the move into plan_data below.
        context_storage.parameters = &plan.parameters->parameters();
        components::operators::operator_ptr node = planner::create_plan(context_storage,
                                                                        function_registry_,
                                                                        plan.sub_queries.back(),
                                                                        limit,
                                                                        &plan.parameters->parameters());

        if (!node) {
            // Surface the error via cursor so execute_plan_full's tail routes
            // through abort.
            co_return execute_result_t{make_cursor(resource(),
                                                   core::error_t(core::error_code_t::create_physical_plan_error,
                                                                 std::pmr::string{"invalid query plan", resource()}))};
        }

        node->set_as_root();

        // === EXPLAIN / EXPLAIN ANALYZE ===
        // Stable handle to the physical root before traverse_plan_ moves `node`: operator left_/
        // right_ are frozen after create_plan, so explain_root walks the exact instances the
        // pipeline drives (and, for ANALYZE, the counters they accumulate). Snapshot oid->name now,
        // BEFORE context_storage is moved into traverse_plan_ below.
        components::operators::operator_ptr explain_root =
            plan.explain != components::logical_plan::explain_type::none ? node : nullptr;
        explain_name_map_t explain_names{resource()};
        const bool explain_analyze = plan.explain == components::logical_plan::explain_type::analyze;
        if (explain_root && explain_analyze) {
            // ANALYZE builds the IR AFTER context_storage is moved into traverse_plan_ below, so it must
            // snapshot oid->name now while the catalog is still alive. Plan-only skips this pass — its
            // single builder walk resolves scan names straight from the still-live catalog (see below).
            explain_name_collector nc{context_storage, explain_names};
            explain_root->explain(nc.sink());
        }
        if (plan.explain == components::logical_plan::explain_type::plan) {
            // Plan-only EXPLAIN: build the physical tree's IR WITHOUT executing. context_storage is still
            // alive, so the builder resolves scan names from the live catalog (no collector).
            if (plan.explain_capture_ir) {
                // A flattened sub-query captured for the main query's InitPlan section: return its IR
                // (structure only, no ANALYZE stats). No render, no execution — the cursor is unused by
                // the caller (it reads captured_explain_ir) but must be a success cursor.
                explain_ir_builder b{resource(), explain_names, &context_storage};
                explain_root->explain(b.sink());
                execute_result_t out{make_cursor(resource())};
                out.captured_explain_ir = b.release();
                co_return std::move(out);
            }
            // Main plan: render the tree, hanging each flattened sub-query's captured IR as an InitPlan
            // (structure only) — like PostgreSQL's plain EXPLAIN.
            co_return execute_result_t{render_explain_(explain_root,
                                                       explain_names,
                                                       &context_storage,
                                                       plan.explain_render_id,
                                                       false,
                                                       std::move(captured_subplans))};
        }

        auto plan_data = traverse_plan_(std::move(node), plan.parameters->parameters(), std::move(context_storage));
        plan_data.analyze = explain_analyze;

        auto result = co_await execute_sub_plan_(session, std::move(plan_data), txn_data, lowest_active_start_time);

        // EXPLAIN ANALYZE: the query just ran with instrumentation. Two sub-cases:
        //   * capture (a flattened sub-plan, explain_capture_ir): build the IR and carry it back in
        //     captured_explain_ir, but KEEP the data cursor — the sub-query loop still compacts it.
        //   * main plan: substitute the rendered plan (+ any captured sub-plan InitPlans) for the data
        //     cursor (only on success — an error stays an error cursor so the tail aborts).
        // The pipeline left the counters on explain_root's instances; build the IR post-exec now.
        // context_storage is gone (moved above), so pass nullptr — the builder reads the snapshot.
        std::optional<explain_plan_node> captured_ir;
        if (explain_analyze && result.cursor->is_success()) {
            if (plan.explain_capture_ir) {
                explain_ir_builder b{resource(), explain_names, nullptr};
                explain_root->explain(b.sink());
                captured_ir = b.release();
            } else {
                result.cursor = render_explain_(explain_root,
                                                explain_names,
                                                nullptr,
                                                plan.explain_render_id,
                                                true,
                                                std::move(captured_subplans));
            }
        }

        // Raw pipeline result. Three cases, distinguished by the vector state
        // and resolved by execute_plan_full's accumulate/commit tail:
        //   * non-DML (DDL, SELECT): dml_* empty (pg_catalog_* populated for DDL).
        //   * DML success: vectors populated; the tail accumulates them onto
        //     transaction_t and (autocommit) runs the commit pipeline.
        //   * DML error: dml_appends / pg_catalog_appends still carry the
        //     not-yet-published ranges so the tail's abort cascade reverts them.
        execute_result_t out{std::move(result.cursor),
                             std::move(result.pg_catalog_appends),
                             std::move(result.pg_catalog_delete_tables),
                             std::move(result.pg_attribute_commit_id_backfills),
                             std::move(result.dml_appends),
                             std::move(result.dml_deletes),
                             std::move(result.dropped_storage_oids),
                             std::move(result.created_storage_oids),
                             std::move(result.created_indexes),
                             result.commit_id};
        out.captured_explain_ir = std::move(captured_ir);
        co_return std::move(out);
    }

    components::cursor::cursor_t_ptr
    executor_t::render_explain_(const components::operators::operator_ptr& explain_root,
                                const explain_name_map_t& names,
                                const services::context_storage_t* cs,
                                uint32_t render_id,
                                bool analyze,
                                std::pmr::vector<explain_plan_node> captured_subplans) {
        // cs != nullptr (plan-only): resolve scan names from the live catalog. cs == nullptr (ANALYZE,
        // after context_storage was moved): read the pre-collected `names` snapshot.
        explain_ir_builder b{resource(), names, cs};
        explain_root->explain(b.sink());
        explain_plan_node root = b.release();
        // Hang each flattened sub-query's captured IR on the main root as an InitPlan. PG attaches
        // InitPlans at the topmost node of the query level; otterbrix runs every flattened sub-query
        // top-level (executor loop), so they are siblings here. Each node's subplan_returns ($M) was
        // stamped in the loop. All nodes share this executor's resource() — no pmr re-anchor.
        for (auto& sp : captured_subplans) {
            root.subplans.push_back(std::move(sp));
        }
        const auto render = resolve_explain_renderer_(render_id);
        if (render_id != 0 && !explain_slot_registered_(render_id)) {
            trace(log_,
                  "executor::explain: render_id {} not registered on this executor — using default (slot 0)",
                  render_id);
        }
        return render(resource(), root, analyze);
    }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan_full(components::session::session_id_t session,
                                  components::logical_plan::execution_plan_t plan) {
        // Full per-query pipeline: session-context fetch, optimize, resolve
        // wrap, catalog resolve, view splice, validate, enrich, planner
        // rewrites, operator pipeline, then the DML/DDL commit (or abort)
        // tail. The dispatcher only routes; ALL txn-state access goes through
        // its txn_*_msg mailbox handlers.
        using node_type = components::logical_plan::node_type;
        using components::logical_plan::node_aggregate_t;
        using components::logical_plan::node_catalog_resolve_t;
        using components::logical_plan::node_create_database_t;
        using components::logical_plan::node_join_t;
        using components::logical_plan::node_match_t;
        using components::logical_plan::node_ptr;
        using components::logical_plan::node_sequence_t;
        using components::logical_plan::node_t;
        using components::logical_plan::resolve_kind;

        // === Sub-query pre-execution (front-to-back) ===
        // The transformer flattens every nested sub-query into plan.sub_queries,
        // ordered so dependencies run first; sub_queries.back() is the main
        // query. Each non-final sub-query is run through this same full pipeline
        // (recursively, as a single-query plan that SHARES the parameter node),
        // its result compacted to a scalar/list via the mapping's compacter, and
        // bound as a parameter the later sub-queries / the main query reference
        // by id. Sharing the node is why execute_plan copies (not drains) the
        // parameters: each binding must survive into the next plan.
        //
        // Plan-only EXPLAIN does not EXECUTE the sub-queries (PostgreSQL: plain EXPLAIN runs no sub-plans),
        // but it still BUILDS each one's physical plan and captures its IR so the output shows the InitPlan
        // structure — exactly like PostgreSQL's plain EXPLAIN. EXPLAIN ANALYZE captures the executed IR
        // (with per-operator stats); normal execution captures nothing. Both EXPLAIN modes hang the captured
        // IRs on the main IR root as InitPlans (PG-faithful attach-at-root — otterbrix runs/plans them all
        // top-level).
        const bool run_sub_queries = plan.explain != components::logical_plan::explain_type::plan;
        const bool plan_only = plan.explain == components::logical_plan::explain_type::plan;
        const bool capture_ir = plan.explain == components::logical_plan::explain_type::analyze;
        std::pmr::vector<explain_plan_node> captured_subplans{resource()};
        for (std::size_t i = 0; (run_sub_queries || plan_only) && i + 1 < plan.sub_queries.size(); ++i) {
            auto* sub_root = services::catalog_resolve::effective_root_node(plan.sub_queries[i].get());
            const node_type sub_type = sub_root ? sub_root->type() : node_type::unused;
            // SQL standard: a sub-query is a query expression — never DML/DDL.
            if (sub_type == node_type::insert_t || sub_type == node_type::update_t || sub_type == node_type::delete_t) {
                co_return execute_result_t{make_cursor(
                    resource(),
                    core::error_t{core::error_code_t::sql_parse_error,
                                  std::pmr::string{"DML statement is not allowed in a sub-query", resource()}})};
            }
            components::logical_plan::execution_plan_t sub_plan{resource(), plan.sub_queries[i], plan.parameters};
            if (plan_only) {
                // Build the sub-query's physical plan + IR, NO execution (returned via captured_explain_ir).
                sub_plan.explain = components::logical_plan::explain_type::plan;
                sub_plan.explain_capture_ir = true;
            } else if (capture_ir) {
                // Instrument the sub-plan and make it BUILD its IR (kept alongside the data cursor).
                sub_plan.explain = components::logical_plan::explain_type::analyze;
                sub_plan.explain_capture_ir = true;
            }
            auto sub_result = co_await execute_plan_full(session, std::move(sub_plan));
            if (sub_result.cursor->is_error()) {
                co_return execute_result_t{std::move(sub_result.cursor)};
            }
            const auto& mapping = plan.sub_query_results[i];
            // PostgreSQL: the argument of WHERE / HAVING must be type boolean. For a bare
            // boolean-context scalar sub-query (`WHERE (SELECT ...)` / `HAVING (SELECT ...)`)
            // reject a non-boolean STATIC output type here — otherwise a numeric scalar would
            // silently coerce to bool (`(SELECT 1)` -> 0 rows, `(SELECT 0)` -> all rows). This
            // is a data-independent check on the sub-plan's resolved schema (stamped by the
            // recursive execute_plan_full's validate above — the SINGLE canonical type source),
            // so it runs BEFORE the plan-only continue: EXPLAIN of the bad query errors too, as
            // in PostgreSQL. NA (a zero-row / NULL scalar, whose static type is BOOLEAN for a
            // bool column anyway) is accepted — it selects nothing, not a type error.
            if (mapping.boolean_required) {
                const auto& sub_node = plan.sub_queries[i];
                assert(sub_node->has_output_types() && "boolean-required sub-query must be schema-stamped");
                const auto out_type = sub_node->output_types().front().type();
                if (out_type != components::types::logical_type::BOOLEAN &&
                    out_type != components::types::logical_type::NA) {
                    co_return execute_result_t{make_cursor(
                        resource(),
                        core::error_t{core::error_code_t::sql_parse_error,
                                      std::pmr::string{"argument of WHERE/HAVING must be type boolean", resource()}})};
                }
            }
            if ((capture_ir || plan_only) && sub_result.captured_explain_ir.has_value()) {
                // Stamp the sub-plan's returned param slot ($M) and buffer it for the main-root attach.
                sub_result.captured_explain_ir->subplan_returns = static_cast<uint32_t>(mapping.id);
                captured_subplans.push_back(std::move(*sub_result.captured_explain_ir));
            }
            if (plan_only) {
                // Plan-only: the IR is captured; there is no data cursor to compact or param to bind.
                continue;
            }
            trace(log_,
                  "DBG subq[{}] cursor success={} size={} cols={}",
                  i,
                  sub_result.cursor->is_success(),
                  sub_result.cursor->size(),
                  sub_result.cursor->column_count());
            auto compacted = mapping.compacter(sub_result.cursor->chunks());
            trace(log_,
                  "DBG subq[{}] compacted has_error={} is_null={}",
                  i,
                  compacted.has_error(),
                  compacted.has_error() ? false : compacted.value().is_null());
            if (compacted.has_error()) {
                co_return execute_result_t{make_cursor(resource(), compacted.error())};
            }
            plan.parameters->set_parameter(mapping.id, std::move(compacted.value()));
        }

        // One round-trip gives the executor everything session-scoped: the
        // (idempotently begun) txn snapshot shared by resolve and the operator
        // pipeline, the session timezone, the explicit-txn flag, and the
        // VACUUM gate value. begin_transaction is idempotent per session, so
        // a DML statement inside an explicit BEGIN joins the existing txn.
        //
        // Move-construct from the awaited value; do NOT default-construct +
        // assign — that element-copies the snapshot into a
        // null_memory_resource-anchored pmr vector and aborts (bad_alloc)
        // under concurrent transactions.
        auto [_tb, tbf] = actor_zeta::send(parent_address_,
                                           &services::dispatcher::manager_dispatcher_t::txn_begin_session_msg,
                                           session);
        services::dispatcher::txn_session_context_t session_ctx = co_await std::move(tbf);
        components::table::transaction_data resolve_txn = session_ctx.txn;
        trace(log_,
              "executor::execute_plan_full: session txn {}, explicit: {}, session: {}",
              resolve_txn.transaction_id,
              session_ctx.is_explicit,
              session.data());

        // Capture the pre-rewrite effective root type: it drives the DDL/DML
        // branch dispatch, the txn-kind decision, and the pass2 rewrite gates
        // below (the planner wraps/replaces nodes destructively later).
        const node_type original_type = [&] {
            auto* r = services::catalog_resolve::effective_root_node(plan.sub_queries.back().get());
            return r ? r->type() : node_type::unused;
        }();

        // SET TIMEZONE: capture the name before the planner consumes the node;
        // surfaced back to the dispatcher via execute_result_t.applied_timezone
        // once the operator pipeline confirms pg_settings was persisted.
        std::pmr::string pending_set_tz_name{resource()};
        if (original_type == node_type::set_timezone_t) {
            auto* tz_node = static_cast<components::logical_plan::node_set_timezone_t*>(
                services::catalog_resolve::effective_root_node(plan.sub_queries.back().get()));
            pending_set_tz_name.assign(tz_node->timezone_name().c_str(), tz_node->timezone_name().size());
        }

        // (O1) The optimizer used to run here, early, before resolve. It now
        // runs as a SINGLE pass AFTER the planner rewrite — see the
        // components::planner::optimize(...) call just before the execute_plan
        // delegate below. This gives the canonical planner → optimizer order.

        // Wrap the plan with catalog_resolve_namespace + catalog_resolve_table
        // for every (db, rel) pair found in the tree. Validate/enrich consume
        // OIDs through the plan-tree idx; the SQL transformer only emits
        // resolves for the outermost target (e.g. INSERT FROM SELECT wraps
        // CopyTestCollection but not the SELECT source TestCollection), so we
        // need to top-up missing tables here. For direct-API callers
        // (wrapper_dispatcher::execute_plan, find, etc.) this builds the full
        // wrap from scratch. Existing resolves in sequence_t already cover
        // their (db, rel) tuples — set-based dedup avoids re-emitting them.
        {
            // Collect resolves that already exist in the plan tree so we don't
            // re-emit them. Operates on the immediate front children of
            // sequence_t (where the transformer puts its resolves); a deeper
            // walk is unnecessary because resolve only consumes front-children.
            std::set<std::string> existing_dbs;
            std::set<std::pair<std::string, std::string>> existing_tbls;
            if (plan.sub_queries.back()->type() == node_type::sequence_t) {
                for (const auto& c : plan.sub_queries.back()->children()) {
                    if (!c)
                        continue;
                    if (c->type() != node_type::catalog_resolve_t)
                        continue;
                    auto* r = static_cast<const node_catalog_resolve_t*>(c.get());
                    if (r->kind() == resolve_kind::namespace_) {
                        existing_dbs.insert(r->dbname());
                    } else if (r->kind() == resolve_kind::table) {
                        existing_tbls.insert({r->dbname(), r->relname()});
                        existing_dbs.insert(r->dbname());
                    }
                }
            }
            std::set<std::string> wrap_dbs;
            std::set<std::pair<std::string, std::string>> wrap_tbls;
            auto add_dbrel = [&](std::string db, std::string rel) {
                if (db.empty())
                    return;
                wrap_dbs.insert(db);
                if (!rel.empty()) {
                    wrap_tbls.insert({std::move(db), std::move(rel)});
                }
            };
            // Iterative pre-order walk (no recursion → no std::function).
            std::vector<const node_t*> stack;
            stack.push_back(plan.sub_queries.back().get());
            while (!stack.empty()) {
                const node_t* n = stack.back();
                stack.pop_back();
                if (!n)
                    continue;
                switch (n->type()) {
                    // DML consumers no longer carry (db, rel) — names
                    // for resolve tracking come from the sibling
                    // resolve_table inside the wrapping sequence_t (the
                    // catalog_resolve_table_t branch below picks them up).
                    case node_type::insert_t:
                    case node_type::update_t:
                    case node_type::delete_t:
                        break;
                    case node_type::aggregate_t: {
                        auto* d = static_cast<const node_aggregate_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::match_t: {
                        auto* d = static_cast<const node_match_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::join_t: {
                        auto* d = static_cast<const node_join_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::create_database_t: {
                        auto* d = static_cast<const node_create_database_t*>(n);
                        if (!d->dbname().empty())
                            wrap_dbs.insert(d->dbname());
                        break;
                    }
                    // create_* / drop_* DDL no longer carry parent names; the
                    // transformer always wraps them with sibling
                    // catalog_resolve_* nodes, so wrap_dbs/wrap_tbls is already
                    // populated from existing_dbs/existing_tbls above.
                    default:
                        break;
                }
                for (const auto& c : n->children()) stack.push_back(c.get());
            }
            // Drop resolves already present so we don't duplicate them.
            for (const auto& db : existing_dbs) wrap_dbs.erase(db);
            for (const auto& t : existing_tbls) wrap_tbls.erase(t);
            if (!wrap_dbs.empty() || !wrap_tbls.empty()) {
                // Collect new resolves to prepend.
                std::vector<node_ptr> new_resolves;
                std::set<std::string> resolved_dbs = existing_dbs;
                for (const auto& db : wrap_dbs) {
                    if (resolved_dbs.insert(db).second) {
                        new_resolves.push_back(
                            components::logical_plan::make_node_catalog_resolve_namespace(resource(),
                                                                                          core::dbname_t{db}));
                    }
                }
                for (const auto& [db, rel] : wrap_tbls) {
                    if (resolved_dbs.insert(db).second) {
                        new_resolves.push_back(
                            components::logical_plan::make_node_catalog_resolve_namespace(resource(),
                                                                                          core::dbname_t{db}));
                    }
                    new_resolves.push_back(
                        components::logical_plan::make_node_catalog_resolve_table(resource(),
                                                                                  core::dbname_t{db},
                                                                                  core::relname_t{rel}));
                }
                if (plan.sub_queries.back()->type() == node_type::sequence_t) {
                    // Splice new resolves AFTER existing leading resolve_*
                    // siblings but BEFORE the consumer node. Order matters:
                    // stamp_oids_from_resolves picks the FIRST resolve_table
                    // as the DML target — preserving original-target priority
                    // means walker-added scan resolves don't shadow it.
                    auto is_resolve_local = [](node_type t) { return t == node_type::catalog_resolve_t; };
                    auto& kids = plan.sub_queries.back()->children();
                    std::vector<node_ptr> merged;
                    merged.reserve(kids.size() + new_resolves.size());
                    std::size_t split = 0;
                    while (split < kids.size() && kids[split] && is_resolve_local(kids[split]->type())) {
                        merged.push_back(std::move(kids[split]));
                        ++split;
                    }
                    for (auto& r : new_resolves) merged.push_back(std::move(r));
                    for (; split < kids.size(); ++split) {
                        merged.push_back(std::move(kids[split]));
                    }
                    kids.clear();
                    for (auto& m : merged) kids.push_back(std::move(m));
                } else {
                    auto seq = boost::intrusive_ptr<node_t>(new node_sequence_t(resource()));
                    for (auto& r : new_resolves) seq->append_child(std::move(r));
                    seq->append_child(std::move(plan.sub_queries.back()));
                    plan.sub_queries.back() = seq;
                }
            }
        }

        // Executor-owned plan context. session_tz arrives from the dispatcher
        // (the sole owner of default_tz_cat_) in the session-context bundle.
        services::context_storage_t context_storage(resource(), log_.clone(), session_ctx.session_tz);

        // Which commit tail runs after the pipeline. DDL needs a real txn so a
        // mid-DDL crash → WAL replay rolls back partially-written pg_catalog
        // records; DML drives the per-range publish (or accumulate) tail. The
        // txn itself was already begun by txn_begin_session_msg above.
        const bool needs_ddl_txn =
            original_type == node_type::create_collection_t || original_type == node_type::create_constraint_t ||
            original_type == node_type::create_sequence_t || original_type == node_type::create_view_t ||
            original_type == node_type::create_macro_t || original_type == node_type::create_type_t ||
            original_type == node_type::create_index_t || original_type == node_type::drop_t ||
            original_type == node_type::create_database_t || original_type == node_type::alter_table_t ||
            original_type == node_type::create_matview_t;
        // Plan-only EXPLAIN of DML must be a true no-op: gate needs_dml_txn so the statement flows
        // through the read-only releases_resolve_txn path (like a SELECT) instead of running an
        // empty commit pipeline (WAL marker + ProcArray barrier for a zero-change txn). EXPLAIN
        // ANALYZE keeps needs_dml_txn true → commits normally.
        const bool is_plan_only_explain = plan.explain == components::logical_plan::explain_type::plan;
        const bool needs_dml_txn = !is_plan_only_explain &&
                                   (original_type == node_type::insert_t || original_type == node_type::update_t ||
                                    original_type == node_type::delete_t);
        // SET TIMEZONE and VACUUM are append/delete-shaped catalog writers that
        // are neither DDL nor DML but still produce committable pg_catalog
        // ranges (SET TIMEZONE → pg_settings append; VACUUM → pg_computed_column
        // tombstone deletes). They ride the SAME append-shaped unified DML tail
        // (accumulate + implicit commit / revert) rather than the DDL tail,
        // which carries no base append/delete handling. Kept as a separate bool
        // (not merged into needs_dml_txn) so the trace text and the dml_*
        // semantics stay literally about INSERT/UPDATE/DELETE.
        const bool needs_commit_txn =
            original_type == node_type::set_timezone_t || original_type == node_type::vacuum_t;

        // Run the catalog_resolve_*_t front-children through their operators via
        // co_await this->execute_plan (not a sync inter-actor call): those
        // operators only do async mailbox sends to disk_address_ (no shared
        // mutable state) and run in this same actor coroutine. resolve_txn is
        // forwarded into both the resolve sub-plan and the final execute_plan
        // delegate so they share one MVCC snapshot.
        //
        // (C4) Shared resolve sub-plan runner — build a sequence_t over the given
        // resolve nodes and run it. The resolve operators are read-only catalog
        // probes that stamp OIDs onto the SAME nodes still in the parent tree
        // (operator_resolve_*_t holds raw pointers to them). Used by the outer
        // resolve pass and the view-expansion fresh-resolve pass. Takes `self` so
        // the coroutine frame allocator finds the PMR resource (the [this] capture
        // is not visible to promise_type::operator new). The throw-away
        // context_storage keeps the caller's own context_storage untouched.
        auto run_resolve_subplan = [this, session, resolve_txn, &session_ctx, &context_storage](
                                       [[maybe_unused]] executor_t* self,
                                       std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes)
            -> executor_t::unique_future<execute_result_t> {
            auto root = boost::intrusive_ptr<components::logical_plan::node_t>(
                new components::logical_plan::node_sequence_t(resource()));
            for (auto& n : resolve_nodes) {
                root->append_child(n);
            }
            auto params = components::logical_plan::make_parameter_node(resource());
            services::context_storage_t cstor{resource(), log_.clone(), context_storage.session_timezone};
            co_return co_await this->execute_plan(session,
                                                  components::logical_plan::execution_plan_t{resource(), root, params},
                                                  std::move(cstor),
                                                  resolve_txn,
                                                  session_ctx.lowest_active_start_time,
                                                  std::pmr::vector<explain_plan_node>{resource()});
        };

        if (plan.sub_queries.back() &&
            plan.sub_queries.back()->type() == components::logical_plan::node_type::sequence_t) {
            auto& kids = plan.sub_queries.back()->children();
            auto is_resolve = [](components::logical_plan::node_type t) {
                return t == components::logical_plan::node_type::catalog_resolve_t;
            };
            std::size_t resolve_count = 0;
            while (resolve_count < kids.size() && kids[resolve_count] && is_resolve(kids[resolve_count]->type())) {
                ++resolve_count;
            }
            if (resolve_count > 0) {
                // Resolve sub-plan over the front children (see run_resolve_subplan).
                std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes{resource()};
                resolve_nodes.reserve(resolve_count);
                for (std::size_t i = 0; i < resolve_count; ++i) {
                    resolve_nodes.push_back(kids[i]);
                }
                auto pass1_result = co_await run_resolve_subplan(this, std::move(resolve_nodes));
                if (pass1_result.cursor->is_error()) {
                    trace(log_,
                          "executor::execute_plan_full: resolve failed: {}",
                          pass1_result.cursor->get_error().what);
                    co_return execute_result_t{std::move(pass1_result.cursor)};
                }
                // Resolves stay in the plan tree so validate/enrich's gather
                // finds them. create_plan_sequence skips catalog_resolve_*_t
                // children when building the executor's left-chain — they have
                // already run, and putting them in operator_insert.left_ would
                // corrupt insert's data input (see create_plan_sequence.cpp).
            }
        }
        // Post-resolve stamp: pure tree-walk re-writing resolved OIDs onto
        // their consumer nodes. (The full resolve index is gathered once into
        // dispatcher_idx below, right before validate/enrich.)
        if (plan.sub_queries.back()) {
            services::catalog_resolve::stamp_oids_from_resolves(plan.sub_queries.back().get());
        }
        // SELECT-time view expansion + fresh-resolve sub-execute. After
        // resolve stamped resolved_metadata.view_sql on
        // catalog_resolve_table_t nodes with relkind=='v', re-parse +
        // re-transform the view body and splice the resulting sub-plan
        // in place. Current scope: only top-level passthrough plans
        // (`SELECT * FROM v`) — the entire logical_plan is replaced with
        // the sub-plan. Elaborate compositions (extra
        // filters/projections/joins on top of v) are not yet handled.
        //
        // The sub-plan's fresh resolves run via `co_await this->execute_plan`,
        // safe by the same reasoning as the outer resolve loop.
        if (plan.sub_queries.back()) {
            if (auto* view_node = services::catalog_resolve::find_first_view_resolve(plan.sub_queries.back().get())) {
                auto exp =
                    services::catalog_resolve::expand_view_body(resource(), view_node->resolved_metadata()->view_sql);
                if (exp.error) {
                    trace(log_, "executor::execute_plan_full: view expansion failed");
                    co_return execute_result_t{std::move(exp.error)};
                }
                if (exp.had_expansion && exp.expanded_plan) {
                    // Full plan replacement — outer is treated as a trivial
                    // passthrough. Preserving outer projections / filters
                    // (splice sub-plan as child of outer consumer) is not yet
                    // handled.
                    plan.sub_queries.back() = std::move(exp.expanded_plan);

                    // Merge the sub-plan's parameter bindings into `parameters`
                    // so downstream operators see view-body constants (e.g.
                    // `col_b > 10`). Safe against id collision because the outer
                    // plan is a trivial passthrough SELECT * with no constants.
                    // (raw storage_parameters here → add_parameter free fn.)
                    if (exp.expanded_params) {
                        for (const auto& [pid, val] : exp.expanded_params->parameters().parameters) {
                            plan.parameters->set_parameter(pid, val);
                        }
                    }

                    // === Resolve sub-plan's fresh resolves ===
                    auto fresh = services::catalog_resolve::extract_unresolved_resolves(plan.sub_queries.back().get());
                    if (!fresh.empty()) {
                        std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes{resource()};
                        resolve_nodes.reserve(fresh.size());
                        for (auto& n : fresh) {
                            resolve_nodes.push_back(n);
                        }
                        auto pass2_result = co_await run_resolve_subplan(this, std::move(resolve_nodes));
                        if (pass2_result.cursor->is_error()) {
                            trace(log_,
                                  "executor::execute_plan_full: view sub-plan resolve failed: {}",
                                  pass2_result.cursor->get_error().what);
                            co_return execute_result_t{std::move(pass2_result.cursor)};
                        }
                    }

                    // The splice replaced the plan tree, so re-stamp the freshly
                    // resolved OIDs onto their consumer nodes; dispatcher_idx is
                    // re-gathered below so validate / enrich see consistent OIDs.
                    services::catalog_resolve::stamp_oids_from_resolves(plan.sub_queries.back().get());
                }
            }
        }
        // Enrich/validate. original_type (captured at function start, before
        // any rewrites) drives a switch of namespace / table / type existence
        // checks (catalog_resolve helpers — no async, only resource()); the
        // default branch runs validate_types + validate_schema, then
        // post_validate_optimize → enrich_plan → planner.create_plan.
        using components::catalog::table_id;
        using components::logical_plan::constraint_kind;
        using components::logical_plan::node_create_collection_t;
        using components::logical_plan::node_create_constraint_t;
        using components::logical_plan::node_create_type_t;
        using components::types::logical_type;

        // Rebuild dispatcher_idx against the (possibly view-spliced) plan
        // tree so validate / enrich / build_id_cfn see fully-stamped OIDs.
        services::catalog_resolve::plan_resolve_index_t dispatcher_idx;
        if (plan.sub_queries.back()) {
            services::catalog_resolve::stamp_oids_from_resolves(plan.sub_queries.back().get());
            services::catalog_resolve::gather_plan_resolve_index(plan.sub_queries.back().get(), &dispatcher_idx);
        }

        // Build qualified_name_t from the effective consumer node; nodes
        // that don't carry user-typed names pull (db, rel) from the
        // sibling resolve_* nodes via drop_target_names_from_resolves.
        const auto* plan_root_for_drop_names = plan.sub_queries.back().get();
        auto build_id_cfn = [plan_root_for_drop_names](const components::logical_plan::node_t* n) -> qualified_name_t {
            using components::logical_plan::node_create_database_t;
            using components::logical_plan::node_create_macro_t;
            using components::logical_plan::node_create_sequence_t;
            using components::logical_plan::node_create_view_t;
            if (!n)
                return {};
            switch (n->type()) {
                case node_type::aggregate_t: {
                    auto* d = static_cast<const node_aggregate_t*>(n);
                    return qualified_name_t{static_cast<const std::string&>(d->dbname()),
                                            static_cast<const std::string&>(d->relname())};
                }
                case node_type::alter_column_t:
                case node_type::alter_table_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::create_collection_t: {
                    auto* d = static_cast<const node_create_collection_t*>(n);
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, static_cast<const std::string&>(d->relname())};
                }
                case node_type::create_constraint_t: {
                    auto* d = static_cast<const node_create_constraint_t*>(n);
                    return qualified_name_t{static_cast<const std::string&>(d->dbname()),
                                            static_cast<const std::string&>(d->relname())};
                }
                case node_type::create_database_t: {
                    auto* d = static_cast<const node_create_database_t*>(n);
                    return qualified_name_t{d->dbname(), std::string{}};
                }
                case node_type::create_index_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::create_macro_t: {
                    auto* d = static_cast<const node_create_macro_t*>(n);
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->macroname()};
                }
                case node_type::create_sequence_t: {
                    auto* d = static_cast<const node_create_sequence_t*>(n);
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->seqname()};
                }
                case node_type::create_view_t: {
                    auto* d = static_cast<const node_create_view_t*>(n);
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->viewname()};
                }
                case node_type::delete_t:
                case node_type::insert_t:
                case node_type::update_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::drop_t: {
                    using components::logical_plan::drop_target_kind;
                    using components::logical_plan::node_drop_t;
                    const auto kind = static_cast<const node_drop_t*>(n)->kind();
                    // DROP TYPE carries no (db, rel) name here.
                    if (kind == drop_target_kind::type) {
                        return {};
                    }
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    // DROP DATABASE keys only on the namespace; no relation name.
                    if (kind == drop_target_kind::database) {
                        return qualified_name_t{names.first, std::string{}};
                    }
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::match_t: {
                    auto* d = static_cast<const node_match_t*>(n);
                    return qualified_name_t{static_cast<const std::string&>(d->dbname()),
                                            static_cast<const std::string&>(d->relname())};
                }
                default:
                    return {};
            }
        };

        // Build identification name from the effective consumer node, not
        // the (potentially transformer-wrapping) sequence_t.
        table_id id(resource(),
                    build_id_cfn(services::catalog_resolve::effective_root_node(plan.sub_queries.back().get())));
        cursor_t_ptr error;
        // Existence checks read from the explicit dispatcher_idx populated
        // above (mirrors the dispatcher's pre-execute pass).
        switch (original_type) {
            case node_type::create_database_t:
                if (!services::dispatcher::check_namespace_exists(resource(), &dispatcher_idx, id).contains_error()) {
                    auto* d = static_cast<const node_create_database_t*>(
                        services::catalog_resolve::effective_root_node(plan.sub_queries.back().get()));
                    if (d && d->if_not_exists()) {
                        error = make_cursor(resource());
                    } else {
                        error = make_cursor(resource(),
                                            core::error_t{core::error_code_t::database_already_exists,
                                                          std::pmr::string{"database already exists", resource()}});
                    }
                }
                break;
            case node_type::create_collection_t: {
                if (!services::dispatcher::check_collection_exists(resource(), &dispatcher_idx, id).contains_error()) {
                    auto* cc = static_cast<const node_create_collection_t*>(
                        services::catalog_resolve::effective_root_node(plan.sub_queries.back().get()));
                    if (cc && cc->if_not_exists()) {
                        error = make_cursor(resource());
                    } else {
                        error = make_cursor(resource(),
                                            core::error_t{core::error_code_t::table_already_exists,
                                                          std::pmr::string{"collection already exists", resource()}});
                    }
                } else {
                    const std::string target_db =
                        id.get_namespace().empty() ? std::string{} : std::string(id.get_namespace().front());
                    const auto str_path = services::catalog_resolve::build_type_search_path_str(target_db);
                    auto* n = static_cast<node_create_collection_t*>(
                        services::catalog_resolve::effective_root_node(plan.sub_queries.back().get()));
                    for (auto& col_def : n->column_definitions()) {
                        if (col_def.type().type() == logical_type::UNKNOWN) {
                            if (col_def.type().type_name().empty()) {
                                break;
                            }
                            const auto lt = components::catalog::pg_name_to_logical_type(col_def.type().type_name());
                            if (lt != logical_type::UNKNOWN) {
                                std::string alias = col_def.type().has_alias() ? col_def.type().alias() : std::string{};
                                col_def.type() = components::types::complex_logical_type{lt};
                                if (!alias.empty()) {
                                    col_def.type().set_alias(alias);
                                }
                                continue;
                            }
                            if (auto err =
                                    services::dispatcher::check_type_exists(resource(),
                                                                            &dispatcher_idx,
                                                                            col_def.type().type_name(),
                                                                            std::span<const std::string>(str_path));
                                err.contains_error()) {
                                error = make_cursor(resource(), err);
                            }
                            if (!error) {
                                const auto* md = services::catalog_resolve::probe_type_in_path(
                                    &dispatcher_idx,
                                    std::string_view(col_def.type().type_name()),
                                    std::span<const std::string>(str_path));
                                if (md) {
                                    std::string alias =
                                        col_def.type().has_alias() ? col_def.type().alias() : std::string{};
                                    col_def.type() = md->type;
                                    if (!alias.empty()) {
                                        col_def.type().set_alias(alias);
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            }
            case node_type::create_type_t: {
                auto* n = static_cast<node_create_type_t*>(
                    services::catalog_resolve::effective_root_node(plan.sub_queries.back().get()));
                components::catalog::oid_t target_ns = components::catalog::well_known_oid::public_namespace;
                const std::string default_path[] = {"public", "pg_catalog"};
                std::span<const std::string> str_path(default_path);
                if (!services::dispatcher::check_type_exists(resource(),
                                                             &dispatcher_idx,
                                                             n->type().type_name(),
                                                             str_path)
                         .contains_error()) {
                    error = make_cursor(
                        resource(),
                        core::error_t{core::error_code_t::schema_error,
                                      std::pmr::string{("type: \'" + n->type().alias() + "\' already exists").c_str(),
                                                       resource()}});
                    break;
                }
                if (n->type().type() == logical_type::STRUCT) {
                    for (auto& field : n->type().child_types()) {
                        if (field.type() == logical_type::UNKNOWN) {
                            const auto lt = components::catalog::pg_name_to_logical_type(field.type_name());
                            if (lt != logical_type::UNKNOWN) {
                                std::string alias = field.has_alias() ? field.alias() : std::string{};
                                field = components::types::complex_logical_type{lt};
                                if (!alias.empty()) {
                                    field.set_alias(alias);
                                }
                                continue;
                            }
                            if (auto err = services::dispatcher::check_type_exists(resource(),
                                                                                   &dispatcher_idx,
                                                                                   field.type_name(),
                                                                                   str_path);
                                err.contains_error()) {
                                error = make_cursor(resource(), err);
                                break;
                            }
                            const auto* md =
                                services::catalog_resolve::probe_type_in_path(&dispatcher_idx,
                                                                              std::string_view(field.type_name()),
                                                                              str_path);
                            if (md) {
                                std::string alias = field.has_alias() ? field.alias() : std::string{};
                                field = md->type;
                                if (!alias.empty()) {
                                    field.set_alias(alias);
                                }
                            }
                        }
                    }
                    if (error) {
                        break;
                    }
                }
                n->set_namespace_oid(target_ns);
                break;
            }
            case node_type::drop_t: {
                using components::logical_plan::drop_target_kind;
                using components::logical_plan::node_drop_t;
                const auto* drop_node = static_cast<const node_drop_t*>(
                    services::catalog_resolve::effective_root_node(plan.sub_queries.back().get()));
                switch (drop_node->kind()) {
                    case drop_target_kind::database:
                        if (auto err = services::dispatcher::check_namespace_exists(resource(), &dispatcher_idx, id);
                            err.contains_error()) {
                            error = make_cursor(resource(), err);
                        }
                        break;
                    case drop_target_kind::collection:
                        // Authoritative existence check via the plan-tree resolve idx.
                        if (auto err = services::dispatcher::check_collection_exists(resource(), &dispatcher_idx, id);
                            err.contains_error()) {
                            error = make_cursor(resource(), err);
                        }
                        break;
                    case drop_target_kind::type: {
                        std::string type_name;
                        if (plan.sub_queries.back()->type() == node_type::sequence_t) {
                            for (const auto& c : plan.sub_queries.back()->children()) {
                                if (c && c->type() == node_type::catalog_resolve_t) {
                                    const auto* rc =
                                        static_cast<const components::logical_plan::node_catalog_resolve_t*>(c.get());
                                    if (rc->kind() == resolve_kind::type) {
                                        type_name = rc->type_name();
                                        break;
                                    }
                                }
                            }
                        }
                        const std::string default_path[] = {"public", "pg_catalog"};
                        std::span<const std::string> str_path(default_path);
                        if (auto err = services::dispatcher::check_type_exists(resource(),
                                                                               &dispatcher_idx,
                                                                               type_name,
                                                                               str_path);
                            err.contains_error()) {
                            error = make_cursor(resource(), err);
                        }
                        break;
                    }
                    case drop_target_kind::sequence:
                    case drop_target_kind::view:
                    case drop_target_kind::macro:
                        // No table schema to validate.
                        break;
                    case drop_target_kind::index: {
                        // DROP INDEX is the one drop kind that still runs
                        // validate_types + validate_schema (the others skip both).
                        auto vt_err = services::dispatcher::validate_types(resource(),
                                                                           &dispatcher_idx,
                                                                           plan.sub_queries.back().get(),
                                                                           context_storage.session_timezone);
                        if (vt_err.contains_error()) {
                            error = make_cursor(resource(), vt_err);
                        } else {
                            auto schema_res = services::dispatcher::validate_schema(resource(),
                                                                                    &dispatcher_idx,
                                                                                    plan.sub_queries.back().get(),
                                                                                    plan.parameters->parameters());
                            if (schema_res.has_error()) {
                                error = make_cursor(resource(), schema_res.error());
                            }
                        }
                        break;
                    }
                }
                break;
            }
            case node_type::set_timezone_t:
            case node_type::checkpoint_t:
            case node_type::vacuum_t:
            // SQL BEGIN/COMMIT/ROLLBACK are leaf control nodes exactly like
            // checkpoint/vacuum: no table schema to validate. Without this
            // case it falls into the default branch and validate_schema's
            // default arm assert(false)s on the unknown node type.
            case node_type::transaction_t:
            case node_type::create_sequence_t:
            case node_type::create_view_t:
            case node_type::create_macro_t:
                break;
            case node_type::alter_table_t:
                break;
            case node_type::create_constraint_t: {
                if (auto err = services::dispatcher::check_collection_exists(resource(), &dispatcher_idx, id);
                    err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                if (!error && !id.get_namespace().empty()) {
                    auto* cstr = static_cast<node_create_constraint_t*>(
                        services::catalog_resolve::effective_root_node(plan.sub_queries.back().get()));
                    if (cstr->kind() == constraint_kind::foreign_key || cstr->kind() == constraint_kind::check) {
                        const auto* tbl_local =
                            services::catalog_resolve::tbl_md_for(&dispatcher_idx,
                                                                  std::string_view(id.get_namespace().front()),
                                                                  std::string_view(id.table_name()));
                        const bool local_is_g = tbl_local && tbl_local->relkind == 'g';
                        bool ref_is_g = false;
                        if (cstr->kind() == constraint_kind::foreign_key &&
                            cstr->ref_table_oid() != components::catalog::INVALID_OID) {
                            const auto* tbl_ref =
                                services::catalog_resolve::tbl_md_for_oid(&dispatcher_idx, cstr->ref_table_oid());
                            ref_is_g = tbl_ref && tbl_ref->relkind == 'g';
                        }
                        if (cstr->kind() == constraint_kind::foreign_key && (local_is_g || ref_is_g)) {
                            error = make_cursor(
                                resource(),
                                core::error_t{core::error_code_t::schema_error,
                                              std::pmr::string{
                                                  "Foreign key constraints are not supported when the referencing or "
                                                  "referenced table is dynamic-schema (relkind='g'). FK enforcement "
                                                  "requires stable column attoids; dynamic-schema columns may evolve. "
                                                  "Convert involved tables to static schema first.",
                                                  resource()}});
                        } else if (cstr->kind() == constraint_kind::check && local_is_g) {
                            error = make_cursor(
                                resource(),
                                core::error_t{core::error_code_t::schema_error,
                                              std::pmr::string{
                                                  "CHECK constraints are not supported on dynamic-schema (relkind='g') "
                                                  "tables. CHECK enforcement requires stable column attoids; "
                                                  "dynamic-schema columns may evolve. Convert the table to static "
                                                  "schema first.",
                                                  resource()}});
                        }
                    }
                }
                break;
            }
            default: {
                auto vt_err = services::dispatcher::validate_types(resource(),
                                                                   &dispatcher_idx,
                                                                   plan.sub_queries.back().get(),
                                                                   context_storage.session_timezone);
                if (vt_err.contains_error()) {
                    error = make_cursor(resource(), vt_err);
                } else {
                    auto schema_res = services::dispatcher::validate_schema(resource(),
                                                                            &dispatcher_idx,
                                                                            plan.sub_queries.back().get(),
                                                                            plan.parameters->parameters());
                    if (schema_res.has_error()) {
                        error = make_cursor(resource(), schema_res.error());
                    }
                }
            }
        }

        if (error) {
            trace(log_, "executor::execute_plan_full: validation error: {}", error->get_error().what);
            co_return execute_result_t{std::move(error)};
        }

        // A DDL statement inside an explicit BEGIN..COMMIT accumulates its catalog
        // rows + created/dropped artifacts onto the session's transaction_t (just
        // like DML) and DEFERS its publish to the SQL COMMIT. Same-txn visibility
        // of the new catalog rows is the MVCC self-write rule (catalog rows carry
        // insert_id == transaction_id, visible to their own txn). The deferral is
        // gated in the DDL commit tail below: run_commit_pipeline_ runs only when
        // !session_ctx.is_explicit; accumulate stays unconditional.

        // CREATE INDEX: indexed table oid captured at rewrite time (the plan
        // tree is move-consumed by the execute_plan delegate before the
        // backfill-commit tail runs). The index name is captured alongside so
        // the CREATE INDEX failure path can drop the engine+agent
        // (manager_index_t::drop_index) without re-probing the consumed plan
        // tree; the pg_index row itself is reverted via the statement's
        // pg_catalog append ranges.
        components::catalog::oid_t create_index_table_oid = components::catalog::INVALID_OID;
        std::pmr::string create_index_name{resource()};

        // Destructive rewrites. enrich_plan / planner.create_plan are NOT
        // idempotent — in particular create_plan wraps insert/update/delete in
        // check_constraint_t / fk_check_t, and running it twice re-wraps on top
        // of the previous wrap (broken plan). The executor is the ONLY side
        // running these passes (the dispatcher routes the raw plan straight here).
        // (O1) The optimizer no longer runs here — it runs as one pass after the
        // planner rewrite, just before the execute_plan delegate.
        {
            // Enrich DML node fields with catalog metadata (NOT NULL, DEFAULT,
            // CHECK exprs), reading exclusively from the plan-tree idx. ctx
            // carries resolve_txn so enrich sees the same MVCC snapshot.
            components::execution_context_t enrich_ctx{session, resolve_txn, context_storage.session_timezone};
            auto ef = services::dispatcher::enrich_plan(resource(),
                                                        plan.sub_queries.back(),
                                                        disk_address_,
                                                        enrich_ctx,
                                                        &dispatcher_idx,
                                                        index_address_,
                                                        &context_storage);
            auto enrich_err = co_await std::move(ef);
            if (enrich_err.contains_error()) {
                trace(log_, "executor::execute_plan_full: enrich error: {}", enrich_err.what);
                co_return execute_result_t{make_cursor(resource(), std::move(enrich_err))};
            }
            // Logical plan rewrite: insert constraint wrapper nodes driven
            // by enriched fields.
            components::planner::planner_t planner;
            plan.sub_queries.back() = planner.create_plan(resource(), std::move(plan.sub_queries.back()));

            // Re-populate context_storage's known_oids / table_metadata from the
            // just-stamped plan tree. The dispatcher captured these BEFORE this
            // executor's resolve ran, so table_oid_dependencies was empty for
            // SELECT WHERE plans that depend on resolve-stamped table_oids on
            // aggregate_t / match_t. Now that the executor owns resolve, this
            // re-capture is mandatory: without it create_plan_match's
            // has_table_oid(table_oid) returns false, falls through to a bare
            // operator_match with no scan child, and SEGFAULTs in
            // operator_select::evaluate (chunk.cols=0 → out-of-bounds chunk.data[0]).
            {
                auto dependency_oids = plan.sub_queries.back()->table_oid_dependencies();
                for (auto oid : dependency_oids) {
                    context_storage.known_oids.insert(oid);
                }
                services::catalog_resolve::plan_resolve_index_t local_idx;
                services::catalog_resolve::gather_plan_resolve_index(plan.sub_queries.back().get(), &local_idx);
                for (const auto& [oid, md_ptr] : local_idx.tbl_md_by_oid) {
                    context_storage.table_metadata[oid] = md_ptr;
                }
            }

            // INSERT relkind='g' wrap + DDL OID-batch allocation. OID
            // allocation goes through the pipeline-routed node_allocate_oids_t
            // leaf (operator_allocate_oids sends to disk_address — no txn
            // state involved).
            //
            // The lambda takes `executor_t* self` as its first arg so the
            // coroutine promise_type::operator new can extract the PMR resource
            // via self->resource() — the [this] capture is not visible to the
            // coroutine frame allocator, and without `self` extract_resource_or_abort
            // fires.
            auto allocate_oids_inline = [this, session, &context_storage]([[maybe_unused]] executor_t* self, std::size_t count)
                -> executor_t::unique_future<std::vector<components::catalog::oid_t>> {
                auto node = components::logical_plan::make_node_allocate_oids(resource(), count);
                components::compute::function_registry_t local_fn_registry{resource()};
                services::context_storage_t cstor{resource(), log_.clone(), context_storage.session_timezone};
                auto op = services::planner::create_plan(cstor,
                                                         local_fn_registry,
                                                         node,
                                                         components::logical_plan::limit_t::unlimit(),
                                                         /*params=*/nullptr);
                if (!op) {
                    co_return std::vector<components::catalog::oid_t>{};
                }
                op->set_as_root();
                components::logical_plan::storage_parameters local_params(resource());
                components::pipeline::context_t pctx{session,
                                                     actor_zeta::address_t::empty_address(),
                                                     actor_zeta::address_t::empty_address(),
                                                     &local_fn_registry,
                                                     local_params};
                pctx.disk_address = disk_address_;
                pctx.txn = components::table::transaction_data{0, 0};
                op->prepare();
                // operator_allocate_oids_t is a sourceless sink (role()==sink,
                // needs_async_finalize()==true): drive it through the SAME streaming
                // seam every other sub-plan uses — execute_pipeline drives its
                // await_async_and_resume (the allocate_oids_batch round-trip + node
                // stamp) via the executor's bottom-up async-finalize pass.
                auto drive_err = co_await drive_subplan_(op, &pctx);
                if (drive_err.contains_error()) {
                    co_return std::vector<components::catalog::oid_t>{};
                }
                if (pctx.has_pending_disk_futures()) {
                    auto futures = pctx.take_pending_disk_futures();
                    for (auto& f : futures) co_await std::move(f);
                }
                co_return node->oids();
            };

            using components::catalog::relkind::computed;
            using components::logical_plan::node_sequence_t;

            // INSERT relkind='g' wrap — wraps INSERT into
            // sequence_t(insert, computed_field_register) so
            // pg_computed_column rows are appended inside the DML txn.
            if (original_type == node_type::insert_t && disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_t resolved_tbl_oid = components::catalog::INVALID_OID;
                bool is_computing = false;
                auto* effective_insert_node =
                    services::catalog_resolve::effective_root_node(plan.sub_queries.back().get());
                auto enriched_oid =
                    effective_insert_node ? effective_insert_node->table_oid() : plan.sub_queries.back()->table_oid();
                if (enriched_oid == components::catalog::INVALID_OID && !plan.sub_queries.back()->children().empty()) {
                    enriched_oid = plan.sub_queries.back()->children().front()->table_oid();
                }
                if (enriched_oid != components::catalog::INVALID_OID) {
                    if (const auto* tbl = services::catalog_resolve::tbl_md_for_oid(&dispatcher_idx, enriched_oid)) {
                        if (tbl->relkind == computed) {
                            is_computing = true;
                            resolved_tbl_oid = tbl->table_oid;
                        }
                    }
                }

                if (is_computing) {
                    std::pmr::vector<components::table::column_definition_t> registered_cols(resource());
                    auto* effective_insert =
                        services::catalog_resolve::effective_root_node(plan.sub_queries.back().get());
                    if (effective_insert) {
                        for (const auto& child : effective_insert->children()) {
                            if (!child || child->type() != components::logical_plan::node_type::data_t) {
                                continue;
                            }
                            auto* data_node = static_cast<const components::logical_plan::node_data_t*>(child.get());
                            const auto& chunk = data_node->data_chunk();
                            registered_cols.reserve(chunk.column_count());
                            for (size_t i = 0; i < chunk.column_count(); ++i) {
                                const auto& type = chunk.data[i].type();
                                assert(type.has_alias());
                                registered_cols.emplace_back(type.alias(), type);
                            }
                            break;
                        }
                    }

                    // node_alter_column_t(op=add, computed=true) carrying the
                    // INSERT-chunk columns in registered_cols; create_plan routes
                    // it to operator_computed_field_register_t. dbname/relname are
                    // not set — the register operator only reads table_oid + columns.
                    auto register_node = components::logical_plan::make_node_alter_column(
                        resource(),
                        components::logical_plan::alter_column_op::add);
                    register_node->set_computed(true);
                    register_node->set_table_oid(resolved_tbl_oid);
                    register_node->set_registered_cols(std::move(registered_cols));

                    auto seq = boost::intrusive_ptr(new node_sequence_t(resource()));
                    seq->append_child(plan.sub_queries.back());
                    seq->append_child(register_node);
                    plan.sub_queries.back() = seq;
                }
            }

            // DDL OID-batch allocation + planner rewrite — ONE path for all DDL
            // kinds (C1). The per-kind OID count lives in
            // planner::compute_oid_demand (single source of truth, mirrors
            // walk_ddl's consumption), so the count formulas are no longer
            // duplicated here. Pre-check (CREATE CONSTRAINT) and post-steps
            // (CREATE INDEX capture, ALTER re-enrich) stay inline.
            auto is_ddl_oid_rewrite = [](node_type t) {
                switch (t) {
                    case node_type::create_collection_t:
                    case node_type::create_database_t:
                    case node_type::create_type_t:
                    case node_type::create_sequence_t:
                    case node_type::create_view_t:
                    case node_type::create_macro_t:
                    case node_type::create_matview_t:
                    case node_type::create_index_t:
                    case node_type::drop_t:
                    case node_type::alter_table_t:
                    case node_type::create_constraint_t:
                        return true;
                    default:
                        return false;
                }
            };
            // DROP INDEX rewrites even without a disk actor; every other DDL kind
            // needs disk (OID alloc + catalog writes route through disk_address_).
            const bool has_disk = disk_address_ != actor_zeta::address_t::empty_address();
            const bool is_drop_index = [&] {
                if (original_type != node_type::drop_t) {
                    return false;
                }
                const auto* dn = static_cast<const components::logical_plan::node_drop_t*>(
                    services::catalog_resolve::effective_root_node(plan.sub_queries.back().get()));
                return dn && dn->kind() == components::logical_plan::drop_target_kind::index;
            }();
            if (is_ddl_oid_rewrite(original_type) && (is_drop_index || has_disk)) {
                auto* eff = services::catalog_resolve::effective_root_node(plan.sub_queries.back().get());

                // CREATE CONSTRAINT: reject an empty/invalid CHECK before allocating an OID.
                if (original_type == node_type::create_constraint_t) {
                    auto* cstr = static_cast<node_create_constraint_t*>(eff);
                    if (cstr->kind() == constraint_kind::check && cstr->check_expr().empty()) {
                        co_return execute_result_t{make_cursor(
                            resource(),
                            core::error_t{
                                core::error_code_t::invalid_constraint,
                                std::pmr::string{"CHECK constraint expression is empty or contains "
                                                 "unsupported constructs (functions, subqueries, and CASE "
                                                 "expressions are not allowed; valid: comparisons, AND/OR/NOT, "
                                                 "IS NULL/IS NOT NULL, column references, and constants)",
                                                 resource()}})};
                    }
                }

                const std::size_t need = components::planner::compute_oid_demand(eff);
                components::catalog::oid_batch_t oid_batch;
                if (need > 0) {
                    oid_batch.oids = co_await allocate_oids_inline(this, need);
                }
                components::planner::planner_t ddl_planner;
                plan.sub_queries.back() =
                    ddl_planner.create_plan(resource(), std::move(plan.sub_queries.back()), std::move(oid_batch));

                // CREATE INDEX: capture indexed table oid + index name NOW — the
                // plan is move-consumed by the execute_plan delegate below, so the
                // backfill/undo tail can no longer probe the tree. The trailing
                // create_index_t (last child of the rewritten sequence_t) carries
                // the pg_index row oid (set by rewrite_create_index) and the name.
                if (original_type == node_type::create_index_t) {
                    if (auto* eff2 = services::catalog_resolve::effective_root_node(plan.sub_queries.back().get());
                        eff2 && !eff2->children().empty()) {
                        auto* back = eff2->children().back().get();
                        if (back && back->type() == node_type::create_index_t) {
                            const auto* ci = static_cast<const components::logical_plan::node_create_index_t*>(back);
                            create_index_table_oid = ci->table_oid();
                            create_index_name.assign(ci->name().c_str(), ci->name().size());
                        }
                    }
                }
                // ALTER TABLE: re-enrich — the planner stamps fresh attoids on
                // rename / computed_field_unregister primitives that didn't exist
                // before it ran. resolve_txn so enrich's pg_computed_column scan
                // sees the INSERT-time register rows committed under that txn.
                else if (original_type == node_type::alter_table_t) {
                    // Re-gather the resolve index against the planner-rewritten
                    // tree (the DDL rewrite replaced the consumer nodes; the
                    // pre-rewrite dispatcher_idx may not cover the new ones).
                    services::catalog_resolve::plan_resolve_index_t reenrich_idx;
                    services::catalog_resolve::gather_plan_resolve_index(plan.sub_queries.back().get(), &reenrich_idx);
                    components::execution_context_t enriched_ctx{session,
                                                                 resolve_txn,
                                                                 context_storage.session_timezone};
                    auto ef2 = services::dispatcher::enrich_plan(resource(),
                                                                 plan.sub_queries.back(),
                                                                 disk_address_,
                                                                 enriched_ctx,
                                                                 &reenrich_idx,
                                                                 index_address_,
                                                                 &context_storage);
                    auto enrich_err2 = co_await std::move(ef2);
                    if (enrich_err2.contains_error()) {
                        trace(log_, "executor::execute_plan_full: ALTER re-enrich error: {}", enrich_err2.what);
                        co_return execute_result_t{make_cursor(resource(), std::move(enrich_err2))};
                    }
                }
            }
        }
        // Unresolved-ALTER no-op guard: a plan whose LITERAL root is still
        // alter_table_t after the rewrites means rewrite_alter_table bailed
        // (table_oid unresolved by enrich) — return no-op success. Wrapped
        // plans (sequence_t root with an alter_table_t child) keep the error
        // path through the pipeline.
        if (original_type == node_type::alter_table_t && plan.sub_queries.back() &&
            plan.sub_queries.back()->type() == node_type::alter_table_t) {
            co_return execute_result_t{make_cursor(resource())};
        }

        // (O1) Single optimizer pass — runs HERE, after every planner rewrite
        // (DML constraint-wrap + DDL lowering), so the schema stamps
        // key.side()/key.path() and table OIDs are present. const-fold +
        // pushdown_filter + hash-join selection; a no-op on DDL sequences.
        // The execute_plan delegate below lowers this optimized tree.
        // The pushdown_aggregate rule decides aggregate pushdown purely by plan
        // shape (like hash-join selection); the only gate is a CAPABILITY
        // precondition: an owning agent must exist. In-memory (disk-less) mode has
        // no agent to push to, so pushable aggregates stay coordinator-side there —
        // a precondition, not a fallback.
        const bool can_push_to_agent = disk_address_ != actor_zeta::address_t::empty_address();
        plan.sub_queries.back() = components::planner::optimize(resource(),
                                                               std::move(plan.sub_queries.back()),
                                                               plan.parameters.get(),
                                                               can_push_to_agent);

        // Build-side selection: fetch live row counts for the child
        // tables of every INNER hash join so create_plan_join can put the smaller
        // side on the hash build. Gated on an owning agent existing (reuse
        // can_push_to_agent); in-memory mode leaves row_counts empty and the swap
        // no-ops. The sequential co_await loop is safe (actor-zeta inter-await
        // guard; operator_vacuum drains storage_total_rows the same way), and a
        // wrong/absent count only picks a slower-but-correct plan (the join output
        // is orientation-restored regardless). INVALID_OID children are dropped by
        // the walk, so nothing invalid is ever sent.
        if (can_push_to_agent) {
            std::pmr::set<components::catalog::oid_t> inner_hash_join_oids{resource()};
            collect_inner_hash_join_oids(plan.sub_queries.back(), inner_hash_join_oids);
            for (auto oid : inner_hash_join_oids) {
                auto [_tr, trf] = actor_zeta::send(disk_address_,
                                                   &services::disk::manager_disk_t::storage_total_rows,
                                                   session,
                                                   oid);
                context_storage.row_counts[oid] = co_await std::move(trf);
            }
        }

        trace(log_, "executor::execute_plan_full: delegating to execute_plan, session: {}", session.data());
        // Operator-pipeline run, forwarding resolve_txn so the operator path
        // sees the same MVCC snapshot the resolves did.
        auto exec_result = co_await execute_plan(session,
                                                 plan,
                                                 std::move(context_storage),
                                                 resolve_txn,
                                                 session_ctx.lowest_active_start_time,
                                                 std::move(captured_subplans));

        // (C2) Shared failure-revert — undo this statement's storage appends +
        // index mirrors and abort the txn. Identical for the DML and DDL failure
        // paths (was copy-pasted). Takes `self` so the coroutine frame allocator
        // finds the PMR resource — the [this] capture is not visible to
        // promise_type::operator new (same pattern as allocate_oids_inline).
        auto revert_failed_txn = [this, session, resolve_txn, &session_ctx](
                                     [[maybe_unused]] executor_t* self,
                                     execute_result_t& exec_result) -> executor_t::unique_future<void> {
            // Storage retire/revert, split by table kind. Base-table DML appends are
            // retired committed-dead IN PLACE (storage_abort_appends): a placed row
            // keeps its physical id until a compact, so the positional WAL records
            // this failed statement's siblings already wrote — and their replay —
            // keep resolving against the numbering the live run used, while the dead
            // stamps keep the compaction gates unblocked. pg_catalog appends keep the
            // physical unwind (storage_revert_appends): catalog rows ride the swap
            // protocol, not positional replay.
            std::vector<components::pg_catalog_append_range_t> abort_ranges;
            abort_ranges.reserve(exec_result.dml_appends.size());
            for (const auto& app : exec_result.dml_appends) {
                abort_ranges.push_back(
                    components::pg_catalog_append_range_t{app.table_oid, app.row_start, app.row_count});
#ifdef DEV_MODE
                // Test-observability: count each base-table DML append range we
                // retire here. A constraint (CHECK/FK) that errors AFTER the DML
                // appended its rows must reach this point with a non-empty
                // dml_appends so the leaked pending stamps are retired (see header).
                g_dml_appends_reverted.fetch_add(1, std::memory_order_relaxed);
#endif
            }
            if (!abort_ranges.empty() && disk_address_ != actor_zeta::address_t::empty_address()) {
                components::execution_context_t abort_ctx{session, resolve_txn, {}};
                auto [_aa, aaf] = actor_zeta::send(disk_address_,
                                                   &services::disk::manager_disk_t::storage_abort_appends,
                                                   abort_ctx,
                                                   std::move(abort_ranges));
                co_await std::move(aaf);
            }
            std::vector<components::pg_catalog_append_range_t> revert_ranges;
            revert_ranges.reserve(exec_result.pg_catalog_appends.size());
            for (auto& pgc : exec_result.pg_catalog_appends) {
                revert_ranges.push_back(std::move(pgc));
            }
            exec_result.pg_catalog_appends.clear();
            if (!revert_ranges.empty() && disk_address_ != actor_zeta::address_t::empty_address()) {
                components::execution_context_t pgc_ctx{session, resolve_txn, {}};
                auto [_pa, paf] = actor_zeta::send(disk_address_,
                                                   &services::disk::manager_disk_t::storage_revert_appends,
                                                   pgc_ctx,
                                                   std::move(revert_ranges));
                co_await std::move(paf);
            }

            // Index revert, two-phase (send-all then await-all), deduped per
            // table_oid. revert_insert ← pending index INSERT bucket (dml_appends);
            // revert_delete ← pending index DELETE bucket (dml_deletes). Both are
            // keyed per (table_oid, txn) and idempotent across duplicate oids.
            if (index_address_ != actor_zeta::address_t::empty_address()) {
                std::pmr::set<components::catalog::oid_t> revert_insert_oids{resource()};
                for (const auto& app : exec_result.dml_appends) {
                    revert_insert_oids.insert(app.table_oid);
                }
                std::pmr::set<components::catalog::oid_t> revert_delete_oids{resource()};
                for (const auto& del : exec_result.dml_deletes) {
                    revert_delete_oids.insert(del.table_oid);
                }
                std::pmr::vector<actor_zeta::unique_future<void>> revert_index_futures{resource()};
                revert_index_futures.reserve(revert_insert_oids.size() + revert_delete_oids.size());
                for (auto oid : revert_insert_oids) {
                    components::execution_context_t abort_ctx{session, resolve_txn, session_ctx.session_tz, oid};
                    auto [_ri, rif] = actor_zeta::send(index_address_,
                                                       &services::index::manager_index_t::revert_insert,
                                                       abort_ctx,
                                                       oid);
                    revert_index_futures.push_back(std::move(rif));
                }
                for (auto oid : revert_delete_oids) {
                    components::execution_context_t abort_ctx{session, resolve_txn, session_ctx.session_tz, oid};
                    auto [_rd, rdf] = actor_zeta::send(index_address_,
                                                       &services::index::manager_index_t::revert_delete,
                                                       abort_ctx,
                                                       oid);
                    revert_index_futures.push_back(std::move(rdf));
                }
                for (auto& rif : revert_index_futures) {
                    co_await std::move(rif);
                }
            }

            // A mutating scan retained past drain (awaiting_apply) is normally
            // released by the storage apply — which this failed statement will never
            // send (the failure landed between drain and apply). The txn-abort
            // OPERATOR does not run on the statement-failure path, and an autocommit
            // statement's session dies with the statement, so no later sweep-on-open
            // can match the pin either. Release the session's retained cursors with
            // the same broadcast the abort operator uses; otherwise compaction AND
            // checkpointing of the target table defer for the process lifetime.
            if (disk_address_ != actor_zeta::address_t::empty_address()) {
                auto [_rp, rpf] = actor_zeta::send(disk_address_,
                                                   &services::disk::manager_disk_t::release_scans_for_session,
                                                   session);
                co_await std::move(rpf);
            }

            auto [_ab, abf] =
                actor_zeta::send(parent_address_, &services::dispatcher::manager_dispatcher_t::txn_abort_msg, session);
            co_await std::move(abf);

            exec_result.dml_appends.clear();
            exec_result.dml_deletes.clear();
        };

        // ===== DML commit / accumulate tail =====
        // ONE publish channel for everything: every successful DML statement
        // parks its ranges on the dispatcher-owned transaction_t
        // (txn_accumulate_msg), and the publish itself is ALWAYS
        // operator_commit_transaction_t — run right here for autocommit
        // (PostgreSQL model: autocommit = implicit COMMIT), or later by the
        // SQL COMMIT statement for explicit txns. The operator drains, batch-
        // publishes storage, commits the index mirrors per table, writes the
        // WAL marker and crosses the ProcArray barrier — in that order.
        if (needs_dml_txn || needs_commit_txn) {
            if (exec_result.cursor->is_success()) {
                services::dispatcher::txn_accumulate_payload_t payload;
                payload.base_appends.reserve(exec_result.dml_appends.size());
                for (const auto& app : exec_result.dml_appends) {
                    payload.base_appends.push_back(
                        components::table::dml_append_range_t{app.table_oid, app.row_start, app.row_count});
                }
                payload.base_deletes.reserve(exec_result.dml_deletes.size());
                for (const auto& del : exec_result.dml_deletes) {
                    payload.base_deletes.push_back(components::table::dml_delete_range_t{del.table_oid, del.txn_id});
                }
                payload.pg_catalog_appends = std::move(exec_result.pg_catalog_appends);
                payload.pg_catalog_delete_tables = std::move(exec_result.pg_catalog_delete_tables);
                payload.backfills = std::move(exec_result.pg_attribute_commit_id_backfills);
                // DROP storage-scrub oids and CREATE storage/index oids (all
                // empty on the DML/SET-TZ/VACUUM path — none of these tears down
                // or brings up a storage/index — but shipped for symmetry with
                // the DDL tail; payload.empty() already accounts for them).
                payload.dropped_storage_oids = std::move(exec_result.dropped_storage_oids);
                payload.created_storage_oids = std::move(exec_result.created_storage_oids);
                payload.created_indexes = std::move(exec_result.created_indexes);
                trace(log_,
                      "executor::execute_plan_full: txn {} — accumulating {} appends, {} deletes ({})",
                      resolve_txn.transaction_id,
                      payload.base_appends.size(),
                      payload.base_deletes.size(),
                      session_ctx.is_explicit ? "publish deferred to COMMIT" : "implicit COMMIT follows");
                if (!payload.empty()) {
                    auto [_ac, acf] = actor_zeta::send(parent_address_,
                                                       &services::dispatcher::manager_dispatcher_t::txn_accumulate_msg,
                                                       session,
                                                       std::move(payload));
                    co_await std::move(acf);
                }
                exec_result.dml_appends.clear();
                exec_result.dml_deletes.clear();
                exec_result.pg_catalog_appends.clear();
                exec_result.pg_catalog_delete_tables.clear();
                exec_result.pg_attribute_commit_id_backfills.clear();
                exec_result.dropped_storage_oids.clear();
                exec_result.created_storage_oids.clear();
                exec_result.created_indexes.clear();

                if (!session_ctx.is_explicit) {
                    // Autocommit: implicit COMMIT through the SAME operator
                    // pipeline SQL COMMIT uses.
                    auto commit_result = co_await run_commit_pipeline_(session,
                                                                       resolve_txn,
                                                                       session_ctx.session_tz,
                                                                       session_ctx.lowest_active_start_time,
                                                                       /*ddl_mode=*/false);
                    if (commit_result.cursor->is_error()) {
                        exec_result.cursor = std::move(commit_result.cursor);
                    }
                }
            } else {
                // Failed DML statement: revert this statement's local ranges and
                // abort the txn (also ends a failed statement's explicit txn).
                co_await revert_failed_txn(this, exec_result);
            }
        }

        // ===== DDL commit tail =====
        // The accumulated pg_catalog swap-info rides to the dispatcher's
        // transaction_t first (ONE accumulate message, base fields empty), then
        // the ddl-commit operator drains it back via txn_commit_drain_msg and
        // publishes in order: flush barrier, WAL, commit, publish.
        if (needs_ddl_txn && exec_result.cursor->is_success()) {
            // CREATE INDEX failure-undo prep: snapshot the pg_index row's append
            // range BEFORE it is moved into the accumulate payload below. The
            // commit drains pg_catalog_appends back from transaction_t, so on a
            // commit (or inline index-commit) failure exec_result no longer
            // holds it — capture it here so the failure path can issue exactly
            // one storage_revert_appends for the pg_index row. Only the pg_index
            // row gates index visibility, so the scoped revert touches it alone;
            // the leftover pg_class / pg_depend rows are left to GC.
            components::pg_catalog_append_range_t create_index_pg_index_range{};
            bool has_create_index_pg_index_range = false;
            if (original_type == node_type::create_index_t) {
                constexpr auto pg_index_oid = components::catalog::well_known_oid::pg_index_table;
                for (const auto& app : exec_result.pg_catalog_appends) {
                    if (app.table_oid == pg_index_oid) {
                        create_index_pg_index_range = app;
                        has_create_index_pg_index_range = true;
                        break;
                    }
                }
            }

            if (!exec_result.pg_catalog_appends.empty() || !exec_result.pg_catalog_delete_tables.empty() ||
                !exec_result.pg_attribute_commit_id_backfills.empty() || !exec_result.dropped_storage_oids.empty() ||
                !exec_result.created_storage_oids.empty() || !exec_result.created_indexes.empty()) {
                services::dispatcher::txn_accumulate_payload_t payload;
                payload.pg_catalog_appends = std::move(exec_result.pg_catalog_appends);
                payload.pg_catalog_delete_tables = std::move(exec_result.pg_catalog_delete_tables);
                payload.backfills = std::move(exec_result.pg_attribute_commit_id_backfills);
                // DROP storage-scrub oids: the commit operator's DROP-GC remap
                // keys off this drained set, independent of ddl-commit mode.
                payload.dropped_storage_oids = std::move(exec_result.dropped_storage_oids);
                // CREATE storage/index oids: parked on transaction_t so an
                // explicit-txn COMMIT publishes them and an explicit-txn ABORT
                // drops the still-uncommitted artifacts. The autocommit path below
                // publishes them inline via run_commit_pipeline_, so the
                // accumulate is just the transit step in both modes.
                payload.created_storage_oids = std::move(exec_result.created_storage_oids);
                payload.created_indexes = std::move(exec_result.created_indexes);
                auto [_ac, acf] = actor_zeta::send(parent_address_,
                                                   &services::dispatcher::manager_dispatcher_t::txn_accumulate_msg,
                                                   session,
                                                   std::move(payload));
                co_await std::move(acf);
                exec_result.pg_catalog_appends.clear();
                exec_result.pg_catalog_delete_tables.clear();
                exec_result.pg_attribute_commit_id_backfills.clear();
                exec_result.dropped_storage_oids.clear();
                exec_result.created_storage_oids.clear();
                exec_result.created_indexes.clear();
            }

            // DDL commit through the SAME commit pipeline DML and SQL COMMIT
            // use (operator_commit_transaction_t in ddl-commit mode: flush
            // barrier + WAL(cid=0) prefix + drain + batch publishes + WAL +
            // publish). dml_appends recorded by the CREATE INDEX backfill are
            // deliberately NOT accumulated: routing them through the operator
            // would storage-re-commit already-committed rows (whole-vector
            // constant_info assert risk); the index-only commit below covers
            // them via the commit_id back-channel.
            exec_result.dml_appends.clear();
            exec_result.dml_deletes.clear();

            // CREATE INDEX failure undo. Reverts the pg_index row append
            // (gates index visibility) and tears down the engine+agent via
            // manager_index_t::drop_index. Invoked from BOTH the commit failure
            // and the inline index-commit failure below — a single closure keeps
            // the two paths identical. drop_index tolerates an unknown engine,
            // so it is safe even when the backfill never reached the engine. The
            // pg_index range was snapshotted before the accumulate move above;
            // the leftover pg_class / pg_depend rows are left to GC (they no
            // longer reference a valid index once indisvalid stays false).
            auto undo_create_index =
                [this, session, resolve_txn, &create_index_pg_index_range, &has_create_index_pg_index_range](
                    [[maybe_unused]] executor_t* self,
                    components::catalog::oid_t table_oid,
                    std::pmr::string index_name) -> executor_t::unique_future<void> {
                if (has_create_index_pg_index_range && disk_address_ != actor_zeta::address_t::empty_address()) {
                    std::vector<components::pg_catalog_append_range_t> revert_ranges;
                    revert_ranges.push_back(create_index_pg_index_range);
                    components::execution_context_t rv_ctx{session, resolve_txn, {}};
                    auto [_rv, rvf] = actor_zeta::send(disk_address_,
                                                       &services::disk::manager_disk_t::storage_revert_appends,
                                                       rv_ctx,
                                                       std::move(revert_ranges));
                    co_await std::move(rvf);
                }
                if (table_oid != components::catalog::INVALID_OID &&
                    index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_di, dif] = actor_zeta::send(index_address_,
                                                       &services::index::manager_index_t::drop_index,
                                                       session,
                                                       table_oid,
                                                       services::index::index_name_t(index_name.c_str()));
                    co_await std::move(dif);
                }
                co_return;
            };

            // DDL inside an explicit BEGIN..COMMIT DEFERS its publish to the
            // SQL COMMIT — accumulate above already parked the catalog rows +
            // created/dropped artifacts on transaction_t, and the COMMIT
            // statement's own operator_commit_transaction_t (run when the user
            // issues COMMIT) publishes the catalog rows. Running run_commit_pipeline_
            // here would publish mid-txn (a partial commit, and other sessions
            // would see the new catalog rows before COMMIT). So gate the whole
            // commit block on !is_explicit; accumulate stays unconditional (above).
            //
            // The inline CREATE INDEX index-commit below is AUTOCOMMIT-ONLY by two
            // independent guards: this !is_explicit guard, and commit_result.commit_id
            // (0 for a deferred txn — no commit ran in this pipeline). For an
            // explicit-txn CREATE INDEX the backfilled index entries stay PENDING
            // (tagged with the txn_id) — the SQL COMMIT operator does NOT yet flip
            // them (its commit_inserts keys off the drained base_appends, which are
            // empty for CREATE INDEX), so explicit-txn CREATE INDEX visibility at
            // COMMIT is a deferred follow-up. The created_index IS parked on
            // transaction_t regardless, so an explicit-txn ABORT drops the
            // half-built index via operator_abort_transaction.
            if (!session_ctx.is_explicit) {
                auto commit_result = co_await run_commit_pipeline_(session,
                                                                   resolve_txn,
                                                                   session_ctx.session_tz,
                                                                   session_ctx.lowest_active_start_time,
                                                                   /*ddl_mode=*/true);
                if (commit_result.cursor->is_error()) {
                    exec_result.cursor = std::move(commit_result.cursor);
                    // A CREATE INDEX whose commit failed never published the
                    // pg_index row nor brought up a usable engine — undo both.
                    if (original_type == node_type::create_index_t) {
                        co_await undo_create_index(this, create_index_table_oid, create_index_name);
                    }
                }
                // Inline CREATE INDEX backfill index-commit (index ONLY — see
                // above). The indexed table oid was captured at rewrite time; the
                // commit_id arrives via the operator's ctx back-channel.
                // commit_result.commit_id is 0 in deferred (explicit) mode, so
                // this block is naturally autocommit-only even inside this guard.
                if (commit_result.commit_id > 0 && original_type == node_type::create_index_t &&
                    index_address_ != actor_zeta::address_t::empty_address()) {
                    trace(log_,
                          "executor::execute_plan_full: CREATE INDEX backfill commit — oid={}, commit_id={}",
                          static_cast<unsigned>(create_index_table_oid),
                          commit_result.commit_id);
                    if (create_index_table_oid != components::catalog::INVALID_OID) {
                        components::execution_context_t swap_ctx{session, resolve_txn, {}};
                        // The CREATE INDEX path commits exactly one table, so pass
                        // a one-element oid vector to the batch commit_inserts.
                        std::pmr::vector<components::catalog::oid_t> commit_oids{resource()};
                        commit_oids.push_back(create_index_table_oid);
                        auto [_ci, cif] = actor_zeta::send(index_address_,
                                                           &services::index::manager_index_t::commit_inserts,
                                                           swap_ctx,
                                                           std::move(commit_oids),
                                                           commit_result.commit_id);
                        // A bitcask write failure here arrives AFTER the storage
                        // commit already published the pg_index row. Revert the
                        // pg_index row and drop the engine+agent so no half-built
                        // index lingers.
                        auto ci_result = co_await std::move(cif);
                        if (ci_result.contains_error()) {
                            exec_result.cursor = make_cursor(resource(), ci_result);
                            co_await undo_create_index(this, create_index_table_oid, create_index_name);
                        }
                    }
                }
            }
        } else if (needs_ddl_txn && exec_result.cursor->is_error()) {
            // ===== DDL failure branch =====
            // A failed DDL statement must not leave its txn orphaned/active
            // (that would pin lowest_active forever). Mirror the failed-DML revert
            // path: revert the catalog (and CREATE INDEX backfill) appends, revert
            // the pending index inserts/deletes, then abort the txn.
            //
            // Verified boundary: the FAILING fragment's own appends never reach
            // exec_result — the operator failed before execute_sub_plan_'s lift,
            // so only EARLIER-completed fragments carry ranges here. Those are
            // exactly the ones that need reverting.
            trace(log_,
                  "executor::execute_plan_full: DDL failed — reverting txn {}, session: {}",
                  resolve_txn.transaction_id,
                  session.data());

            co_await revert_failed_txn(this, exec_result);
        }

        // SET TIMEZONE — the operator pipeline persisted the ('TimeZone', name)
        // row to pg_settings. Surface the name so the dispatcher refreshes its
        // solely-owned default_tz_cat_.
        if (original_type == node_type::set_timezone_t && exec_result.cursor->is_success() &&
            !pending_set_tz_name.empty()) {
            exec_result.applied_timezone.assign(pending_set_tz_name.data(), pending_set_tz_name.size());
        }

        // ===== read-only txn release =====
        // Plans that neither commit nor accumulate used to leave the
        // resolve-scope txn active forever, pinning lowest_active (and thereby
        // starving the DROP-GC horizon). Release it. Exclusions:
        //   - explicit txns (a SELECT inside BEGIN..COMMIT must not abort it);
        //   - BEGIN itself (the operator just marked the txn explicit);
        //   - COMMIT/ROLLBACK (their operators already ended the txn);
        //   - needs_commit_txn plans (SET TIMEZONE / VACUUM): their pg_catalog
        //     writes were accumulated and committed by the DML tail above, so
        //     aborting here would discard the very rows they persisted.
        const bool releases_resolve_txn = !needs_ddl_txn && !needs_dml_txn && !needs_commit_txn &&
                                          !session_ctx.is_explicit && original_type != node_type::transaction_t;
        if (releases_resolve_txn) {
            auto [_rl, rlf] =
                actor_zeta::send(parent_address_, &services::dispatcher::manager_dispatcher_t::txn_abort_msg, session);
            co_await std::move(rlf);
        }

        co_return std::move(exec_result);
    }

    executor_t::unique_future<std::unique_ptr<function_result_t>>
    executor_t::register_udf(components::session::session_id_t session, components::compute::function_ptr function) {
        trace(log_, "executor::register_udf, session: {}, {}", session.data(), function->name());
        std::string name = function->name();
        auto signatures = function->get_signatures();
        auto res = function_registry_.add_function(std::move(function));
        co_return std::make_unique<function_result_t>(std::move(res));
    }

    executor_t::unique_future<bool> executor_t::set_explain_renderer(uint32_t id, explain_render_fn fn) {
        // Register the host-supplied renderer into THIS executor's own registry at slot `id` (a POD
        // fn-pointer, no shared state). Reject a null renderer or an out-of-range id with `false` —
        // never report success while installing nothing, and never grow the registry unboundedly on a
        // huge host id (that would allocate gigabytes and, with exceptions disabled, abort the process).
        if (fn == nullptr || id >= kExplainRendererSlotLimit) {
            co_return false;
        }
        // Grow with the default renderer so any skipped slot resolves to the built-in postgres.
        if (explain_renderers_.size() <= id) {
            explain_renderers_.resize(static_cast<std::size_t>(id) + 1, &render_postgres);
        }
        explain_renderers_[id] = fn;
        co_return true;
    }

    plan_t executor_t::traverse_plan_(components::operators::operator_ptr&& plan,
                                      const components::logical_plan::storage_parameters& parameters,
                                      services::context_storage_t&& context_storage) {
        std::stack<components::operators::operator_ptr> look_up;
        std::stack<components::operators::operator_ptr> sub_plans;
        look_up.push(plan);
        while (!look_up.empty()) {
            auto check_op = look_up.top();
            while (check_op->right() == nullptr) {
                check_op = check_op->left();
                if (check_op == nullptr) {
                    break;
                }
            }
            sub_plans.push(look_up.top());
            look_up.pop();
            if (check_op != nullptr) {
                look_up.push(check_op->right());
                look_up.push(check_op->left());
            }
        }

        trace(log_, "executor::subplans count {}", sub_plans.size());

        return plan_t{std::move(sub_plans), &parameters, std::move(context_storage)};
    }

    executor_t::unique_future<core::result_wrapper_t<components::operators::chunks_vector_t>>
    executor_t::execute_pipeline(components::operators::operator_ptr root, components::pipeline::context_t* ctx) {
        namespace ops = components::operators;
#ifdef DEV_MODE
        g_streaming_pipeline_runs.fetch_add(1, std::memory_order_relaxed);
#endif
        // Linearize the left-chain into pipeline order: chain[0] = source ... chain.back() = root.
        std::pmr::vector<ops::operator_t*> chain{resource()};
        for (ops::operator_t* op = root.get(); op != nullptr; op = op->left().get()) {
            chain.push_back(op);
        }
        std::reverse(chain.begin(), chain.end());

        // EXPLAIN ANALYZE flag for this invocation (zero clock sampling when off).
        const bool analyze = ctx->analyze;

        // Everything below the bottom-most NOT-yet-executed operator is a materialized
        // sub-plan: traverse_plan_ splits a join's build AND probe sides into their own
        // sub-plans, which run (and mark_executed) before this one. Re-driving such an
        // operator's source_next would read an already-drained cursor (0 rows). So drive
        // source_next only when this pipeline owns its scan source (chain bottom); when
        // the bottom is already executed, stream from that operator's materialized output_.
        // A materialized sub-plan (a join build/probe side split off by
        // traverse_plan_) marks only its ROOT operator executed and stashes the rows
        // in that root's output_; the drained SOURCE beneath it stays un-executed. So
        // when a side is more than one operator deep — e.g. a single-table filter
        // pushed below a join lowers to a streaming match/full_scan over its scan
        // source — the executed operators are NOT a contiguous bottom prefix:
        // chain[k] (the sub-plan root) is executed while chain[k-1] (its drained
        // source) is not. Take the materialized boundary as the operator just ABOVE
        // the TOPMOST executed op: everything at or below it belongs to an already-run
        // sub-plan (stream from that root's output_), and re-driving the drained
        // source below it would read an empty cursor. A bottom-scan-owned pipeline
        // (nothing pre-executed) leaves start == 0.
        std::size_t start = 0;
        for (std::size_t i = 0; i < chain.size(); ++i) {
            if (chain[i]->is_executed()) {
                start = i + 1;
            }
        }
        // SOURCELESS SINK BOTTOM: a DDL/txn/catalog-read operator (create_collection,
        // set_timezone, begin_transaction, resolve_*, ...) OR a producing recursive_cte
        // lowers to a sink whose chain bottom has no left child — no scan source.
        // The streaming executor admits the single-leaf shape, a multi-node all-sink
        // chain (CREATE INDEX = backfill->metadata; a multi-resolve front-pass =
        // resolve_table->resolve_namespace), AND a PRODUCING bottom with streaming/sink
        // ancestors (top-level WITH RECURSIVE = [select -> sort -> match ->
        // recursive_cte]). For ALL of them the bottom sink's entire effect lives in
        // its await_async_and_resume (a cross-actor commit and/or the fixpoint that
        // PRODUCES output_), which the dedicated block below drives FIRST — so chain[0]
        // is handled there, not in the [op_start, end) FLUSH/async-finalize passes.
        // op_start therefore becomes 1 for this shape (the ANCESTOR range chain[1..]):
        // the bottom's produced rows are pumped UP through those ancestors, which are
        // then FLUSHed + async-finalized bottom-up. (chain[0]->role()==source is the
        // normal pumped case; a NOT-executed non-source bottom is the sourceless sink.)
        const bool sourceless_sink_root = start == 0 && chain.front()->role() != ops::pipeline_role::source;
        const std::size_t op_start = sourceless_sink_root ? 1 : ((start == 0) ? 1 : start);

        // EXPLAIN ANALYZE loops: bump ONLY the ops this invocation drives — chain.front() when it
        // owns the source / sourceless bottom (start==0), plus chain[op_start..). Not chain[0..start-1]
        // (already-materialized boundary + its drained source), or a single-pass join shows loops=2.
        if (analyze) {
            if (start == 0) {
                chain.front()->bump_analyze_loop();
            }
            for (std::size_t i = op_start; i < chain.size(); ++i) {
                chain[i]->bump_analyze_loop();
            }
        }

        // A producing sourceless bottom (recursive_cte) whose ancestors are REAL query
        // operators (sort/select/match) must stream its rows UP through them. The
        // discriminator is "not all-sink": if EVERY op on the chain is a sink it is an
        // independent-producer metadata chain (resolve/ddl/create-index) that must NOT
        // pump (its sink ancestors don't override push()); any streaming op on the chain
        // means a real pipeline whose ancestors consume the bottom's rows. Only consulted
        // for the sourceless_sink_root shape.
        bool pumpable_ancestors = false;
        if (sourceless_sink_root) {
            for (ops::operator_t* op : chain) {
                if (op->role() != ops::pipeline_role::sink) {
                    pumpable_ancestors = true; // a streaming op on the chain -> real pipeline
                    break;
                }
            }
        }

        ops::chunks_vector_t output{resource()};

        // Push one batch up through chain[op_start..]: a streaming op transforms its input
        // into the next stage; a sink op folds it into bounded state and emits nothing.
        // Chunks that survive the top of a pure-streaming pipeline are collected as output.
        auto pump_one = [&](components::vector::data_chunk_t&& batch) -> core::error_t {
            ops::chunks_vector_t stage{resource()};
            stage.push_back(std::move(batch));
            for (std::size_t i = op_start; i < chain.size(); ++i) {
                ops::chunks_vector_t produced{resource()};
                const analyze_scope scope{analyze};
                for (auto& in : stage) {
                    auto err = chain[i]->push(ctx, std::move(in), produced);
                    if (err.contains_error()) {
                        return err;
                    }
                }
                if (analyze) {
                    chain[i]->record_analyze(count_rows(produced), scope.elapsed());
                }
                stage = std::move(produced);
            }
            for (auto& c : stage) {
                output.push_back(std::move(c));
            }
            return core::error_t::no_error();
        };

        // DML flush bounding. Locate the first async-finalize (DML) sink in the
        // driven range [op_start, end); when the config threshold is set, the
        // mid-pump gate below flushes it INCREMENTALLY once it buffers >= threshold
        // rows, keeping peak memory bounded. A SECOND async-finalize sink above it
        // (a parent constraint) must accumulate its input across those partial
        // flushes — signalled to the operators via ctx->dml_has_parent_constraint.
        // dml_idx is a pure LOCAL (never parked on ctx). threshold==0 ⇒ the gate
        // never fires (mid-flush disabled).
        std::size_t dml_idx = chain.size();
        for (std::size_t i = op_start; i < chain.size(); ++i) {
            if (chain[i]->needs_async_finalize()) {
                dml_idx = i;
                break;
            }
        }
        bool parent_constraint = false;
        for (std::size_t i = dml_idx + 1; i < chain.size(); ++i) {
            if (chain[i]->needs_async_finalize()) {
                parent_constraint = true;
                break;
            }
        }
        ctx->dml_has_parent_constraint = parent_constraint;

        // PUMP.
        if (sourceless_sink_root) {
            // No source at the bottom: the bottom sink (chain[0]) produces / commits
            // its entire effect in await_async_and_resume, which MUST run BEFORE we
            // pump its rows up. (A bare DDL/txn leaf, an all-sink CREATE INDEX chain,
            // and a producing recursive_cte all share this: their work is the await,
            // not a push-fed finalize.) Drive it here, deepest-first — exactly the
            // commit the executor's bottom-up async-finalize pass drives — so the
            // FLUSH/async-finalize passes below operate only on the ANCESTORS.
            const analyze_scope front_scope{analyze};
            if (chain.front()->needs_async_finalize()) {
                co_await chain.front()->await_async_and_resume(ctx);
                if (chain.front()->has_error()) {
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(chain.front()->get_error());
                }
            }
            // EXPLAIN ANALYZE: the producing bottom (e.g. a recursive_cte fixpoint) is driven ONLY here
            // — never via a push()/finalize record site — so without this its plan line would read
            // rows=0 / actual time=0.000ms regardless of what it produced. Time only the production.
            if (analyze) {
                chain.front()->record_analyze(
                    chain.front()->output() ? count_rows(chain.front()->output()->chunks()) : 0,
                    front_scope.elapsed());
            }
            // Stream any rows the bottom PRODUCED (recursive_cte's fixpoint output_) UP
            // through the ancestors chain[1..] (e.g. sort -> select, or match -> sort ->
            // select). COPY (not move) the chunks: output_ is a shared intrusive_ptr
            // operator_data the bottom still owns (and run_subplan's caller may re-read).
            //
            // Gate on `pumpable_ancestors` (the chain is NOT all-sink): a producing
            // bottom under REAL query operators (sort/select/match — all override push())
            // pumps its rows up. An ALL-SINK sourceless chain (CREATE INDEX =
            // backfill->metadata; a multi-resolve front-pass = resolve_table->
            // resolve_namespace) is different: each level is an INDEPENDENT producing
            // metadata sink that sets its OWN output_ in its own await and does NOT
            // override push() — pumping the bottom's rows into them would hit the default
            // "not a streaming/sink op" push error. For that shape we pump nothing; the
            // FLUSH/async-finalize passes below drive each ancestor sink's own await
            // bottom-up (the original all-sink behavior), and drive_subplan_ reads the
            // ROOT's output_. (A pure-commit DDL/txn leaf has no ancestors, so
            // op_start == chain.size() and this is skipped regardless.)
            if (pumpable_ancestors && chain.front()->output()) {
                for (const auto& c : chain.front()->output()->chunks()) {
                    auto err = pump_one(c.partial_copy(resource(), 0, c.size()));
                    if (err.contains_error()) {
                        co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(err));
                    }
                    // Same mid-pump flush gate as the scan-source and materialized-input
                    // branches: a DML sink fed by a PRODUCING bottom (recursive_cte
                    // fixpoint output) must honor dml_flush_row_threshold too, or it
                    // buffers the entire produced row set.
                    if (auto flush_err = co_await maybe_mid_flush(chain, dml_idx, ctx); flush_err.contains_error()) {
                        co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(flush_err));
                    }
                }
            }
        } else if (start == 0) {
            ops::operator_t* source = chain.front();
            while (true) {
                const analyze_scope scope{analyze};
                auto next = co_await source->source_next(ctx);
                if (next.has_error()) {
                    co_return next.convert_error<ops::chunks_vector_t>();
                }
                auto batch = std::move(next.value());
                if (batch.data.empty()) {
                    break; // 0-column drain sentinel (a schema'd 0-row batch is real input, e.g.
                           // the empty-guard a scalar aggregate needs to emit COUNT=0)
                }
                if (analyze) {
                    // source emits a single data_chunk_t per fetch (not a chunks_vector_t).
                    source->record_analyze(batch.size(), scope.elapsed());
                }
                auto err = pump_one(std::move(batch));
                if (err.contains_error()) {
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(err));
                }
                if (auto flush_err = co_await maybe_mid_flush(chain, dml_idx, ctx); flush_err.contains_error()) {
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(flush_err));
                }
            }
        } else if (chain[start - 1]->output()) {
            // Materialized input: stream the already-executed operator's output_ chunks through the
            // remaining (un-executed) operators. COPY (not move) the chunks — this operator's
            // output_ is a shared (intrusive_ptr) operator_data that may be read again elsewhere
            // (e.g. a recursive CTE working set, or a build side reused across iterations), so
            // moving its chunks out would empty it for the other reader.
            for (const auto& c : chain[start - 1]->output()->chunks()) {
                auto err = pump_one(c.partial_copy(resource(), 0, c.size()));
                if (err.contains_error()) {
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(err));
                }
                if (auto flush_err = co_await maybe_mid_flush(chain, dml_idx, ctx); flush_err.contains_error()) {
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(flush_err));
                }
            }
        }

        // FLUSH: drain sink state bottom-up. A sink (group/agg/sort) emits its accumulated
        // result here; that result must still flow through the operators ABOVE it in the chain
        // (e.g. a projection on top of GROUP BY), so each finalized chunk is pushed through
        // chain[i+1..]. A downstream sink absorbs it (and emits later when its own turn comes,
        // since i < j is processed first). Streaming operators finalize to a no-op.
        for (std::size_t i = op_start; i < chain.size(); ++i) {
            ops::chunks_vector_t fin{resource()};
            const analyze_scope scope{analyze};
            auto err = chain[i]->finalize(ctx, fin);
            if (err.contains_error()) {
                co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(err));
            }
            if (analyze) {
                chain[i]->record_analyze(count_rows(fin), scope.elapsed());
            }
            for (auto& c : fin) {
                ops::chunks_vector_t stage{resource()};
                stage.push_back(std::move(c));
                for (std::size_t j = i + 1; j < chain.size(); ++j) {
                    ops::chunks_vector_t produced{resource()};
                    const analyze_scope scope{analyze};
                    for (auto& in : stage) {
                        auto e = chain[j]->push(ctx, std::move(in), produced);
                        if (e.contains_error()) {
                            co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(e));
                        }
                    }
                    if (analyze) {
                        chain[j]->record_analyze(count_rows(produced), scope.elapsed());
                    }
                    stage = std::move(produced);
                }
                for (auto& s : stage) {
                    output.push_back(std::move(s));
                }
            }
        }

        // Async-finalize commit, BOTTOM-UP. The pump fed each sink's input via
        // push(); now drive every operator whose finalize is an asynchronous
        // cross-actor commit (DML: WAL->storage->index->swap-info). push()/finalize()
        // are synchronous, so each commit runs HERE in the executor's coroutine
        // (which owns the cross-actor await), exactly as the legacy await loop does
        // for the materialize path. await_async_and_resume sets the operator's
        // output_ (RETURNING / affected-row count) and marks it executed.
        //
        // Iterate over only the operators THIS pipeline drove ([op_start, end)) and
        // go DEEPEST-FIRST: chain[op_start] is the deepest op in this range, chain.back()
        // the root, so ascending index == bottom-up. For a sourceless sink bottom the
        // real chain[0] was already driven (deepest-first) in the PUMP block above, so
        // this range (op_start==1) covers only its ancestors — still bottom-up overall.
        // Every op in this range that sets needs_async_finalize() is driven here,
        // bottom-up: a chain may hold several async ops (e.g. a spillable sink
        // under a DML sink, or a constraint above a DML), each awaited in
        // ascending (deepest-first) index order. Stop on the first error.
        //
        // This is the FINAL flush: any mid-pump partial flushes above ran with
        // dml_flush_is_final=false; restore the default so the DML sink's await
        // knows to close out (final index/swap-info, RETURNING count, executed).
        ctx->dml_flush_is_final = true;
        for (std::size_t i = op_start; i < chain.size(); ++i) {
            ops::operator_t* op = chain[i];
            if (!op->needs_async_finalize()) {
                continue;
            }
            const analyze_scope scope{analyze};
            co_await op->await_async_and_resume(ctx);
            if (op->has_error()) {
                co_return core::result_wrapper_t<ops::chunks_vector_t>(op->get_error());
            }
            if (analyze) {
                op->record_analyze(op->output() ? count_rows(op->output()->chunks()) : 0, scope.elapsed());
            }
        }

        co_return output;
    }

    executor_t::unique_future<core::error_t>
    executor_t::maybe_mid_flush(std::pmr::vector<components::operators::operator_t*>& chain,
                                std::size_t dml_idx,
                                components::pipeline::context_t* ctx) {
        // Mid-pump flush gate: co_await OUTSIDE the synchronous pump_one lambda (a
        // lambda cannot co_await). NON-final flush — the sink stays open for the
        // remaining batches. MEMBER coroutine so `this` supplies the frame
        // memory_resource; holds EXACTLY ONE co_await => lost-wakeup-safe.
        if (dml_flush_row_threshold_ != 0 && dml_idx < chain.size() &&
            chain[dml_idx]->buffered_rows() >= dml_flush_row_threshold_) {
            ctx->dml_flush_is_final = false;
#ifdef DEV_MODE
            g_dml_flush_count.fetch_add(1, std::memory_order_relaxed);
#endif
            co_await chain[dml_idx]->await_async_and_resume(ctx);
            if (chain[dml_idx]->has_error()) {
                co_return chain[dml_idx]->get_error();
            }
        }
        co_return core::error_t::no_error();
    }

    executor_t::unique_future<core::error_t>
    executor_t::materialize_build_sides_(components::operators::operator_ptr root,
                                         components::pipeline::context_t* ctx) {
        // Walk the LEFT chain (the streaming spine). For each operator that has a RIGHT
        // (build) child not yet executed, drive that whole right subtree to completion
        // FIRST — it is itself a sub-plan (its own build sides materialized recursively
        // by the drive_subplan_ call below) — so the streaming join can read its
        // materialized output_ when it builds the hash table. Already-executed right
        // subtrees (the normal traverse_plan_-split flow) short-circuit, so this is a
        // no-op outside the run_subplan-without-split case (recursive-CTE term).
        namespace ops = components::operators;
        for (ops::operator_t* op = root.get(); op != nullptr; op = op->left().get()) {
            auto right = op->right();
            if (right && !right->is_executed()) {
                right->prepare();
                auto err = co_await drive_subplan_(right, ctx);
                if (err.contains_error()) {
                    co_return err;
                }
            }
        }
        co_return core::error_t::no_error();
    }

    executor_t::unique_future<core::error_t> executor_t::drive_subplan_(components::operators::operator_ptr root,
                                                                        components::pipeline::context_t* ctx) {
        // THE single drive seam, shared by execute_sub_plan_ and run_subplan (no
        // duplication). The caller has already prepared `root`. EVERY reachable plan
        // streams through execute_pipeline (bounded memory): the producing-sourceless-
        // sink-bottom shape is supported, so the legacy materialized execution path is
        // gone. Build sides (join RIGHT children) must be materialized before the pump:
        // the top-level flow gets them from traverse_plan_'s sub-plan split; a single-
        // root run_subplan (recursive-CTE term) does not, so materialize them here. No-op
        // when already executed (the split flow).
        auto build_err = co_await materialize_build_sides_(root, ctx);
        if (build_err.contains_error()) {
            co_return build_err;
        }
        auto piped = co_await execute_pipeline(root, ctx);
        if (piped.has_error()) {
            co_return piped.error();
        }
        if (!root->is_executed()) { // a DML sink already set output_ + executed in execute_pipeline
            root->set_output(components::operators::make_operator_data(resource(), std::move(piped.value())));
            root->mark_executed();
        }
        // Detect errors set asynchronously in operators (e.g. fk_cascade root with RESTRICT).
        if (root->has_error()) {
            co_return root->get_error();
        }
        co_return core::error_t::no_error();
    }

    executor_t::unique_future<core::result_wrapper_t<components::operators::chunks_vector_t>>
    executor_t::run_subplan(components::operators::operator_ptr root, components::pipeline::context_t* ctx) {
        // subplan_runner_t entry point: run a prepared child sub-plan to completion
        // through the shared drive seam and hand back its output chunks. Invoked
        // INTRA-actor by an operator via ctx->runner; runs inside this executor
        // actor's own coroutine (operators are not actors), so the cross-actor
        // awaits inside drive_subplan_ are owned by the executor and lost-wakeup-safe.
        namespace ops = components::operators;
        if (!root) {
            co_return core::result_wrapper_t<ops::chunks_vector_t>(
                core::error_t{core::error_code_t::create_physical_plan_error,
                              std::pmr::string{"run_subplan: null root", resource()}});
        }
        root->prepare();
        auto err = co_await drive_subplan_(root, ctx);
        if (err.contains_error()) {
            co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(err));
        }
        // Copy (not move) the chunks out of output_: the root operator owns its
        // output_ (a shared intrusive_ptr operator_data) and may be read again by
        // the caller, so draining it would corrupt that reader. A drained sub-plan
        // with no output_ returns an empty vector.
        ops::chunks_vector_t out{resource()};
        if (root->output()) {
            const auto& chunks = root->output()->chunks();
            out.reserve(chunks.size());
            for (const auto& c : chunks) {
                out.push_back(c.partial_copy(resource(), 0, c.size()));
            }
        }
        co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(out));
    }

    executor_t::unique_future<sub_plan_result_t>
    executor_t::execute_sub_plan_(components::session::session_id_t session,
                                  plan_t plan_data,
                                  components::table::transaction_data txn,
                                  uint64_t lowest_active_start_time) {
        cursor_t_ptr cursor;
        sub_plan_result_t result_tracking;

        while (!plan_data.sub_plans.empty()) {
            auto plan = plan_data.sub_plans.top();
            trace(log_, "executor::execute_sub_plan, session: {}", session.data());

            if (!plan) {
                cursor = make_cursor(resource(),
                                     core::error_t(core::error_code_t::create_physical_plan_error,
                                                   std::pmr::string{"invalid query plan", resource()}));
                break;
            }

            // sender = parent_address_ (the dispatcher): the txn operators
            // (begin/commit/abort) and the DROP-GC mark reach the dispatcher's
            // mailbox handlers via ctx->current_message_sender.
            components::pipeline::context_t pipeline_context{session,
                                                             address(),
                                                             parent_address_,
                                                             &function_registry_,
                                                             *plan_data.parameters};
            pipeline_context.disk_address = disk_address_;
            pipeline_context.index_address = index_address_;
            pipeline_context.wal_address = wal_address_;
            pipeline_context.txn = txn;
            pipeline_context.session_tz = plan_data.context_storage_.session_timezone;
            // VACUUM/MVCC GC threshold. operator_vacuum_t reads this to gate
            // manager_disk_t::vacuum_all + manager_index_t::cleanup_all_versions.
            // The value arrives with the session context fetched at plan start.
            pipeline_context.lowest_active_start_time = lowest_active_start_time;
            // Publish ourselves as the sub-plan runner: an operator that needs to
            // run a child sub-plan through this SAME streaming executor reaches us
            // via ctx->runner->run_subplan (intra-actor; see subplan_runner_t).
            pipeline_context.runner = this;
            // EXPLAIN ANALYZE: enable per-operator instrumentation in execute_pipeline.
            pipeline_context.analyze = plan_data.analyze;

            // Prepare the operator tree (connects children in aggregation, etc.)
            plan->prepare();

            // Lift the BASE-table + FK-cascade DML append/delete ranges the DML
            // operators recorded in their await_async_and_resume. Factored out of the
            // success-path lift block below so the CONSTRAINT-ERROR path can lift them
            // too: a CHECK/fk_check operator sits ABOVE the DML and is driven AFTER it
            // (bottom-up async-finalize), so by the time it errors the DML has already
            // done its WAL-first storage_append and stamped these ranges. If we break
            // out of the loop without lifting them, exec_result.dml_appends is empty and
            // the failed-statement abort tail (revert_failed_txn / operator_abort_
            // transaction) has NOTHING to revert — the physically-appended (uncommitted)
            // row LINGERS (invisible via MVCC, but a real storage leak: row_group.count
            // stays bumped, the slot is never reclaimed). Lifting here on BOTH paths
            // hands the recorded range to the abort tail so storage_revert_appends ->
            // row_group_t::revert_append truncates the slot back. Idempotent: it
            // drains and clears both range lists, so the success-path block below
            // (which lifts the rest) never double-counts.
            auto lift_dml_ranges = [&pipeline_context, &result_tracking]() {
                for (const auto& app : pipeline_context.dml_appends) {
                    result_tracking.dml_appends.push_back({app.table_oid, app.row_start, app.row_count});
                }
                for (const auto& del : pipeline_context.dml_deletes) {
                    result_tracking.dml_deletes.push_back({del.table_oid, del.txn_id});
                }
                pipeline_context.dml_appends.clear();
                pipeline_context.dml_deletes.clear();
            };

            // Drive the sub-plan to completion through the shared streaming seam
            // (drive_subplan_ -> execute_pipeline). On success `plan` is executed with
            // output_ set; the switch below reads plan->output().
            {
                auto drive_err = co_await drive_subplan_(plan, &pipeline_context);
                if (drive_err.contains_error()) {
                    // Constraint-error (or any operator-error) path: the DML child may
                    // have already appended + stamped its range before the operator
                    // above it failed. Lift the recorded range so the abort tail reverts
                    // the physical append (see lift_dml_ranges).
                    lift_dml_ranges();
                    cursor = make_cursor(resource(), std::move(drive_err));
                    break;
                }
            }

            switch (plan->type()) {
                case components::operators::operator_type::insert: {
                    trace(log_, "executor::execute_plan : operators::operator_type::insert");
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->chunks()));
                    } else {
                        cursor = make_cursor(resource(), core::error_t::no_error());
                    }
                    break;
                }

                case components::operators::operator_type::remove: {
                    trace(log_, "executor::execute_plan : operators::operator_type::remove");
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->chunks()));
                    } else {
                        cursor = make_cursor(resource(), core::error_t::no_error());
                    }
                    break;
                }

                case components::operators::operator_type::update: {
                    trace(log_, "executor::execute_plan : operators::operator_type::update");
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->chunks()));
                    } else {
                        cursor = make_cursor(resource(), core::error_t::no_error());
                    }
                    break;
                }

                default: {
                    trace(log_,
                          "executor::execute_plan : operator_type={}, session: {}",
                          static_cast<int>(plan->type()),
                          session.data());

                    if (plan->is_root()) {
                        if (plan->output()) {
                            cursor = make_cursor(resource(), std::move(plan->output()->chunks()));
                        } else {
                            cursor = make_cursor(resource(), core::error_t::no_error());
                        }
                    } else {
                        cursor = make_cursor(resource(), core::error_t::no_error());
                    }
                    break;
                }
            }

            if (cursor->is_error()) {
                // Same reasoning as the drive-error break above: lift any DML range the
                // operators recorded before this cursor-level error so the abort tail
                // reverts the physical append. Idempotent (drains the dml_* slots).
                lift_dml_ranges();
                break;
            }

            if (pipeline_context.has_pending_disk_futures()) {
                auto disk_futures = pipeline_context.take_pending_disk_futures();
                for (auto& fut : disk_futures) {
                    co_await std::move(fut);
                }
            }

            // Lift pg_catalog swap info from this fragment's pipeline
            // context into the per-call sub_plan_result_t accumulated across
            // sub-plan iterations. execute_plan then forwards them into the
            // returned execute_result_t for the dispatcher to aggregate onto
            // transaction_t.
            for (auto& a : pipeline_context.pg_catalog_appends) {
                result_tracking.pg_catalog_appends.push_back(std::move(a));
            }
            for (auto& d : pipeline_context.pg_catalog_delete_tables) {
                result_tracking.pg_catalog_delete_tables.insert(std::move(d));
            }
            // backfill markers emitted by ALTER COLUMN ADD/DROP/RENAME so they
            // propagate alongside pg_catalog_appends down to transaction_t /
            // operator_commit_transaction.
            for (auto& bf : pipeline_context.pg_attribute_commit_id_backfills) {
                result_tracking.pg_attribute_commit_id_backfills.push_back(bf);
            }
            pipeline_context.pg_catalog_appends.clear();
            pipeline_context.pg_catalog_delete_tables.clear();
            pipeline_context.pg_attribute_commit_id_backfills.clear();

            // Lift the BASE-table + FK-cascade DML append/delete ranges recorded by
            // operator_insert / _delete / _update / _fk_cascade inside their
            // await_async_and_resume. Shared with the constraint-error path above (see
            // lift_dml_ranges): push a range per sub-plan rather than overwriting, so FK
            // cascade across >=2 tables keeps every child's publish (dml_append_range_t).
            lift_dml_ranges();
            // Lift the DROP storage-scrub oids: cascade-delete operators record
            // every storage whose backing files they tore down. Accumulate (not
            // overwrite) so a multi-table DROP keeps every dropped oid; the
            // accumulate tail ships them in txn_accumulate_payload_t so the
            // commit operator's DROP-GC remap keys off the drained set.
            for (auto oid : pipeline_context.dropped_storage_oids) {
                result_tracking.dropped_storage_oids.push_back(oid);
            }
            pipeline_context.dropped_storage_oids.clear();
            // Lift the CREATE back-channel: operator_create_collection /
            // operator_create_matview record each new storage oid, and
            // operator_create_index_backfill each new {table_oid, name}.
            // Accumulate (not overwrite) so a multi-statement DDL sequence keeps
            // every created artifact; the accumulate tail ships them in
            // txn_accumulate_payload_t so COMMIT publishes them and ABORT drops
            // the still-uncommitted ones. Mirror of the dropped_storage_oids lift.
            for (auto oid : pipeline_context.created_storage_oids) {
                result_tracking.created_storage_oids.push_back(oid);
            }
            pipeline_context.created_storage_oids.clear();
            for (auto& index : pipeline_context.created_indexes) {
                result_tracking.created_indexes.push_back(std::move(index));
            }
            pipeline_context.created_indexes.clear();
            // Commit back-channel: operator_commit_transaction_t recorded the
            // commit_id it drained.
            if (pipeline_context.committed_id != 0) {
                result_tracking.commit_id = pipeline_context.committed_id;
            }
            // (The single-slot dml_* fields + cascade vectors were already drained and
            // zeroed by lift_dml_ranges() above.)

            plan_data.sub_plans.pop();
        }

        trace(log_, "executor::execute_sub_plan finished, success: {}", cursor->is_success());
        result_tracking.cursor = std::move(cursor);
        co_return std::move(result_tracking);
    }

    executor_t::unique_future<execute_result_t>
    executor_t::run_commit_pipeline_(components::session::session_id_t session,
                                     components::table::transaction_data txn,
                                     core::date::timezone_offset_t session_tz,
                                     uint64_t lowest_active_start_time,
                                     bool ddl_mode) {
        // ONE commit publisher for autocommit DML, DDL and SQL COMMIT: the
        // node lowers to operator_commit_transaction_t, which drains the
        // dispatcher-owned transaction_t (txn_commit_drain_msg), batch-
        // publishes storage, commits the index mirrors per table, writes the
        // WAL marker and crosses the ProcArray barrier (txn_publish_msg).
        auto commit_node =
            components::logical_plan::make_node_transaction(resource(),
                                                            components::logical_plan::transaction_op::commit);
        if (ddl_mode) {
            // DDL mode prepends the flush durability barrier + WAL(cid=0)
            // record inside the operator.
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            commit_node->set_is_ddl_commit(true);
            commit_node->set_txn_id(txn.transaction_id);
            commit_node->set_database_oid(db_oid);
        }
        auto cparams = components::logical_plan::make_parameter_node(resource());
        services::context_storage_t cstor(resource(), log_.clone(), session_tz);
        co_return co_await execute_plan(
            session,
            components::logical_plan::execution_plan_t{resource(), std::move(commit_node), std::move(cparams)},
            std::move(cstor),
            txn,
            lowest_active_start_time,
            std::pmr::vector<explain_plan_node>{resource()});
    }

} // namespace services::collection::executor
