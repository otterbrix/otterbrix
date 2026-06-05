#include "executor.hpp"

#include <array>

#include <components/catalog/catalog_codes.hpp>
#include <components/context/execution_context.hpp>
#include <components/planner/planner.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_allocate_oids.hpp>
#include <components/logical_plan/node_catalog_resolve_function.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/logical_plan/node_computed_field_register.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
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
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/enrich_logical_plan.hpp>
#include <services/dispatcher/plan_resolve_index.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>
#include <components/planner/optimizer.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_commit_transaction.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>

using namespace components::cursor;

namespace services::collection::executor {

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

    plan_t::plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                   components::logical_plan::storage_parameters parameters,
                   services::context_storage_t&& context_storage,
                   components::logical_plan::limit_t limit)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage)
        , limit(limit) {}

    executor_t::executor_t(std::pmr::memory_resource* resource,
                           actor_zeta::address_t parent_address,
                           actor_zeta::address_t wal_address,
                           actor_zeta::address_t disk_address,
                           actor_zeta::address_t index_address,
                           log_t&& log)
        : actor_zeta::basic_actor<executor_t>{resource}
        , parent_address_(std::move(parent_address))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
        , log_(log)
        , function_registry_(resource) {
        register_default_functions(function_registry_);
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
                             components::logical_plan::node_ptr logical_plan,
                             components::logical_plan::storage_parameters parameters,
                             services::context_storage_t context_storage,
                             components::table::transaction_data txn,
                             uint64_t lowest_active_start_time) {
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
        auto* limit_lookup_node = logical_plan.get();
        if (limit_lookup_node && limit_lookup_node->type() == components::logical_plan::node_type::sequence_t) {
            // Find first non-resolve child as the limit-carrying consumer.
            auto is_catalog_resolve = [](components::logical_plan::node_type t) {
                return t == components::logical_plan::node_type::catalog_resolve_namespace_t ||
                       t == components::logical_plan::node_type::catalog_resolve_table_t ||
                       t == components::logical_plan::node_type::catalog_resolve_type_t ||
                       t == components::logical_plan::node_type::catalog_resolve_function_t ||
                       t == components::logical_plan::node_type::catalog_resolve_constraint_t;
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
        context_storage.parameters = &parameters;
        components::operators::operator_ptr plan =
            planner::create_plan(context_storage, function_registry_, logical_plan, limit, &parameters);

        if (!plan) {
            // Surface the error via cursor so execute_plan_full's tail routes
            // through abort.
            co_return execute_result_t{
                make_cursor(resource(),
                            core::error_t(core::error_code_t::create_physical_plan_error,
                                          std::pmr::string{"invalid query plan", resource()}))};
        }

        plan->set_as_root();

        auto plan_data = traverse_plan_(std::move(plan), std::move(parameters), std::move(context_storage));
        plan_data.limit = limit;

        auto result = co_await execute_sub_plan_(session, std::move(plan_data), txn_data, lowest_active_start_time);

        // Raw pipeline result. Three cases, distinguished by the vector state
        // and resolved by execute_plan_full's accumulate/commit tail:
        //   * non-DML (DDL, SELECT): dml_* empty (pg_catalog_* populated for DDL).
        //   * DML success: vectors populated; the tail accumulates them onto
        //     transaction_t and (autocommit) runs the commit pipeline.
        //   * DML error: dml_appends / pg_catalog_appends still carry the
        //     not-yet-published ranges so the tail's abort cascade reverts them.
        co_return execute_result_t{std::move(result.cursor),
                                   std::move(result.pg_catalog_appends),
                                   std::move(result.pg_catalog_delete_tables),
                                   std::move(result.pg_attribute_commit_id_backfills),
                                   std::move(result.dml_appends),
                                   std::move(result.dml_deletes),
                                   result.commit_id};
    }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan_full(components::session::session_id_t session,
                                   components::logical_plan::node_ptr logical_plan,
                                   components::logical_plan::parameter_node_ptr params) {
        // Full per-query pipeline: session-context fetch, optimize, resolve
        // wrap, catalog resolve, view splice, validate, enrich, planner
        // rewrites, operator pipeline, then the DML/DDL commit (or abort)
        // tail. The dispatcher only routes; ALL txn-state access goes through
        // its txn_*_msg mailbox handlers.
        using node_type = components::logical_plan::node_type;
        using components::logical_plan::node_t;
        using components::logical_plan::node_ptr;
        using components::logical_plan::node_aggregate_t;
        using components::logical_plan::node_match_t;
        using components::logical_plan::node_join_t;
        using components::logical_plan::node_create_database_t;
        using components::logical_plan::node_catalog_resolve_namespace_t;
        using components::logical_plan::node_catalog_resolve_table_t;
        using components::logical_plan::node_sequence_t;

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
            auto* r = services::catalog_resolve::effective_root_node(logical_plan.get());
            return r ? r->type() : node_type::unused;
        }();

        // SET TIMEZONE: capture the name before the planner consumes the node;
        // surfaced back to the dispatcher via execute_result_t.applied_timezone
        // once the operator pipeline confirms pg_settings was persisted.
        std::pmr::string pending_set_tz_name{resource()};
        if (original_type == node_type::set_timezone_t) {
            auto* tz_node = static_cast<components::logical_plan::node_set_timezone_t*>(
                services::catalog_resolve::effective_root_node(logical_plan.get()));
            pending_set_tz_name.assign(tz_node->timezone_name().c_str(),
                                       tz_node->timezone_name().size());
        }

        // Optimizer: constant folding, etc. Needs the parameter NODE (before
        // the destructive take_parameters below).
        logical_plan = components::planner::optimize(resource(), logical_plan, params.get());

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
            if (logical_plan->type() == node_type::sequence_t) {
                for (const auto& c : logical_plan->children()) {
                    if (!c)
                        continue;
                    if (c->type() == node_type::catalog_resolve_namespace_t) {
                        auto* r = static_cast<const node_catalog_resolve_namespace_t*>(c.get());
                        existing_dbs.insert(r->dbname());
                    } else if (c->type() == node_type::catalog_resolve_table_t) {
                        auto* r = static_cast<const node_catalog_resolve_table_t*>(c.get());
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
            stack.push_back(logical_plan.get());
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
                if (logical_plan->type() == node_type::sequence_t) {
                    // Splice new resolves AFTER existing leading resolve_*
                    // siblings but BEFORE the consumer node. Order matters:
                    // stamp_oids_from_resolves picks the FIRST resolve_table
                    // as the DML target — preserving original-target priority
                    // means walker-added scan resolves don't shadow it.
                    auto is_resolve_local = [](node_type t) {
                        return t == node_type::catalog_resolve_namespace_t || t == node_type::catalog_resolve_table_t ||
                               t == node_type::catalog_resolve_type_t || t == node_type::catalog_resolve_function_t ||
                               t == node_type::catalog_resolve_constraint_t;
                    };
                    auto& kids = logical_plan->children();
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
                    seq->append_child(std::move(logical_plan));
                    logical_plan = seq;
                }
            }
        }

        // Destructive: consumes the parameter node into storage_parameters.
        // Everything below (validate_schema, create_plan, the operator
        // pipeline) reads this local.
        components::logical_plan::storage_parameters parameters = params->take_parameters();

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
            original_type == node_type::create_index_t || original_type == node_type::drop_index_t ||
            original_type == node_type::drop_database_t || original_type == node_type::drop_collection_t ||
            original_type == node_type::drop_type_t || original_type == node_type::drop_sequence_t ||
            original_type == node_type::drop_view_t || original_type == node_type::drop_macro_t ||
            original_type == node_type::create_database_t || original_type == node_type::alter_table_t ||
            original_type == node_type::create_matview_t;
        const bool needs_dml_txn =
            original_type == node_type::insert_t || original_type == node_type::update_t ||
            original_type == node_type::delete_t;
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
        if (logical_plan && logical_plan->type() == components::logical_plan::node_type::sequence_t) {
            auto& kids = logical_plan->children();
            auto is_resolve = [](components::logical_plan::node_type t) {
                return t == components::logical_plan::node_type::catalog_resolve_namespace_t ||
                       t == components::logical_plan::node_type::catalog_resolve_table_t ||
                       t == components::logical_plan::node_type::catalog_resolve_type_t ||
                       t == components::logical_plan::node_type::catalog_resolve_function_t ||
                       t == components::logical_plan::node_type::catalog_resolve_constraint_t;
            };
            std::size_t resolve_count = 0;
            while (resolve_count < kids.size() && kids[resolve_count] &&
                   is_resolve(kids[resolve_count]->type())) {
                ++resolve_count;
            }
            if (resolve_count > 0) {
                // Resolve sub-plan over the front children. operator_resolve_*_t
                // holds a raw pointer to each logical node — the SAME objects
                // still in the parent sequence_t — so OIDs stamped during resolve
                // become visible to the parent's validate/enrich pass.
                auto pass1_root = boost::intrusive_ptr<components::logical_plan::node_t>(
                    new components::logical_plan::node_sequence_t(resource()));
                for (std::size_t i = 0; i < resolve_count; ++i) {
                    pass1_root->append_child(kids[i]);
                }
                auto pass1_params = components::logical_plan::make_parameter_node(resource());
                // Throw-away context_storage_t so the caller's `context_storage`
                // (move-consumed by the final delegate) survives untouched. It
                // carries only what the resolve operators read (resource / log /
                // session_timezone); known_oids / table_metadata stay empty
                // because resolves stamp onto the plan tree, not context_storage.
                services::context_storage_t pass1_context_storage{resource(),
                                                                   log_.clone(),
                                                                   context_storage.session_timezone};
                auto pass1_result = co_await this->execute_plan(session,
                                                                 pass1_root,
                                                                 pass1_params->take_parameters(),
                                                                 std::move(pass1_context_storage),
                                                                 resolve_txn,
                                                                 session_ctx.lowest_active_start_time);
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
        if (logical_plan) {
            services::catalog_resolve::stamp_oids_from_resolves(logical_plan.get());
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
        if (logical_plan) {
            if (auto* view_node = services::catalog_resolve::find_first_view_resolve(logical_plan.get())) {
                auto exp = services::catalog_resolve::expand_view_body(resource(),
                                                                       view_node->resolved_metadata()->view_sql);
                if (exp.error) {
                    trace(log_, "executor::execute_plan_full: view expansion failed");
                    co_return execute_result_t{std::move(exp.error)};
                }
                if (exp.had_expansion && exp.expanded_plan) {
                    // Full plan replacement — outer is treated as a trivial
                    // passthrough. Preserving outer projections / filters
                    // (splice sub-plan as child of outer consumer) is not yet
                    // handled.
                    logical_plan = std::move(exp.expanded_plan);

                    // Merge the sub-plan's parameter bindings into `parameters`
                    // so downstream operators see view-body constants (e.g.
                    // `col_b > 10`). Safe against id collision because the outer
                    // plan is a trivial passthrough SELECT * with no constants.
                    // (raw storage_parameters here → add_parameter free fn.)
                    if (exp.expanded_params) {
                        for (const auto& [pid, val] : exp.expanded_params->parameters().parameters) {
                            components::logical_plan::add_parameter(parameters, pid, val);
                        }
                    }

                    // === Resolve sub-plan's fresh resolves ===
                    auto fresh = services::catalog_resolve::extract_unresolved_resolves(logical_plan.get());
                    if (!fresh.empty()) {
                        auto pass2_root = boost::intrusive_ptr<components::logical_plan::node_t>(
                            new components::logical_plan::node_sequence_t(resource()));
                        for (auto& n : fresh) {
                            pass2_root->append_child(n);
                        }
                        auto pass2_params = components::logical_plan::make_parameter_node(resource());
                        // Throw-away context_storage_t, as in the outer resolve
                        // loop, so the caller's context_storage survives untouched.
                        services::context_storage_t pass2_context_storage{resource(),
                                                                          log_.clone(),
                                                                          context_storage.session_timezone};
                        auto pass2_result = co_await this->execute_plan(session,
                                                                         pass2_root,
                                                                         pass2_params->take_parameters(),
                                                                         std::move(pass2_context_storage),
                                                                         resolve_txn,
                                                                         session_ctx.lowest_active_start_time);
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
                    services::catalog_resolve::stamp_oids_from_resolves(logical_plan.get());
                }
            }
        }
        // Enrich/validate. original_type (captured at function start, before
        // any rewrites) drives a switch of namespace / table / type existence
        // checks (catalog_resolve helpers — no async, only resource()); the
        // default branch runs validate_types + validate_schema, then
        // post_validate_optimize → enrich_plan → planner.create_plan.
        using components::logical_plan::node_create_collection_t;
        using components::logical_plan::node_create_constraint_t;
        using components::logical_plan::node_create_type_t;
        using components::catalog::table_id;
        using components::logical_plan::constraint_kind;
        using components::types::logical_type;

        // Rebuild dispatcher_idx against the (possibly view-spliced) plan
        // tree so validate / enrich / build_id_cfn see fully-stamped OIDs.
        services::catalog_resolve::plan_resolve_index_t dispatcher_idx;
        if (logical_plan) {
            services::catalog_resolve::stamp_oids_from_resolves(logical_plan.get());
            services::catalog_resolve::gather_plan_resolve_index(logical_plan.get(), dispatcher_idx);
        }

        // Build qualified_name_t from the effective consumer node; nodes
        // that don't carry user-typed names pull (db, rel) from the
        // sibling resolve_* nodes via drop_target_names_from_resolves.
        const auto* plan_root_for_drop_names = logical_plan.get();
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
                case node_type::alter_column_add_t:
                case node_type::alter_column_drop_t:
                case node_type::alter_column_rename_t:
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
                case node_type::update_t:
                case node_type::drop_collection_t:
                case node_type::drop_index_t:
                case node_type::drop_macro_t:
                case node_type::drop_sequence_t:
                case node_type::drop_view_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::drop_database_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, std::string{}};
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
                    build_id_cfn(services::catalog_resolve::effective_root_node(logical_plan.get())));
        cursor_t_ptr error;
        // Existence checks read from the explicit dispatcher_idx populated
        // above (mirrors the dispatcher's pre-execute pass).
        switch (original_type) {
            case node_type::create_database_t:
                if (!services::dispatcher::check_namespace_exists(resource(), &dispatcher_idx, id).contains_error()) {
                    auto* d = static_cast<const node_create_database_t*>(
                        services::catalog_resolve::effective_root_node(logical_plan.get()));
                    if (d && d->if_not_exists()) {
                        error = make_cursor(resource());
                    } else {
                        error = make_cursor(resource(),
                                            core::error_t{core::error_code_t::database_already_exists,
                                                          std::pmr::string{"database already exists", resource()}});
                    }
                }
                break;
            case node_type::drop_database_t:
                if (auto err = services::dispatcher::check_namespace_exists(resource(), &dispatcher_idx, id);
                    err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                break;
            case node_type::create_collection_t: {
                if (!services::dispatcher::check_collection_exists(resource(), &dispatcher_idx, id)
                         .contains_error()) {
                    auto* cc = static_cast<const node_create_collection_t*>(
                        services::catalog_resolve::effective_root_node(logical_plan.get()));
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
                        services::catalog_resolve::effective_root_node(logical_plan.get()));
                    for (auto& col_def : n->column_definitions()) {
                        if (col_def.type().type() == logical_type::UNKNOWN) {
                            if (col_def.type().type_name().empty()) {
                                break;
                            }
                            const auto lt =
                                components::catalog::pg_name_to_logical_type(col_def.type().type_name());
                            if (lt != logical_type::UNKNOWN) {
                                std::string alias =
                                    col_def.type().has_alias() ? col_def.type().alias() : std::string{};
                                col_def.type() = components::types::complex_logical_type{lt};
                                if (!alias.empty()) {
                                    col_def.type().set_alias(alias);
                                }
                                continue;
                            }
                            if (auto err = services::dispatcher::check_type_exists(
                                    resource(),
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
            case node_type::drop_collection_t: {
                // Authoritative existence check via the plan-tree resolve idx.
                if (auto err = services::dispatcher::check_collection_exists(resource(), &dispatcher_idx, id);
                    err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                break;
            }
            case node_type::create_type_t: {
                auto* n = static_cast<node_create_type_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
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
                            const auto* md = services::catalog_resolve::probe_type_in_path(
                                &dispatcher_idx, std::string_view(field.type_name()), str_path);
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
            case node_type::drop_type_t: {
                std::string type_name;
                if (logical_plan->type() == node_type::sequence_t) {
                    for (const auto& c : logical_plan->children()) {
                        if (c && c->type() == node_type::catalog_resolve_type_t) {
                            type_name =
                                static_cast<const components::logical_plan::node_catalog_resolve_type_t*>(c.get())
                                    ->type_name();
                            break;
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
            case node_type::set_timezone_t:
            case node_type::checkpoint_t:
            case node_type::vacuum_t:
            // SQL BEGIN/COMMIT/ROLLBACK are leaf control nodes exactly like
            // checkpoint/vacuum: no table schema to validate. Without these
            // cases they fall into the default branch and validate_schema's
            // default arm assert(false)s on the unknown node type.
            case node_type::begin_transaction_t:
            case node_type::commit_transaction_t:
            case node_type::abort_transaction_t:
            case node_type::create_sequence_t:
            case node_type::drop_sequence_t:
            case node_type::create_view_t:
            case node_type::drop_view_t:
            case node_type::create_macro_t:
            case node_type::drop_macro_t:
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
                        services::catalog_resolve::effective_root_node(logical_plan.get()));
                    if (cstr->kind() == constraint_kind::foreign_key ||
                        cstr->kind() == constraint_kind::check) {
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
                                core::error_t{
                                    core::error_code_t::schema_error,
                                    std::pmr::string{
                                        "Foreign key constraints are not supported when the referencing or "
                                        "referenced table is dynamic-schema (relkind='g'). FK enforcement "
                                        "requires stable column attoids; dynamic-schema columns may evolve. "
                                        "Convert involved tables to static schema first.",
                                        resource()}});
                        } else if (cstr->kind() == constraint_kind::check && local_is_g) {
                            error = make_cursor(
                                resource(),
                                core::error_t{
                                    core::error_code_t::schema_error,
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
                                                                    logical_plan.get(),
                                                                    context_storage.session_timezone);
                if (vt_err.contains_error()) {
                    error = make_cursor(resource(), vt_err);
                } else {
                    auto schema_res = services::dispatcher::validate_schema(resource(),
                                                                              &dispatcher_idx,
                                                                              logical_plan.get(),
                                                                              parameters);
                    if (schema_res.has_error()) {
                        error = make_cursor(resource(), schema_res.error());
                    }
                }
            }
        }

        if (error) {
            trace(log_,
                  "executor::execute_plan_full: validation error: {}",
                  error->get_error().what);
            co_return execute_result_t{std::move(error)};
        }

        // CREATE INDEX: indexed table oid captured at rewrite time (the plan
        // tree is move-consumed by the execute_plan delegate before the
        // backfill-commit tail runs).
        components::catalog::oid_t create_index_table_oid = components::catalog::INVALID_OID;

        // Destructive rewrites. post_validate_optimize / enrich_plan /
        // planner.create_plan are NOT idempotent — in particular create_plan
        // wraps insert/update/delete in check_constraint_t / fk_check_t, and
        // running it twice re-wraps on top of the previous wrap (broken plan).
        // The executor is the ONLY side running these passes (the dispatcher
        // routes the raw plan straight here).
        {
            logical_plan = components::planner::post_validate_optimize(resource(), std::move(logical_plan));

            // Enrich DML node fields with catalog metadata (NOT NULL, DEFAULT,
            // CHECK exprs), reading exclusively from the plan-tree idx. ctx
            // carries resolve_txn so enrich sees the same MVCC snapshot.
            components::execution_context_t enrich_ctx{session,
                                                       resolve_txn,
                                                       context_storage.session_timezone};
            auto ef = services::dispatcher::enrich_plan(resource(),
                                                         logical_plan,
                                                         disk_address_,
                                                         enrich_ctx,
                                                         index_address_,
                                                         &context_storage);
            co_await std::move(ef);
            // Logical plan rewrite: insert constraint wrapper nodes driven
            // by enriched fields.
            components::planner::planner_t planner;
            logical_plan = planner.create_plan(resource(), std::move(logical_plan));

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
                auto dependency_oids = logical_plan->table_oid_dependencies();
                for (auto oid : dependency_oids) {
                    context_storage.known_oids.insert(oid);
                }
                services::catalog_resolve::plan_resolve_index_t local_idx;
                services::catalog_resolve::gather_plan_resolve_index(logical_plan.get(), local_idx);
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
            auto allocate_oids_inline =
                [this, session, &context_storage](executor_t* self, std::size_t count)
                -> executor_t::unique_future<std::vector<components::catalog::oid_t>> {
                (void)self;
                auto node = components::logical_plan::make_node_allocate_oids(resource(), count);
                components::compute::function_registry_t local_fn_registry{resource()};
                services::context_storage_t cstor{resource(),
                                                   log_.clone(),
                                                   context_storage.session_timezone};
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
                op->on_execute(&pctx);
                while (!op->is_executed()) {
                    auto waiting = op->find_waiting_operator();
                    if (!waiting)
                        break;
                    co_await waiting->await_async_and_resume(&pctx);
                    op->on_execute(&pctx);
                }
                if (pctx.has_pending_disk_futures()) {
                    auto futures = pctx.take_pending_disk_futures();
                    for (auto& f : futures)
                        co_await std::move(f);
                }
                co_return node->oids();
            };

            using components::logical_plan::node_create_matview_t;
            using components::logical_plan::node_sequence_t;
            using components::catalog::relkind::computed;

            // INSERT relkind='g' wrap — wraps INSERT into
            // sequence_t(insert, computed_field_register) so
            // pg_computed_column rows are appended inside the DML txn.
            if (original_type == node_type::insert_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_t resolved_tbl_oid = components::catalog::INVALID_OID;
                bool is_computing = false;
                auto* effective_insert_node = services::catalog_resolve::effective_root_node(logical_plan.get());
                auto enriched_oid = effective_insert_node ? effective_insert_node->table_oid()
                                                          : logical_plan->table_oid();
                if (enriched_oid == components::catalog::INVALID_OID && !logical_plan->children().empty()) {
                    enriched_oid = logical_plan->children().front()->table_oid();
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
                    std::vector<components::table::column_definition_t> registered_cols;
                    auto* effective_insert = services::catalog_resolve::effective_root_node(logical_plan.get());
                    if (effective_insert) {
                        for (const auto& child : effective_insert->children()) {
                            if (!child || child->type() != components::logical_plan::node_type::data_t) {
                                continue;
                            }
                            auto* data_node =
                                static_cast<const components::logical_plan::node_data_t*>(child.get());
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

                    auto insert_names =
                        services::catalog_resolve::drop_target_names_from_resolves(logical_plan.get());
                    auto register_node = boost::intrusive_ptr(
                        new components::logical_plan::node_computed_field_register_t(
                            resource(),
                            core::dbname_t{insert_names.first},
                            core::relname_t{insert_names.second},
                            resolved_tbl_oid,
                            std::move(registered_cols)));

                    auto seq = boost::intrusive_ptr(new node_sequence_t(resource()));
                    seq->append_child(logical_plan);
                    seq->append_child(register_node);
                    logical_plan = seq;
                }
            }

            // CREATE COLLECTION → planner rewrite to
            // sequence_t(create_collection_t, primitive_write × N).
            if (original_type == node_type::create_collection_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                auto* cc = static_cast<node_create_collection_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                const std::size_t need = 1 + cc->column_definitions().size();
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(this, need);
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE DATABASE → planner rewrite into sequence_t(primitive_write
            // on pg_namespace).
            if (original_type == node_type::create_database_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(this, std::size_t{1});
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE TYPE → planner rewrite.
            //   STRUCT → 1 + N OIDs (pg_class.oid + N×pg_attribute.attoid).
            //   ENUM/other → 1 OID (pg_type.oid).
            if (original_type == node_type::create_type_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                auto* ct = static_cast<node_create_type_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                const std::size_t need = (ct->type().type() == logical_type::STRUCT)
                                             ? std::size_t{1} + ct->type().child_types().size()
                                             : std::size_t{1};
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(this, need);
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE SEQUENCE/VIEW/MACRO → planner rewrite to
            // sequence_t(primitive_write × N).
            //   SEQUENCE → 1 OID; VIEW → 2 OIDs; MACRO → 2 OIDs.
            if ((original_type == node_type::create_sequence_t ||
                 original_type == node_type::create_view_t ||
                 original_type == node_type::create_macro_t) &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                const std::size_t need =
                    (original_type == node_type::create_sequence_t) ? std::size_t{1} : std::size_t{2};
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(this, need);
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE MATERIALIZED VIEW → mv_oid + N×attoid + rule_oid = 2 + N.
            if (original_type == node_type::create_matview_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                auto* cm = static_cast<node_create_matview_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                const std::size_t col_count = cm ? cm->inferred_columns().size() : std::size_t{0};
                const std::size_t need = 2 + col_count;
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(this, need);
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE INDEX → planner rewrite to
            // sequence_t(primitive_write × N, create_index_t).
            if (original_type == node_type::create_index_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(this, std::size_t{1});
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
                // Capture the indexed table oid NOW for the post-pipeline
                // backfill commit_insert: logical_plan is move-consumed by the
                // execute_plan delegate below, so the tail cannot probe the
                // plan tree anymore.
                if (auto* eff = services::catalog_resolve::effective_root_node(logical_plan.get());
                    eff && !eff->children().empty()) {
                    auto* back = eff->children().back().get();
                    if (back && back->type() == node_type::create_index_t) {
                        create_index_table_oid =
                            static_cast<const components::logical_plan::node_create_index_t*>(back)->table_oid();
                    }
                }
            }

            // DROP INDEX → planner rewrite to
            // sequence_t(primitive_delete × N, drop_index_t). No OIDs.
            if (original_type == node_type::drop_index_t) {
                components::catalog::oid_batch_t oid_batch;
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // ALTER TABLE → planner rewrite to
            // sequence_t(alter_column_{add,rename,drop}_t × N). No OID batch
            // (alter_column_add_t allocates its own attoid at execution).
            // Re-enrich afterwards: the planner stamps fresh attoids on rename /
            // computed_field_unregister primitives that didn't exist before it
            // ran. Use resolve_txn so enrich's pg_computed_column scan sees the
            // INSERT-time register rows committed under that txn.
            if (original_type == node_type::alter_table_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_batch_t oid_batch; // intentionally empty
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
                components::execution_context_t enriched_ctx{session,
                                                              resolve_txn,
                                                              context_storage.session_timezone};
                auto ef2 = services::dispatcher::enrich_plan(resource(),
                                                              logical_plan,
                                                              disk_address_,
                                                              enriched_ctx,
                                                              index_address_,
                                                              &context_storage);
                co_await std::move(ef2);
            }

            // DROP DATABASE / TABLE / TYPE / SEQUENCE / VIEW / MACRO →
            // planner rewrite to node_dynamic_cascade_delete_t. No OIDs.
            if ((original_type == node_type::drop_database_t ||
                 original_type == node_type::drop_collection_t ||
                 original_type == node_type::drop_type_t ||
                 original_type == node_type::drop_sequence_t ||
                 original_type == node_type::drop_view_t ||
                 original_type == node_type::drop_macro_t) &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_batch_t oid_batch; // intentionally empty
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE CONSTRAINT → planner rewrite to sequence_t(primitive_write
            // on pg_constraint+pg_depend). CHECK expressions are
            // pre-validated for non-empty here so we do not waste an OID on
            // a doomed constraint.
            if (original_type == node_type::create_constraint_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                auto* cstr = static_cast<node_create_constraint_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                if (cstr->kind() == constraint_kind::check && cstr->check_expr().empty()) {
                    co_return execute_result_t{make_cursor(
                        resource(),
                        core::error_t{core::error_code_t::other_error,
                                      std::pmr::string{"CHECK constraint expression is empty or contains "
                                                       "unsupported constructs (functions, subqueries, and CASE "
                                                       "expressions are not allowed; valid: comparisons, AND/OR/NOT, "
                                                       "IS NULL/IS NOT NULL, column references, and constants)",
                                                       resource()}})};
                }
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(this, std::size_t{1});
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }
        }
        // Unresolved-ALTER no-op guard: a plan whose LITERAL root is still
        // alter_table_t after the rewrites means rewrite_alter_table bailed
        // (table_oid unresolved by enrich) — return no-op success. Wrapped
        // plans (sequence_t root with an alter_table_t child) keep the error
        // path through the pipeline.
        if (original_type == node_type::alter_table_t && logical_plan &&
            logical_plan->type() == node_type::alter_table_t) {
            co_return execute_result_t{make_cursor(resource())};
        }

        trace(log_,
              "executor::execute_plan_full: delegating to execute_plan, session: {}",
              session.data());
        // Operator-pipeline run, forwarding resolve_txn so the operator path
        // sees the same MVCC snapshot the resolves did.
        auto exec_result = co_await execute_plan(session,
                                                 std::move(logical_plan),
                                                 std::move(parameters),
                                                 std::move(context_storage),
                                                 resolve_txn,
                                                 session_ctx.lowest_active_start_time);

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
                    payload.base_deletes.push_back(
                        components::table::dml_delete_range_t{del.table_oid, del.txn_id});
                }
                payload.pg_catalog_appends = std::move(exec_result.pg_catalog_appends);
                payload.pg_catalog_delete_tables = std::move(exec_result.pg_catalog_delete_tables);
                payload.backfills = std::move(exec_result.pg_attribute_commit_id_backfills);
                trace(log_,
                      "executor::execute_plan_full: txn {} — accumulating {} appends, {} deletes ({})",
                      resolve_txn.transaction_id,
                      payload.base_appends.size(),
                      payload.base_deletes.size(),
                      session_ctx.is_explicit ? "publish deferred to COMMIT" : "implicit COMMIT follows");
                if (!payload.empty()) {
                    auto [_ac, acf] = actor_zeta::send(
                        parent_address_,
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
                // Failed DML statement: revert this statement's local ranges
                // and abort the txn (also ends a failed statement's explicit
                // txn).
                //
                // Storage revert: fold every DML append range AND the
                // pg_catalog append ranges into a SINGLE storage_revert_appends
                // call. Each range carries its own table_oid (the handler routes
                // per-range), so one send covers both sets. The ctx table_oid is
                // unused by storage_revert_appends.
                std::vector<components::pg_catalog_append_range_t> revert_ranges;
                revert_ranges.reserve(exec_result.dml_appends.size() + exec_result.pg_catalog_appends.size());
                for (const auto& app : exec_result.dml_appends) {
                    revert_ranges.push_back(
                        components::pg_catalog_append_range_t{app.table_oid, app.row_start, app.row_count});
                }
                for (auto& pgc : exec_result.pg_catalog_appends) {
                    revert_ranges.push_back(std::move(pgc));
                }
                exec_result.pg_catalog_appends.clear();
                if (!revert_ranges.empty()) {
                    components::execution_context_t pgc_ctx{session, resolve_txn, {}};
                    auto [_pa, paf] = actor_zeta::send(disk_address_,
                                                       &services::disk::manager_disk_t::storage_revert_appends,
                                                       pgc_ctx,
                                                       std::move(revert_ranges));
                    co_await std::move(paf);
                }

                // Index revert: revert_insert is keyed per (table_oid, txn_id)
                // and reverts ALL uncommitted entries for that pair, so it is
                // idempotent across duplicate table oids — dedup to unique oids
                // (avoids redundant sends) and fan out two-phase: send every
                // revert_insert first, then await each.
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    std::pmr::set<components::catalog::oid_t> revert_index_oids{resource()};
                    for (const auto& app : exec_result.dml_appends) {
                        revert_index_oids.insert(app.table_oid);
                    }
                    std::pmr::vector<actor_zeta::unique_future<void>> revert_index_futures{resource()};
                    revert_index_futures.reserve(revert_index_oids.size());
                    for (auto oid : revert_index_oids) {
                        components::execution_context_t abort_ctx{session,
                                                                  resolve_txn,
                                                                  session_ctx.session_tz,
                                                                  oid};
                        auto [_ri, rif] = actor_zeta::send(index_address_,
                                                           &services::index::manager_index_t::revert_insert,
                                                           abort_ctx,
                                                           oid);
                        revert_index_futures.push_back(std::move(rif));
                    }
                    for (auto& rif : revert_index_futures) {
                        co_await std::move(rif);
                    }
                }

                auto [_ab, abf] = actor_zeta::send(
                    parent_address_,
                    &services::dispatcher::manager_dispatcher_t::txn_abort_msg,
                    session);
                co_await std::move(abf);

                exec_result.dml_appends.clear();
                exec_result.dml_deletes.clear();
            }
        }

        // ===== DDL commit tail =====
        // The accumulated pg_catalog swap-info rides to the dispatcher's
        // transaction_t first (ONE accumulate message, base fields empty), then
        // the ddl-commit operator drains it back via txn_commit_drain_msg and
        // publishes in order: flush barrier, WAL, commit, publish.
        if (needs_ddl_txn && exec_result.cursor->is_success()) {
            if (!exec_result.pg_catalog_appends.empty() || !exec_result.pg_catalog_delete_tables.empty() ||
                !exec_result.pg_attribute_commit_id_backfills.empty()) {
                services::dispatcher::txn_accumulate_payload_t payload;
                payload.pg_catalog_appends = std::move(exec_result.pg_catalog_appends);
                payload.pg_catalog_delete_tables = std::move(exec_result.pg_catalog_delete_tables);
                payload.backfills = std::move(exec_result.pg_attribute_commit_id_backfills);
                auto [_ac, acf] = actor_zeta::send(
                    parent_address_,
                    &services::dispatcher::manager_dispatcher_t::txn_accumulate_msg,
                    session,
                    std::move(payload));
                co_await std::move(acf);
                exec_result.pg_catalog_appends.clear();
                exec_result.pg_catalog_delete_tables.clear();
                exec_result.pg_attribute_commit_id_backfills.clear();
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
            auto commit_result = co_await run_commit_pipeline_(session,
                                                               resolve_txn,
                                                               session_ctx.session_tz,
                                                               session_ctx.lowest_active_start_time,
                                                               /*ddl_mode=*/true);
            if (commit_result.cursor->is_error()) {
                exec_result.cursor = std::move(commit_result.cursor);
            }
            // Inline CREATE INDEX backfill index-commit (index ONLY — see
            // above). The indexed table oid was captured at rewrite time; the
            // commit_id arrives via the operator's ctx back-channel.
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
                    // Today bitcask is assert+abort terminal, so
                    // contains_error() never trips on the CREATE INDEX path.
                    auto ci_result = co_await std::move(cif);
                    if (ci_result.contains_error()) {
                        // TODO: index-side abort path for CREATE INDEX
                        // failure (revert pg_index row, drop disk agent).
                    }
                }
            }
        }
        // Deliberate: a FAILED DDL statement does NOT abort its txn (it stays
        // orphaned/active); fixing that is a separate task.

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
        const bool releases_resolve_txn =
            !needs_ddl_txn && !needs_dml_txn && !needs_commit_txn && !session_ctx.is_explicit &&
            original_type != node_type::begin_transaction_t &&
            original_type != node_type::commit_transaction_t &&
            original_type != node_type::abort_transaction_t;
        if (releases_resolve_txn) {
            auto [_rl, rlf] = actor_zeta::send(
                parent_address_,
                &services::dispatcher::manager_dispatcher_t::txn_abort_msg,
                session);
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

    plan_t executor_t::traverse_plan_(components::operators::operator_ptr&& plan,
                                      components::logical_plan::storage_parameters&& parameters,
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

        return plan_t{std::move(sub_plans), parameters, std::move(context_storage)};
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
                                                             plan_data.parameters};
            pipeline_context.disk_address = disk_address_;
            pipeline_context.index_address = index_address_;
            pipeline_context.wal_address = wal_address_;
            pipeline_context.txn = txn;
            pipeline_context.session_tz = plan_data.context_storage_.session_timezone;
            // VACUUM/MVCC GC threshold. operator_vacuum_t reads this to gate
            // manager_disk_t::vacuum_all + manager_index_t::cleanup_all_versions.
            // The value arrives with the session context fetched at plan start.
            pipeline_context.lowest_active_start_time = lowest_active_start_time;

            // Prepare the operator tree (connects children in aggregation, etc.)
            plan->prepare();

            // Execute the plan tree (scan operators send I/O requests and enter waiting state)
            plan->on_execute(&pipeline_context);

            if (plan->has_error()) {
                cursor = make_cursor(resource(), plan->get_error());
                break;
            }

            // Await all waiting operators (multiple scans in a join, etc.)
            while (!plan->is_executed()) {
                auto waiting_op = plan->find_waiting_operator();
                if (!waiting_op) {
                    error(log_,
                          "Plan not executed and no waiting operator! session: {}, plan type: {}",
                          session.data(),
                          static_cast<int>(plan->type()));
                    assert(plan->has_error());
                    cursor = make_cursor(resource(), plan->get_error());
                    break;
                }
                trace(log_, "executor: found waiting operator, type={}", static_cast<int>(waiting_op->type()));
                // DML operators (insert/remove/update) self-contain
                // WAL + storage + index I/O inside their await_async_and_resume.
                // Every operator is dispatched uniformly via the same
                // coroutine entry point.
                co_await waiting_op->await_async_and_resume(&pipeline_context);
                // Propagate errors set during async resume (fk_check, fk_cascade,
                // DML on disk failure, etc.)
                if (waiting_op->has_error()) {
                    cursor = make_cursor(resource(), waiting_op->get_error());
                    break;
                }
                trace(log_, "executor: after await completed");
                // Re-execute: completed scan allows parent to proceed, may find next waiting scan
                plan->on_execute(&pipeline_context);
            }
            if (cursor && cursor->is_error())
                break;

            // Detect errors set asynchronously in operators (e.g. fk_cascade root with RESTRICT).
            if (plan->has_error()) {
                cursor = make_cursor(resource(), plan->get_error());
                break;
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
                            auto& chunks = plan->output()->chunks();
                            // Apply post-sort limit across multi-chunk output.
                            if (plan_data.limit.limit() > 0) {
                                uint64_t remaining = static_cast<uint64_t>(plan_data.limit.limit());
                                for (auto& c : chunks) {
                                    if (remaining == 0) {
                                        c.set_cardinality(0);
                                        continue;
                                    }
                                    if (c.size() > remaining) {
                                        c.set_cardinality(remaining);
                                        remaining = 0;
                                    } else {
                                        remaining -= c.size();
                                    }
                                }
                            }
                            cursor = make_cursor(resource(), std::move(chunks));
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

            // Lift DML swap-info recorded by operator_insert / _delete / _update
            // inside await_async_and_resume. Push a range per sub-plan rather
            // than overwriting, so FK cascade across >=2 tables keeps every
            // child's publish (see dml_append_range_t).
            if (pipeline_context.dml_append_row_count > 0) {
                result_tracking.dml_appends.push_back({pipeline_context.dml_table_oid,
                                                       pipeline_context.dml_append_row_start,
                                                       pipeline_context.dml_append_row_count});
            }
            if (pipeline_context.dml_delete_txn_id != 0) {
                result_tracking.dml_deletes.push_back({pipeline_context.dml_table_oid,
                                                       pipeline_context.dml_delete_txn_id});
            }
            // Commit back-channel: operator_commit_transaction_t recorded the
            // commit_id it drained.
            if (pipeline_context.committed_id != 0) {
                result_tracking.commit_id = pipeline_context.committed_id;
            }
            pipeline_context.dml_append_row_start = 0;
            pipeline_context.dml_append_row_count = 0;
            pipeline_context.dml_delete_txn_id = 0;
            pipeline_context.dml_table_oid = components::catalog::INVALID_OID;

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
            boost::intrusive_ptr(new components::logical_plan::node_commit_transaction_t(resource()));
        if (ddl_mode) {
            // DDL mode prepends the flush durability barrier + WAL(cid=0)
            // record inside the operator.
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            commit_node->set_is_ddl_commit(true);
            commit_node->set_txn_id(txn.transaction_id);
            commit_node->set_database_oid(db_oid);
        }
        components::logical_plan::storage_parameters cparams(resource());
        services::context_storage_t cstor(resource(), log_.clone(), session_tz);
        co_return co_await execute_plan(session,
                                        std::move(commit_node),
                                        std::move(cparams),
                                        std::move(cstor),
                                        txn,
                                        lowest_active_start_time);
    }

} // namespace services::collection::executor
