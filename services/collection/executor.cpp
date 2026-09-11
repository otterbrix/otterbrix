#include "executor.hpp"

#include <array>
#include <atomic>
#include <chrono>

#include <components/casts/default_casts.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/context/execution_context.hpp>
#include <components/planner/planner.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>
#include <components/logical_plan/effective_table_oid.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_allocate_oids.hpp>
#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_register_cast.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_set_timezone.hpp>
#include <components/logical_plan/node_transaction.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/view_expansion.hpp>
#include <core/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/enrich_logical_plan.hpp>
#include <services/dispatcher/resolve_type.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

using namespace components::cursor;

namespace services::collection::executor {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_streaming_pipeline_runs{0};
        std::atomic<uint64_t> g_dml_appends_reverted{0};
        std::atomic<uint64_t> g_dml_flush_count{0};
        std::atomic<uint64_t> g_index_reconcile_staged_ranges{0};
        std::atomic<void (*)(uint64_t)> g_dml_pre_drive_hook{nullptr};
        oid_alloc_interposer_t* g_oid_alloc_interposer = nullptr;
    } // namespace

    uint64_t streaming_pipeline_runs() noexcept { return g_streaming_pipeline_runs.load(std::memory_order_relaxed); }
    uint64_t dml_appends_reverted() noexcept { return g_dml_appends_reverted.load(std::memory_order_relaxed); }
    uint64_t dml_flush_count() noexcept { return g_dml_flush_count.load(std::memory_order_relaxed); }
    uint64_t index_reconcile_staged_ranges() noexcept {
        return g_index_reconcile_staged_ranges.load(std::memory_order_relaxed);
    }
    void dev_set_dml_pre_drive_hook(void (*hook)(uint64_t)) noexcept { g_dml_pre_drive_hook.store(hook); }

    void dev_set_oid_alloc_interposer(oid_alloc_interposer_t* interposer) { g_oid_alloc_interposer = interposer; }
    oid_alloc_interposer_t* dev_oid_alloc_interposer() { return g_oid_alloc_interposer; }
#endif

    // Ensures behavior() handles every dispatch_traits method — a missed case is a silent message loss.
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
            actor_zeta::msg_id<executor_t, &executor_t::unregister_udf>,
            actor_zeta::msg_id<executor_t, &executor_t::register_cast>,
            actor_zeta::msg_id<executor_t, &executor_t::unregister_cast>,
            actor_zeta::msg_id<executor_t, &executor_t::set_explain_renderer>,
            actor_zeta::msg_id<executor_t, &executor_t::poke_msg>,
            actor_zeta::msg_id<executor_t, &executor_t::unregister_udf_uid>,
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
        inline uint64_t count_rows(const components::operators::chunks_vector_t& chunks) noexcept {
            uint64_t rows = 0;
            for (const auto& c : chunks) {
                rows += c.size();
            }
            return rows;
        }

        struct analyze_scope {
            std::chrono::steady_clock::time_point t0;
            explicit analyze_scope(bool on) noexcept
                : t0(on ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{}) {}
            [[nodiscard]] std::chrono::nanoseconds elapsed() const noexcept {
                return std::chrono::steady_clock::now() - t0;
            }
        };

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
                    const auto left_oid = components::logical_plan::effective_table_oid(node->children().front());
                    const auto right_oid = components::logical_plan::effective_table_oid(node->children().back());
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
        // Moved, not copied — a pmr copy ctor doesn't propagate the allocator, silently rebinding off the arena.
        , context_storage_(std::move(context_storage)) {}

    executor_t::executor_t(std::pmr::memory_resource* resource,
                           actor_zeta::address_t parent_address,
                           actor_zeta::address_t wal_address,
                           actor_zeta::address_t disk_address,
                           actor_zeta::address_t index_address,
                           log_t&& log,
                           uint64_t dml_flush_row_threshold,
                           planner::create_plan_rule_t create_plan_rule,
                           components::planner::optimizer_pass_t optimizer_pass)
        : actor_zeta::basic_actor<executor_t>{resource}
        , parent_address_(std::move(parent_address))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
        , log_(log)
        , function_registry_(resource)
        , cast_registry_(resource)
        , create_plan_rule_(create_plan_rule)
        , optimizer_pass_(optimizer_pass)
        , dml_flush_row_threshold_(dml_flush_row_threshold)
        , explain_renderers_(resource) {
        register_default_functions(function_registry_);
        components::casts::register_default_casts(cast_registry_);
        explain_renderers_.push_back(&render_postgres);
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
            case actor_zeta::msg_id<executor_t, &executor_t::unregister_udf>: {
                co_await actor_zeta::dispatch(this, &executor_t::unregister_udf, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::register_cast>: {
                co_await actor_zeta::dispatch(this, &executor_t::register_cast, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::unregister_cast>: {
                co_await actor_zeta::dispatch(this, &executor_t::unregister_cast, msg);
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
            case actor_zeta::msg_id<executor_t, &executor_t::unregister_udf_uid>: {
                co_await actor_zeta::dispatch(this, &executor_t::unregister_udf_uid, msg);
                break;
            }
            default:
                break;
        }
    }

    // Poke target only, for the dispatcher's lost-wakeup watchdog (docs/actor-zeta-lost-wakeup.md);
    // remove with it.
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

        components::table::transaction_data txn_data = txn;

        auto limit = components::logical_plan::limit_t::unlimit();
        auto* limit_lookup_node = plan.sub_queries.back().get();
        if (limit_lookup_node && limit_lookup_node->type() == components::logical_plan::node_type::sequence_t) {
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

        context_storage.parameters = &plan.parameters->parameters();
        context_storage.create_plan_rule = create_plan_rule_;
        components::operators::operator_ptr node = planner::create_plan(context_storage,
                                                                        function_registry_,
                                                                        plan.sub_queries.back(),
                                                                        limit,
                                                                        &plan.parameters->parameters());

        if (!node) {
            co_return execute_result_t{make_cursor(resource(),
                                                   core::error_t(core::error_code_t::create_physical_plan_error,
                                                                 std::pmr::string{"invalid query plan", resource()}))};
        }

        node->set_as_root();

        components::operators::operator_ptr explain_root =
            plan.explain != components::logical_plan::explain_type::none ? node : nullptr;
        explain_name_map_t explain_names{resource()};
        const bool explain_analyze = plan.explain == components::logical_plan::explain_type::analyze;
        if (explain_root && explain_analyze) {
            explain_name_collector nc{context_storage, explain_names};
            explain_root->explain(nc.sink());
        }
        if (plan.explain == components::logical_plan::explain_type::plan) {
            if (plan.explain_capture_ir) {
                explain_ir_builder b{resource(), explain_names, &context_storage};
                explain_root->explain(b.sink());
                execute_result_t out{make_cursor(resource())};
                out.captured_explain_ir = b.release();
                co_return std::move(out);
            }
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
        explain_ir_builder b{resource(), names, cs};
        explain_root->explain(b.sink());
        explain_plan_node root = b.release();
        for (auto& sp : captured_subplans) {
            root.subplans.push_back(std::move(sp));
        }
        const auto render = resolve_explain_renderer_(render_id);
        if (render_id != 0 && !explain_slot_registered_(render_id)) {
            // Resolving to slot 0 is intended (see test_explain.cpp); an unregistered render_id is still logged.
            error(log_,
                  "executor::explain: render_id {} is not registered on this executor — rendering with the "
                  "default (slot 0)",
                  render_id);
        }
        return render(resource(), root, analyze);
    }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan_full(components::session::session_id_t session,
                                  components::logical_plan::execution_plan_t plan) {
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

        const bool run_sub_queries = plan.explain != components::logical_plan::explain_type::plan;
        const bool plan_only = plan.explain == components::logical_plan::explain_type::plan;
        const bool capture_ir = plan.explain == components::logical_plan::explain_type::analyze;
        std::pmr::vector<explain_plan_node> captured_subplans{resource()};
        for (std::size_t i = 0; (run_sub_queries || plan_only) && i + 1 < plan.sub_queries.size(); ++i) {
            auto* sub_root = plan.sub_queries[i].get();
            const node_type sub_type = sub_root ? sub_root->type() : node_type::unused;
            if (sub_type == node_type::insert_t || sub_type == node_type::update_t || sub_type == node_type::delete_t) {
                co_return execute_result_t{make_cursor(
                    resource(),
                    core::error_t{core::error_code_t::sql_parse_error,
                                  std::pmr::string{"DML statement is not allowed in a sub-query", resource()}})};
            }
            components::logical_plan::execution_plan_t sub_plan{resource(), plan.sub_queries[i], plan.parameters};
            if (plan_only) {
                sub_plan.explain = components::logical_plan::explain_type::plan;
                sub_plan.explain_capture_ir = true;
            } else if (capture_ir) {
                sub_plan.explain = components::logical_plan::explain_type::analyze;
                sub_plan.explain_capture_ir = true;
            }
            auto sub_result = co_await execute_plan_full(session, std::move(sub_plan));
            if (sub_result.cursor->is_error()) {
                co_return execute_result_t{std::move(sub_result.cursor)};
            }
            const auto& mapping = plan.sub_query_results[i];

            if (plan.sub_queries[i]->has_output_types()) {
                const auto& column_type = plan.sub_queries[i]->output_types().front();
                plan.parameters->set_parameter(
                    mapping.id,
                    mapping.compacter == &components::vector::compact_to_single_value
                        ? components::types::logical_value_t{resource(), column_type}
                        : components::types::logical_value_t::create_array(resource(), column_type, {}));
            }
            if (mapping.boolean_required) {
                const auto& sub_node = plan.sub_queries[i];
                if (!sub_node->has_output_types()) {
                    co_return execute_result_t{make_cursor(
                        resource(),
                        core::error_t{core::error_code_t::sql_parse_error,
                                      std::pmr::string{"argument of WHERE/HAVING must be type boolean: the sub-query's "
                                                       "output type could not be resolved from the schema",
                                                       resource()}})};
                }
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
                sub_result.captured_explain_ir->subplan_returns = static_cast<uint32_t>(mapping.id);
                captured_subplans.push_back(std::move(*sub_result.captured_explain_ir));
            }
            if (plan_only) {
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
            if (mapping.array_equality && compacted.value().is_null()) {
                const auto& sub_node = plan.sub_queries[i];
                if (!sub_node->has_output_types()) {
                    co_return execute_result_t{make_cursor(
                        resource(),
                        core::error_t{core::error_code_t::sql_parse_error,
                                      std::pmr::string{"ARRAY(SELECT ...): the sub-query's element type could "
                                                       "not be resolved from the schema",
                                                       resource()}})};
                }
                plan.parameters->set_parameter(
                    mapping.id,
                    components::types::logical_value_t::create_array(resource(), sub_node->output_types().front(), {}));
                continue;
            }
            plan.parameters->set_parameter(mapping.id, std::move(compacted.value()));
        }

        // Move-construct, not default-construct+assign — the latter risks bad_alloc.
        auto [_tb, tbf] =
            actor_zeta::otterbrix::send(parent_address_,
                                        &services::dispatcher::manager_dispatcher_t::txn_begin_session_msg,
                                        session);
        services::dispatcher::txn_session_context_t session_ctx = co_await std::move(tbf);
        components::table::transaction_data resolve_txn = session_ctx.txn;
        trace(log_,
              "executor::execute_plan_full: session txn {}, explicit: {}, session: {}",
              resolve_txn.transaction_id,
              session_ctx.is_explicit,
              session.data());

        const node_type original_type = [&] {
            auto* r = plan.sub_queries.back().get();
            return r ? r->type() : node_type::unused;
        }();

        std::pmr::string pending_set_tz_name{resource()};
        if (original_type == node_type::set_timezone_t) {
            auto* tz_node = static_cast<components::logical_plan::node_set_timezone_t*>(plan.sub_queries.back().get());
            pending_set_tz_name.assign(tz_node->timezone_name().c_str(), tz_node->timezone_name().size());
        }

        services::dispatcher::register_plan_targets(resource(), plan.sub_queries.back().get(), &plan.catalog_resolves);

        services::context_storage_t context_storage(resource(), log_.clone(), session_ctx.session_tz);

        const bool needs_ddl_txn =
            original_type == node_type::create_collection_t || original_type == node_type::create_constraint_t ||
            original_type == node_type::create_sequence_t || original_type == node_type::create_view_t ||
            original_type == node_type::create_macro_t || original_type == node_type::create_type_t ||
            original_type == node_type::create_index_t || original_type == node_type::drop_t ||
            original_type == node_type::create_database_t || original_type == node_type::alter_table_t ||
            original_type == node_type::create_matview_t;
        const bool is_plan_only_explain = plan.explain == components::logical_plan::explain_type::plan;
        const bool needs_dml_txn =
            !is_plan_only_explain && (original_type == node_type::insert_t || original_type == node_type::update_t ||
                                      original_type == node_type::delete_t);
        const bool needs_commit_txn =
            original_type == node_type::set_timezone_t || original_type == node_type::vacuum_t;

        auto run_resolve_subplan = [this, session, resolve_txn, &session_ctx, &context_storage, &plan](
                                       [[maybe_unused]] executor_t* self,
                                       std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes)
            -> executor_t::unique_future<execute_result_t> {
            auto root = boost::intrusive_ptr<components::logical_plan::node_t>(
                new components::logical_plan::node_sequence_t(resource()));
            for (auto& n : resolve_nodes) {
                root->append_child(n);
            }
            auto params = components::logical_plan::make_parameter_node(resource());
            services::context_storage_t cstor{resource(),
                                              log_.clone(),
                                              context_storage.execution_context.timezone_offset};
            cstor.catalog_resolves = &plan.catalog_resolves;
            co_return co_await this->execute_plan(session,
                                                  components::logical_plan::execution_plan_t{resource(), root, params},
                                                  std::move(cstor),
                                                  resolve_txn,
                                                  session_ctx.lowest_active_start_time,
                                                  std::pmr::vector<explain_plan_node>{resource()});
        };

        auto collect_resolve_nodes = [](const components::logical_plan::catalog_resolves_t& resolves,
                                        std::pmr::vector<components::logical_plan::node_ptr>& out) {
            for (const auto* slot :
                 {&resolves.database, &resolves.namespaces, &resolves.tables, &resolves.types, &resolves.constraints}) {
                if (*slot && !(*slot)->empty()) {
                    out.push_back(*slot);
                }
            }
        };

        {
            std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes{resource()};
            collect_resolve_nodes(plan.catalog_resolves, resolve_nodes);
            if (!resolve_nodes.empty()) {
                auto pass1_result = co_await run_resolve_subplan(this, std::move(resolve_nodes));
                if (pass1_result.cursor->is_error()) {
                    trace(log_,
                          "executor::execute_plan_full: resolve failed: {}",
                          pass1_result.cursor->get_error().what);
                    co_return execute_result_t{std::move(pass1_result.cursor)};
                }
            }
        }
        if (plan.sub_queries.back()) {
            auto* root = plan.sub_queries.back().get();
            if (auto dml_err = components::planner::reject_view_dml_target(plan.catalog_resolves, root);
                dml_err.contains_error()) {
                co_return execute_result_t{make_cursor(resource(), std::move(dml_err))};
            }
            for (std::size_t depth = 0;; ++depth) {
                auto refs = components::planner::collect_view_references(resource(), plan.catalog_resolves, root);
                if (refs.empty()) {
                    break;
                }
                if (depth >= components::planner::max_view_expansion_depth) {
                    co_return execute_result_t{make_cursor(
                        resource(),
                        core::error_t(core::error_code_t::sql_parse_error,
                                      std::pmr::string{"view expansion nesting limit exceeded", resource()}))};
                }
                // Snapshot body SQL first — merge_catalog_resolves reallocates entries; `refs` points into it.
                std::pmr::vector<std::string> body_sqls{resource()};
                body_sqls.reserve(refs.size());
                for (const auto& ref : refs) {
                    body_sqls.push_back(ref.entry->table_md->view_sql);
                }
                for (std::size_t i = 0; i < refs.size(); ++i) {
                    auto& ref = refs[i];
                    auto body = components::planner::expand_view_body(resource(), body_sqls[i]);
                    if (body.error.contains_error()) {
                        trace(log_, "executor::execute_plan_full: view expansion failed: {}", body.error.what);
                        co_return execute_result_t{make_cursor(resource(), std::move(body.error))};
                    }
                    components::planner::renumber_body_parameters(resource(),
                                                                  body.plan.get(),
                                                                  body.params,
                                                                  plan.parameters);
                    if (auto err = components::planner::splice_view_body(ref.node, std::move(body.plan));
                        err.contains_error()) {
                        co_return execute_result_t{make_cursor(resource(), std::move(err))};
                    }
                    if (body.resolves) {
                        services::dispatcher::merge_catalog_resolves(resource(), plan.catalog_resolves, *body.resolves);
                    }
                }
                if (services::catalog_resolve::has_unresolved_entries(plan.catalog_resolves)) {
                    std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes{resource()};
                    collect_resolve_nodes(plan.catalog_resolves, resolve_nodes);
                    auto pass2_result = co_await run_resolve_subplan(this, std::move(resolve_nodes));
                    if (pass2_result.cursor->is_error()) {
                        trace(log_,
                              "executor::execute_plan_full: view sub-plan resolve failed: {}",
                              pass2_result.cursor->get_error().what);
                        co_return execute_result_t{std::move(pass2_result.cursor)};
                    }
                }
            }
        }
        if (plan.sub_queries.back()) {
            services::dispatcher::bind_catalog_data(plan.sub_queries.back().get(), plan.catalog_resolves);
        }
        context_storage.catalog_resolves = &plan.catalog_resolves;

        using components::catalog::table_id;
        using components::logical_plan::constraint_kind;
        using components::logical_plan::node_create_collection_t;
        using components::logical_plan::node_create_constraint_t;
        using components::logical_plan::node_create_type_t;
        using components::types::logical_type;

        if (original_type == node_type::register_cast_t || original_type == node_type::unregister_cast_t) {
            auto* root = plan.sub_queries.back().get();
            components::types::complex_logical_type src;
            components::types::complex_logical_type tgt;
            if (original_type == node_type::register_cast_t) {
                auto* rc = static_cast<components::logical_plan::node_register_cast_t*>(root);
                src = rc->source();
                tgt = rc->target();
            } else {
                auto* uc = static_cast<components::logical_plan::node_unregister_cast_t*>(root);
                src = uc->source();
                tgt = uc->target();
            }
            services::dispatcher::resolve_one_type(src, &plan.catalog_resolves);
            services::dispatcher::resolve_one_type(tgt, &plan.catalog_resolves);
            if (src.type() == logical_type::UNKNOWN || tgt.type() == logical_type::UNKNOWN) {
                co_return execute_result_t{make_cursor(
                    resource(),
                    core::error_t{core::error_code_t::schema_error,
                                  std::pmr::string{"cast source or target type is not registered", resource()}})};
            }
            const bool exists = cast_registry_.contains(src, tgt);
            if (original_type == node_type::register_cast_t && exists) {
                co_return execute_result_t{
                    make_cursor(resource(),
                                core::error_t{core::error_code_t::schema_error,
                                              std::pmr::string{"cast is already registered", resource()}})};
            }
            if (original_type == node_type::unregister_cast_t && !exists) {
                co_return execute_result_t{
                    make_cursor(resource(),
                                core::error_t{core::error_code_t::schema_error,
                                              std::pmr::string{"cast is not registered", resource()}})};
            }
            execute_result_t ok{make_cursor(resource())};
            ok.resolved_cast = std::make_pair(std::move(src), std::move(tgt));
            co_return ok;
        }

        auto build_id_cfn = [](const components::logical_plan::node_t* n) -> qualified_name_t {
            using components::logical_plan::node_alter_table_t;
            using components::logical_plan::node_create_database_t;
            using components::logical_plan::node_create_index_t;
            using components::logical_plan::node_create_macro_t;
            using components::logical_plan::node_create_sequence_t;
            using components::logical_plan::node_create_view_t;
            using components::logical_plan::node_delete_t;
            using components::logical_plan::node_insert_t;
            using components::logical_plan::node_update_t;
            if (!n)
                return {};
            switch (n->type()) {
                case node_type::aggregate_t: {
                    auto* d = static_cast<const node_aggregate_t*>(n);
                    return qualified_name_t{static_cast<const std::string&>(d->dbname()),
                                            static_cast<const std::string&>(d->relname())};
                }
                case node_type::alter_column_t:
                    return {};
                case node_type::alter_table_t: {
                    auto* d = static_cast<const node_alter_table_t*>(n);
                    return qualified_name_t{d->dbname(), d->relname()};
                }
                case node_type::create_collection_t: {
                    auto* d = static_cast<const node_create_collection_t*>(n);
                    return qualified_name_t{d->dbname(), static_cast<const std::string&>(d->relname())};
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
                    auto* d = static_cast<const node_create_index_t*>(n);
                    return qualified_name_t{d->dbname(), d->relname()};
                }
                case node_type::create_macro_t: {
                    auto* d = static_cast<const node_create_macro_t*>(n);
                    return qualified_name_t{d->dbname(), d->macroname()};
                }
                case node_type::create_sequence_t: {
                    auto* d = static_cast<const node_create_sequence_t*>(n);
                    return qualified_name_t{d->dbname(), d->seqname()};
                }
                case node_type::create_view_t: {
                    auto* d = static_cast<const node_create_view_t*>(n);
                    return qualified_name_t{d->dbname(), d->viewname()};
                }
                case node_type::delete_t: {
                    auto* d = static_cast<const node_delete_t*>(n);
                    return qualified_name_t{d->dbname(), d->relname()};
                }
                case node_type::insert_t: {
                    auto* d = static_cast<const node_insert_t*>(n);
                    return qualified_name_t{d->dbname(), d->relname()};
                }
                case node_type::update_t: {
                    auto* d = static_cast<const node_update_t*>(n);
                    return qualified_name_t{d->dbname(), d->relname()};
                }
                case node_type::drop_t: {
                    using components::logical_plan::drop_target_kind;
                    using components::logical_plan::node_drop_t;
                    auto* d = static_cast<const node_drop_t*>(n);
                    if (d->kind() == drop_target_kind::type) {
                        return {};
                    }
                    if (d->kind() == drop_target_kind::database) {
                        return qualified_name_t{d->dbname(), std::string{}};
                    }
                    return qualified_name_t{d->dbname(), d->relname()};
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

        table_id id(resource(), build_id_cfn(plan.sub_queries.back().get()));
        cursor_t_ptr error;
        switch (original_type) {
            case node_type::create_database_t:
                if (!services::dispatcher::check_namespace_exists(resource(), &plan.catalog_resolves, id)
                         .contains_error()) {
                    auto* d = static_cast<const node_create_database_t*>(plan.sub_queries.back().get());
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
                if (!services::dispatcher::check_collection_exists(resource(), &plan.catalog_resolves, id)
                         .contains_error()) {
                    auto* cc = static_cast<const node_create_collection_t*>(plan.sub_queries.back().get());
                    if (cc && cc->if_not_exists()) {
                        error = make_cursor(resource());
                    } else {
                        error = make_cursor(resource(),
                                            core::error_t{core::error_code_t::table_already_exists,
                                                          std::pmr::string{"collection already exists", resource()}});
                    }
                } else {
                    const std::string target_db{id.database()};
                    const auto str_path = services::catalog_resolve::build_type_search_path_str(target_db);
                    auto* n = static_cast<node_create_collection_t*>(plan.sub_queries.back().get());
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
                                                                            &plan.catalog_resolves,
                                                                            col_def.type().type_name(),
                                                                            std::span<const std::string>(str_path));
                                err.contains_error()) {
                                error = make_cursor(resource(), err);
                            }
                            if (!error) {
                                const auto* md = services::catalog_resolve::probe_type_in_path(
                                    plan.catalog_resolves,
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
                    if (!error) {
                        for (const auto& col_def : n->column_definitions()) {
                            if (auto type_err =
                                    services::dispatcher::gate_persistable_type(resource(),
                                                                                "column '" + col_def.name() + "'",
                                                                                col_def.type());
                                type_err.contains_error()) {
                                error = make_cursor(resource(), type_err);
                                break;
                            }
                        }
                    }
                    if (!error) {
                        if (auto default_err =
                                services::dispatcher::convert_column_defaults(resource(),
                                                                              &cast_registry_,
                                                                              context_storage.execution_context,
                                                                              n->column_definitions());
                            default_err.contains_error()) {
                            error = make_cursor(resource(), default_err);
                        }
                    }
                }
                break;
            }
            case node_type::create_type_t: {
                auto* n = static_cast<node_create_type_t*>(plan.sub_queries.back().get());
                components::catalog::oid_t target_ns = components::catalog::well_known_oid::public_namespace;
                const std::string default_path[] = {"public", "pg_catalog"};
                std::span<const std::string> str_path(default_path);
                if (!services::dispatcher::check_type_exists(resource(),
                                                             &plan.catalog_resolves,
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
                                                                                   &plan.catalog_resolves,
                                                                                   field.type_name(),
                                                                                   str_path);
                                err.contains_error()) {
                                error = make_cursor(resource(), err);
                                break;
                            }
                            const auto* md =
                                services::catalog_resolve::probe_type_in_path(plan.catalog_resolves,
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
                if (auto type_err = services::dispatcher::gate_persistable_type(resource(),
                                                                                "type '" + n->type().type_name() + "'",
                                                                                n->type());
                    type_err.contains_error()) {
                    error = make_cursor(resource(), type_err);
                    break;
                }
                n->set_namespace_oid(target_ns);
                break;
            }
            case node_type::drop_t: {
                using components::logical_plan::drop_target_kind;
                using components::logical_plan::node_drop_t;
                const auto* drop_node = static_cast<const node_drop_t*>(plan.sub_queries.back().get());
                switch (drop_node->kind()) {
                    case drop_target_kind::database:
                        if (auto err =
                                services::dispatcher::check_namespace_exists(resource(), &plan.catalog_resolves, id);
                            err.contains_error()) {
                            error = make_cursor(resource(), err);
                        } else if (components::catalog::is_catalog_table(drop_node->namespace_oid())) {
                            error = make_cursor(
                                resource(),
                                core::error_t{core::error_code_t::sql_parse_error,
                                              std::pmr::string{"cannot drop a built-in namespace: the database "
                                                               "system requires it",
                                                               resource()}});
                        }
                        break;
                    case drop_target_kind::collection:
                        if (auto err =
                                services::dispatcher::check_collection_exists(resource(), &plan.catalog_resolves, id);
                            err.contains_error()) {
                            error = make_cursor(resource(), err);
                        } else if (components::catalog::is_catalog_table(drop_node->table_oid())) {
                            // Same rule as the DML arms — DDL never touches a system catalog (orphans storage).
                            error = make_cursor(
                                resource(),
                                core::error_t{core::error_code_t::sql_parse_error,
                                              std::pmr::string{"cannot drop a system catalog table", resource()}});
                        }
                        break;
                    case drop_target_kind::type: {
                        const std::string& type_name = drop_node->relname();
                        const std::string default_path[] = {"public", "pg_catalog"};
                        std::span<const std::string> str_path(default_path);
                        if (auto err = services::dispatcher::check_type_exists(resource(),
                                                                               &plan.catalog_resolves,
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
                        break;
                    case drop_target_kind::index: {
                        auto vt_err = services::dispatcher::validate_types(resource(),
                                                                           &plan.catalog_resolves,
                                                                           plan.sub_queries.back().get(),
                                                                           context_storage.execution_context);
                        if (vt_err.contains_error()) {
                            error = make_cursor(resource(), vt_err);
                        } else {
                            services::dispatcher::validation::validation_context_t validation_context{
                                resource(),
                                &plan.catalog_resolves,
                                cast_registry_,
                                function_registry_,
                                context_storage.execution_context};
                            auto schema_res = services::dispatcher::validate_schema(validation_context,
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
            // Leaf control nodes like checkpoint/vacuum — omitting this hits validate_schema's default assert(false).
            case node_type::transaction_t:
            case node_type::create_sequence_t:
            case node_type::create_view_t:
            case node_type::create_macro_t:
                break;
            case node_type::alter_table_t: {
                const auto* alter_node =
                    static_cast<const components::logical_plan::node_alter_table_t*>(plan.sub_queries.back().get());
                if (components::catalog::is_catalog_table(alter_node->table_oid())) {
                    // System catalog shape is fixed at bootstrap; altering it desyncs positional column readers.
                    error =
                        make_cursor(resource(),
                                    core::error_t{core::error_code_t::sql_parse_error,
                                                  std::pmr::string{"cannot alter a system catalog table", resource()}});
                    break;
                }
                for (const auto& cmd : alter_node->subcommands()) {
                    if (cmd.kind != components::logical_plan::alter_table_kind::add_column) {
                        continue;
                    }
                    if (auto type_err =
                            services::dispatcher::gate_persistable_type(resource(),
                                                                        "column '" + cmd.column.name() + "'",
                                                                        cmd.column.type());
                        type_err.contains_error()) {
                        error = make_cursor(resource(), type_err);
                        break;
                    }
                }
                break;
            }
            case node_type::create_constraint_t: {
                if (auto err = services::dispatcher::check_collection_exists(resource(), &plan.catalog_resolves, id);
                    err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                if (!error && !id.database().empty()) {
                    auto* cstr = static_cast<node_create_constraint_t*>(plan.sub_queries.back().get());
                    const bool key_kind =
                        cstr->kind() == constraint_kind::unique || cstr->kind() == constraint_kind::primary_key;
                    if (cstr->kind() == constraint_kind::foreign_key || cstr->kind() == constraint_kind::check ||
                        key_kind) {
                        const auto* tbl_local =
                            plan.catalog_resolves.table_md(id.database(), std::string_view(id.table_name()));
                        const bool local_is_g = tbl_local && tbl_local->relkind == 'g';
                        bool ref_is_g = false;
                        if (cstr->kind() == constraint_kind::foreign_key &&
                            cstr->ref_table_oid() != components::catalog::INVALID_OID) {
                            const auto* tbl_ref = plan.catalog_resolves.table_md(cstr->ref_table_oid());
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
                        } else if (key_kind && local_is_g) {
                            error = make_cursor(
                                resource(),
                                core::error_t{
                                    core::error_code_t::schema_error,
                                    std::pmr::string{
                                        "UNIQUE / PRIMARY KEY constraints are not supported on dynamic-schema "
                                        "(relkind='g') tables. Key enforcement requires stable column attoids; "
                                        "dynamic-schema columns live in pg_computed_column and may evolve. "
                                        "Convert the table to static schema first.",
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
                        if (!error && cstr->kind() == constraint_kind::check) {
                            services::dispatcher::validation::validation_context_t validation_context{
                                resource(),
                                &plan.catalog_resolves,
                                cast_registry_,
                                function_registry_,
                                context_storage.execution_context};
                            auto schema_res = services::dispatcher::validate_schema(validation_context,
                                                                                    plan.sub_queries.back().get(),
                                                                                    plan.parameters->parameters());
                            if (schema_res.has_error()) {
                                error = make_cursor(resource(), schema_res.error());
                            }
                        }
                    }
                }
                break;
            }
            case node_type::create_index_t: {
                const auto* index_node =
                    static_cast<const components::logical_plan::node_create_index_t*>(plan.sub_queries.back().get());
                if (components::catalog::is_catalog_table(index_node->table_oid())) {
                    error =
                        make_cursor(resource(),
                                    core::error_t{core::error_code_t::sql_parse_error,
                                                  std::pmr::string{"cannot create an index on a system catalog table",
                                                                   resource()}});
                    break;
                }
                [[fallthrough]];
            }
            default: {
                services::dispatcher::resolve_expression_types(plan.sub_queries.back(), &plan.catalog_resolves);
                auto vt_err = services::dispatcher::validate_types(resource(),
                                                                   &plan.catalog_resolves,
                                                                   plan.sub_queries.back().get(),
                                                                   context_storage.execution_context);
                if (vt_err.contains_error()) {
                    error = make_cursor(resource(), vt_err);
                } else {
                    const auto& bound_params = plan.parameters->parameters();
                    components::logical_plan::storage_parameters validate_params(resource());
                    bool overridden = false;
                    for (size_t i = 0; i + 1 < plan.sub_queries.size(); ++i) {
                        const auto& m = plan.sub_query_results[i];
                        if (m.compacter != &components::vector::compact_to_single_value) {
                            continue;
                        }
                        auto it = bound_params.parameters.find(m.id);
                        if (it == bound_params.parameters.end() || !it->second.is_null() ||
                            !plan.sub_queries[i]->has_output_types()) {
                            continue;
                        }
                        if (!overridden) {
                            validate_params.parameters = bound_params.parameters;
                            overridden = true;
                        }
                        validate_params.parameters.find(m.id)->second =
                            components::types::logical_value_t(resource(), plan.sub_queries[i]->output_types().front());
                    }
                    services::dispatcher::validation::validation_context_t validation_context{
                        resource(),
                        &plan.catalog_resolves,
                        cast_registry_,
                        function_registry_,
                        context_storage.execution_context};
                    auto schema_res =
                        services::dispatcher::validate_schema(validation_context,
                                                              plan.sub_queries.back().get(),
                                                              overridden ? validate_params : bound_params);
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

        components::catalog::oid_t create_index_table_oid = components::catalog::INVALID_OID;
        components::catalog::oid_t create_index_oid = components::catalog::INVALID_OID;

        {
            components::execution_context_t enrich_ctx{session,
                                                       resolve_txn,
                                                       context_storage.execution_context.timezone_offset};
            auto ef = services::dispatcher::enrich_plan(resource(),
                                                        plan.sub_queries.back(),
                                                        enrich_ctx,
                                                        &plan.catalog_resolves,
                                                        index_address_,
                                                        &context_storage);
            auto enrich_err = co_await std::move(ef);
            if (enrich_err.contains_error()) {
                trace(log_, "executor::execute_plan_full: enrich error: {}", enrich_err.what);
                co_return execute_result_t{make_cursor(resource(), std::move(enrich_err))};
            }
            {
                services::dispatcher::validation::validation_context_t constraint_context{
                    resource(),
                    &plan.catalog_resolves,
                    cast_registry_,
                    function_registry_,
                    context_storage.execution_context};
                if (auto bind_err = services::dispatcher::resolve_constraint_predicates(constraint_context,
                                                                                        plan.sub_queries.back().get(),
                                                                                        plan.parameters->parameters());
                    bind_err.contains_error()) {
                    co_return execute_result_t{make_cursor(resource(), std::move(bind_err))};
                }
            }
            components::planner::planner_t planner;
            plan.sub_queries.back() = planner.create_plan(resource(), std::move(plan.sub_queries.back()));

            // Re-populate known_oids/table_metadata from the freshly-stamped tree (captured before the executor's
            // own resolve ran) — without it, create_plan_match SEGFAULTs on a scan-less match.
            {
                auto dependency_oids = plan.sub_queries.back()->table_oid_dependencies();
                for (auto oid : dependency_oids) {
                    context_storage.known_oids.insert(oid);
                }
                if (plan.catalog_resolves.tables) {
                    for (const auto& entry : plan.catalog_resolves.tables->entries()) {
                        if (entry.table_md.has_value()) {
                            context_storage.table_metadata[entry.table_md->table_oid] = &entry.table_md.value();
                        }
                    }
                }
            }

            // DDL OID-batch allocation via node_allocate_oids_t; `self` for the same reason as run_resolve_subplan.
            // Must not `co_return {}` on failure — that reads as success and stamps a garbage identity.
            auto allocate_oids_inline = [this, session, &context_storage]([[maybe_unused]] executor_t* self,
                                                                          std::size_t count)
                -> executor_t::unique_future<core::result_wrapper_t<std::vector<components::catalog::oid_t>>> {
                auto node = components::logical_plan::make_node_allocate_oids(resource(), count);
                components::compute::function_registry_t local_fn_registry{resource()};
                services::context_storage_t cstor{resource(),
                                                  log_.clone(),
                                                  context_storage.execution_context.timezone_offset};
                auto op = services::planner::create_plan(cstor,
                                                         local_fn_registry,
                                                         node,
                                                         components::logical_plan::limit_t::unlimit(),
                                                         /*params=*/nullptr);
                if (!op) {
                    co_return core::result_wrapper_t<std::vector<components::catalog::oid_t>>{
                        core::error_t{core::error_code_t::create_physical_plan_error,
                                      std::pmr::string{"OID allocation round: no physical plan for "
                                                       "node_allocate_oids_t",
                                                       resource()}}};
                }
                op->set_as_root();
                components::logical_plan::storage_parameters local_params(resource());
                components::pipeline::context_t pctx{session,
                                                     actor_zeta::address_t::empty_address(),
                                                     actor_zeta::address_t::empty_address(),
                                                     &local_fn_registry,
                                                     local_params,
                                                     disk_address_,
                                                     index_address_,
                                                     wal_address_};
                pctx.txn = components::table::transaction_data{0, 0};
                op->prepare();
                auto drive_err = co_await drive_subplan_(op, &pctx);
                if (drive_err.contains_error()) {
                    co_return core::result_wrapper_t<std::vector<components::catalog::oid_t>>{std::move(drive_err)};
                }
                if (pctx.has_pending_disk_futures()) {
                    auto futures = pctx.take_pending_disk_futures();
                    for (auto& f : futures) co_await std::move(f);
                }
                auto allocated = node->oids();
#ifdef DEV_MODE
                // The one consultation of the OID-allocation fault seam (see executor.hpp); off unless a test armed it.
                if (auto* interposer = dev_oid_alloc_interposer(); interposer != nullptr) {
                    allocated = interposer->substitute(count, std::move(allocated));
                }
#endif
                co_return core::result_wrapper_t<std::vector<components::catalog::oid_t>>{std::move(allocated)};
            };

            using components::catalog::relkind::computed;
            using components::logical_plan::node_sequence_t;

            if (original_type == node_type::insert_t) {
                components::catalog::oid_t resolved_tbl_oid = components::catalog::INVALID_OID;
                bool is_computing = false;
                auto* effective_insert_node = plan.sub_queries.back().get();
                auto enriched_oid =
                    effective_insert_node ? effective_insert_node->table_oid() : plan.sub_queries.back()->table_oid();
                if (enriched_oid == components::catalog::INVALID_OID && !plan.sub_queries.back()->children().empty()) {
                    enriched_oid = plan.sub_queries.back()->children().front()->table_oid();
                }
                if (enriched_oid != components::catalog::INVALID_OID) {
                    if (const auto* tbl = plan.catalog_resolves.table_md(enriched_oid)) {
                        if (tbl->relkind == computed) {
                            is_computing = true;
                            resolved_tbl_oid = tbl->table_oid;
                        }
                    }
                }

                if (is_computing) {
                    std::pmr::vector<components::table::column_definition_t> registered_cols(resource());
                    auto* effective_insert = plan.sub_queries.back().get();
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
                        if (registered_cols.empty()) {
                            const components::logical_plan::node_t* select_child = nullptr;
                            for (const auto& child : effective_insert->children()) {
                                if (child && child->type() != components::logical_plan::node_type::data_t) {
                                    select_child = child.get();
                                    break;
                                }
                            }
                            if (select_child) {
                                if (!select_child->has_output_types()) {
                                    co_return execute_result_t{make_cursor(
                                        resource(),
                                        core::error_t{
                                            core::error_code_t::schema_error,
                                            std::pmr::string{"INSERT ... SELECT into a dynamic-schema table: the "
                                                             "source schema could not be resolved, so the written "
                                                             "columns cannot be registered in the catalog",
                                                             resource()}})};
                                }
                                registered_cols.reserve(select_child->output_types().size());
                                for (const auto& type : select_child->output_types()) {
                                    if (!type.has_alias()) {
                                        co_return execute_result_t{make_cursor(
                                            resource(),
                                            core::error_t{
                                                core::error_code_t::schema_error,
                                                std::pmr::string{"INSERT ... SELECT into a dynamic-schema table: "
                                                                 "every source column needs a name to register; "
                                                                 "alias the expression (AS <name>)",
                                                                 resource()}})};
                                    }
                                    registered_cols.emplace_back(type.alias(), type);
                                }
                            }
                        }
                    }

                    // node_alter_column_t(op=add, computed=true) carries registered_cols to computed_field_register.
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
            const bool has_disk = disk_address_ != actor_zeta::address_t::empty_address();
            const bool is_drop_index = [&] {
                if (original_type != node_type::drop_t) {
                    return false;
                }
                const auto* dn =
                    static_cast<const components::logical_plan::node_drop_t*>(plan.sub_queries.back().get());
                return dn && dn->kind() == components::logical_plan::drop_target_kind::index;
            }();
            if (is_ddl_oid_rewrite(original_type) && (is_drop_index || has_disk)) {
                auto* eff = plan.sub_queries.back().get();

                if (original_type == node_type::create_constraint_t) {
                    auto* cstr = static_cast<node_create_constraint_t*>(eff);
                    if (cstr->kind() == constraint_kind::check && cstr->check_expression_sql().empty()) {
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
                std::vector<components::catalog::oid_t> allocated_oids;
                if (need > 0) {
                    auto allocated = co_await allocate_oids_inline(this, need);
                    if (allocated.has_error()) {
                        // Refuse the statement — else a catalog row is stamped with an identity nothing allocated.
                        co_return execute_result_t{make_cursor(resource(), allocated.error())};
                    }
                    allocated_oids = std::move(allocated.value());
                }
                components::planner::planner_t ddl_planner;
                auto rewritten = ddl_planner.create_plan(resource(),
                                                         std::move(plan.sub_queries.back()),
                                                         std::move(allocated_oids),
                                                         need);
                if (rewritten.has_error()) {
                    co_return execute_result_t{make_cursor(resource(), rewritten.error())};
                }
                plan.sub_queries.back() = std::move(rewritten.value());

                if (original_type == node_type::create_index_t) {
                    if (auto* eff2 = plan.sub_queries.back().get(); eff2 && !eff2->children().empty()) {
                        auto* back = eff2->children().back().get();
                        if (back && back->type() == node_type::create_index_t) {
                            const auto* ci = static_cast<const components::logical_plan::node_create_index_t*>(back);
                            create_index_table_oid = ci->table_oid();
                            create_index_oid = ci->index_oid();
                        }
                    }
                } else if (original_type == node_type::alter_table_t) {
                    {
                        std::pmr::vector<components::logical_plan::node_t*> pending{resource()};
                        std::pmr::vector<components::logical_plan::node_alter_column_t*> add_nodes{resource()};
                        std::vector<components::table::column_definition_t> add_columns;
                        pending.push_back(plan.sub_queries.back().get());
                        while (!pending.empty()) {
                            auto* pending_node = pending.back();
                            pending.pop_back();
                            if (!pending_node) {
                                continue;
                            }
                            if (pending_node->type() == node_type::alter_column_t) {
                                auto* alter_column =
                                    static_cast<components::logical_plan::node_alter_column_t*>(pending_node);
                                if (alter_column->op() == components::logical_plan::alter_column_op::add &&
                                    alter_column->column().has_default_value()) {
                                    add_nodes.push_back(alter_column);
                                    add_columns.push_back(alter_column->column());
                                }
                                continue;
                            }
                            for (const auto& child : pending_node->children()) {
                                pending.push_back(child.get());
                            }
                        }
                        if (!add_columns.empty()) {
                            if (auto default_err =
                                    services::dispatcher::convert_column_defaults(resource(),
                                                                                  &cast_registry_,
                                                                                  context_storage.execution_context,
                                                                                  add_columns);
                                default_err.contains_error()) {
                                co_return execute_result_t{make_cursor(resource(), std::move(default_err))};
                            }
                            for (std::size_t i = 0; i < add_nodes.size(); ++i) {
                                add_nodes[i]->set_column(std::move(add_columns[i]));
                            }
                        }
                    }
                    components::execution_context_t enriched_ctx{session,
                                                                 resolve_txn,
                                                                 context_storage.execution_context.timezone_offset};
                    auto ef2 = services::dispatcher::enrich_plan(resource(),
                                                                 plan.sub_queries.back(),
                                                                 enriched_ctx,
                                                                 &plan.catalog_resolves,
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
        if (original_type == node_type::alter_table_t && plan.sub_queries.back() &&
            plan.sub_queries.back()->type() == node_type::alter_table_t) {
            const auto* alter_node =
                static_cast<const components::logical_plan::node_alter_table_t*>(plan.sub_queries.back().get());
            std::pmr::string msg{resource()};
            msg.append("ALTER TABLE: relation \"");
            if (!alter_node->dbname().empty()) {
                msg.append(alter_node->dbname().data(), alter_node->dbname().size());
                msg.push_back('.');
            }
            msg.append(alter_node->relname().data(), alter_node->relname().size());
            msg.append("\" does not exist");
            co_return execute_result_t{
                make_cursor(resource(), core::error_t{core::error_code_t::table_not_exists, std::move(msg)})};
        }

        const bool can_push_to_agent = disk_address_ != actor_zeta::address_t::empty_address();
        plan.sub_queries.back() = components::planner::optimize(resource(),
                                                                std::move(plan.sub_queries.back()),
                                                                plan.parameters.get(),
                                                                &plan.catalog_resolves,
                                                                can_push_to_agent,
                                                                optimizer_pass_);

        if (can_push_to_agent) {
            std::pmr::set<components::catalog::oid_t> inner_hash_join_oids{resource()};
            collect_inner_hash_join_oids(plan.sub_queries.back(), inner_hash_join_oids);
            for (auto oid : inner_hash_join_oids) {
                auto [_tr, trf] = actor_zeta::otterbrix::send(disk_address_,
                                                              &services::disk::manager_disk_t::storage_total_rows,
                                                              session,
                                                              oid);
                auto rows_r = co_await std::move(trf);
                if (!rows_r.has_error()) {
                    context_storage.row_counts[oid] = rows_r.value();
                }
            }
        }

        trace(log_, "executor::execute_plan_full: delegating to execute_plan, session: {}", session.data());
        auto exec_result = co_await execute_plan(session,
                                                 plan,
                                                 std::move(context_storage),
                                                 resolve_txn,
                                                 session_ctx.lowest_active_start_time,
                                                 std::move(captured_subplans));

        auto revert_failed_txn = [this, session, resolve_txn, &session_ctx](
                                     [[maybe_unused]] executor_t* self,
                                     execute_result_t& exec_result) -> executor_t::unique_future<void> {
            std::vector<components::pg_catalog_append_range_t> revert_ranges;
            revert_ranges.reserve(exec_result.dml_appends.size() + exec_result.pg_catalog_appends.size());
            for (const auto& app : exec_result.dml_appends) {
                revert_ranges.push_back(
                    components::pg_catalog_append_range_t{app.table_oid, app.row_start, app.row_count});
#ifdef DEV_MODE
                g_dml_appends_reverted.fetch_add(1, std::memory_order_relaxed);
#endif
            }
            for (auto& pgc : exec_result.pg_catalog_appends) {
                revert_ranges.push_back(std::move(pgc));
            }
            exec_result.pg_catalog_appends.clear();
            if (!revert_ranges.empty()) {
                components::execution_context_t pgc_ctx{session, resolve_txn, {}};
                auto [_pa, paf] = actor_zeta::otterbrix::send(disk_address_,
                                                              &services::disk::manager_disk_t::storage_revert_appends,
                                                              pgc_ctx,
                                                              std::move(revert_ranges),
                                                              /*tail_only=*/false);
                if (const auto reverted = co_await std::move(paf); reverted.contains_error()) {
                    ::error(log_, "executor: pg_catalog append rollback did not complete: {}", reverted.what);
                }
            }

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
                    auto [_ri, rif] = actor_zeta::otterbrix::send(index_address_,
                                                                  &services::index::manager_index_t::revert_insert,
                                                                  abort_ctx,
                                                                  oid);
                    revert_index_futures.push_back(std::move(rif));
                }
                for (auto oid : revert_delete_oids) {
                    components::execution_context_t abort_ctx{session, resolve_txn, session_ctx.session_tz, oid};
                    auto [_rd, rdf] = actor_zeta::otterbrix::send(index_address_,
                                                                  &services::index::manager_index_t::revert_delete,
                                                                  abort_ctx,
                                                                  oid);
                    revert_index_futures.push_back(std::move(rdf));
                }
                for (auto& rif : revert_index_futures) {
                    co_await std::move(rif);
                }
            }

            // Without this storage_revert_deletes (mirrors operator_abort_transaction), the next UPDATE's delete silently no-ops.
            if (resolve_txn.transaction_id != 0 &&
                (!exec_result.dml_deletes.empty() || !exec_result.pg_catalog_delete_tables.empty())) {
                std::set<components::catalog::oid_t> revert_set{exec_result.pg_catalog_delete_tables.begin(),
                                                                exec_result.pg_catalog_delete_tables.end()};
                for (const auto& del : exec_result.dml_deletes) {
                    revert_set.insert(del.table_oid);
                }
                std::vector<components::catalog::oid_t> revert_delete_tables{revert_set.begin(), revert_set.end()};
                components::execution_context_t rd_ctx{session, resolve_txn, session_ctx.session_tz};
                auto [_rd, rdf] = actor_zeta::otterbrix::send(disk_address_,
                                                              &services::disk::manager_disk_t::storage_revert_deletes,
                                                              rd_ctx,
                                                              std::move(revert_delete_tables));
                co_await std::move(rdf);
            }
            exec_result.pg_catalog_delete_tables.clear();

            auto [_ab, abf] = actor_zeta::otterbrix::send(parent_address_,
                                                          &services::dispatcher::manager_dispatcher_t::txn_abort_msg,
                                                          session);
            co_await std::move(abf);

            exec_result.dml_appends.clear();
            exec_result.dml_deletes.clear();
        };

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
                    auto [_ac, acf] =
                        actor_zeta::otterbrix::send(parent_address_,
                                                    &services::dispatcher::manager_dispatcher_t::txn_accumulate_msg,
                                                    session,
                                                    std::move(payload));
                    // transaction_inactive means the ranges were parked NOWHERE — swallowing it fakes success.
                    auto accumulate_err = co_await std::move(acf);
                    if (accumulate_err.contains_error()) {
                        exec_result.cursor = make_cursor(resource(), std::move(accumulate_err));
                        // NOT a no-op: base_appends/base_deletes were copied (not moved), so this un-appends the rows.
                        co_await revert_failed_txn(this, exec_result);
                    }
                }
                exec_result.dml_appends.clear();
                exec_result.dml_deletes.clear();
                exec_result.pg_catalog_appends.clear();
                exec_result.pg_catalog_delete_tables.clear();
                exec_result.pg_attribute_commit_id_backfills.clear();
                exec_result.dropped_storage_oids.clear();
                exec_result.created_storage_oids.clear();
                exec_result.created_indexes.clear();

                if (!session_ctx.is_explicit && exec_result.cursor->is_success()) {
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
                co_await revert_failed_txn(this, exec_result);
            }
        }

        if (needs_ddl_txn && exec_result.cursor->is_success()) {
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
                payload.dropped_storage_oids = std::move(exec_result.dropped_storage_oids);
                payload.created_storage_oids = std::move(exec_result.created_storage_oids);
                payload.created_indexes = std::move(exec_result.created_indexes);
                auto [_ac, acf] =
                    actor_zeta::otterbrix::send(parent_address_,
                                                &services::dispatcher::manager_dispatcher_t::txn_accumulate_msg,
                                                session,
                                                std::move(payload));
                auto accumulate_err = co_await std::move(acf);
                if (accumulate_err.contains_error()) {
                    exec_result.cursor = make_cursor(resource(), std::move(accumulate_err));
                }
                exec_result.pg_catalog_appends.clear();
                exec_result.pg_catalog_delete_tables.clear();
                exec_result.pg_attribute_commit_id_backfills.clear();
                exec_result.dropped_storage_oids.clear();
                exec_result.created_storage_oids.clear();
                exec_result.created_indexes.clear();
            }

            exec_result.dml_appends.clear();
            exec_result.dml_deletes.clear();

            auto undo_create_index =
                [this, session, resolve_txn, &create_index_pg_index_range, &has_create_index_pg_index_range](
                    [[maybe_unused]] executor_t* self,
                    components::catalog::oid_t table_oid,
                    components::catalog::oid_t index_oid) -> executor_t::unique_future<void> {
                if (has_create_index_pg_index_range) {
                    std::vector<components::pg_catalog_append_range_t> revert_ranges;
                    revert_ranges.push_back(create_index_pg_index_range);
                    components::execution_context_t rv_ctx{session, resolve_txn, {}};
                    auto [_rv, rvf] =
                        actor_zeta::otterbrix::send(disk_address_,
                                                    &services::disk::manager_disk_t::storage_revert_appends,
                                                    rv_ctx,
                                                    std::move(revert_ranges),
                                                    /*tail_only=*/false);
                    if (const auto reverted = co_await std::move(rvf); reverted.contains_error()) {
                        ::error(log_, "executor: pg_index append rollback did not complete: {}", reverted.what);
                    }
                }
                if (table_oid != components::catalog::INVALID_OID &&
                    index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_di, dif] = actor_zeta::otterbrix::send(index_address_,
                                                                  &services::index::manager_index_t::drop_index,
                                                                  session,
                                                                  table_oid,
                                                                  index_oid);
                    co_await std::move(dif);
                }
                co_return;
            };

            if (exec_result.cursor->is_error() && original_type == node_type::create_index_t) {
                co_await undo_create_index(this, create_index_table_oid, create_index_oid);
            }

            if (!session_ctx.is_explicit && exec_result.cursor->is_success()) {
                auto commit_result = co_await run_commit_pipeline_(session,
                                                                   resolve_txn,
                                                                   session_ctx.session_tz,
                                                                   session_ctx.lowest_active_start_time,
                                                                   /*ddl_mode=*/true);
                if (commit_result.cursor->is_error()) {
                    exec_result.cursor = std::move(commit_result.cursor);
                    if (original_type == node_type::create_index_t) {
                        co_await undo_create_index(this, create_index_table_oid, create_index_oid);
                    }
                }
                if (commit_result.commit_id > 0 && original_type == node_type::create_index_t &&
                    index_address_ != actor_zeta::address_t::empty_address()) {
                    trace(log_,
                          "executor::execute_plan_full: CREATE INDEX backfill commit — oid={}, commit_id={}",
                          static_cast<unsigned>(create_index_table_oid),
                          commit_result.commit_id);
                    if (create_index_table_oid != components::catalog::INVALID_OID) {
                        components::execution_context_t swap_ctx{session, resolve_txn, {}};
                        std::pmr::vector<components::catalog::oid_t> commit_oids{resource()};
                        commit_oids.push_back(create_index_table_oid);
                        auto [_ci, cif] = actor_zeta::otterbrix::send(index_address_,
                                                                      &services::index::manager_index_t::commit_inserts,
                                                                      swap_ctx,
                                                                      std::move(commit_oids),
                                                                      commit_result.commit_id);
                        auto ci_result = co_await std::move(cif);
                        if (ci_result.contains_error()) {
                            exec_result.cursor = make_cursor(resource(), ci_result);
                            co_await undo_create_index(this, create_index_table_oid, create_index_oid);
                        }
                    }
                }
            }
        } else if (needs_ddl_txn && exec_result.cursor->is_error()) {
            trace(log_,
                  "executor::execute_plan_full: DDL failed — reverting txn {}, session: {}",
                  resolve_txn.transaction_id,
                  session.data());

            co_await revert_failed_txn(this, exec_result);
        }

        if (original_type == node_type::set_timezone_t && exec_result.cursor->is_success() &&
            !pending_set_tz_name.empty()) {
            exec_result.applied_timezone.assign(pending_set_tz_name.data(), pending_set_tz_name.size());
        }

        // Must release the resolve-scope txn here, or it pins lowest_active forever.
        const bool releases_resolve_txn = !needs_ddl_txn && !needs_dml_txn && !needs_commit_txn &&
                                          !session_ctx.is_explicit && original_type != node_type::transaction_t;
        if (releases_resolve_txn) {
            auto [_rl, rlf] = actor_zeta::otterbrix::send(parent_address_,
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
        for (const auto& [registered_name, uid] : function_registry_.get_functions()) {
            if (registered_name != name) {
                continue;
            }
            const auto* registered = function_registry_.get_function(uid);
            if (registered == nullptr ||
                components::compute::check_signature_conflicts(registered->get_signatures(), signatures)) {
                continue;
            }
            co_return std::make_unique<function_result_t>(core::error_t(
                core::error_code_t::function_registry_error,
                std::pmr::string{"function '" + name + "' is already registered with this signature", resource()}));
        }
        auto res = function_registry_.add_function(std::move(function));
        co_return std::make_unique<function_result_t>(std::move(res));
    }

    executor_t::unique_future<bool>
    executor_t::unregister_udf(components::session::session_id_t session,
                               std::string name,
                               std::pmr::vector<components::types::complex_logical_type> inputs) {
        trace(log_, "executor::unregister_udf, session: {}, {}", session.data(), name);
        co_return function_registry_.remove_function_by_signature(name, inputs);
    }

    executor_t::unique_future<bool> executor_t::unregister_udf_uid(components::session::session_id_t session,
                                                                   components::compute::function_uid uid) {
        trace(log_, "executor::unregister_udf_uid, session: {}, uid: {}", session.data(), uid);
        co_return function_registry_.remove_function(uid);
    }

    executor_t::unique_future<bool> executor_t::register_cast(components::session::session_id_t session,
                                                              components::types::complex_logical_type source,
                                                              components::types::complex_logical_type target,
                                                              components::casts::cast_entry entry) {
        trace(log_, "executor::register_cast, session: {}", session.data());
        auto err = cast_registry_.add(source, target, components::casts::cast_entry(entry));
        co_return !err.contains_error();
    }

    executor_t::unique_future<bool> executor_t::unregister_cast(components::session::session_id_t session,
                                                                components::types::complex_logical_type source,
                                                                components::types::complex_logical_type target) {
        trace(log_, "executor::unregister_cast, session: {}", session.data());
        co_return cast_registry_.remove(source, target);
    }

    executor_t::unique_future<bool> executor_t::set_explain_renderer(uint32_t id, explain_render_fn fn) {
        if (fn == nullptr || id >= kExplainRendererSlotLimit) {
            co_return false;
        }
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
        std::pmr::vector<ops::operator_t*> chain{resource()};
        for (ops::operator_t* op = root.get(); op != nullptr; op = op->left().get()) {
            chain.push_back(op);
        }
        std::reverse(chain.begin(), chain.end());

        const bool analyze = ctx->analyze;

        std::size_t start = 0;
        for (std::size_t i = 0; i < chain.size(); ++i) {
            if (chain[i]->is_executed()) {
                start = i + 1;
            }
        }
        const bool sourceless_sink_root = start == 0 && chain.front()->role() != ops::pipeline_role::source;
        const std::size_t op_start = sourceless_sink_root ? 1 : ((start == 0) ? 1 : start);

        if (analyze) {
            if (start == 0) {
                chain.front()->bump_analyze_loop();
            }
            for (std::size_t i = op_start; i < chain.size(); ++i) {
                chain[i]->bump_analyze_loop();
            }
        }

        bool pumpable_ancestors = false;
        if (sourceless_sink_root) {
            pumpable_ancestors = chain.front()->produces_query_rows();
            for (ops::operator_t* op : chain) {
                if (pumpable_ancestors) {
                    break;
                }
                if (op->role() != ops::pipeline_role::sink) {
                    pumpable_ancestors = true;
                }
            }
        }

        ops::chunks_vector_t output{resource()};

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

        if (sourceless_sink_root) {
            const analyze_scope front_scope{analyze};
            if (chain.front()->needs_async_finalize()) {
                co_await chain.front()->await_async_and_resume(ctx);
                if (chain.front()->has_error()) {
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(chain.front()->get_error());
                }
            }
            // The producing bottom is driven only here — without it, its EXPLAIN ANALYZE line reads rows=0.
            if (analyze) {
                chain.front()->record_analyze(chain.front()->output() ? count_rows(chain.front()->output()->chunks())
                                                                      : 0,
                                              front_scope.elapsed());
            }
            if (pumpable_ancestors && chain.front()->output()) {
                for (const auto& c : chain.front()->output()->chunks()) {
                    auto err = pump_one(c.partial_copy(resource(), 0, c.size()));
                    if (err.contains_error()) {
                        co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(err));
                    }
                    if (auto flush_err = co_await maybe_mid_flush(chain, dml_idx, ctx); flush_err.contains_error()) {
                        co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(flush_err));
                    }
                }
            }
        } else if (start == 0) {
            ops::operator_t* source = chain.front();
            auto release_source_cursor = [&](std::pmr::memory_resource*) -> actor_zeta::unique_future<void> {
                if (source->holds_open_cursor()) {
                    co_await source->release_cursor(ctx);
                }
                co_return;
            };
            while (true) {
                const analyze_scope scope{analyze};
                auto next = co_await source->source_next(ctx);
                if (next.has_error()) {
                    co_await release_source_cursor(resource());
                    co_return next.convert_error<ops::chunks_vector_t>();
                }
                auto batch = std::move(next.value());
                if (batch.data.empty()) {
                    break; // 0-column drain sentinel (a schema'd 0-row batch is real input, e.g.
                           // the empty-guard a scalar aggregate needs to emit COUNT=0)
                }
                if (analyze) {
                    source->record_analyze(batch.size(), scope.elapsed());
                }
                auto err = pump_one(std::move(batch));
                if (err.contains_error()) {
                    co_await release_source_cursor(resource());
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(err));
                }
                if (auto flush_err = co_await maybe_mid_flush(chain, dml_idx, ctx); flush_err.contains_error()) {
                    co_await release_source_cursor(resource());
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(std::move(flush_err));
                }
            }
            co_await release_source_cursor(resource());
        } else if (chain[start - 1]->output()) {
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
        // co_await must live outside the synchronous pump_one lambda; exactly one co_await keeps it lost-wakeup-safe.
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
        auto build_err = co_await materialize_build_sides_(root, ctx);
        if (build_err.contains_error()) {
            co_return build_err;
        }
        auto piped = co_await execute_pipeline(root, ctx);
        if (piped.has_error()) {
            co_return piped.error();
        }
        if (!root->is_executed()) {
            root->set_output(components::operators::make_operator_data(resource(), std::move(piped.value())));
            root->mark_executed();
        }
        if (root->has_error()) {
            co_return root->get_error();
        }
        co_return core::error_t::no_error();
    }

    executor_t::unique_future<core::result_wrapper_t<components::operators::chunks_vector_t>>
    executor_t::run_subplan(components::operators::operator_ptr root, components::pipeline::context_t* ctx) {
        // subplan_runner_t entry point, invoked intra-actor, so awaits in drive_subplan_ stay lost-wakeup-safe.
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

            components::pipeline::context_t pipeline_context{session,
                                                             address(),
                                                             parent_address_,
                                                             &function_registry_,
                                                             *plan_data.parameters,
                                                             disk_address_,
                                                             index_address_,
                                                             wal_address_};
            pipeline_context.txn = txn;
            pipeline_context.execution_context = plan_data.context_storage_.execution_context;
            pipeline_context.lowest_active_start_time = lowest_active_start_time;
            pipeline_context.runner = this;
            pipeline_context.analyze = plan_data.analyze;

            plan->prepare();

            // Factored out so the CONSTRAINT-ERROR path can lift these ranges too, else the appended row leaks.
            auto lift_dml_ranges = [&pipeline_context, &result_tracking]() {
                for (const auto& app : pipeline_context.dml_appends) {
                    result_tracking.dml_appends.push_back({app.table_oid, app.row_start, app.row_count});
                }
                for (const auto& del : pipeline_context.dml_deletes) {
                    result_tracking.dml_deletes.push_back({del.table_oid, del.txn_id});
                }
                for (auto& app : pipeline_context.pg_catalog_appends) {
                    result_tracking.pg_catalog_appends.push_back(std::move(app));
                }
                for (auto& d : pipeline_context.pg_catalog_delete_tables) {
                    result_tracking.pg_catalog_delete_tables.insert(std::move(d));
                }
                pipeline_context.dml_appends.clear();
                pipeline_context.dml_deletes.clear();
                pipeline_context.pg_catalog_appends.clear();
                pipeline_context.pg_catalog_delete_tables.clear();
            };

            {
#ifdef DEV_MODE
                if (auto* hook = g_dml_pre_drive_hook.load()) {
                    const auto root_type = plan->type();
                    if (root_type == components::operators::operator_type::insert ||
                        root_type == components::operators::operator_type::update ||
                        root_type == components::operators::operator_type::remove) {
                        hook(session.data());
                    }
                }
#endif
                auto drive_err = co_await drive_subplan_(plan, &pipeline_context);
                if (drive_err.contains_error()) {
                    lift_dml_ranges();
                    cursor = make_cursor(resource(), std::move(drive_err));
                    break;
                }
            }

            // Post-append reconciliation against the LIVE index: a concurrent CREATE INDEX can predate this stamp.
            if (!pipeline_context.dml_appends.empty() && index_address_ != actor_zeta::address_t::empty_address()) {
                auto reconcile = [this, session, &pipeline_context](
                                     std::pmr::memory_resource* res) -> actor_zeta::unique_future<core::error_t> {
                    std::pmr::unordered_map<components::catalog::oid_t,
                                            std::pmr::vector<services::index::index_row_range_t>>
                        by_oid(res);
                    for (const auto& app : pipeline_context.dml_appends) {
                        by_oid.try_emplace(app.table_oid)
                            .first->second.push_back(
                                services::index::index_row_range_t{static_cast<uint64_t>(app.row_start),
                                                                   app.row_count});
                    }
                    for (auto& [oid, ranges] : by_oid) {
                        components::execution_context_t exec_ctx{session,
                                                                 pipeline_context.txn,
                                                                 pipeline_context.execution_context.timezone_offset,
                                                                 oid};
                        auto [_q, qf] =
                            actor_zeta::otterbrix::send(index_address_,
                                                        &services::index::manager_index_t::unmirrored_ranges,
                                                        exec_ctx,
                                                        oid,
                                                        std::move(ranges));
                        auto missing = co_await std::move(qf);
                        for (const auto& gap : missing) {
                            components::vector::vector_t fetch_ids(res,
                                                                   components::types::logical_type::BIGINT,
                                                                   gap.row_count);
                            for (uint64_t k = 0; k < gap.row_count; ++k) {
                                fetch_ids.data<int64_t>()[k] = static_cast<int64_t>(gap.row_start + k);
                            }
                            auto [_f, ff] = actor_zeta::otterbrix::send(disk_address_,
                                                                        &services::disk::manager_disk_t::storage_fetch,
                                                                        session,
                                                                        oid,
                                                                        std::move(fetch_ids),
                                                                        gap.row_count,
                                                                        std::vector<size_t>{},
                                                                        components::table::transaction_data{},
                                                                        components::table::fetch_visibility_t::RAW,
                                                                        /*limit=*/int64_t{-1},
                                                                        services::disk::k_fetch_epoch_unchecked);
                            auto rows_r = co_await std::move(ff);
                            if (rows_r.has_error()) {
                                co_return rows_r.error();
                            }
                            auto [_s, sf] = actor_zeta::otterbrix::send(index_address_,
                                                                        &services::index::manager_index_t::insert_rows,
                                                                        exec_ctx,
                                                                        oid,
                                                                        std::move(rows_r.value()),
                                                                        gap.row_start,
                                                                        gap.row_count);
                            auto stage_err = co_await std::move(sf);
                            if (stage_err.contains_error()) {
                                co_return stage_err;
                            }
#ifdef DEV_MODE
                            g_index_reconcile_staged_ranges.fetch_add(1, std::memory_order_relaxed);
#endif
                        }
                    }
                    co_return core::error_t::no_error();
                };
                auto reconcile_err = co_await reconcile(resource());
                if (reconcile_err.contains_error()) {
                    // Rows are in the table but not the index — fail so the abort tail reverts the appends.
                    lift_dml_ranges();
                    cursor = make_cursor(resource(), std::move(reconcile_err));
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
                lift_dml_ranges();
                break;
            }

            if (pipeline_context.has_pending_disk_futures()) {
                auto disk_futures = pipeline_context.take_pending_disk_futures();
                for (auto& fut : disk_futures) {
                    co_await std::move(fut);
                }
            }

            for (auto& a : pipeline_context.pg_catalog_appends) {
                result_tracking.pg_catalog_appends.push_back(std::move(a));
            }
            for (auto& d : pipeline_context.pg_catalog_delete_tables) {
                result_tracking.pg_catalog_delete_tables.insert(std::move(d));
            }
            for (auto& bf : pipeline_context.pg_attribute_commit_id_backfills) {
                result_tracking.pg_attribute_commit_id_backfills.push_back(bf);
            }
            pipeline_context.pg_catalog_appends.clear();
            pipeline_context.pg_catalog_delete_tables.clear();
            pipeline_context.pg_attribute_commit_id_backfills.clear();

            lift_dml_ranges();
            for (auto oid : pipeline_context.dropped_storage_oids) {
                result_tracking.dropped_storage_oids.push_back(oid);
            }
            pipeline_context.dropped_storage_oids.clear();
            for (auto oid : pipeline_context.created_storage_oids) {
                result_tracking.created_storage_oids.push_back(oid);
            }
            pipeline_context.created_storage_oids.clear();
            for (auto& index : pipeline_context.created_indexes) {
                result_tracking.created_indexes.push_back(std::move(index));
            }
            pipeline_context.created_indexes.clear();
            if (pipeline_context.committed_id != 0) {
                result_tracking.commit_id = pipeline_context.committed_id;
            }
            // (dml_* fields + cascade vectors were already drained and zeroed by lift_dml_ranges() above.)

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
        auto commit_node =
            components::logical_plan::make_node_transaction(resource(),
                                                            components::logical_plan::transaction_op::commit);
        if (ddl_mode) {
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
