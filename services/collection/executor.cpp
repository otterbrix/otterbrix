#include "executor.hpp"

#include <components/catalog/alter_column_validators.hpp>
#include <components/logical_plan/node_dynamic_cascade_delete.hpp>
#include <components/physical_plan/operators/operator_dynamic_cascade_delete.hpp>
#include <components/physical_plan/operators/operator_sequence.hpp>

#include <algorithm>

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>

#include <components/casts/default_casts.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/place.hpp>
#include <components/catalog/type_display.hpp>
#include <components/context/execution_context.hpp>
#include <components/planner/planner.hpp>
#include <components/vector/vector_builders.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <components/catalog/system_table_schemas.hpp>
#include <components/logical_plan/detached_block.hpp>
#include <components/logical_plan/effective_table_oid.hpp>
#include <components/logical_plan/for_each_plan_call.hpp>
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
#include <components/logical_plan/node_create_schema.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_deallocate.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_execute.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_insert_on_conflict.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_prepare.hpp>
#include <components/logical_plan/node_refresh_matview.hpp>
#include <components/logical_plan/node_register_cast.hpp>
#include <components/logical_plan/node_register_udf.hpp>
#include <components/logical_plan/node_remote.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_set_setting.hpp>
#include <components/logical_plan/node_transaction.hpp>
#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/scan/guard_chunk.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/prepared_expansion.hpp>
#include <components/planner/view_expansion.hpp>
#include <components/sql/transformer/utils.hpp>
#include <core/executor.hpp>
#include <services/collection/view_definition.hpp>
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
        std::atomic<source_await_hook_fn> g_source_await_hook{nullptr};
        std::atomic<std::int64_t> g_user_functions_held{0};
        oid_alloc_interposer_t* g_oid_alloc_interposer = nullptr;
    } // namespace

    uint64_t streaming_pipeline_runs() noexcept { return g_streaming_pipeline_runs.load(std::memory_order_relaxed); }
    uint64_t dml_appends_reverted() noexcept { return g_dml_appends_reverted.load(std::memory_order_relaxed); }
    uint64_t dml_flush_count() noexcept { return g_dml_flush_count.load(std::memory_order_relaxed); }
    uint64_t index_reconcile_staged_ranges() noexcept {
        return g_index_reconcile_staged_ranges.load(std::memory_order_relaxed);
    }
    void dev_set_dml_pre_drive_hook(void (*hook)(uint64_t)) noexcept { g_dml_pre_drive_hook.store(hook); }
    void dev_set_source_await_hook(source_await_hook_fn hook) noexcept { g_source_await_hook.store(hook); }
    std::int64_t dev_user_functions_held() noexcept { return g_user_functions_held.load(); }

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
            actor_zeta::msg_id<executor_t, &executor_t::apply_registry_changes>,
            actor_zeta::msg_id<executor_t, &executor_t::set_explain_renderer>,
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

        bool has_returning(const components::logical_plan::node_t* node) {
            using components::logical_plan::node_type;
            switch (node->type()) {
                case node_type::insert_t:
                    return !static_cast<const components::logical_plan::node_insert_t*>(node)->returning().empty();
                case node_type::insert_on_conflict_t:
                    return has_returning(node->children().front().get());
                case node_type::update_t:
                    return !static_cast<const components::logical_plan::node_update_t*>(node)->returning().empty();
                case node_type::delete_t:
                    return !static_cast<const components::logical_plan::node_delete_t*>(node)->returning().empty();
                default:
                    return false;
            }
        }

        // A statement a describe answers no columns for and checks nothing of, as PostgreSQL 18 answers NoData:
        // neither a query nor a write.
        bool is_utility(const components::logical_plan::node_t& root) {
            using components::logical_plan::node_type;
            switch (root.type()) {
                case node_type::create_collection_t:
                case node_type::create_database_t:
                case node_type::create_schema_t:
                case node_type::create_index_t:
                case node_type::create_type_t:
                case node_type::drop_t:
                case node_type::create_sequence_t:
                case node_type::create_view_t:
                case node_type::create_macro_t:
                case node_type::create_matview_t:
                case node_type::refresh_matview_t:
                case node_type::checkpoint_t:
                case node_type::vacuum_t:
                case node_type::alter_table_t:
                case node_type::create_constraint_t:
                case node_type::register_udf_t:
                case node_type::unregister_udf_t:
                case node_type::register_cast_t:
                case node_type::unregister_cast_t:
                case node_type::transaction_t:
                case node_type::set_setting_t:
                case node_type::prepare_t:
                case node_type::deallocate_t:
                    return true;
                default:
                    return false;
            }
        }

        // PostgreSQL 18 equalRowTypes: the same number of columns, each of the same name and type.
        bool same_row_type(const std::pmr::vector<components::types::complex_logical_type>& expected,
                           const std::pmr::vector<components::types::complex_logical_type>& found) {
            if (expected.size() != found.size()) {
                return false;
            }
            for (std::size_t i = 0; i < expected.size(); ++i) {
                const std::string& expected_name = expected[i].has_alias() ? expected[i].alias() : std::string{};
                const std::string& found_name = found[i].has_alias() ? found[i].alias() : std::string{};
                if (expected[i] != found[i] || expected_name != found_name) {
                    return false;
                }
            }
            return true;
        }

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

    namespace {
        // A sequence's steps, each a plan of its own; none for any other operator.
        std::span<const components::operators::operator_ptr> steps_of(const components::operators::operator_t* op) {
            if (op == nullptr || op->type() != components::operators::operator_type::sequence) {
                return {};
            }
            return static_cast<const components::operators::operator_sequence_t*>(op)->steps();
        }
    } // namespace

    plan_t::plan_t(components::operators::operator_ptr root,
                   std::stack<components::operators::operator_ptr>&& sub_plans,
                   const components::logical_plan::storage_parameters* parameters,
                   services::context_storage_t&& context_storage)
        : root(std::move(root))
        , sub_plans(std::move(sub_plans))
        , parameters(parameters)
        // Moved, not copied — a pmr copy ctor doesn't propagate the allocator, silently rebinding off the arena.
        , context_storage_(std::move(context_storage)) {}

    executor_t::executor_t(std::pmr::memory_resource* resource,
                           actor_zeta::address_t parent_address,
                           actor_zeta::address_t wal_address,
                           actor_zeta::address_t disk_address,
                           actor_zeta::address_t index_address,
                           log_t&& log,
                           const configuration::config_execution& execution,
                           components::planner::primitives_t primitives)
        : actor_zeta::basic_actor<executor_t>{resource}
        , parent_address_(std::move(parent_address))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
        , log_(log)
        , function_registry_(resource)
        , cast_registry_(resource)
        , backends_(resource)
        , dml_flush_row_threshold_(execution.dml_flush_row_threshold)
        , explain_renderers_(resource) {
        register_default_functions(function_registry_);
        components::casts::register_default_casts(cast_registry_);
        explain_renderers_.push_back(&render_postgres);
        backends_.reserve(primitives.backends.size());
        for (const auto& backend : primitives.backends) {
            backends_.push_back(
                backend_t{std::pmr::string{backend.connection, resource}, backend.address, backend.max_rows_per_write});
        }
    }

    actor_zeta::behavior_t executor_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<executor_t, &executor_t::execute_plan_full>: {
                co_await actor_zeta::dispatch(this, &executor_t::execute_plan_full, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::apply_registry_changes>: {
                co_await actor_zeta::dispatch(this, &executor_t::apply_registry_changes, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::set_explain_renderer>: {
                co_await actor_zeta::dispatch(this, &executor_t::set_explain_renderer, msg);
                break;
            }
            default:
                break;
        }
    }

    auto executor_t::make_type() const noexcept -> const char* { return "executor"; }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan(components::session::session_id_t session,
                             components::logical_plan::execution_plan_t plan,
                             services::context_storage_t context_storage,
                             statement_registries_t registries,
                             components::table::transaction_data txn,
                             uint64_t lowest_active_start_time,
                             std::pmr::vector<explain_plan_node> captured_subplans) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        using namespace components::logical_plan;

        components::table::transaction_data txn_data = txn;

        auto limit = components::logical_plan::limit_t::all();
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
        context_storage.functions = registries.functions;
        auto planned = planner::create_plan(context_storage,
                                            *registries.functions,
                                            plan.sub_queries.back(),
                                            limit,
                                            &plan.parameters->parameters());
        if (planned.has_error()) {
            co_return execute_result_t{make_cursor(resource(), planned.error())};
        }
        components::operators::operator_ptr node = std::move(planned.value());

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
                             result.commit_id,
                             result.applied_setting_scope,
                             std::move(result.applied_settings)};
        out.captured_explain_ir = std::move(captured_ir);
        out.referenced_deletes = std::move(result.referenced_deletes);
        out.written = result.written;
        out.participants = std::move(result.participants);
        out.host_rows = std::move(result.host_rows);
        out.dropped_functions = std::move(result.dropped_functions);
        out.prepared_change = std::move(result.prepared_change);
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
                                  components::logical_plan::execution_plan_t plan,
                                  services::dispatcher::txn_session_context_t session_ctx,
                                  services::dispatcher::registry_changes_t own_changes) {
        if (own_changes.empty()) {
            co_return co_await execute_statement_(session,
                                                  std::move(plan),
                                                  std::move(session_ctx),
                                                  own_registries_(),
                                                  std::pmr::vector<expanded_view_t>{resource()});
        }
        components::compute::function_registry_t functions{function_registry_, resource()};
        components::casts::cast_registry_t casts{cast_registry_, resource()};
        if (auto refused = own_changes.apply_to(functions, &casts, resource()); refused.contains_error()) {
            co_return execute_result_t{make_cursor(resource(), std::move(refused))};
        }
        co_return co_await execute_statement_(session,
                                              std::move(plan),
                                              std::move(session_ctx),
                                              statement_registries_t{&functions, &casts},
                                              std::pmr::vector<expanded_view_t>{resource()});
    }

    executor_t::unique_future<core::error_t>
    executor_t::apply_registry_changes(services::dispatcher::registry_changes_t changes) {
        trace(log_,
              "executor::apply_registry_changes: {} functions, {} casts",
              changes.functions.size(),
              changes.casts.size());
        auto applied = changes.apply_to(function_registry_, &cast_registry_, resource());
#ifdef DEV_MODE
        if (!applied.contains_error()) {
            for (const auto& change : changes.functions) {
                g_user_functions_held.fetch_add(change.function ? 1 : -1);
            }
        }
#endif
        co_return applied;
    }

    executor_t::unique_future<execute_result_t>
    executor_t::run_resolve_subplan_(components::session::session_id_t session,
                                     const services::dispatcher::txn_session_context_t& session_ctx,
                                     const services::context_storage_t& statement,
                                     components::logical_plan::catalog_resolves_t* resolves,
                                     std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes) {
        auto root = boost::intrusive_ptr<components::logical_plan::node_t>(
            new components::logical_plan::node_sequence_t(resource()));
        for (auto& n : resolve_nodes) {
            root->append_child(n);
        }
        auto params = components::logical_plan::make_parameter_node(resource());
        services::context_storage_t cstor{resource(), log_.clone(), statement.execution_context};
        cstor.place = statement.place;
        cstor.catalog_resolves = resolves;
        // The resolve operators read no registry.
        co_return co_await execute_plan(session,
                                        components::logical_plan::execution_plan_t{resource(), root, params},
                                        std::move(cstor),
                                        own_registries_(),
                                        session_ctx.txn,
                                        session_ctx.lowest_active_start_time,
                                        std::pmr::vector<explain_plan_node>{resource()});
    }

    namespace {
        // The DROPs of a planned statement whose closure may reach a connection's database: a type (a column of a
        // connection's table may be of it), any schema or database (a type of it may reach one), a connection's
        // table.
        void collect_drop_seeds(components::logical_plan::node_t* node,
                                const components::logical_plan::catalog_resolves_t& resolves,
                                std::pmr::vector<components::logical_plan::node_dynamic_cascade_delete_t*>& drops) {
            using components::logical_plan::node_type;
            namespace wk = components::catalog::well_known_oid;
            if (node == nullptr) {
                return;
            }
            if (node->type() == node_type::dynamic_cascade_delete_t) {
                auto* drop = static_cast<components::logical_plan::node_dynamic_cascade_delete_t*>(node);
                bool reaches = false;
                switch (drop->seed_classid()) {
                    case wk::pg_type_table:
                    case wk::pg_namespace_table:
                    case wk::pg_database_table:
                        reaches = true;
                        break;
                    case wk::pg_class_table: {
                        const auto* table = resolves.table_md(drop->target());
                        reaches = table != nullptr && table->address.is_remote();
                        break;
                    }
                    default:
                        break;
                }
                if (reaches) {
                    drops.push_back(drop);
                }
            }
            for (const auto& child : node->children()) {
                collect_drop_seeds(child.get(), resolves, drops);
            }
        }
    } // namespace

    executor_t::unique_future<core::error_t>
    executor_t::stamp_storage_targets_(components::session::session_id_t session,
                                       const services::dispatcher::txn_session_context_t& session_ctx,
                                       const services::context_storage_t& statement,
                                       components::table::transaction_data txn,
                                       components::logical_plan::execution_plan_t& plan) {
        std::pmr::vector<components::logical_plan::node_dynamic_cascade_delete_t*> drops{resource()};
        collect_drop_seeds(plan.sub_queries.back().get(), plan.catalog_resolves, drops);
        if (drops.empty()) {
            co_return core::error_t::no_error();
        }
        components::logical_plan::storage_parameters local_params(resource());
        components::pipeline::context_t pctx{session,
                                             address(),
                                             parent_address_,
                                             nullptr,
                                             local_params,
                                             disk_address_,
                                             index_address_,
                                             wal_address_};
        pctx.txn = txn;
        pctx.place = statement.place;

        std::pmr::vector<std::pmr::vector<components::catalog::oid_t>> found{resource()};
        found.reserve(drops.size());
        bool unresolved = false;
        for (auto* drop : drops) {
            auto closure = co_await components::operators::drop_closure(resource(),
                                                                        &pctx,
                                                                        drop->seed_classid(),
                                                                        drop->seed_objid(),
                                                                        drop->behavior(),
                                                                        drop->target(),
                                                                        drop->relkind(),
                                                                        core::columnname_t{});
            if (closure.has_error()) {
                co_return closure.error();
            }
            for (const auto table_oid : closure.value()) {
                if (plan.catalog_resolves.table_md(table_oid) == nullptr) {
                    components::logical_plan::resolve_entry_t entry;
                    entry.relation_oid = table_oid;
                    plan.catalog_resolves.ensure(resource(), components::logical_plan::resolve_kind::table)
                        .add(std::move(entry));
                    unresolved = true;
                }
            }
            found.push_back(std::move(closure.value()));
        }
        if (unresolved) {
            std::pmr::vector<components::logical_plan::node_ptr> nodes{resource()};
            nodes.push_back(plan.catalog_resolves.tables);
            auto resolved = co_await run_resolve_subplan_(session,
                                                          session_ctx,
                                                          statement,
                                                          &plan.catalog_resolves,
                                                          std::move(nodes));
            if (resolved.cursor->is_error()) {
                co_return resolved.cursor->get_error();
            }
            if (auto unbound = bind_storages_(plan.catalog_resolves, true); unbound.contains_error()) {
                co_return unbound;
            }
        }
        std::pmr::vector<std::string> connections{resource()};
        for (std::size_t i = 0; i < drops.size(); ++i) {
            std::pmr::vector<components::logical_plan::storage_target_t> targets{resource()};
            for (const auto table_oid : found[i]) {
                const auto* table = plan.catalog_resolves.table_md(table_oid);
                if (table == nullptr) {
                    // Unreachable: the closure read the table's pg_class row in this statement's snapshot.
                    std::fprintf(stderr,
                                 "stamp_storage_targets_: table oid %u of the DROP's closure did not resolve\n",
                                 static_cast<unsigned>(table_oid));
                    assert(false && "stamp_storage_targets_: a table of the DROP's closure did not resolve");
                    std::abort();
                }
                if (std::find(connections.begin(), connections.end(), table->connection) == connections.end()) {
                    connections.push_back(table->connection);
                }
                targets.push_back({table_oid, table->address, services::planner::qualified_name_of(*table)});
            }
            drops[i]->set_storage_targets(std::move(targets));
        }
        if (connections.empty()) {
            co_return core::error_t::no_error();
        }
        // DDL on the backend runs only in autocommit.
        if (!plan.commits_when_done) {
            std::pmr::string msg{"connection \"", resource()};
            msg.append(connections.front());
            msg.append("\" only supports DDL in autocommit");
            co_return core::error_t{core::error_code_t::unimplemented_yet, std::move(msg)};
        }
        co_return check_write_place_(session_ctx, std::span<const std::string>{connections.data(), connections.size()});
    }

    core::result_wrapper_t<components::table::storage_address_t>
    executor_t::storage_of_connection_(std::string_view connection) const {
        if (connection.empty()) {
            return components::table::storage_address_t{disk_address_, {}};
        }
        for (const auto& backend : backends_) {
            if (backend.connection == connection) {
                return components::table::storage_address_t{backend.address,
                                                            std::string{connection},
                                                            backend.max_rows_per_write};
            }
        }
        std::pmr::string msg{"connection \"", resource()};
        msg.append(connection);
        msg.append("\" does not exist");
        return core::error_t{core::error_code_t::undefined_object, std::move(msg)};
    }

    core::error_t executor_t::bind_storages_(components::logical_plan::catalog_resolves_t& resolves,
                                             bool catalog_only) const {
        if (!resolves.tables) {
            return core::error_t::no_error();
        }
        for (auto& entry : resolves.tables->entries()) {
            if (!entry.table_md.has_value()) {
                continue;
            }
            auto& table = *entry.table_md;
            auto address = storage_of_connection_(table.connection);
            if (address.has_error()) {
                if (!catalog_only) {
                    return address.error();
                }
                table.address =
                    components::table::storage_address_t{actor_zeta::address_t::empty_address(), table.connection};
                continue;
            }
            table.address = std::move(address.value());
        }
        return core::error_t::no_error();
    }

    core::error_t executor_t::check_write_place_(const services::dispatcher::txn_session_context_t& session_ctx,
                                                 std::span<const std::string> written_connections) const {
        std::string place = session_ctx.write_connection;
        for (const auto& connection : written_connections) {
            if (connection.empty()) {
                continue;
            }
            if (place.empty()) {
                place = connection;
                continue;
            }
            if (place == connection) {
                continue;
            }
            std::pmr::string msg{"transaction already wrote to connection \"", resource()};
            msg.append(place);
            msg.append("\"; writing to connection \"");
            msg.append(connection);
            msg.append("\" in the same transaction is not supported");
            return core::error_t{core::error_code_t::unimplemented_yet, std::move(msg)};
        }
        return core::error_t::no_error();
    }

    executor_t::unique_future<core::error_t>
    executor_t::write_host_rows_(components::session::session_id_t session,
                                 const services::dispatcher::txn_session_context_t& session_ctx,
                                 statement_registries_t registries,
                                 std::vector<components::host_rows_t> rows) {
        for (const auto& written : rows) {
            for (const auto* chunks : {&written.insert, &written.delete_keys}) {
                for (const auto& chunk : *chunks) {
                    if (auto refused = components::operators::check_chunk_width(resource(), chunk);
                        refused.contains_error()) {
                        co_return refused;
                    }
                }
            }
        }
        for (auto& written : rows) {
            // The host's table, read by its oid: its name for the statements below.
            components::logical_plan::catalog_resolves_t resolves;
            {
                components::logical_plan::resolve_entry_t entry;
                entry.relation_oid = written.table;
                resolves.ensure(resource(), components::logical_plan::resolve_kind::table).add(std::move(entry));
                std::pmr::vector<components::logical_plan::node_ptr> nodes{resource()};
                nodes.push_back(resolves.tables);
                services::context_storage_t resolve_storage{resource(), log_.clone(), session_ctx.settings};
                auto resolved =
                    co_await run_resolve_subplan_(session, session_ctx, resolve_storage, &resolves, std::move(nodes));
                if (resolved.cursor->is_error()) {
                    co_return resolved.cursor->get_error();
                }
            }
            const auto* table = resolves.table_md(written.table);
            if (table == nullptr) {
                std::pmr::string msg{"the host's rows name table oid ", resource()};
                msg.append(std::to_string(static_cast<unsigned>(written.table)));
                msg.append(", which does not exist");
                co_return core::error_t{core::error_code_t::table_not_exists, std::move(msg)};
            }
            const qualified_name_t name{core::connection_name_t{table->connection},
                                        core::dbname_t{table->database},
                                        core::schema_t{table->schema},
                                        core::relname_t{table->name}};
            const auto plan_of = [this, table](components::logical_plan::node_ptr root,
                                               components::logical_plan::parameter_node_ptr parameters) {
                components::logical_plan::execution_plan_t plan{resource(), std::move(root), std::move(parameters)};
                plan.commits_when_done = false;
                components::logical_plan::resolve_entry_t entry;
                entry.connection = table->connection;
                entry.database = table->database;
                entry.schema = table->schema;
                entry.relname = table->name;
                entry.relation_oid = table->table_oid;
                plan.catalog_resolves.ensure(resource(), components::logical_plan::resolve_kind::table)
                    .add(std::move(entry));
                return plan;
            };
            for (auto& chunk : written.insert) {
                auto insert = components::logical_plan::make_node_insert(
                    resource(),
                    components::vector::adopted(resource(), std::move(chunk)));
                insert->set_target(name);
                auto done = co_await execute_statement_(
                    session,
                    plan_of(std::move(insert), components::logical_plan::make_parameter_node(resource())),
                    session_ctx,
                    registries,
                    std::pmr::vector<expanded_view_t>{resource()});
                if (done.cursor->is_error()) {
                    co_return done.cursor->get_error();
                }
            }
            if (written.delete_column >= table->columns.size()) {
                if (written.delete_keys.empty()) {
                    continue;
                }
                std::pmr::string msg{"the host's rows delete by column ", resource()};
                msg.append(std::to_string(written.delete_column));
                msg.append(" of table \"");
                msg.append(name.to_string());
                msg.append("\", which has no such column");
                co_return core::error_t{core::error_code_t::protocol_violation, std::move(msg)};
            }
            const auto& key_column = table->columns[written.delete_column].attname;
            for (auto& foreign_keys : written.delete_keys) {
                const auto keys = components::vector::adopted(resource(), std::move(foreign_keys));
                for (uint64_t row = 0; row < keys.size(); ++row) {
                    auto parameters = components::logical_plan::make_parameter_node(resource());
                    auto key = parameters->add_parameter(
                        components::vector::make_constant_copy(resource(),
                                                               components::vector::vector_t(keys.data.front(), row, 1)),
                        components::vector::parameter_kind::literal);
                    auto erase = components::logical_plan::make_node_delete(
                        resource(),
                        components::logical_plan::make_node_match(
                            resource(),
                            name,
                            components::expressions::make_compare_expression(
                                resource(),
                                components::expressions::compare_type::eq,
                                components::expressions::key_t{resource(), key_column},
                                key)),
                        components::logical_plan::make_node_limit(resource(),
                                                                  components::logical_plan::limit_t::all()));
                    erase->set_target(name);
                    auto done = co_await execute_statement_(session,
                                                            plan_of(std::move(erase), std::move(parameters)),
                                                            session_ctx,
                                                            registries,
                                                            std::pmr::vector<expanded_view_t>{resource()});
                    if (done.cursor->is_error()) {
                        co_return done.cursor->get_error();
                    }
                }
            }
        }
        co_return core::error_t::no_error();
    }

    namespace {

        using components::logical_plan::node_ptr;
        using components::logical_plan::node_type;

        // A node a backend may take whole: a SELECT block, a join, an UPDATE / DELETE.
        bool is_relation(node_type type) noexcept {
            return type == node_type::aggregate_t || type == node_type::join_t || type == node_type::update_t ||
                   type == node_type::delete_t;
        }

        // Does `node`'s whole sub-tree read the tables of one backend, and hold only what a block may hold?
        struct block_reach_t {
            bool fits{true};
            const components::table::storage_address_t* address{nullptr};
        };

        block_reach_t reach_of(const components::logical_plan::node_t& node) {
            if (!components::logical_plan::may_be_in_a_block(node.type())) {
                return {false, nullptr};
            }
            block_reach_t reach;
            if (const auto* table = node.table_metadata(); table != nullptr) {
                if (!table->address.is_remote() || table->address.empty()) {
                    return {false, nullptr};
                }
                reach.address = &table->address;
            }
            for (const auto& child : node.children()) {
                if (!child) {
                    continue;
                }
                const auto inner = reach_of(*child);
                if (!inner.fits) {
                    return {false, nullptr};
                }
                if (inner.address == nullptr) {
                    continue;
                }
                if (reach.address != nullptr && !reach.address->same_storage(*inner.address)) {
                    return {false, nullptr};
                }
                reach.address = inner.address;
            }
            return reach;
        }

        struct block_candidate_t {
            node_ptr* slot;
            components::table::storage_address_t address;
        };

        // The largest sub-trees of one backend's tables, top-down.
        void collect_remote_blocks(node_ptr& slot, std::pmr::vector<block_candidate_t>& candidates) {
            if (!slot) {
                return;
            }
            if (is_relation(slot->type())) {
                const auto reach = reach_of(*slot);
                if (reach.fits && reach.address != nullptr) {
                    candidates.push_back(block_candidate_t{&slot, *reach.address});
                    return;
                }
            }
            for (auto& child : slot->children()) {
                collect_remote_blocks(child, candidates);
            }
        }

        // The slots of `slot`'s sub-tree in preorder, with the size of each one's sub-tree.
        void preorder_of(node_ptr& slot, std::pmr::vector<node_ptr*>& order, std::pmr::vector<std::size_t>& sizes) {
            const auto position = order.size();
            order.push_back(&slot);
            sizes.push_back(1);
            if (!slot) {
                return;
            }
            for (auto& child : slot->children()) {
                preorder_of(child, order, sizes);
            }
            sizes[position] = order.size() - position;
        }

        void tables_of(const components::logical_plan::node_t& node,
                       std::pmr::vector<components::catalog::oid_t>& out) {
            if (node.table_oid() != components::catalog::INVALID_OID &&
                std::find(out.begin(), out.end(), node.table_oid()) == out.end()) {
                out.push_back(node.table_oid());
            }
            for (const auto& child : node.children()) {
                if (child) {
                    tables_of(*child, out);
                }
            }
        }

        // The conjuncts of a WHERE: an AND's operands, or the one condition.
        std::pmr::vector<components::expressions::expression_ptr>
        conjuncts_of(std::pmr::memory_resource* resource, const components::expressions::expression_ptr& where) {
            std::pmr::vector<components::expressions::expression_ptr> out{resource};
            if (where && where->group() == components::expressions::expression_group::compare) {
                const auto& compare = static_cast<const components::expressions::compare_expression_t&>(*where);
                if (compare.type() == components::expressions::compare_type::union_and) {
                    out.assign(compare.children().begin(), compare.children().end());
                    return out;
                }
            }
            out.push_back(where);
            return out;
        }

        core::error_t take_refused(std::pmr::memory_resource* resource,
                                   const components::table::storage_address_t& address,
                                   std::string_view why) {
            std::pmr::string what{"the backend of connection \"", resource};
            what += address.connection();
            what += "\" took a block wrongly: ";
            what.append(why.data(), why.size());
            return core::error_t{core::error_code_t::protocol_violation, std::move(what)};
        }

        // What a backend took of one offered block, put in the plan: a taken node becomes a node_remote_t; a SELECT
        // block taken with a residual WHERE keeps its clauses above a node_remote_t that reads its table's rows.
        core::error_t apply_takes(std::pmr::memory_resource* resource,
                                  block_candidate_t& candidate,
                                  std::pmr::vector<services::disk::block_take_t>& takes) {
            using components::logical_plan::node_remote_t;
            std::pmr::vector<node_ptr*> order{resource};
            std::pmr::vector<std::size_t> sizes{resource};
            preorder_of(*candidate.slot, order, sizes);
            std::pmr::vector<bool> taken(order.size(), false, resource);
            for (const auto& take : takes) {
                if (take.preorder >= order.size()) {
                    return take_refused(resource, candidate.address, "no node of the block has that position");
                }
                const node_ptr& node = *order[take.preorder];
                if (!node || !is_relation(node->type())) {
                    return take_refused(resource, candidate.address, "a taken node is a relation");
                }
                if ((node->type() == node_type::update_t || node->type() == node_type::delete_t) &&
                    take.preorder != 0) {
                    return take_refused(resource, candidate.address, "an UPDATE / DELETE is taken only as the block");
                }
                if (!take.residual.empty() && node->type() != node_type::aggregate_t) {
                    return take_refused(resource, candidate.address, "only a SELECT block leaves a residual WHERE");
                }
                for (std::size_t inside = take.preorder; inside < take.preorder + sizes[take.preorder]; ++inside) {
                    if (taken[inside]) {
                        return take_refused(resource, candidate.address, "two taken nodes overlap");
                    }
                }
                for (std::size_t outside = 0; outside < take.preorder; ++outside) {
                    if (taken[outside] && take.preorder < outside + sizes[outside]) {
                        return take_refused(resource, candidate.address, "two taken nodes overlap");
                    }
                }
                taken[take.preorder] = true;
            }
            // Deepest first: replacing a node never frees a slot still to be replaced (taken nodes do not overlap).
            std::sort(takes.begin(), takes.end(), [](const auto& a, const auto& b) { return a.preorder > b.preorder; });
            for (auto& take : takes) {
                node_ptr& slot = *order[take.preorder];
                std::pmr::vector<std::pmr::string> explain{resource};
                for (const auto& line : take.explain) {
                    explain.emplace_back(line);
                }
                const bool write = slot->type() == node_type::update_t || slot->type() == node_type::delete_t;
                components::logical_plan::node_remote_ptr remote{
                    new node_remote_t(resource,
                                      write ? node_remote_t::kind_t::write : node_remote_t::kind_t::read,
                                      candidate.address,
                                      std::pmr::string{take.request, resource},
                                      std::move(explain))};
                tables_of(*slot, remote->tables());
                if (write) {
                    const auto& returning =
                        slot->type() == node_type::update_t
                            ? static_cast<const components::logical_plan::node_update_t&>(*slot).returning()
                            : static_cast<const components::logical_plan::node_delete_t&>(*slot).returning();
                    remote->returning().assign(returning.begin(), returning.end());
                    if (slot->type() == node_type::delete_t) {
                        remote->scan_columns() =
                            static_cast<const components::logical_plan::node_delete_t&>(*slot).projected_cols();
                    }
                    remote->set_table_oid(slot->table_oid());
                    slot = remote;
                    continue;
                }
                if (take.residual.empty()) {
                    if (!slot->has_output_types()) {
                        return take_refused(resource, candidate.address, "the block's columns are not known");
                    }
                    remote->set_output_types(
                        std::pmr::vector<components::types::complex_logical_type>{slot->output_types().begin(),
                                                                                  slot->output_types().end(),
                                                                                  resource});
                    remote->set_result_alias(slot->result_alias());
                    slot = remote;
                    continue;
                }
                // The backend reads the table with the WHERE conjuncts it took; the SELECT block keeps the rest
                // over the table's rows.
                if (slot->type() != node_type::aggregate_t) {
                    return take_refused(resource, candidate.address, "a residual WHERE is left of a node of no WHERE");
                }
                auto& aggregate = static_cast<components::logical_plan::node_aggregate_t&>(*slot);
                const auto* table = aggregate.table_metadata();
                if (table == nullptr) {
                    return take_refused(resource, candidate.address, "a residual WHERE is left of a table's block");
                }
                std::pmr::vector<components::types::complex_logical_type> columns{resource};
                for (const auto& column : table->columns) {
                    auto type = column.type;
                    type.set_alias(column.attname);
                    columns.push_back(std::move(type));
                }
                remote->set_output_types(std::move(columns));
                bool has_where = false;
                for (auto& child : aggregate.children()) {
                    if (!child || child->type() != node_type::match_t || child->expressions().empty()) {
                        continue;
                    }
                    has_where = true;
                    const auto conjuncts = conjuncts_of(resource, child->expressions().front());
                    std::pmr::vector<components::expressions::expression_ptr> kept{resource};
                    for (const auto position : take.residual) {
                        if (position >= conjuncts.size()) {
                            return take_refused(resource, candidate.address, "a residual conjunct is not in the WHERE");
                        }
                        kept.push_back(conjuncts[position]);
                    }
                    if (kept.size() == 1) {
                        child->expressions().front() = kept.front();
                    } else {
                        auto both = components::expressions::make_compare_union_expression(
                            resource,
                            components::expressions::compare_type::union_and);
                        for (const auto& conjunct : kept) {
                            both->append_child(conjunct);
                        }
                        child->expressions().front() = both;
                    }
                }
                if (!has_where) {
                    return take_refused(resource, candidate.address, "a residual WHERE is left of a block without one");
                }
                aggregate.set_projected_cols({});
                aggregate.clear_source_identity();
                aggregate.children().insert(aggregate.children().begin(), remote);
            }
            return core::error_t::no_error();
        }

    } // namespace

    executor_t::unique_future<core::error_t>
    executor_t::offer_blocks_(components::session::session_id_t session,
                              components::logical_plan::execution_plan_t& plan,
                              components::table::transaction_data txn,
                              const components::graph_execution_context& settings) {
        // The statement's own tree: each sub-query was offered when it ran as a statement of its own.
        std::pmr::vector<block_candidate_t> candidates{resource()};
        collect_remote_blocks(plan.sub_queries.back(), candidates);
        if (candidates.empty()) {
            co_return core::error_t::no_error();
        }
        const components::execution_context_t ctx{session, txn, settings.timezone_offset};
        std::pmr::vector<unique_future<core::result_wrapper_t<std::pmr::vector<services::disk::block_take_t>>>> offers{
            resource()};
        offers.reserve(candidates.size());
        for (const auto& candidate : candidates) {
            auto* target = candidate.address.actor().resource();
            auto block = components::logical_plan::detach_block(target, **candidate.slot);
            if (block.has_error()) {
                co_return block.error();
            }
            auto [_o, of] = actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_offer>(
                candidate.address,
                ctx,
                std::move(block.value()),
                plan.parameters->parameters().clone(target));
            offers.push_back(std::move(of));
        }
        // Every offer left before the first reply is awaited; candidates never overlap, so each reply is applied as
        // it comes.
        for (std::size_t i = 0; i < candidates.size(); ++i) {
            auto reply = co_await std::move(offers[i]);
            if (reply.has_error()) {
                co_return reply.error();
            }
            if (auto applied = apply_takes(resource(), candidates[i], reply.value()); applied.contains_error()) {
                co_return applied;
            }
        }
        co_return core::error_t::no_error();
    }

    executor_t::unique_future<core::error_t>
    executor_t::pin_functions_(components::session::session_id_t session,
                               components::table::transaction_data txn,
                               const components::compute::function_registry_t& functions,
                               const components::logical_plan::resolved_table_metadata_t* view,
                               components::logical_plan::node_t* body) {
        auto function_oids = view_function_oids(resource(), *view);
        if (function_oids.empty()) {
            co_return core::error_t::no_error();
        }
        std::pmr::vector<std::uint64_t> key_columns{resource()};
        key_columns.push_back(components::catalog::pg_proc_col::oid);
        auto [_rp, rpf] = actor_zeta::otterbrix::send(disk_address_,
                                                      &services::disk::manager_disk_t::read_chunks_by_keys,
                                                      components::execution_context_t{session, txn, {}},
                                                      components::catalog::well_known_oid::pg_proc_table,
                                                      std::move(key_columns),
                                                      components::operators::make_keys_batch(resource(), function_oids),
                                                      std::pmr::vector<std::uint64_t>{resource()});
        auto proc_chunks = co_await std::move(rpf);
        if (proc_chunks.has_error()) {
            co_return core::error_on(resource(), proc_chunks.error());
        }
        co_return pin_view_functions(resource(), *view, proc_chunks.value(), functions, body);
    }

    // REFRESH MATERIALIZED VIEW (PostgreSQL 18 matview.c): the matview's rows are deleted and its stored body is
    // inserted again (refresh_matview_plan), as two statements of this one transaction.
    executor_t::unique_future<execute_result_t>
    executor_t::refresh_matview_(components::session::session_id_t session,
                                 const components::logical_plan::execution_plan_t& plan,
                                 const services::dispatcher::txn_session_context_t& session_ctx,
                                 statement_registries_t registries,
                                 const components::graph_execution_context& settings) {
        const auto* refresh =
            static_cast<const components::logical_plan::node_refresh_matview_t*>(plan.sub_queries.back().get());
        const auto& matview_name = refresh->target();
        const auto* matview = plan.catalog_resolves.table_md(matview_name);
        if (matview == nullptr) {
            co_return execute_result_t{make_cursor(resource(), missing_relation_(matview_name))};
        }
        if (matview->relkind != components::catalog::relkind::materialized_view) {
            co_return execute_result_t{
                make_cursor(resource(),
                            wrong_relation_kind(resource(),
                                                matview_name.collection.t,
                                                components::catalog::relkind::materialized_view))};
        }
        {
            auto done =
                co_await execute_statement_(session,
                                            components::planner::refresh_matview_delete_plan(resource(), *matview),
                                            session_ctx,
                                            registries,
                                            std::pmr::vector<expanded_view_t>{resource()});
            if (done.cursor->is_error()) {
                co_return done;
            }
        }
        if (refresh->with_data()) {
            auto refill = components::planner::refresh_matview_plan(resource(), *matview);
            if (refill.has_error()) {
                co_return execute_result_t{make_cursor(resource(), refill.error())};
            }
            auto insert = std::move(refill.value());
            if (auto pinned = co_await pin_functions_(session,
                                                      session_ctx.txn,
                                                      *registries.functions,
                                                      matview,
                                                      insert.reference->children().front().get());
                pinned.contains_error()) {
                co_return execute_result_t{make_cursor(resource(), std::move(pinned))};
            }
            insert.plan.commits_when_done = false;
            std::pmr::vector<expanded_view_t> stored_body{resource()};
            stored_body.push_back({std::move(insert.reference), *matview});
            auto done = co_await execute_statement_(session,
                                                    std::move(insert.plan),
                                                    session_ctx,
                                                    registries,
                                                    std::move(stored_body));
            if (done.cursor->is_error()) {
                co_return done;
            }
        }
        if (plan.commits_when_done) {
            auto committed = co_await run_commit_pipeline_(session,
                                                           session_ctx.txn,
                                                           settings,
                                                           session_ctx.lowest_active_start_time,
                                                           /*ddl_mode=*/false);
            if (committed.cursor->is_error()) {
                co_return committed;
            }
        }
        co_return execute_result_t{make_cursor(resource())};
    }

    // PREPARE (PostgreSQL 18 PrepareQuery): the body is described, with the declared types bound as typed NULLs; a
    // body that does not describe is refused, and nothing is stored.
    executor_t::unique_future<core::error_t>
    executor_t::describe_prepared_(components::session::session_id_t session,
                                   components::logical_plan::node_prepare_t& prepare,
                                   const services::dispatcher::txn_session_context_t& session_ctx,
                                   statement_registries_t registries) {
        if (prepare.name().empty()) {
            co_return core::error_t{core::error_code_t::protocol_violation,
                                    std::pmr::string{"invalid statement name: must not be empty", resource()}};
        }
        std::pmr::vector<components::vector::vector_t> declared{resource()};
        declared.reserve(prepare.declared_types().size());
        for (const auto& type : prepare.declared_types()) {
            declared.push_back(components::vector::make_null(resource(), type));
        }
        auto body = components::planner::expand_prepared(resource(),
                                                         prepare.body_text(),
                                                         declared,
                                                         components::planner::prepared_use_t::describe);
        if (body.has_error()) {
            co_return core::error_on(resource(), body.error());
        }
        body.value().explain = components::logical_plan::explain_type::describe;
        auto described = co_await execute_statement_(session,
                                                     std::move(body.value()),
                                                     session_ctx,
                                                     registries,
                                                     std::pmr::vector<expanded_view_t>{resource()});
        if (described.cursor->is_error()) {
            co_return core::error_on(resource(), described.cursor->get_error());
        }
        components::logical_plan::prepared_statement_t statement{prepare.body_text(), {}, {}};
        statement.parameter_types.assign(prepare.declared_types().begin(), prepare.declared_types().end());
        const auto& deduced = described.cursor->parameter_types();
        for (std::size_t i = statement.parameter_types.size(); i < deduced.size(); ++i) {
            statement.parameter_types.push_back(deduced[i]);
        }
        statement.columns.assign(described.cursor->type_data().begin(), described.cursor->type_data().end());
        prepare.set_described(std::move(statement), session_ctx.prepared.has_value());
        co_return core::error_t::no_error();
    }

    core::error_t
    executor_t::refuse_execute_argument_(const components::logical_plan::node_t* argument,
                                         const components::compute::function_registry_t& functions) const {
        bool aggregate = false;
        bool set_returning = false;
        auto call = [&](const auto& found, const components::logical_plan::node_t&, bool) {
            if constexpr (std::is_same_v<std::decay_t<decltype(found)>,
                                         components::expressions::function_expression_t>) {
                // A call is of the kind every overload of its name is of.
                const auto uids = functions.find_functions(found.name());
                bool aggregates_only = !uids.empty();
                bool expands_only = !uids.empty();
                for (const auto uid : uids) {
                    const auto* function = functions.get_function(uid);
                    if (function == nullptr) {
                        continue;
                    }
                    for (const auto& signature : function->get_signatures()) {
                        aggregates_only = aggregates_only &&
                                          signature.function_type == components::compute::function_type_t::aggregate;
                        expands_only =
                            expands_only && signature.function_type == components::compute::function_type_t::expand;
                    }
                }
                aggregate = aggregate || aggregates_only;
                set_returning = set_returning || expands_only;
            } else {
                set_returning = true;
            }
        };
        components::logical_plan::for_each_plan_call(argument, call);
        if (aggregate) {
            return core::error_t{
                core::error_code_t::grouping_error,
                std::pmr::string{"aggregate functions are not allowed in EXECUTE parameters", resource()}};
        }
        if (set_returning) {
            return core::error_t{
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"set-returning functions are not allowed in EXECUTE parameters", resource()}};
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<components::vector::vector_t>
    executor_t::coerce_argument_(const components::vector::vector_t& value,
                                 const components::types::complex_logical_type& expected,
                                 std::size_t number,
                                 bool quoted_literal,
                                 const components::casts::cast_registry_t& casts,
                                 const components::graph_execution_context& settings) const {
        if (value.is_null(0)) {
            return components::vector::make_null(resource(), expected);
        }
        if (value.type() == expected) {
            return components::vector::make_constant_copy(resource(), value, 0);
        }
        // coerce_to_target_type, COERCION_ASSIGNMENT: as an INSERT's VALUES cell.
        auto conversion = casts.resolve_assignment(value.type(), expected, quoted_literal);
        if (!conversion.has_value()) {
            return core::error_t{core::error_code_t::datatype_mismatch,
                                 std::pmr::string{"parameter $" + std::to_string(number) + " of type " +
                                                      components::catalog::format_type_be(value.type(), false) +
                                                      " cannot be coerced to the expected type " +
                                                      components::catalog::format_type_be(expected, false),
                                                  resource()}};
        }
        const auto source = components::vector::make_flat_copy(resource(), value, 1);
        components::vector::vector_t converted{resource(), expected, 1};
        if (auto failed = (*conversion)(components::casts::cast_kind::cast, source, &converted, settings, 1);
            failed.contains_error()) {
            return failed;
        }
        return converted;
    }

    // EXECUTE (PostgreSQL 18 ExecuteQuery): the statement of the name, its arguments' number checked first, then their
    // forms; each argument is evaluated, coerced to its parameter's type and bound into a fresh plan of the stored
    // text, which runs as this statement, in its transaction, with its EXPLAIN.
    executor_t::unique_future<execute_result_t>
    executor_t::execute_prepared_(components::session::session_id_t session,
                                  components::logical_plan::execution_plan_t plan,
                                  services::dispatcher::txn_session_context_t session_ctx,
                                  statement_registries_t registries) {
        using components::logical_plan::explain_type;
        const auto* execute =
            static_cast<const components::logical_plan::node_execute_t*>(plan.sub_queries.back().get());
        // PostgreSQL 18 describes an EXPLAIN without looking at what it explains.
        if (plan.explain == explain_type::describe_explain) {
            co_return execute_result_t{
                describe_(plan,
                          services::dispatcher::validation::named_schema{resource()},
                          std::pmr::vector<components::types::complex_logical_type>{resource()})};
        }
        if (!session_ctx.prepared.has_value()) {
            if (plan.explain == explain_type::describe) {
                co_return execute_result_t{make_cursor(resource())};
            }
            co_return execute_result_t{make_cursor(
                resource(),
                core::error_t{
                    core::error_code_t::undefined_prepared_statement,
                    std::pmr::string{"prepared statement \"" + execute->name() + "\" does not exist", resource()}})};
        }
        const auto& statement = *session_ctx.prepared;
        if (plan.explain == explain_type::describe) {
            auto described =
                make_cursor(resource(),
                            std::pmr::vector<components::types::complex_logical_type>{statement.columns.begin(),
                                                                                      statement.columns.end(),
                                                                                      resource()});
            described->set_parameter_types(
                std::pmr::vector<components::types::complex_logical_type>{statement.parameter_types.begin(),
                                                                          statement.parameter_types.end(),
                                                                          resource()});
            co_return execute_result_t{std::move(described)};
        }
        const auto& arguments = execute->arguments();
        if (arguments.size() != statement.parameter_types.size()) {
            co_return execute_result_t{make_cursor(
                resource(),
                core::error_t{core::error_code_t::sql_parse_error,
                              std::pmr::string{"wrong number of parameters for prepared statement \"" +
                                                   execute->name() + "\"\nDETAIL: Expected " +
                                                   std::to_string(statement.parameter_types.size()) +
                                                   " parameters but got " + std::to_string(arguments.size()) + ".",
                                               resource()}})};
        }
        // PostgreSQL 18 EvaluateParams analyzes the arguments in order: the plans are of the arguments before the
        // one the transformer refused.
        for (std::size_t i = 0; i + 1 < plan.sub_queries.size(); ++i) {
            if (auto refused = refuse_execute_argument_(plan.sub_queries[i].get(), *registries.functions);
                refused.contains_error()) {
                co_return execute_result_t{make_cursor(resource(), std::move(refused))};
            }
        }
        if (execute->refusal().contains_error()) {
            co_return execute_result_t{make_cursor(resource(), core::error_on(resource(), execute->refusal()))};
        }
        // The arguments are evaluated for EXPLAIN too, and nothing of them is shown.
        std::pmr::vector<explain_plan_node> unshown{resource()};
        if (auto failed =
                co_await run_sub_queries_(session, plan, session_ctx, registries, explain_type::none, unshown);
            failed.contains_error()) {
            co_return execute_result_t{make_cursor(resource(), std::move(failed))};
        }
        const components::graph_execution_context settings{.timezone_offset = session_ctx.settings.timezone_offset};
        std::pmr::vector<components::vector::vector_t> values{resource()};
        values.reserve(arguments.size());
        for (std::size_t i = 0; i < arguments.size(); ++i) {
            auto coerced = coerce_argument_(plan.parameters->parameter(arguments[i].slot),
                                            statement.parameter_types[i],
                                            i + 1,
                                            arguments[i].quoted_literal,
                                            *registries.casts,
                                            settings);
            if (coerced.has_error()) {
                co_return execute_result_t{make_cursor(resource(), coerced.error())};
            }
            values.push_back(std::move(coerced.value()));
        }
        auto body = components::planner::expand_prepared(resource(),
                                                         statement.text,
                                                         values,
                                                         components::planner::prepared_use_t::execute);
        if (body.has_error()) {
            co_return execute_result_t{make_cursor(resource(), body.error())};
        }
        auto& run = body.value();
        run.explain = plan.explain;
        run.explain_render_id = plan.explain_render_id;
        run.commits_when_done = plan.commits_when_done;
        run.expected_columns.emplace(statement.columns.begin(), statement.columns.end(), resource());
        co_return co_await execute_statement_(session,
                                              std::move(run),
                                              std::move(session_ctx),
                                              registries,
                                              std::pmr::vector<expanded_view_t>{resource()});
    }

    core::error_t executor_t::prepare_column_(const components::logical_plan::catalog_resolves_t& resolves,
                                              const components::graph_execution_context& settings,
                                              const components::casts::cast_registry_t& casts,
                                              components::table::column_definition_t& column) {
        using components::types::logical_type;
        auto& type = column.type();
        if (type.type() == logical_type::UNKNOWN && !type.type_name().empty()) {
            const std::string alias = type.has_alias() ? type.alias() : std::string{};
            const auto written = components::sql::transform::type_reference_of(type);
            if (auto missing = services::dispatcher::check_type_exists(resource(), &resolves, written);
                missing.contains_error()) {
                return missing;
            }
            if (const auto* md = resolves.type_md(written)) {
                type = md->type;
                // The column depends on its type in pg_depend through it, as in PostgreSQL 18.
                column.set_atttypid(static_cast<std::uint32_t>(md->type_oid));
            } else {
                type = components::types::complex_logical_type{
                    components::catalog::pg_name_to_logical_type(written.collection.t)};
            }
            if (!alias.empty()) {
                type.set_alias(alias);
            }
        }
        if (auto unpersistable = services::dispatcher::gate_persistable_type(resource(),
                                                                             "column '" + column.name() + "'",
                                                                             column.type());
            unpersistable.contains_error()) {
            return unpersistable;
        }
        if (auto unstorable =
                services::dispatcher::gate_storable_type(resource(), "column '" + column.name() + "'", column.type());
            unstorable.contains_error()) {
            return unstorable;
        }
        return services::dispatcher::convert_column_default(resource(), &casts, settings, column);
    }

    executor_t::own_entries_t
    executor_t::own_entries_of(const components::logical_plan::catalog_resolves_t& resolves) noexcept {
        return {resolves.tables ? resolves.tables->entries().size() : std::size_t{0},
                resolves.types ? resolves.types->entries().size() : std::size_t{0}};
    }

    core::error_t executor_t::missing_relation_(const qualified_name_t& name) const {
        std::pmr::string msg{"relation \"", resource()};
        msg.append(name.to_string());
        msg.append("\" does not exist");
        return core::error_t{core::error_code_t::table_not_exists, std::move(msg)};
    }

    namespace {
        bool if_exists_of(const components::logical_plan::node_t& root) {
            using components::logical_plan::node_type;
            switch (root.type()) {
                case node_type::drop_t:
                    return static_cast<const components::logical_plan::node_drop_t&>(root).if_exists();
                case node_type::alter_table_t:
                    return static_cast<const components::logical_plan::node_alter_table_t&>(root).if_exists();
                case node_type::create_constraint_t:
                    return static_cast<const components::logical_plan::node_create_constraint_t&>(root).if_exists();
                default:
                    return false;
            }
        }

        bool if_not_exists_of(const components::logical_plan::node_t& root) {
            using components::logical_plan::node_type;
            switch (root.type()) {
                case node_type::create_database_t:
                    return static_cast<const components::logical_plan::node_create_database_t&>(root).if_not_exists();
                case node_type::create_schema_t:
                    return static_cast<const components::logical_plan::node_create_schema_t&>(root).if_not_exists();
                case node_type::create_collection_t:
                    return static_cast<const components::logical_plan::node_create_collection_t&>(root).if_not_exists();
                default:
                    return false;
            }
        }
    } // namespace

    core::error_t executor_t::missing_target_(const components::logical_plan::execution_plan_t& plan,
                                              const qualified_name_t& target) const {
        using components::logical_plan::drop_target_kind;
        using components::logical_plan::node_type;
        const auto& root = *plan.sub_queries.back();
        const auto& resolves = plan.catalog_resolves;
        const auto relation_missing = [&](const qualified_name_t& name) { return resolves.table_md(name) == nullptr; };
        switch (root.type()) {
            case node_type::insert_t:
            case node_type::insert_on_conflict_t:
            case node_type::update_t:
            case node_type::delete_t:
                return services::dispatcher::check_relation_levels(resource(), &resolves, target);
            case node_type::create_collection_t:
            case node_type::create_sequence_t:
            case node_type::create_view_t:
            case node_type::create_macro_t:
            case node_type::create_type_t:
                // IF NOT EXISTS does not cover it: what is missing is the schema, not the object.
                return target.schema.t.empty()
                           ? core::error_t::no_error()
                           : services::dispatcher::check_namespace_exists(resource(), &resolves, target);
            case node_type::alter_table_t: {
                if (!relation_missing(target)) {
                    return core::error_t::no_error();
                }
                if (auto level = services::dispatcher::check_relation_levels(resource(), &resolves, target);
                    level.contains_error()) {
                    return level;
                }
                std::pmr::string msg{"ALTER TABLE: relation \"", resource()};
                msg.append(target.to_string());
                msg.append("\" does not exist");
                return core::error_t{core::error_code_t::table_not_exists, std::move(msg)};
            }
            case node_type::create_constraint_t: {
                if (!relation_missing(target)) {
                    return core::error_t::no_error();
                }
                auto level = services::dispatcher::check_relation_levels(resource(), &resolves, target);
                return level.contains_error() ? std::move(level) : missing_relation_(target);
            }
            case node_type::drop_t:
                break;
            default:
                return core::error_t::no_error();
        }
        const auto& drop = static_cast<const components::logical_plan::node_drop_t&>(root);
        switch (drop.kind()) {
            case drop_target_kind::database:
                return services::dispatcher::check_database_exists(resource(), &resolves, target);
            case drop_target_kind::schema:
                return services::dispatcher::check_namespace_exists(resource(), &resolves, target);
            case drop_target_kind::collection: {
                auto err = services::dispatcher::check_collection_exists(resource(), &resolves, target);
                if (!err.contains_error()) {
                    return err;
                }
                return err.type == core::error_code_t::table_not_exists ? missing_relation_(target) : std::move(err);
            }
            case drop_target_kind::type: {
                if (!services::dispatcher::check_type_exists(resource(), &resolves, drop.target()).contains_error()) {
                    return core::error_t::no_error();
                }
                std::pmr::string msg{"type \"", resource()};
                msg.append(drop.target().to_string());
                msg.append("\" does not exist");
                return core::error_t{core::error_code_t::undefined_object, std::move(msg)};
            }
            case drop_target_kind::sequence:
            case drop_target_kind::macro:
            case drop_target_kind::view:
            case drop_target_kind::materialized_view: {
                // A relation of every kind: the missing database or schema first, as for a table.
                auto err = services::dispatcher::check_collection_exists(resource(), &resolves, target);
                if (!err.contains_error()) {
                    return err;
                }
                return err.type == core::error_code_t::table_not_exists ? missing_relation_(target) : std::move(err);
            }
            case drop_target_kind::index: {
                // The index's own pg_class row, as resolve found it by name.
                const auto& index_name = drop.index_name().t;
                const qualified_name_t index{target.connection,
                                             target.database,
                                             target.schema,
                                             core::relname_t{index_name}};
                if (!relation_missing(index)) {
                    return core::error_t::no_error();
                }
                if (auto level = services::dispatcher::check_relation_levels(resource(), &resolves, index);
                    level.contains_error()) {
                    return level;
                }
                std::pmr::string msg{"DROP INDEX: index ", resource()};
                msg.append(target.to_string());
                msg.append(".");
                msg.append(index_name);
                msg.append(" does not exist");
                return core::error_t{core::error_code_t::undefined_object, std::move(msg)};
            }
        }
        return core::error_t::no_error();
    }

    core::error_t executor_t::existing_target_(const components::logical_plan::execution_plan_t& plan,
                                               const qualified_name_t& target) const {
        using components::logical_plan::node_type;
        const auto& resolves = plan.catalog_resolves;
        const auto already_exists = [this](core::error_code_t code, std::string_view kind, const std::string& name) {
            std::pmr::string msg{kind, resource()};
            msg.append(" \"");
            msg.append(name);
            msg.append("\" already exists");
            return core::error_t{code, std::move(msg)};
        };
        switch (plan.sub_queries.back()->type()) {
            case node_type::create_database_t:
                if (services::dispatcher::check_database_exists(resource(), &resolves, target).contains_error()) {
                    return core::error_t::no_error();
                }
                return already_exists(core::error_code_t::database_already_exists, "database", target.to_string());
            case node_type::create_schema_t:
                if (services::dispatcher::check_namespace_exists(resource(), &resolves, target).contains_error()) {
                    return core::error_t::no_error();
                }
                return already_exists(core::error_code_t::schema_already_exists, "schema", target.schema.t);
            case node_type::create_collection_t:
                if (services::dispatcher::check_collection_exists(resource(), &resolves, target).contains_error()) {
                    return core::error_t::no_error();
                }
                return core::error_t{core::error_code_t::table_already_exists,
                                     std::pmr::string{"collection already exists", resource()}};
            default:
                return core::error_t::no_error();
        }
    }

    namespace {
        core::error_t only_tables_in_connection(std::pmr::memory_resource* resource, std::string_view connection) {
            std::pmr::string msg{"only tables can be created in a schema of connection \"", resource};
            msg.append(connection);
            msg.append("\"");
            return core::error_t{core::error_code_t::unimplemented_yet, std::move(msg)};
        }
    } // namespace

    core::error_t executor_t::check_connection_target_(const components::logical_plan::execution_plan_t& plan,
                                                       const qualified_name_t& target) const {
        using components::logical_plan::node_type;
        const auto* root = plan.sub_queries.back().get();
        const auto type = root->type();
        // A database of a connection is created and dropped by a host's plan: created empty, and only for a
        // connection the host's map has; dropped with what is in it.
        if (type == node_type::create_database_t) {
            if (auto backend = storage_of_connection_(target.connection.t); backend.has_error()) {
                return backend.error();
            }
            return core::error_t::no_error();
        }
        if (type == node_type::drop_t) {
            if (static_cast<const components::logical_plan::node_drop_t*>(root)->kind() ==
                components::logical_plan::drop_target_kind::database) {
                return core::error_t::no_error();
            }
        }
        const auto refuse = [this](core::error_code_t code, std::string_view what, const std::string& connection) {
            std::pmr::string msg{what, resource()};
            msg.append(" \"");
            msg.append(connection);
            msg.append("\"");
            return core::error_t{code, std::move(msg)};
        };
        // The connection the statement's target is of: written or completed with the place for a CREATE, the
        // resolved table's for what a statement finds.
        std::string connection = target.connection.t;
        if (type == node_type::drop_t) {
            connection = components::catalog::complete_name(plan.catalog_resolves.place,
                                                            target.connection.t,
                                                            target.database.t,
                                                            target.schema.t)
                             .connection;
        }
        if (const auto* table = plan.catalog_resolves.table_md(target); table != nullptr) {
            connection = table->connection;
        }
        if (connection.empty()) {
            return core::error_t::no_error();
        }
        switch (type) {
            case node_type::create_view_t:
            case node_type::create_sequence_t:
            case node_type::create_macro_t:
            case node_type::create_type_t:
                return only_tables_in_connection(resource(), connection);
            case node_type::create_index_t:
            case node_type::create_constraint_t:
                return refuse(core::error_code_t::unimplemented_yet,
                              "indexes and constraints are not kept for a table of connection",
                              connection);
            case node_type::alter_table_t: {
                const auto& alter = static_cast<const components::logical_plan::node_alter_table_t&>(*root);
                for (const auto& sub : alter.subcommands()) {
                    if (sub.kind != components::logical_plan::alter_table_kind::add_column &&
                        sub.kind != components::logical_plan::alter_table_kind::drop_column) {
                        return refuse(core::error_code_t::unimplemented_yet,
                                      "ALTER TABLE of a table of a connection takes ADD COLUMN and DROP COLUMN only; "
                                      "connection",
                                      connection);
                    }
                    if (sub.kind == components::logical_plan::alter_table_kind::add_column &&
                        sub.column.has_default_value()) {
                        return refuse(core::error_code_t::unimplemented_yet,
                                      "keys, checks and defaults are not kept for a table of connection",
                                      connection);
                    }
                }
                break;
            }
            case node_type::create_collection_t: {
                // The backend keeps its own keys, checks, defaults and NOT NULL.
                const auto& create = static_cast<const components::logical_plan::node_create_collection_t&>(*root);
                const bool constrained =
                    !create.constraints().empty() ||
                    std::any_of(
                        create.children().begin(),
                        create.children().end(),
                        [](const auto& child) { return child && child->type() == node_type::create_constraint_t; }) ||
                    std::any_of(create.column_definitions().begin(),
                                create.column_definitions().end(),
                                [](const auto& column) { return column.has_default_value(); });
                if (constrained) {
                    return refuse(core::error_code_t::unimplemented_yet,
                                  "keys, checks and defaults are not kept for a table of connection",
                                  connection);
                }
                break;
            }
            case node_type::create_schema_t:
            case node_type::drop_t:
                break;
            default:
                return core::error_t::no_error();
        }
        // DDL on the backend cannot be undone by a ROLLBACK (#663).
        if (!plan.commits_when_done) {
            std::pmr::string msg{"connection \"", resource()};
            msg.append(connection);
            msg.append("\" only supports DDL in autocommit");
            return core::error_t{core::error_code_t::unimplemented_yet, std::move(msg)};
        }
        return core::error_t::no_error();
    }

    // PostgreSQL 18's order: existence before the target's columns and subcommands, except that a CREATE TABLE
    // without IF NOT EXISTS reports a column named twice before the table that already exists.
    core::result_wrapper_t<executor_t::target_outcome_t>
    executor_t::decide_target_(components::logical_plan::execution_plan_t& plan, const qualified_name_t& target) {
        using components::logical_plan::node_type;
        auto* root = plan.sub_queries.back().get();
        if (root == nullptr) {
            return target_outcome_t::act;
        }
        // A missing level of the name comes first: before IF [NOT] EXISTS and before what a connection's database
        // refuses.
        if (auto missing = missing_target_(plan, target); missing.contains_error()) {
            if (!if_exists_of(*root)) {
                return missing;
            }
            return target_outcome_t::nothing_to_do;
        }
        // IF NOT EXISTS of an object that is there does nothing, as for a local table, before what a connection's
        // database refuses.
        auto existing = existing_target_(plan, target);
        if (existing.contains_error() && if_not_exists_of(*root)) {
            return target_outcome_t::nothing_to_do;
        }
        if (auto refused = check_connection_target_(plan, target); refused.contains_error()) {
            return refused;
        }
        if (root->type() == node_type::create_collection_t) {
            auto* create = static_cast<components::logical_plan::node_create_collection_t*>(root);
            if (auto dup = services::dispatcher::check_column_names_unique(resource(), create->column_definitions());
                dup.contains_error()) {
                return dup;
            }
        }
        if (existing.contains_error()) {
            return existing;
        }
        if (root->type() != node_type::alter_table_t) {
            return target_outcome_t::act;
        }
        auto* alter = static_cast<components::logical_plan::node_alter_table_t*>(root);
        const auto* table = plan.catalog_resolves.table_md(target);
        if (components::catalog::in_system_schema(table->namespace_oid)) {
            // System catalog shape is fixed at bootstrap; altering it desyncs positional column readers.
            return core::error_t{core::error_code_t::sql_parse_error,
                                 std::pmr::string{"cannot alter a system catalog table", resource()}};
        }
        auto checked = check_alter_subcommands_(plan.catalog_resolves, *table, alter->subcommands());
        if (checked.has_error()) {
            return checked.error();
        }
        return checked.value() == alter_subcommands_t::none_left ? target_outcome_t::nothing_to_do
                                                                 : target_outcome_t::act;
    }

    core::error_t executor_t::check_drop_target_(const components::logical_plan::execution_plan_t& plan,
                                                 const qualified_name_t& target) const {
        using components::logical_plan::drop_target_kind;
        using components::logical_plan::node_drop_t;
        const auto* drop_node = static_cast<const node_drop_t*>(plan.sub_queries.back().get());
        auto refuse_built_in = [this](std::string_view kind, const std::string& name) {
            std::pmr::string msg{"cannot drop the built-in ", resource()};
            msg.append(kind);
            msg.append(" ");
            msg.append(name);
            msg.append(": the database system requires it");
            return core::error_t{core::error_code_t::sql_parse_error, std::move(msg)};
        };
        switch (drop_node->kind()) {
            case drop_target_kind::database:
                if (drop_node->database_oid() == components::catalog::well_known_oid::main_database) {
                    return refuse_built_in("database", target.database.t);
                }
                return core::error_t::no_error();
            case drop_target_kind::schema:
                // Every database's information_schema is the system's.
                if (components::catalog::is_catalog_table(drop_node->namespace_oid()) ||
                    target.schema.t == "information_schema") {
                    return refuse_built_in("schema", target.schema.t);
                }
                return core::error_t::no_error();
            case drop_target_kind::collection:
                if (const auto* table = plan.catalog_resolves.table_md(target);
                    table != nullptr && components::catalog::in_system_schema(table->namespace_oid)) {
                    // Same rule as the DML arms — DDL never touches a system catalog (orphans storage).
                    return core::error_t{core::error_code_t::sql_parse_error,
                                         std::pmr::string{"cannot drop a system catalog table", resource()}};
                }
                return core::error_t::no_error();
            case drop_target_kind::type: {
                const std::string& type_name = drop_node->target().collection.t;
                if (components::catalog::pg_name_to_logical_type(type_name) ==
                    components::types::logical_type::UNKNOWN) {
                    return core::error_t::no_error();
                }
                // PostgreSQL 18 dependency.c: a pinned object.
                std::pmr::string msg{"cannot drop type ", resource()};
                msg.append(type_name);
                msg.append(" because it is required by the database system");
                return core::error_t{core::error_code_t::dependent_objects_still_exist, std::move(msg)};
            }
            case drop_target_kind::view:
            case drop_target_kind::materialized_view: {
                const auto* md = plan.catalog_resolves.table_md(target);
                // The statement names the kind it drops (PostgreSQL: wrong object type).
                const char expected = drop_node->kind() == drop_target_kind::view
                                          ? components::catalog::relkind::view
                                          : components::catalog::relkind::materialized_view;
                if (md != nullptr && md->relkind != expected) {
                    return wrong_relation_kind_to_drop(resource(), target.collection.t, expected, md->relkind);
                }
                return core::error_t::no_error();
            }
            case drop_target_kind::sequence:
            case drop_target_kind::macro:
            case drop_target_kind::index:
                return core::error_t::no_error();
        }
        return core::error_t::no_error();
    }

    // CREATE [OR REPLACE] VIEW: the body is validated, its columns, bindings and dependencies described onto the node,
    // and the user functions it calls bound to their pg_proc rows.
    executor_t::unique_future<core::error_t>
    executor_t::validate_create_view_(components::session::session_id_t session,
                                      components::table::transaction_data txn,
                                      components::logical_plan::execution_plan_t& plan,
                                      const components::graph_execution_context& settings,
                                      statement_registries_t registries,
                                      own_entries_t own,
                                      const std::pmr::vector<expanded_view_t>& expanded_views) {
        auto* view = static_cast<components::logical_plan::node_create_view_t*>(plan.sub_queries.back().get());
        const auto body = view->body();
        services::dispatcher::resolve_expression_types(body, &plan.catalog_resolves);
        if (auto vt_err =
                services::dispatcher::validate_types(resource(), &plan.catalog_resolves, body.get(), settings);
            vt_err.contains_error()) {
            co_return vt_err;
        }
        services::dispatcher::validation::column_uses_t uses{resource()};
        const services::dispatcher::validation::validation_context_t validation_context{resource(),
                                                                                        &plan.catalog_resolves,
                                                                                        *registries.casts,
                                                                                        *registries.functions,
                                                                                        settings,
                                                                                        &uses};
        auto output =
            services::dispatcher::validate_schema(validation_context, body.get(), plan.parameters->parameters());
        if (output.has_error()) {
            co_return core::error_on(resource(), output.error());
        }
        const auto* existing =
            view->replace() ? plan.catalog_resolves.table_md(qualified_name_t{view->target().connection,
                                                                              view->target().database,
                                                                              view->target().schema,
                                                                              core::relname_t{view->viewname().t}})
                            : nullptr;
        if (existing != nullptr) {
            view->set_replaced_oid(existing->table_oid);
        }
        if (auto described = describe_view_body(resource(),
                                                *view,
                                                output.value(),
                                                plan.catalog_resolves,
                                                own.tables,
                                                own.types,
                                                uses);
            described.contains_error()) {
            co_return described;
        }
        if (existing != nullptr) {
            std::pmr::vector<components::catalog::oid_t> read_views{resource()};
            for (const auto& expanded : expanded_views) {
                read_views.push_back(expanded.view.table_oid);
            }
            if (auto replaced = check_view_replacement(resource(), *view, *existing, read_views);
                replaced.contains_error()) {
                co_return replaced;
            }
        }
        const auto functions = view_body_user_functions(resource(), body.get());
        if (functions.empty()) {
            co_return core::error_t::no_error();
        }
        assert(disk_address_ != actor_zeta::address_t::empty_address() && "an executor always has a disk");
        const components::execution_context_t proc_ctx{session, txn, {}};
        std::pmr::vector<std::string> names{resource()};
        std::pmr::vector<services::disk::resolve_function_result_t> rows{resource()};
        for (const auto& use : functions) {
            const auto* function = registries.functions->get_function(use.uid);
            if (function == nullptr || std::find(names.begin(), names.end(), function->name()) != names.end()) {
                continue;
            }
            names.emplace_back(function->name());
            auto [_rf, rff] = actor_zeta::otterbrix::send(disk_address_,
                                                          &services::disk::manager_disk_t::resolve_function_by_name,
                                                          proc_ctx,
                                                          std::string{function->name()});
            auto procs = co_await std::move(rff);
            if (procs.has_error()) {
                co_return core::error_on(resource(), procs.error());
            }
            rows.insert(rows.end(), procs.value().begin(), procs.value().end());
        }
        co_return describe_view_functions(resource(), *view, *registries.functions, functions, rows);
    }

    // ALTER TABLE's subcommands against what resolve found, before any operator runs. A DROP CONSTRAINT gets its
    // constraint's oid here; a missing one is refused. A subcommand written with IF EXISTS whose column / constraint is
    // missing is skipped alone (PostgreSQL: a notice); the others still apply, and the operators keep refusing a
    // missing column.
    core::result_wrapper_t<executor_t::alter_subcommands_t>
    executor_t::check_alter_subcommands_(const components::logical_plan::catalog_resolves_t& resolves,
                                         const components::logical_plan::resolved_table_metadata_t& table,
                                         std::vector<components::logical_plan::alter_table_subcommand_t>& subcommands) {
        using components::logical_plan::alter_table_kind;
        // The columns as each subcommand finds them: the earlier subcommands of the statement applied, as PostgreSQL
        // 18 checks them one after another. All of them are checked before any runs, so a connection's backend sees
        // none of a statement that is refused.
        const bool ordinary = table.relkind == components::catalog::relkind::regular;
        std::pmr::vector<std::string> names{resource()};
        for (const auto& column : table.columns) {
            names.push_back(column.attname);
        }
        for (std::size_t index = 0; index < subcommands.size();) {
            auto& sub = subcommands[index];
            bool missing = false;
            if (sub.kind == alter_table_kind::drop_column) {
                const auto found = std::find(names.begin(), names.end(), sub.column_name);
                missing = found == names.end();
                if (!missing && ordinary) {
                    names.erase(found);
                } else if (missing && !sub.if_exists && ordinary) {
                    std::pmr::string msg{"column \"", resource()};
                    msg.append(sub.column_name);
                    msg.append("\" of relation \"");
                    msg.append(table.name);
                    msg.append("\" does not exist; use DROP COLUMN IF EXISTS to ignore it");
                    return core::error_t{core::error_code_t::undefined_column, std::move(msg)};
                }
            } else if (sub.kind == alter_table_kind::add_column && ordinary) {
                if (auto duplicate =
                        components::catalog::alter_column_validators::validate_column_not_duplicate(resource(),
                                                                                                    names,
                                                                                                    sub.column.name());
                    duplicate.contains_error()) {
                    return duplicate;
                }
                names.push_back(sub.column.name());
            } else if (sub.kind == alter_table_kind::drop_constraint) {
                sub.constraint_oid = resolves.constraint_oid(table.table_oid, sub.constraint_name);
                missing = sub.constraint_oid == components::catalog::INVALID_OID;
                if (missing && !sub.if_exists) {
                    std::pmr::string msg{"constraint \"", resource()};
                    msg.append(sub.constraint_name);
                    msg.append("\" of relation \"");
                    msg.append(table.name);
                    msg.append("\" does not exist");
                    return core::error_t{core::error_code_t::undefined_object, std::move(msg)};
                }
            }
            if (missing && sub.if_exists) {
                info(log_,
                     "notice: {} \"{}\" of relation \"{}\" does not exist, skipping",
                     sub.kind == alter_table_kind::drop_column ? "column" : "constraint",
                     sub.kind == alter_table_kind::drop_column ? sub.column_name : sub.constraint_name,
                     table.name);
                subcommands.erase(subcommands.begin() + static_cast<std::ptrdiff_t>(index));
                continue;
            }
            ++index;
        }
        return subcommands.empty() ? alter_subcommands_t::none_left : alter_subcommands_t::remain;
    }

    namespace {
        // A CREATE without a database or schema creates in the session's place (PostgreSQL 18: the first schema
        // of the search path); CREATE SCHEMA in the place's database.
        void complete_create_target(components::logical_plan::node_t& root, const components::catalog::place_t& place) {
            using components::logical_plan::node_type;
            switch (root.type()) {
                case node_type::create_collection_t:
                case node_type::create_view_t:
                case node_type::create_sequence_t:
                case node_type::create_macro_t:
                case node_type::create_type_t:
                case node_type::create_schema_t:
                    break;
                default:
                    return;
            }
            auto target = root.target();
            auto completed =
                components::catalog::complete_name(place, target.connection.t, target.database.t, target.schema.t);
            target.connection = core::connection_name_t{std::move(completed.connection)};
            target.database = core::dbname_t{std::move(completed.database)};
            target.schema = core::schema_t{std::move(completed.schema)};
            root.set_target(std::move(target));
        }

        // The schema a call is looked up in, as written: none for a bare call or pg_catalog's (the place's schema
        // completes a bare one).
        bool names_a_user_schema(const function_qualified_name_t& name) {
            return !name.schema.t.empty() && name.schema.t != "pg_catalog";
        }

        qualified_name_t schema_of_call(const function_qualified_name_t& name) {
            return qualified_name_t{name.database, name.schema, core::relname_t{}};
        }

        // a call reaches functions as a relation name reaches relations. The schemas the calls name
        // (the place's, for a bare call of a name a user function has) are resolved with the statement.
        void register_call_schemas(std::pmr::memory_resource* resource,
                                   components::logical_plan::node_t* root,
                                   const components::compute::function_registry_t& registry,
                                   components::logical_plan::catalog_resolves_t& resolves) {
            const auto register_schema = [&](const function_qualified_name_t& name) {
                const bool names_the_place = name.function.t == components::compute::current_database_name ||
                                             name.function.t == components::compute::current_schema_name;
                if (names_a_user_schema(name)) {
                    components::sql::transform::register_catalog_resolve_namespace(resource,
                                                                                   &resolves,
                                                                                   schema_of_call(name));
                    return;
                }
                const auto uids = registry.find_functions(name.function.t);
                const bool user_function = name.schema.t.empty() && std::any_of(uids.begin(), uids.end(), [](auto uid) {
                                               return !components::compute::is_builtin(uid);
                                           });
                if (user_function || names_the_place) {
                    resolves.ensure(resource, components::logical_plan::resolve_kind::namespace_)
                        .add(components::logical_plan::resolve_entry_t{});
                }
            };
            auto visit = [&](const auto& call, const auto&, bool) { register_schema(call.full_name()); };
            components::logical_plan::for_each_plan_call(root, visit);
        }

        // Whether a pg_proc row of `function` has these proargmatchers; a function without a signature has one row,
        // with none (proc_signatures).
        bool has_signature(const components::compute::function& function, std::string_view proargmatchers) {
            const auto signatures = function.get_signatures();
            if (signatures.empty()) {
                return proargmatchers.empty();
            }
            return std::any_of(signatures.begin(), signatures.end(), [proargmatchers](const auto& signature) {
                return components::catalog::encode_proargmatchers(signature.input_types) == proargmatchers;
            });
        }

        // A function is registered in and dropped from the session's place; the types of a cast the host named are
        // read from the catalog.
        void register_registry_change_names(std::pmr::memory_resource* resource,
                                            const components::logical_plan::node_t* root,
                                            components::logical_plan::catalog_resolves_t& resolves) {
            using components::logical_plan::node_type;
            if (root == nullptr) {
                return;
            }
            const auto register_types = [&](const components::types::complex_logical_type& source,
                                            const components::types::complex_logical_type& target) {
                std::vector<qualified_name_t> written;
                for (const auto* type : {&source, &target}) {
                    if (type->type() == components::types::logical_type::UNKNOWN) {
                        written.push_back(components::sql::transform::type_reference_of(*type));
                    }
                }
                components::sql::transform::register_catalog_resolve_types(resource, &resolves, written);
            };
            switch (root->type()) {
                case node_type::register_udf_t:
                case node_type::unregister_udf_t:
                    resolves.ensure(resource, components::logical_plan::resolve_kind::namespace_)
                        .add(components::logical_plan::resolve_entry_t{});
                    return;
                case node_type::register_cast_t: {
                    const auto* cast = static_cast<const components::logical_plan::node_register_cast_t*>(root);
                    register_types(cast->source(), cast->target());
                    return;
                }
                case node_type::unregister_cast_t: {
                    const auto* cast = static_cast<const components::logical_plan::node_unregister_cast_t*>(root);
                    register_types(cast->source(), cast->target());
                    return;
                }
                default:
                    return;
            }
        }

        // The schema a call is looked up in: a qualified call's own; a bare call the place's, a bare call in a CHECK
        // its table's schema's whatever the place: CREATE TABLE's target schema, the constrained table's for ADD
        // CONSTRAINT and a write.
        void scope_calls(components::logical_plan::node_t* root,
                         const components::logical_plan::catalog_resolves_t& resolves) {
            using components::logical_plan::node_type;
            const auto table_schema = [&](const components::logical_plan::node_t& owner) {
                if (root->type() == node_type::create_collection_t) {
                    return resolves.namespace_oid(root->target());
                }
                if (const auto* table = owner.table_metadata(); table != nullptr) {
                    return table->namespace_oid;
                }
                const auto* table = resolves.table_md(owner.target());
                return table != nullptr ? table->namespace_oid : components::catalog::INVALID_OID;
            };
            auto visit = [&](auto& call, const components::logical_plan::node_t& owner, bool in_check) {
                const auto& name = call.full_name();
                components::compute::function_scope_t scope{.builtins = true, .schema_oid = 0};
                if (names_a_user_schema(name)) {
                    scope.builtins = false;
                    scope.schema_oid = resolves.namespace_oid(schema_of_call(name));
                } else if (name.schema.t.empty()) {
                    scope.schema_oid = in_check ? table_schema(owner) : resolves.namespace_oid(qualified_name_t{});
                }
                call.set_scope(scope);
            };
            components::logical_plan::for_each_plan_call(root, visit);
        }

        // current_database() / current_schema(): the place's name as the catalog has it, NULL when that level is
        // gone (PostgreSQL 18 answers a dropped schema so); a written argument is no call of theirs.
        core::error_t bind_place_calls(std::pmr::memory_resource* resource,
                                       components::logical_plan::node_t* root,
                                       const components::logical_plan::catalog_resolves_t& resolves,
                                       components::logical_plan::parameter_node_t& parameters) {
            const auto* place = resolves.namespace_entry(qualified_name_t{});
            core::error_t refused = core::error_t::no_error();
            auto visit = [&](auto& call, const auto&, bool) {
                if constexpr (std::is_same_v<std::decay_t<decltype(call)>,
                                             components::expressions::function_expression_t>) {
                    const auto& name = call.full_name();
                    const bool database = name.function.t == components::compute::current_database_name;
                    if (names_a_user_schema(name) ||
                        (!database && name.function.t != components::compute::current_schema_name)) {
                        return;
                    }
                    if (!call.args().empty()) {
                        if (!refused.contains_error()) {
                            refused = core::error_t{
                                core::error_code_t::unrecognized_function,
                                std::pmr::string{"function " + name.to_string() + " takes no arguments", resource}};
                        }
                        return;
                    }
                    const bool exists = place != nullptr && (database ? place->database_oid : place->namespace_oid) !=
                                                                components::catalog::INVALID_OID;
                    const auto& value = database ? resolves.place.database : resolves.place.schema;
                    auto bound =
                        exists ? components::vector::make_scalar(resource, std::string_view{value})
                               : components::vector::make_null(resource,
                                                               components::types::complex_logical_type{
                                                                   components::types::logical_type::STRING_LITERAL});
                    call.args().emplace_back(
                        parameters.add_parameter(std::move(bound), components::vector::parameter_kind::literal));
                }
            };
            components::logical_plan::for_each_plan_call(root, visit);
            return refused;
        }

        // SET [LOCAL] search_path = '<place>': the place completed with the current one.
        struct set_place_t {
            bool sets{false};
            components::catalog::place_t place;
        };

        core::result_wrapper_t<set_place_t> place_set_by(std::pmr::memory_resource* resource,
                                                         const components::logical_plan::node_t* root,
                                                         const components::catalog::place_t& current) {
            if (root == nullptr || root->type() != components::logical_plan::node_type::set_setting_t) {
                return set_place_t{};
            }
            const auto* set = static_cast<const components::logical_plan::node_set_setting_t*>(root);
            if (set->scope() == components::catalog::setting_scope_t::system) {
                return set_place_t{};
            }
            for (const auto& assignment : set->assignments()) {
                if (assignment.id != components::catalog::setting_id::search_path || assignment.is_default) {
                    continue;
                }
                auto canonical = components::catalog::canonical_search_path(assignment.value, resource);
                if (canonical.has_error()) {
                    return canonical.error();
                }
                return set_place_t{true, components::catalog::complete_place(canonical.value(), current)};
            }
            return set_place_t{};
        }

        // The SET with its search_path spelled as the session keeps it: the full place.
        components::logical_plan::node_ptr with_full_place(std::pmr::memory_resource* resource,
                                                           const components::logical_plan::node_t* root,
                                                           const components::catalog::place_t& place) {
            const auto* set = static_cast<const components::logical_plan::node_set_setting_t*>(root);
            std::pmr::vector<components::catalog::setting_assignment_t> assignments{resource};
            for (const auto& assignment : set->assignments()) {
                if (assignment.is_default) {
                    assignments.push_back(
                        components::catalog::setting_assignment_t::to_default(assignment.id, resource));
                } else if (assignment.id == components::catalog::setting_id::search_path) {
                    assignments.emplace_back(assignment.id, place.to_string(), resource);
                } else {
                    assignments.emplace_back(assignment.id, assignment.value, resource);
                }
            }
            return components::logical_plan::make_node_set_setting(resource, set->scope(), std::move(assignments));
        }
    } // namespace

    core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>
    executor_t::parameter_types_of_(const components::logical_plan::execution_plan_t& plan) const {
        using components::logical_plan::node_type;
        using components::types::complex_logical_type;
        using components::types::logical_type;
        const auto* root = services::dispatcher::validation::output_node_of(plan.sub_queries.back().get());
        const components::logical_plan::node_insert_t* insert = nullptr;
        if (root != nullptr && root->type() == node_type::insert_t) {
            insert = static_cast<const components::logical_plan::node_insert_t*>(root);
        } else if (root != nullptr && root->type() == node_type::insert_on_conflict_t) {
            insert = static_cast<const components::logical_plan::node_insert_on_conflict_t*>(root)->insert();
        }

        // A VALUES column of the INSERT: its own type, or the target column's while it holds only untyped $n
        // (PostgreSQL 18 transformInsertRow coerces each one to its column).
        auto insert_column_type = [&](std::size_t column) {
            if (insert == nullptr || insert->children().empty() ||
                insert->children().front()->type() != node_type::data_t) {
                return complex_logical_type{logical_type::NA};
            }
            const auto* data =
                static_cast<const components::logical_plan::node_data_t*>(insert->children().front().get());
            if (data->chunks().empty() || column >= data->chunks().front().column_count()) {
                return complex_logical_type{logical_type::NA};
            }
            auto type = data->chunks().front().data[column].type();
            if (type.type() == logical_type::NA && column < insert->column_bindings().size()) {
                type = insert->column_bindings()[column].target_type;
            }
            return type;
        };
        auto undetermined = [&](std::size_t number) {
            return core::error_t{
                core::error_code_t::indeterminate_datatype,
                std::pmr::string{"could not determine data type of parameter $" + std::to_string(number), resource()}};
        };

        std::pmr::vector<complex_logical_type> parameters{resource()};
        for (const auto& parameter : plan.external_parameters) {
            // PostgreSQL 18 pg_analyze_and_rewrite_varparams: a number the statement skips has no type.
            if (parameters.size() + 1 < parameter.number) {
                return undetermined(parameters.size() + 1);
            }
            std::optional<complex_logical_type> type;
            auto take = [&](complex_logical_type candidate) {
                if (candidate.type() == logical_type::NA) {
                    return true;
                }
                candidate.set_alias(std::string{});
                if (type && !(*type == candidate)) {
                    return false;
                }
                type = std::move(candidate);
                return true;
            };
            bool consistent = true;
            if (parameter.bound_type.type() != logical_type::NA) {
                consistent = take(parameter.bound_type);
            } else {
                if (parameter.slot) {
                    consistent = take(plan.parameters->parameters().type(*parameter.slot));
                }
                for (const auto column : parameter.insert_columns) {
                    consistent = consistent && take(insert_column_type(column));
                }
            }
            if (!consistent) {
                return core::error_t{
                    core::error_code_t::ambiguous_parameter,
                    std::pmr::string{"inconsistent types deduced for parameter $" + std::to_string(parameter.number),
                                     resource()}};
            }
            if (!type) {
                return undetermined(parameter.number);
            }
            parameters.push_back(std::move(*type));
        }
        return parameters;
    }

    std::pmr::vector<components::types::complex_logical_type>
    executor_t::result_columns_(const components::logical_plan::execution_plan_t& plan,
                                const services::dispatcher::validation::named_schema& returning) const {
        using components::logical_plan::node_type;
        const auto* root = services::dispatcher::validation::output_node_of(plan.sub_queries.back().get());
        std::pmr::vector<components::types::complex_logical_type> columns{resource()};
        const node_type kind = root ? root->type() : node_type::unused;
        if (kind == node_type::insert_t || kind == node_type::insert_on_conflict_t || kind == node_type::update_t ||
            kind == node_type::delete_t) {
            for (const auto& column : returning) {
                columns.push_back(column.type);
            }
        } else if (root != nullptr && root->has_output_types()) {
            columns.assign(root->output_types().begin(), root->output_types().end());
        }
        return columns;
    }

    cursor_t_ptr
    executor_t::describe_(const components::logical_plan::execution_plan_t& plan,
                          const services::dispatcher::validation::named_schema& returning,
                          const std::pmr::vector<components::types::complex_logical_type>& parameters) const {
        std::pmr::vector<components::types::complex_logical_type> columns{resource()};
        if (plan.explain == components::logical_plan::explain_type::describe_explain) {
            columns.emplace_back(components::types::logical_type::STRING_LITERAL, "QUERY PLAN");
        } else {
            columns = result_columns_(plan, returning);
        }
        auto described = make_cursor(resource(), std::move(columns));
        described->set_parameter_types(parameters);
        return described;
    }

    core::result_wrapper_t<services::dispatcher::registry_changes_t>
    executor_t::prepare_registry_change_(components::logical_plan::execution_plan_t& plan,
                                         statement_registries_t registries,
                                         components::compute::function_uid reserved_uid) {
        using components::logical_plan::node_type;
        using components::types::logical_type;
        services::dispatcher::registry_changes_t change;
        auto* root = plan.sub_queries.back().get();
        if (root == nullptr) {
            return change;
        }
        const auto& resolves = plan.catalog_resolves;
        const auto place_schema = [&]() -> core::result_wrapper_t<components::catalog::oid_t> {
            if (auto missing = services::dispatcher::check_namespace_exists(resource(), &resolves, qualified_name_t{});
                missing.contains_error()) {
                return missing;
            }
            if (!resolves.place.connection.empty()) {
                return only_tables_in_connection(resource(), resolves.place.connection);
            }
            return resolves.namespace_oid(qualified_name_t{});
        };
        const auto cast_types = [&](const components::types::complex_logical_type& source,
                                    const components::types::complex_logical_type& target)
            -> core::result_wrapper_t<
                std::pair<components::types::complex_logical_type, components::types::complex_logical_type>> {
            auto resolved_source = source;
            auto resolved_target = target;
            services::dispatcher::resolve_one_type(resolved_source, &resolves);
            services::dispatcher::resolve_one_type(resolved_target, &resolves);
            if (resolved_source.type() == logical_type::UNKNOWN || resolved_target.type() == logical_type::UNKNOWN) {
                return core::error_t{core::error_code_t::undefined_object,
                                     std::pmr::string{"cast source or target type is not registered", resource()}};
            }
            return std::make_pair(std::move(resolved_source), std::move(resolved_target));
        };
        switch (root->type()) {
            case node_type::register_udf_t: {
                auto* node = static_cast<components::logical_plan::node_register_udf_t*>(root);
                if (node->function() == nullptr) {
                    return core::error_t{core::error_code_t::invalid_parameter,
                                         std::pmr::string{"register_udf: no function to register", resource()}};
                }
                auto schema = place_schema();
                if (schema.has_error()) {
                    return schema.error();
                }
                const std::string name{node->function()->name()};
                const auto signatures = node->function()->get_signatures();
                for (const auto& [registered_name, uid] : registries.functions->get_functions()) {
                    if (registered_name != name || (!components::compute::is_builtin(uid) &&
                                                    registries.functions->schema_of(uid) != schema.value())) {
                        continue;
                    }
                    const auto* registered = registries.functions->get_function(uid);
                    if (registered == nullptr ||
                        components::compute::check_signature_conflicts(registered->get_signatures(), signatures)) {
                        continue;
                    }
                    return core::error_t{
                        core::error_code_t::duplicate_function,
                        std::pmr::string{"function '" + name + "' is already registered with this signature",
                                         resource()}};
                }
                assert(reserved_uid != components::compute::invalid_function_uid &&
                       "the dispatcher reserves the uid of every register_udf");
                node->set_identity(reserved_uid, schema.value());
                change.functions.push_back(
                    services::dispatcher::function_change_t{reserved_uid,
                                                            schema.value(),
                                                            node->function()->get_copy(resource())});
                return change;
            }
            case node_type::unregister_udf_t: {
                auto* node = static_cast<components::logical_plan::node_unregister_udf_t*>(root);
                const auto& name = node->function_name();
                // A builtin is required by the system, as in PostgreSQL, which refuses to drop one.
                const auto named = registries.functions->find_functions(name);
                const auto builtin_overload = [&](components::compute::function_uid uid) {
                    const auto* function = registries.functions->get_function(uid);
                    if (!components::compute::is_builtin(uid) || function == nullptr) {
                        return false;
                    }
                    const auto signatures = function->get_signatures();
                    return std::any_of(signatures.begin(), signatures.end(), [&](const auto& signature) {
                        return signature.matches_inputs(node->inputs());
                    });
                };
                const bool builtin = std::any_of(named.begin(), named.end(), builtin_overload);
                if (builtin) {
                    return core::error_t{core::error_code_t::dependent_objects_still_exist,
                                         std::pmr::string{"unregister_udf: '" + name +
                                                              "' is a builtin function and cannot be unregistered",
                                                          resource()}};
                }
                auto schema = place_schema();
                if (schema.has_error()) {
                    return schema.error();
                }
                node->set_schema_oid(schema.value());
                const auto uid = registries.functions->find_overload(name, node->inputs(), schema.value());
                if (uid != components::compute::invalid_function_uid) {
                    change.functions.push_back(
                        services::dispatcher::function_change_t{uid,
                                                                schema.value(),
                                                                components::compute::no_function()});
                }
                return change;
            }
            case node_type::register_cast_t: {
                auto* node = static_cast<components::logical_plan::node_register_cast_t*>(root);
                auto types = cast_types(node->source(), node->target());
                if (types.has_error()) {
                    return types.error();
                }
                auto& [source, target] = types.value();
                if (registries.casts->contains(source, target)) {
                    return core::error_t{core::error_code_t::duplicate_object,
                                         std::pmr::string{"cast is already registered", resource()}};
                }
                change.casts.push_back(services::dispatcher::cast_change_t{source, target, node->entry()});
                node->set_types(std::move(source), std::move(target));
                return change;
            }
            case node_type::unregister_cast_t: {
                auto* node = static_cast<components::logical_plan::node_unregister_cast_t*>(root);
                auto types = cast_types(node->source(), node->target());
                if (types.has_error()) {
                    return types.error();
                }
                auto& [source, target] = types.value();
                if (!registries.casts->contains(source, target)) {
                    return core::error_t{core::error_code_t::undefined_object,
                                         std::pmr::string{"cast is not registered", resource()}};
                }
                change.casts.push_back(services::dispatcher::cast_change_t{source, target, std::nullopt});
                node->set_types(std::move(source), std::move(target));
                return change;
            }
            default:
                return change;
        }
    }

    executor_t::unique_future<core::error_t>
    executor_t::run_sub_queries_(components::session::session_id_t session,
                                 components::logical_plan::execution_plan_t& plan,
                                 const services::dispatcher::txn_session_context_t& session_ctx,
                                 statement_registries_t registries,
                                 components::logical_plan::explain_type explain,
                                 std::pmr::vector<explain_plan_node>& captured_subplans) {
        using node_type = components::logical_plan::node_type;
        const bool plan_only = explain == components::logical_plan::explain_type::plan;
        const bool capture_ir = explain == components::logical_plan::explain_type::analyze;
        const bool describe = components::logical_plan::is_describe(explain);
        for (std::size_t i = 0; i + 1 < plan.sub_queries.size(); ++i) {
            auto* sub_root = plan.sub_queries[i].get();
            const node_type sub_type = sub_root ? sub_root->type() : node_type::unused;
            if (sub_type == node_type::insert_t || sub_type == node_type::insert_on_conflict_t ||
                sub_type == node_type::update_t || sub_type == node_type::delete_t) {
                co_return core::error_t{core::error_code_t::sql_parse_error,
                                        std::pmr::string{"DML statement is not allowed in a sub-query", resource()}};
            }
            components::logical_plan::execution_plan_t sub_plan{resource(), plan.sub_queries[i], plan.parameters};
            sub_plan.external_parameters = plan.external_parameters;
            if (describe) {
                sub_plan.explain = components::logical_plan::explain_type::describe;
            } else if (plan_only) {
                sub_plan.explain = components::logical_plan::explain_type::plan;
                sub_plan.explain_capture_ir = true;
            } else if (capture_ir) {
                sub_plan.explain = components::logical_plan::explain_type::analyze;
                sub_plan.explain_capture_ir = true;
            }
            auto sub_result = co_await execute_statement_(session,
                                                          std::move(sub_plan),
                                                          session_ctx,
                                                          registries,
                                                          std::pmr::vector<expanded_view_t>{resource()},
                                                          true);
            if (sub_result.cursor->is_error()) {
                co_return core::error_on(resource(), sub_result.cursor->get_error());
            }
            const auto& mapping = plan.sub_query_results[i];
            const auto& sub_node = plan.sub_queries[i];

            auto result_type = plan.parameters->parameters().type(mapping.id);
            if (components::vector::is_placeholder(result_type)) {
                if (!sub_node->has_output_types() || sub_node->output_types().size() != 1) {
                    co_return core::error_t{core::error_code_t::sql_parse_error,
                                            std::pmr::string{"subquery must return only one column", resource()}};
                }
                result_type = components::vector::filled_placeholder(result_type, sub_node->output_types().front());
            }

            if (mapping.boolean_required) {
                const auto out_type = sub_node->output_types().front().type();
                if (out_type != components::types::logical_type::BOOLEAN &&
                    out_type != components::types::logical_type::NA) {
                    co_return core::error_t{
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"argument of WHERE/HAVING must be type boolean", resource()}};
                }
            }
            if ((capture_ir || plan_only) && sub_result.captured_explain_ir.has_value()) {
                sub_result.captured_explain_ir->subplan_returns = static_cast<uint32_t>(mapping.id);
                captured_subplans.push_back(std::move(*sub_result.captured_explain_ir));
            }
            if (plan_only || describe) {
                // Nothing runs: a NULL of the result's type stands for it in the plan the EXPLAIN renders.
                plan.parameters->set_parameter(mapping.id, components::vector::make_null(resource(), result_type));
                continue;
            }
            trace(log_,
                  "DBG subq[{}] cursor success={} size={} cols={}",
                  i,
                  sub_result.cursor->is_success(),
                  sub_result.cursor->size(),
                  sub_result.cursor->column_count());
            auto compacted = mapping.compacter(resource(), sub_result.cursor->chunks(), result_type);
            trace(log_, "DBG subq[{}] compacted has_error={}", i, compacted.has_error());
            if (compacted.has_error()) {
                co_return core::error_on(resource(), compacted.error());
            }
            plan.parameters->set_parameter(mapping.id, std::move(compacted.value()));
        }

        co_return core::error_t::no_error();
    }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_statement_(components::session::session_id_t session,
                                   components::logical_plan::execution_plan_t plan,
                                   services::dispatcher::txn_session_context_t session_ctx,
                                   statement_registries_t registries,
                                   std::pmr::vector<expanded_view_t> expanded_views,
                                   bool sub_query) {
        using node_type = components::logical_plan::node_type;
        using components::logical_plan::node_aggregate_t;
        using components::logical_plan::node_catalog_resolve_t;
        using components::logical_plan::node_join_t;
        using components::logical_plan::node_match_t;
        using components::logical_plan::node_ptr;
        using components::logical_plan::node_sequence_t;
        using components::logical_plan::node_t;
        using components::logical_plan::resolve_kind;

        const bool describe = components::logical_plan::is_describe(plan.explain);
        if (describe && plan.sub_queries.back() && is_utility(*plan.sub_queries.back())) {
            co_return execute_result_t{make_cursor(resource())};
        }
        if (auto* statement = plan.sub_queries.back().get(); statement != nullptr) {
            switch (statement->type()) {
                case node_type::execute_t:
                    co_return co_await execute_prepared_(session, std::move(plan), std::move(session_ctx), registries);
                case node_type::prepare_t:
                    if (auto refused = co_await describe_prepared_(
                            session,
                            *static_cast<components::logical_plan::node_prepare_t*>(statement),
                            session_ctx,
                            registries);
                        refused.contains_error()) {
                        co_return execute_result_t{make_cursor(resource(), std::move(refused))};
                    }
                    break;
                case node_type::deallocate_t:
                    static_cast<components::logical_plan::node_deallocate_t*>(statement)->set_exists(
                        session_ctx.prepared.has_value());
                    break;
                default:
                    break;
            }
        }

        std::pmr::vector<explain_plan_node> captured_subplans{resource()};
        if (auto failed =
                co_await run_sub_queries_(session, plan, session_ctx, registries, plan.explain, captured_subplans);
            failed.contains_error()) {
            co_return execute_result_t{make_cursor(resource(), std::move(failed))};
        }

        components::table::transaction_data resolve_txn = session_ctx.txn;
        trace(log_,
              "executor::execute_plan_full: session txn {}, ends with the statement: {}, session: {}",
              resolve_txn.transaction_id,
              plan.commits_when_done,
              session.data());

        const node_type original_type = [&] {
            auto* r = plan.sub_queries.back().get();
            return r ? r->type() : node_type::unused;
        }();

        const auto place = components::catalog::place_of(session_ctx.settings.search_path);
        plan.catalog_resolves.place = place;
        if (auto* root = plan.sub_queries.back().get()) {
            complete_create_target(*root, place);
        }
        auto set_place = place_set_by(resource(), plan.sub_queries.back().get(), place);
        if (set_place.has_error()) {
            co_return execute_result_t{make_cursor(resource(), set_place.error())};
        }
        const qualified_name_t set_place_name{core::connection_name_t{set_place.value().place.connection},
                                              core::dbname_t{set_place.value().place.database},
                                              core::schema_t{set_place.value().place.schema},
                                              core::relname_t{}};
        if (set_place.value().sets) {
            components::sql::transform::register_catalog_resolve_namespace(resource(),
                                                                           &plan.catalog_resolves,
                                                                           set_place_name);
        }

        services::dispatcher::register_plan_targets(resource(), plan.sub_queries.back().get(), &plan.catalog_resolves);
        register_call_schemas(resource(), plan.sub_queries.back().get(), *registries.functions, plan.catalog_resolves);
        register_registry_change_names(resource(), plan.sub_queries.back().get(), plan.catalog_resolves);
        services::context_storage_t context_storage(resource(), log_.clone(), session_ctx.settings);
        context_storage.place = place;
        context_storage.disk_address = disk_address_;

        if (original_type == node_type::transaction_t && plan.explain == components::logical_plan::explain_type::none &&
            static_cast<const components::logical_plan::node_transaction_t*>(plan.sub_queries.back().get())->op() ==
                components::logical_plan::transaction_op::commit) {
            co_return co_await run_commit_pipeline_(session,
                                                    resolve_txn,
                                                    context_storage.execution_context,
                                                    session_ctx.lowest_active_start_time,
                                                    /*ddl_mode=*/false);
        }

        const bool needs_ddl_txn =
            original_type == node_type::create_collection_t || original_type == node_type::create_constraint_t ||
            original_type == node_type::create_sequence_t || original_type == node_type::create_view_t ||
            original_type == node_type::create_macro_t || original_type == node_type::create_type_t ||
            original_type == node_type::create_index_t || original_type == node_type::drop_t ||
            original_type == node_type::create_database_t || original_type == node_type::create_schema_t ||
            original_type == node_type::alter_table_t || original_type == node_type::register_udf_t ||
            original_type == node_type::unregister_udf_t || original_type == node_type::register_cast_t ||
            original_type == node_type::unregister_cast_t;
        const bool is_plan_only_explain = plan.explain == components::logical_plan::explain_type::plan;
        const bool needs_dml_txn =
            !is_plan_only_explain &&
            (original_type == node_type::insert_t || original_type == node_type::insert_on_conflict_t ||
             original_type == node_type::update_t || original_type == node_type::delete_t);
        const bool alters_system =
            original_type == node_type::set_setting_t &&
            static_cast<const components::logical_plan::node_set_setting_t*>(plan.sub_queries.back().get())->scope() ==
                components::catalog::setting_scope_t::system;
        const bool needs_commit_txn = alters_system || original_type == node_type::vacuum_t;
        const bool modified_rows = needs_dml_txn && !has_returning(plan.sub_queries.back().get());

        auto collect_resolve_nodes = [](const components::logical_plan::catalog_resolves_t& resolves,
                                        std::pmr::vector<components::logical_plan::node_ptr>& out) {
            for (const auto* slot :
                 {&resolves.database, &resolves.namespaces, &resolves.tables, &resolves.types, &resolves.constraints}) {
                if (*slot && !(*slot)->empty()) {
                    out.push_back(*slot);
                }
            }
        };

        // The statement's own names come first; the entries a view expansion adds come after them.
        const own_entries_t own = own_entries_of(plan.catalog_resolves);
        {
            std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes{resource()};
            collect_resolve_nodes(plan.catalog_resolves, resolve_nodes);
            if (!resolve_nodes.empty()) {
                auto pass1_result = co_await run_resolve_subplan_(session,
                                                                  session_ctx,
                                                                  context_storage,
                                                                  &plan.catalog_resolves,
                                                                  std::move(resolve_nodes));
                if (pass1_result.cursor->is_error()) {
                    trace(log_,
                          "executor::execute_plan_full: resolve failed: {}",
                          pass1_result.cursor->get_error().what);
                    co_return execute_result_t{std::move(pass1_result.cursor)};
                }
            }
        }
        // PostgreSQL 18 checks a search_path's schemas on use; a place is checked when it is set and
        // kept by name.
        if (set_place.value().sets) {
            if (auto missing =
                    services::dispatcher::check_namespace_exists(resource(), &plan.catalog_resolves, set_place_name);
                missing.contains_error()) {
                co_return execute_result_t{make_cursor(resource(), std::move(missing))};
            }
            plan.sub_queries.back() =
                with_full_place(resource(), plan.sub_queries.back().get(), set_place.value().place);
        }
        if (original_type == node_type::refresh_matview_t) {
            co_return co_await refresh_matview_(session,
                                                plan,
                                                session_ctx,
                                                registries,
                                                context_storage.execution_context);
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
                // Snapshot the views first — merging reallocates entries; `refs` points into them.
                std::pmr::vector<components::logical_plan::resolved_table_metadata_t> views{resource()};
                views.reserve(refs.size());
                for (const auto& ref : refs) {
                    views.push_back(*ref.entry->table_md);
                }
                for (std::size_t i = 0; i < refs.size(); ++i) {
                    auto& ref = refs[i];
                    auto bound = components::planner::bind_view_body(resource(), views[i]);
                    if (bound.has_error()) {
                        trace(log_, "executor::execute_plan_full: view expansion failed: {}", bound.error().what);
                        co_return execute_result_t{make_cursor(resource(), bound.error())};
                    }
                    auto& body = bound.value();
                    services::dispatcher::register_plan_targets(resource(), body.plan.get(), &body.resolves);
                    if (auto err = co_await pin_functions_(session,
                                                           resolve_txn,
                                                           *registries.functions,
                                                           &views[i],
                                                           body.plan.get());
                        err.contains_error()) {
                        co_return execute_result_t{make_cursor(resource(), std::move(err))};
                    }
                    components::planner::renumber_body_parameters(resource(),
                                                                  body.plan.get(),
                                                                  body.params,
                                                                  plan.parameters);
                    auto projected = components::planner::project_view_body(resource(), std::move(body.plan), views[i]);
                    if (auto err = components::planner::splice_view_body(ref.node, projected); err.contains_error()) {
                        co_return execute_result_t{make_cursor(resource(), std::move(err))};
                    }
                    if (auto err = components::planner::merge_view_body_resolves(resource(),
                                                                                 plan.catalog_resolves,
                                                                                 body.resolves);
                        err.contains_error()) {
                        co_return execute_result_t{make_cursor(resource(), std::move(err))};
                    }
                    expanded_views.push_back({components::logical_plan::node_ptr{ref.node}, std::move(views[i])});
                }
                register_call_schemas(resource(), root, *registries.functions, plan.catalog_resolves);
                // The resolve operators leave what an earlier pass resolved: this reads the bodies' names only.
                {
                    std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes{resource()};
                    collect_resolve_nodes(plan.catalog_resolves, resolve_nodes);
                    auto pass2_result = co_await run_resolve_subplan_(session,
                                                                      session_ctx,
                                                                      context_storage,
                                                                      &plan.catalog_resolves,
                                                                      std::move(resolve_nodes));
                    if (pass2_result.cursor->is_error()) {
                        trace(log_,
                              "executor::execute_plan_full: view sub-plan resolve failed: {}",
                              pass2_result.cursor->get_error().what);
                        co_return execute_result_t{std::move(pass2_result.cursor)};
                    }
                }
            }
        }
        if (auto refused =
                bind_place_calls(resource(), plan.sub_queries.back().get(), plan.catalog_resolves, *plan.parameters);
            refused.contains_error()) {
            co_return execute_result_t{make_cursor(resource(), std::move(refused))};
        }
        if (auto stale = services::catalog_resolve::refuse_stale_pins(resource(), plan.catalog_resolves);
            stale.contains_error()) {
            co_return execute_result_t{make_cursor(resource(), std::move(stale))};
        }
        // A DROP takes the objects of a connection the host's map lost from the catalog alone.
        if (auto unbound = bind_storages_(plan.catalog_resolves, original_type == node_type::drop_t);
            unbound.contains_error()) {
            co_return execute_result_t{make_cursor(resource(), std::move(unbound))};
        }
        if (auto crossing =
                services::catalog_resolve::refuse_cross_database_references(resource(), plan.catalog_resolves);
            crossing.contains_error()) {
            co_return execute_result_t{make_cursor(resource(), std::move(crossing))};
        }
        if (plan.sub_queries.back()) {
            services::dispatcher::bind_catalog_data(plan.sub_queries.back().get(), plan.catalog_resolves);
            // With the CHECK predicates bind_catalog_data brought.
            scope_calls(plan.sub_queries.back().get(), plan.catalog_resolves);
        }
        context_storage.catalog_resolves = &plan.catalog_resolves;

        using components::logical_plan::constraint_kind;
        using components::logical_plan::node_create_collection_t;
        using components::logical_plan::node_create_constraint_t;
        using components::logical_plan::node_create_type_t;
        using components::types::logical_type;

        auto registry_change = prepare_registry_change_(plan, registries, session_ctx.function_uid);
        if (registry_change.has_error()) {
            co_return execute_result_t{make_cursor(resource(), registry_change.error())};
        }
        auto statement_changes = std::move(registry_change.value());

        const qualified_name_t target =
            plan.sub_queries.back() ? plan.sub_queries.back()->target() : qualified_name_t{};
        {
            auto outcome = decide_target_(plan, target);
            if (outcome.has_error()) {
                co_return execute_result_t{make_cursor(resource(), outcome.error())};
            }
            if (outcome.value() == target_outcome_t::nothing_to_do) {
                co_return execute_result_t{make_cursor(resource())};
            }
        }
        {
            // The connection the statement writes to: one per transaction.
            std::string written;
            if (needs_dml_txn || original_type == node_type::alter_table_t ||
                original_type == node_type::create_collection_t) {
                written = target.connection.t;
                if (const auto* table = plan.catalog_resolves.table_md(target); table != nullptr) {
                    written = table->connection;
                }
            }
            // A describe writes nothing.
            if (auto refused = describe ? core::error_t::no_error()
                                        : check_write_place_(session_ctx, std::span<const std::string>{&written, 1});
                refused.contains_error()) {
                co_return execute_result_t{make_cursor(resource(), std::move(refused))};
            }
        }
        cursor_t_ptr error;
        services::dispatcher::validation::parameter_types_t parameter_types{
            resource(),
            plan.external_parameters,
            plan.parameters->parameters(),
            services::dispatcher::validation::output_node_of(plan.sub_queries.back().get())};
        services::dispatcher::validation::named_schema returning{resource()};
        // A CREATE never writes into a schema the catalog does not hold: its rows would have no namespace. IF NOT
        // EXISTS does not cover it, since what is missing is the schema, not the object.
        const bool creates_in_a_schema =
            original_type == node_type::create_collection_t || original_type == node_type::create_sequence_t ||
            original_type == node_type::create_view_t || original_type == node_type::create_macro_t ||
            original_type == node_type::create_index_t || original_type == node_type::create_type_t;
        if (creates_in_a_schema && !target.schema.t.empty()) {
            if (auto missing = services::dispatcher::check_namespace_exists(resource(), &plan.catalog_resolves, target);
                missing.contains_error()) {
                co_return execute_result_t{make_cursor(resource(), std::move(missing))};
            }
            if (original_type != node_type::create_index_t &&
                components::catalog::in_system_schema(plan.catalog_resolves.namespace_oid(target))) {
                std::pmr::string msg{"cannot modify system catalog \"", resource()};
                msg.append(target.to_string());
                msg.append("\"");
                co_return execute_result_t{
                    make_cursor(resource(), core::error_t{core::error_code_t::sql_parse_error, std::move(msg)})};
            }
        }
        switch (original_type) {
            case node_type::create_database_t:
            case node_type::create_schema_t:
                break;
            case node_type::create_collection_t: {
                auto* n = static_cast<node_create_collection_t*>(plan.sub_queries.back().get());
                auto storage = storage_of_connection_(target.connection.t);
                if (storage.has_error()) {
                    error = make_cursor(resource(), storage.error());
                    break;
                }
                n->set_storage_address(std::move(storage.value()));
                for (auto& col_def : n->column_definitions()) {
                    if (auto invalid = prepare_column_(plan.catalog_resolves,
                                                       context_storage.execution_context,
                                                       *registries.casts,
                                                       col_def);
                        invalid.contains_error()) {
                        error = make_cursor(resource(), std::move(invalid));
                        break;
                    }
                }
                break;
            }
            case node_type::create_type_t: {
                auto* n = static_cast<node_create_type_t*>(plan.sub_queries.back().get());
                const qualified_name_t created{target.database, target.schema, core::relname_t{n->type().type_name()}};
                if (plan.catalog_resolves.type_md(created) != nullptr) {
                    error = make_cursor(
                        resource(),
                        core::error_t{core::error_code_t::duplicate_object,
                                      std::pmr::string{("type: \'" + n->type().alias() + "\' already exists").c_str(),
                                                       resource()}});
                    break;
                }
                if (n->type().type() == logical_type::STRUCT) {
                    for (auto& field : n->type().child_types()) {
                        if (field.type() != logical_type::UNKNOWN) {
                            continue;
                        }
                        const auto written = components::sql::transform::type_reference_of(field);
                        if (auto err =
                                services::dispatcher::check_type_exists(resource(), &plan.catalog_resolves, written);
                            err.contains_error()) {
                            error = make_cursor(resource(), err);
                            break;
                        }
                        std::string alias = field.has_alias() ? field.alias() : std::string{};
                        if (const auto* md = plan.catalog_resolves.type_md(written)) {
                            field = md->type;
                        } else {
                            field = components::types::complex_logical_type{
                                components::catalog::pg_name_to_logical_type(written.collection.t)};
                        }
                        if (!alias.empty()) {
                            field.set_alias(alias);
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
                n->set_namespace_oid(plan.catalog_resolves.namespace_oid(target));
                break;
            }
            case node_type::drop_t:
                if (auto refused = check_drop_target_(plan, target); refused.contains_error()) {
                    error = make_cursor(resource(), std::move(refused));
                }
                break;
            case node_type::set_setting_t:
                if (alters_system && !plan.commits_when_done) {
                    // PostgreSQL 18 PreventInTransactionBlock.
                    error = make_cursor(resource(),
                                        core::error_t{core::error_code_t::active_sql_transaction,
                                                      std::pmr::string{"ALTER SYSTEM cannot run inside a transaction "
                                                                       "block",
                                                                       resource()}});
                }
                break;
            // Checked by prepare_registry_change_.
            case node_type::register_udf_t:
            case node_type::unregister_udf_t:
            case node_type::register_cast_t:
            case node_type::unregister_cast_t:
            case node_type::checkpoint_t:
            case node_type::vacuum_t:
            // Leaf control nodes like checkpoint/vacuum — omitting this hits validate_schema's default assert(false).
            case node_type::transaction_t:
            case node_type::create_sequence_t:
            case node_type::create_macro_t:
            case node_type::prepare_t:
            case node_type::deallocate_t:
                break;
            case node_type::create_view_t:
                if (auto invalid = co_await validate_create_view_(session,
                                                                  resolve_txn,
                                                                  plan,
                                                                  context_storage.execution_context,
                                                                  registries,
                                                                  own,
                                                                  expanded_views);
                    invalid.contains_error()) {
                    error = make_cursor(resource(), std::move(invalid));
                }
                break;
            case node_type::alter_table_t: {
                auto* alter_node =
                    static_cast<components::logical_plan::node_alter_table_t*>(plan.sub_queries.back().get());
                for (auto& cmd : alter_node->subcommands()) {
                    if (cmd.kind != components::logical_plan::alter_table_kind::add_column) {
                        continue;
                    }
                    if (auto invalid = prepare_column_(plan.catalog_resolves,
                                                       context_storage.execution_context,
                                                       *registries.casts,
                                                       cmd.column);
                        invalid.contains_error()) {
                        error = make_cursor(resource(), std::move(invalid));
                        break;
                    }
                }
                break;
            }
            case node_type::create_constraint_t: {
                auto* cstr = static_cast<node_create_constraint_t*>(plan.sub_queries.back().get());
                {
                    const bool key_kind =
                        cstr->kind() == constraint_kind::unique || cstr->kind() == constraint_kind::primary_key;
                    if (cstr->kind() == constraint_kind::foreign_key || cstr->kind() == constraint_kind::check ||
                        key_kind) {
                        const auto* tbl_local = plan.catalog_resolves.table_md(target);
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
                                core::error_t{core::error_code_t::unimplemented_yet,
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
                                    core::error_code_t::unimplemented_yet,
                                    std::pmr::string{
                                        "UNIQUE / PRIMARY KEY constraints are not supported on dynamic-schema "
                                        "(relkind='g') tables. Key enforcement requires stable column attoids; "
                                        "dynamic-schema columns live in pg_computed_column and may evolve. "
                                        "Convert the table to static schema first.",
                                        resource()}});
                        } else if (cstr->kind() == constraint_kind::check && local_is_g) {
                            error = make_cursor(
                                resource(),
                                core::error_t{core::error_code_t::unimplemented_yet,
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
                                *registries.casts,
                                *registries.functions,
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
                if (index_node->table_metadata() != nullptr &&
                    components::catalog::in_system_schema(index_node->table_metadata()->namespace_oid)) {
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
                    services::dispatcher::validation::validation_context_t validation_context{
                        resource(),
                        &plan.catalog_resolves,
                        *registries.casts,
                        *registries.functions,
                        context_storage.execution_context};
                    validation_context.parameter_types = &parameter_types;
                    validation_context.returning = &returning;
                    auto schema_res = services::dispatcher::validate_schema(validation_context,
                                                                            plan.sub_queries.back().get(),
                                                                            plan.parameters->parameters());
                    if (schema_res.has_error()) {
                        error = make_cursor(resource(), schema_res.error());
                    } else if (auto untyped = parameter_types.check_all_typed(); untyped.contains_error()) {
                        error = make_cursor(resource(), std::move(untyped));
                    } else {
                        parameter_types.type_slots(*plan.parameters);
                    }
                }
            }
        }

        if (!error && !expanded_views.empty()) {
            for (const auto& expanded : expanded_views) {
                if (auto stale =
                        check_expanded_view(resource(), expanded.view, *expanded.reference->children().front());
                    stale.contains_error()) {
                    error = make_cursor(resource(), std::move(stale));
                    break;
                }
            }
        }
        if (error) {
            trace(log_, "executor::execute_plan_full: validation error: {}", error->get_error().what);
            co_return execute_result_t{std::move(error)};
        }
        if (plan.expected_columns.has_value() &&
            !same_row_type(*plan.expected_columns, result_columns_(plan, returning))) {
            co_return execute_result_t{
                make_cursor(resource(),
                            core::error_t{core::error_code_t::unimplemented_yet,
                                          std::pmr::string{"cached plan must not change result type", resource()}})};
        }
        if (sub_query) {
            if (describe) {
                co_return execute_result_t{make_cursor(resource())};
            }
        } else if (auto parameters = parameter_types_of_(plan); parameters.has_error()) {
            co_return execute_result_t{make_cursor(resource(), parameters.error())};
        } else if (describe) {
            co_return execute_result_t{describe_(plan, returning, parameters.value())};
        }

        components::catalog::oid_t create_index_table_oid = components::catalog::INVALID_OID;
        components::catalog::oid_t create_index_oid = components::catalog::INVALID_OID;
        components::catalog::oid_t created_table_oid = components::catalog::INVALID_OID;

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
            // The CHECK predicates enrich read from the catalog: the schemas their qualified calls name are resolved
            // now; a bare call is its table's schema's.
            {
                std::pmr::vector<components::logical_plan::node_ptr> schemas{resource()};
                auto named = [&](auto& call, const components::logical_plan::node_t&, bool in_check) {
                    const auto& name = call.full_name();
                    if (in_check && names_a_user_schema(name) &&
                        plan.catalog_resolves.namespace_entry(schema_of_call(name)) == nullptr) {
                        components::sql::transform::register_catalog_resolve_namespace(resource(),
                                                                                       &plan.catalog_resolves,
                                                                                       schema_of_call(name));
                        if (schemas.empty()) {
                            schemas.push_back(plan.catalog_resolves.namespaces);
                        }
                    }
                };
                components::logical_plan::for_each_plan_call(plan.sub_queries.back().get(), named);
                if (!schemas.empty()) {
                    auto resolved = co_await run_resolve_subplan_(session,
                                                                  session_ctx,
                                                                  context_storage,
                                                                  &plan.catalog_resolves,
                                                                  std::move(schemas));
                    if (resolved.cursor->is_error()) {
                        co_return execute_result_t{std::move(resolved.cursor)};
                    }
                }
                scope_calls(plan.sub_queries.back().get(), plan.catalog_resolves);
            }
            {
                services::dispatcher::validation::validation_context_t constraint_context{
                    resource(),
                    &plan.catalog_resolves,
                    *registries.casts,
                    *registries.functions,
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
                            context_storage.table_metadata[entry.table_md->table_oid] = &*entry.table_md;
                        }
                    }
                }
            }

            // DDL OID-batch allocation via node_allocate_oids_t; `self` supplies the coroutine frame its memory_resource.
            // Must not `co_return {}` on failure — that reads as success and stamps a garbage identity.
            auto allocate_oids_inline = [this, session, &context_storage, registries]([[maybe_unused]] executor_t* self,
                                                                                      std::size_t count)
                -> executor_t::unique_future<core::result_wrapper_t<std::vector<components::catalog::oid_t>>> {
                auto node = components::logical_plan::make_node_allocate_oids(resource(), count);
                services::context_storage_t cstor{resource(), log_.clone(), context_storage.execution_context};
                auto planned = services::planner::create_plan(cstor,
                                                              *registries.functions,
                                                              node,
                                                              components::logical_plan::limit_t::all(),
                                                              /*params=*/nullptr);
                if (planned.has_error()) {
                    co_return core::result_wrapper_t<std::vector<components::catalog::oid_t>>{
                        core::error_on(resource(), planned.error())};
                }
                auto op = std::move(planned.value());
                op->set_as_root();
                components::logical_plan::storage_parameters local_params(resource());
                components::pipeline::context_t pctx{session,
                                                     actor_zeta::address_t::empty_address(),
                                                     actor_zeta::address_t::empty_address(),
                                                     nullptr,
                                                     local_params,
                                                     disk_address_,
                                                     index_address_,
                                                     wal_address_};
                pctx.txn = components::table::transaction_data::committed();
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
                                            core::error_code_t::internal_error,
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
                                                core::error_code_t::unimplemented_yet,
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
                    case node_type::create_schema_t:
                    case node_type::create_type_t:
                    case node_type::create_sequence_t:
                    case node_type::create_view_t:
                    case node_type::create_macro_t:
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
                                core::error_code_t::unimplemented_yet,
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
                if (auto reached =
                        co_await stamp_storage_targets_(session, session_ctx, context_storage, resolve_txn, plan);
                    reached.contains_error()) {
                    co_return execute_result_t{make_cursor(resource(), std::move(reached))};
                }

                if (original_type == node_type::create_collection_t) {
                    created_table_oid = eff->table_oid();
                } else if (original_type == node_type::create_index_t) {
                    if (auto* eff2 = plan.sub_queries.back().get(); eff2 && !eff2->children().empty()) {
                        auto* back = eff2->children().back().get();
                        if (back && back->type() == node_type::create_index_t) {
                            const auto* ci = static_cast<const components::logical_plan::node_create_index_t*>(back);
                            create_index_table_oid = ci->table_oid();
                            create_index_oid = ci->index_oid();
                        }
                    }
                } else if (original_type == node_type::alter_table_t) {
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
        const bool can_push_to_agent = disk_address_ != actor_zeta::address_t::empty_address();
        plan.sub_queries.back() = components::planner::optimize(resource(),
                                                                std::move(plan.sub_queries.back()),
                                                                plan.parameters.get(),
                                                                context_storage.execution_context,
                                                                can_push_to_agent);
        if (auto offered = co_await offer_blocks_(session, plan, resolve_txn, context_storage.execution_context);
            offered.contains_error()) {
            co_return execute_result_t{make_cursor(resource(), std::move(offered))};
        }

        if (can_push_to_agent) {
            std::pmr::set<components::catalog::oid_t> inner_hash_join_oids{resource()};
            collect_inner_hash_join_oids(plan.sub_queries.back(), inner_hash_join_oids);
            for (auto oid : inner_hash_join_oids) {
                auto address = services::planner::storage_address_of(context_storage, oid);
                if (address.has_error()) {
                    co_return execute_result_t{make_cursor(resource(), address.error())};
                }
                auto [_tr, trf] =
                    actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_total_rows>(address.value(),
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
                                                 registries,
                                                 resolve_txn,
                                                 session_ctx.lowest_active_start_time,
                                                 std::move(captured_subplans));
        // The functions a DROP took with its dependents leave the registries with their rows; a function of a previous
        // process has rows only, and one the statement drops itself is in its changes already.
        if (exec_result.cursor->is_success()) {
            for (const auto& dropped : exec_result.dropped_functions) {
                const auto uid = static_cast<components::compute::function_uid>(dropped.prouid);
                const auto* function = registries.functions->get_function(uid);
                const bool held = !components::compute::is_builtin(uid) && function != nullptr &&
                                  function->name() == dropped.name &&
                                  registries.functions->schema_of(uid) == dropped.namespace_oid &&
                                  has_signature(*function, dropped.proargmatchers);
                const bool known = std::any_of(
                    statement_changes.functions.begin(),
                    statement_changes.functions.end(),
                    [uid](const services::dispatcher::function_change_t& change) { return change.uid == uid; });
                if (held && !known) {
                    statement_changes.functions.push_back(
                        services::dispatcher::function_change_t{uid,
                                                                registries.functions->schema_of(uid),
                                                                components::compute::no_function()});
                }
            }
        }
        // The type before the offer: a backend may have taken the UPDATE / DELETE node.
        if (modified_rows && plan.explain == components::logical_plan::explain_type::none &&
            exec_result.cursor->is_success()) {
            exec_result.cursor->set_written(exec_result.written);
        }
        if (created_table_oid != components::catalog::INVALID_OID && exec_result.cursor->is_success()) {
            exec_result.cursor->set_created_oid(created_table_oid);
        }

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
                components::table::txn_abort_drain_t undo;
                undo.txn = resolve_txn;
                undo.base_appends = std::move(revert_ranges);
                auto [_pa, paf] =
                    actor_zeta::otterbrix::send(disk_address_,
                                                &services::disk::manager_disk_t::undo_appends_and_storages,
                                                session,
                                                std::move(undo));
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
                    components::execution_context_t abort_ctx{session,
                                                              resolve_txn,
                                                              session_ctx.settings.timezone_offset,
                                                              oid};
                    auto [_ri, rif] = actor_zeta::otterbrix::send(index_address_,
                                                                  &services::index::manager_index_t::revert_insert,
                                                                  abort_ctx,
                                                                  oid);
                    revert_index_futures.push_back(std::move(rif));
                }
                for (auto oid : revert_delete_oids) {
                    components::execution_context_t abort_ctx{session,
                                                              resolve_txn,
                                                              session_ctx.settings.timezone_offset,
                                                              oid};
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

            // Without the delete marks undone (mirrors operator_abort_transaction), the next UPDATE's delete silently
            // no-ops; the column stamps go the same way.
            std::pmr::set<components::catalog::oid_t> undone_tables{exec_result.pg_catalog_delete_tables.begin(),
                                                                    exec_result.pg_catalog_delete_tables.end(),
                                                                    resource()};
            for (const auto& del : exec_result.dml_deletes) {
                undone_tables.insert(del.table_oid);
            }
            for (const auto& backfill : exec_result.pg_attribute_commit_id_backfills) {
                if (const auto table_oid = backfill.column_stamped_table();
                    table_oid != components::catalog::INVALID_OID) {
                    undone_tables.insert(table_oid);
                }
            }
            components::execution_context_t abort_ctx{session, resolve_txn, session_ctx.settings.timezone_offset};
            if (resolve_txn.transaction_id != 0 && !undone_tables.empty()) {
                auto [_rd, rdf] = actor_zeta::otterbrix::send<&services::disk::storage_contract::abort_transaction>(
                    components::table::storage_address_t{disk_address_, {}},
                    abort_ctx,
                    uint64_t{0},
                    std::move(undone_tables));
                if (const auto unstamped = co_await std::move(rdf); unstamped.contains_error()) {
                    ::error(log_, "executor: the statement's delete marks were not undone: {}", unstamped.what);
                }
            }
            exec_result.pg_catalog_delete_tables.clear();
            exec_result.pg_attribute_commit_id_backfills.clear();

            // A backend the statement wrote to or read from ends its part of the statement's transaction now.
            for (const auto& participant : exec_result.participants) {
                if (participant.address.empty()) {
                    continue;
                }
                auto [_pa, paf] = actor_zeta::otterbrix::send<&services::disk::storage_contract::abort_transaction>(
                    participant.address,
                    abort_ctx,
                    uint64_t{0},
                    std::pmr::set<components::catalog::oid_t>{participant.tables.begin(),
                                                              participant.tables.end(),
                                                              resource()});
                if (const auto aborted = co_await std::move(paf); aborted.contains_error()) {
                    ::error(log_, "executor: a backend refused the statement's rollback: {}", aborted.what);
                }
            }
            exec_result.participants.clear();

            exec_result.dml_appends.clear();
            exec_result.dml_deletes.clear();
        };

        // something failed, but transaction may be recovered in the future
        auto park_in_transaction = [this, session, resolve_txn, &plan, &revert_failed_txn, &statement_changes](
                                       [[maybe_unused]] executor_t* self,
                                       execute_result_t& exec_result) -> executor_t::unique_future<void> {
            services::dispatcher::txn_accumulate_payload_t payload;
            // The transaction's own statements see them from now on; the registries of the others take them before
            // its commit is published.
            if (exec_result.cursor->is_success()) {
                payload.registry_changes = std::move(statement_changes);
            }
            payload.base_appends.reserve(exec_result.dml_appends.size());
            for (const auto& app : exec_result.dml_appends) {
                payload.base_appends.push_back(
                    components::table::dml_append_range_t{app.table_oid, app.row_start, app.row_count});
            }
            payload.base_deletes.reserve(exec_result.dml_deletes.size());
            for (const auto& del : exec_result.dml_deletes) {
                payload.base_deletes.push_back(components::table::dml_delete_range_t{del.table_oid, del.txn_id});
            }
            // Copied, not moved: a refused accumulate reverts them.
            payload.pg_catalog_appends = exec_result.pg_catalog_appends;
            payload.pg_catalog_delete_tables = exec_result.pg_catalog_delete_tables;
            payload.backfills = std::move(exec_result.pg_attribute_commit_id_backfills);
            payload.dropped_storage_oids = std::move(exec_result.dropped_storage_oids);
            payload.created_storage_oids = std::move(exec_result.created_storage_oids);
            payload.created_indexes = std::move(exec_result.created_indexes);
            payload.referenced_deletes = std::move(exec_result.referenced_deletes);
            payload.participants = exec_result.participants;
            trace(log_,
                  "executor::execute_plan_full: txn {} — accumulating {} appends, {} deletes ({})",
                  resolve_txn.transaction_id,
                  payload.base_appends.size(),
                  payload.base_deletes.size(),
                  plan.commits_when_done ? "COMMIT follows" : "publish deferred to COMMIT");
            if (!payload.empty()) {
                auto [_ac, acf] =
                    actor_zeta::otterbrix::send(parent_address_,
                                                &services::dispatcher::manager_dispatcher_t::txn_accumulate_msg,
                                                session,
                                                resolve_txn.transaction_id,
                                                std::move(payload));
                // An error here means the ranges were parked NOWHERE — swallowing it fakes success.
                auto accumulate_err = co_await std::move(acf);
                if (accumulate_err.contains_error()) {
                    // A failed statement keeps its own error.
                    if (exec_result.cursor->is_success()) {
                        exec_result.cursor = make_cursor(resource(), std::move(accumulate_err));
                    }
                    co_await revert_failed_txn(this, exec_result);
                }
            }
            co_return;
        };

        // A read of a backend is a part of the transaction too: its storages get the transaction's end.
        const bool reads_participants = !needs_ddl_txn && !exec_result.participants.empty();
        if (needs_dml_txn || needs_commit_txn || reads_participants) {
            // Transaction stays failed, and only ROLLBACK or ROLLBACK TO SAVEPOINT are allowed
            if (exec_result.cursor->is_success() || !plan.commits_when_done) {
                co_await park_in_transaction(this, exec_result);
                exec_result.dml_appends.clear();
                exec_result.dml_deletes.clear();
                exec_result.pg_catalog_appends.clear();
                exec_result.pg_catalog_delete_tables.clear();
                exec_result.pg_attribute_commit_id_backfills.clear();
                exec_result.dropped_storage_oids.clear();
                exec_result.created_storage_oids.clear();
                exec_result.created_indexes.clear();
                exec_result.participants.clear();

                if (plan.commits_when_done && exec_result.cursor->is_success()) {
                    auto commit_result = co_await run_commit_pipeline_(session,
                                                                       resolve_txn,
                                                                       context_storage.execution_context,
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

        if (needs_ddl_txn && exec_result.cursor->is_success() && !exec_result.host_rows.empty()) {
            if (auto written =
                    co_await write_host_rows_(session, session_ctx, registries, std::move(exec_result.host_rows));
                written.contains_error()) {
                exec_result.cursor = make_cursor(resource(), std::move(written));
            }
            exec_result.host_rows.clear();
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

            // The rows a host's DDL operator wrote into otterbrix tables commit with the catalog's.
            co_await park_in_transaction(this, exec_result);
            if (exec_result.cursor->is_error()) {
                has_create_index_pg_index_range = false;
            }
            exec_result.pg_catalog_appends.clear();
            exec_result.pg_catalog_delete_tables.clear();
            exec_result.pg_attribute_commit_id_backfills.clear();
            exec_result.dropped_storage_oids.clear();
            exec_result.created_storage_oids.clear();
            exec_result.created_indexes.clear();
            exec_result.dml_appends.clear();
            exec_result.dml_deletes.clear();
            exec_result.participants.clear();

            auto undo_create_index =
                [this, session, resolve_txn, &create_index_pg_index_range, &has_create_index_pg_index_range](
                    [[maybe_unused]] executor_t* self,
                    components::catalog::oid_t table_oid,
                    components::catalog::oid_t index_oid) -> executor_t::unique_future<void> {
                if (has_create_index_pg_index_range) {
                    components::table::txn_abort_drain_t undo;
                    undo.txn = resolve_txn;
                    undo.swap_appends.push_back(create_index_pg_index_range);
                    auto [_rv, rvf] =
                        actor_zeta::otterbrix::send(disk_address_,
                                                    &services::disk::manager_disk_t::undo_appends_and_storages,
                                                    session,
                                                    std::move(undo));
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

            if (plan.commits_when_done && exec_result.cursor->is_success()) {
                auto commit_result = co_await run_commit_pipeline_(session,
                                                                   resolve_txn,
                                                                   context_storage.execution_context,
                                                                   session_ctx.lowest_active_start_time,
                                                                   /*ddl_mode=*/true);
                if (commit_result.cursor->is_error()) {
                    exec_result.cursor = std::move(commit_result.cursor);
                    if (original_type == node_type::create_index_t) {
                        co_await undo_create_index(this, create_index_table_oid, create_index_oid);
                    }
                }
            }
        } else if (needs_ddl_txn && exec_result.cursor->is_error() && !plan.commits_when_done) {
            trace(log_,
                  "executor::execute_plan_full: DDL failed — parking its writes on txn {}, session: {}",
                  resolve_txn.transaction_id,
                  session.data());

            co_await park_in_transaction(this, exec_result);
        } else if (needs_ddl_txn && exec_result.cursor->is_error()) {
            trace(log_,
                  "executor::execute_plan_full: DDL failed — reverting txn {}, session: {}",
                  resolve_txn.transaction_id,
                  session.data());

            co_await revert_failed_txn(this, exec_result);
        }

        co_return std::move(exec_result);
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
            auto popped = look_up.top();
            sub_plans.push(popped);
            look_up.pop();
            if (check_op != nullptr) {
                look_up.push(check_op->right());
                look_up.push(check_op->left());
            }
            // A sequence's steps run before it, first step first: pushed last, the first step is popped last and so
            // lands on top of sub_plans.
            for (const auto& step : steps_of(popped.get())) {
                look_up.push(step);
            }
        }

        trace(log_, "executor::subplans count {}", sub_plans.size());

        return plan_t{std::move(plan), std::move(sub_plans), &parameters, std::move(context_storage)};
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
                auto pending_next = source->source_next(ctx);
#ifdef DEV_MODE
                if (auto* hook = g_source_await_hook.load()) {
                    hook(source);
                }
#endif
                auto next = co_await std::move(pending_next);
                if (next.has_error()) {
                    co_await release_source_cursor(resource());
                    co_return next.convert_error<ops::chunks_vector_t>();
                }
                if (!next.value().has_value()) {
                    break;
                }
                auto batch = std::move(*next.value());
                // Every source slices its batches (a storage's answer is held to it when it arrives).
                assert(batch.size() <= components::vector::DEFAULT_VECTOR_CAPACITY &&
                       "a source emitted a batch over DEFAULT_VECTOR_CAPACITY");
                if (batch.size() > components::vector::DEFAULT_VECTOR_CAPACITY) {
                    co_await release_source_cursor(resource());
                    std::pmr::string what{"source batch of ", resource()};
                    what += std::to_string(batch.size());
                    what += " rows exceeds the ";
                    what += std::to_string(components::vector::DEFAULT_VECTOR_CAPACITY);
                    what += "-row limit per batch; the source must slice it";
                    co_return core::result_wrapper_t<ops::chunks_vector_t>(
                        core::error_t(core::error_code_t::protocol_violation, std::move(what)));
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
        // co_await must live outside the synchronous pump_one lambda.
        const uint64_t threshold =
            dml_idx < chain.size() ? chain[dml_idx]->flush_row_threshold(dml_flush_row_threshold_) : 0;
        if (threshold != 0 && chain[dml_idx]->buffered_rows() >= threshold) {
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
                auto err = co_await drive_opened_subplan_(right, ctx);
                if (err.contains_error()) {
                    co_return err;
                }
            }
        }
        co_return core::error_t::no_error();
    }

    executor_t::unique_future<core::error_t> executor_t::drive_subplan_(components::operators::operator_ptr root,
                                                                        components::pipeline::context_t* ctx) {
        opened_sources_t opened{resource()};
        open_sources_(root.get(), ctx, opened);
        auto open_err = co_await await_opened_(opened, nullptr);
        if (open_err.contains_error()) {
            co_return open_err;
        }
        co_return co_await drive_opened_subplan_(root, ctx);
    }

    namespace {
        void collect_unexecuted(components::operators::operator_t* root,
                                std::pmr::vector<components::operators::operator_t*>& out) {
            std::pmr::vector<components::operators::operator_t*> pending{out.get_allocator().resource()};
            pending.push_back(root);
            while (!pending.empty()) {
                auto* op = pending.back();
                pending.pop_back();
                if (op == nullptr || op->is_executed()) {
                    continue;
                }
                out.push_back(op);
                pending.push_back(op->left().get());
                pending.push_back(op->right().get());
                for (const auto& step : steps_of(op)) {
                    pending.push_back(step.get());
                }
            }
        }
    } // namespace

    void executor_t::open_sources_(components::operators::operator_t* root,
                                   components::pipeline::context_t* ctx,
                                   opened_sources_t& opened) {
        std::pmr::vector<components::operators::operator_t*> ops{resource()};
        collect_unexecuted(root, ops);
        for (auto* op : ops) {
            if (op->role() == components::operators::pipeline_role::source || op->opens_at_start()) {
                opened.push_back(opened_source_t{op, op->open(ctx)});
            }
        }
    }

    executor_t::unique_future<core::error_t>
    executor_t::await_opens_at_start_(opened_sources_t& opened,
                                      const std::pmr::vector<components::operators::operator_t*>& wanted) {
        for (auto* op : wanted) {
            for (auto& source : opened) {
                if (source.op != op || !source.ready.valid()) {
                    continue;
                }
                auto err = co_await std::move(source.ready);
                if (err.contains_error()) {
                    co_return err;
                }
            }
        }
        co_return core::error_t::no_error();
    }

    executor_t::unique_future<core::error_t> executor_t::await_opened_(opened_sources_t& opened,
                                                                       components::operators::operator_t* piece) {
        std::pmr::vector<components::operators::operator_t*> in_piece{resource()};
        if (piece != nullptr) {
            collect_unexecuted(piece, in_piece);
        }
        auto first_err = core::error_t::no_error();
        for (auto& source : opened) {
            if (!source.ready.valid() ||
                (piece != nullptr && std::find(in_piece.begin(), in_piece.end(), source.op) == in_piece.end())) {
                continue;
            }
#ifdef DEV_MODE
            if (auto* hook = g_source_await_hook.load()) {
                hook(source.op);
            }
#endif
            auto err = co_await std::move(source.ready);
            if (err.contains_error() && !first_err.contains_error()) {
                first_err = std::move(err);
            }
        }
        co_return first_err;
    }

    executor_t::unique_future<core::error_t> executor_t::drive_opened_subplan_(components::operators::operator_ptr root,
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
                core::error_t{core::error_code_t::internal_error,
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

    components::pipeline::context_t executor_t::make_pipeline_context_(components::session::session_id_t session,
                                                                       const plan_t& plan_data,
                                                                       components::table::transaction_data txn,
                                                                       uint64_t lowest_active_start_time) {
        components::pipeline::context_t context{session,
                                                address(),
                                                parent_address_,
                                                plan_data.context_storage_.functions,
                                                *plan_data.parameters,
                                                disk_address_,
                                                index_address_,
                                                wal_address_};
        context.txn = txn;
        context.execution_context = plan_data.context_storage_.execution_context;
        context.place = plan_data.context_storage_.place;
        context.lowest_active_start_time = lowest_active_start_time;
        context.runner = this;
        return context;
    }

    namespace {
        // A piece is what traverse_plan_ drives as one pipeline: the left chain from its root down to the first
        // operator with a right input (that operator included). Its left and right inputs are pieces of their own.
        struct piece_t {
            components::operators::operator_t* root;
            std::size_t parent; // no_piece for the plan root
            std::pmr::vector<components::operators::operator_t*> opens_at_start;
        };
        constexpr std::size_t no_piece = static_cast<std::size_t>(-1);

        void
        collect_pieces(components::operators::operator_t* root, std::size_t parent, std::pmr::vector<piece_t>& pieces) {
            const std::size_t self = pieces.size();
            pieces.push_back(
                piece_t{root,
                        parent,
                        std::pmr::vector<components::operators::operator_t*>{pieces.get_allocator().resource()}});
            for (const auto& step : steps_of(root)) {
                collect_pieces(step.get(), self, pieces);
            }
            for (auto* op = root; op != nullptr; op = op->left().get()) {
                if (op->opens_at_start()) {
                    pieces[self].opens_at_start.push_back(op);
                }
                if (op->right()) {
                    collect_pieces(op->right().get(), self, pieces);
                    if (op->left()) {
                        collect_pieces(op->left().get(), self, pieces);
                    }
                    return;
                }
            }
        }

        // The operators that open at start in the piece rooted at `root` and in every piece above it, outermost first.
        std::pmr::vector<components::operators::operator_t*>
        opens_at_start_on_path(const std::pmr::vector<piece_t>& pieces,
                               components::operators::operator_t* root,
                               std::pmr::memory_resource* resource) {
            std::pmr::vector<components::operators::operator_t*> wanted{resource};
            std::size_t at = no_piece;
            for (std::size_t i = 0; i < pieces.size(); ++i) {
                if (pieces[i].root == root) {
                    at = i;
                    break;
                }
            }
            std::pmr::vector<std::size_t> path{resource};
            for (; at != no_piece; at = pieces[at].parent) {
                path.push_back(at);
            }
            for (auto it = path.rbegin(); it != path.rend(); ++it) {
                wanted.insert(wanted.end(), pieces[*it].opens_at_start.begin(), pieces[*it].opens_at_start.end());
            }
            return wanted;
        }
    } // namespace

    executor_t::unique_future<sub_plan_result_t>
    executor_t::execute_sub_plan_(components::session::session_id_t session,
                                  plan_t plan_data,
                                  components::table::transaction_data txn,
                                  uint64_t lowest_active_start_time) {
        cursor_t_ptr cursor;
        sub_plan_result_t result_tracking{.applied_settings =
                                              std::pmr::vector<components::catalog::setting_assignment_t>{resource()}};

        opened_sources_t opened{resource()};
        auto open_context = make_pipeline_context_(session, plan_data, txn, lowest_active_start_time);
        open_sources_(plan_data.root.get(), &open_context, opened);

        std::pmr::vector<piece_t> pieces{resource()};
        collect_pieces(plan_data.root.get(), no_piece, pieces);

        while (!plan_data.sub_plans.empty()) {
            auto plan = plan_data.sub_plans.top();
            trace(log_, "executor::execute_sub_plan, session: {}", session.data());

            if (!plan) {
                cursor = make_cursor(resource(),
                                     core::error_t(core::error_code_t::internal_error,
                                                   std::pmr::string{"invalid query plan", resource()}));
                break;
            }

            auto pipeline_context = make_pipeline_context_(session, plan_data, txn, lowest_active_start_time);
            pipeline_context.analyze = plan_data.analyze;

            plan->prepare();

            // Top-down, as PostgreSQL starts a Limit before it asks below for a row: the Limit nodes of this piece
            // and of the pieces above it (the path up to the plan root, outermost first) are started first; those of
            // a piece beside it are not, they start when that piece runs.
            if (auto start_err =
                    co_await await_opens_at_start_(opened, opens_at_start_on_path(pieces, plan.get(), resource()));
                start_err.contains_error()) {
                cursor = make_cursor(resource(), std::move(start_err));
                break;
            }

            if (auto open_err = co_await await_opened_(opened, plan.get()); open_err.contains_error()) {
                cursor = make_cursor(resource(), std::move(open_err));
                break;
            }

            // Factored out so the CONSTRAINT-ERROR path can lift these ranges too, else the appended row leaks.
            auto lift_dml_ranges = [&pipeline_context, &result_tracking, &open_context]() {
                for (const auto& participant : open_context.participants) {
                    components::table::merge_participant(result_tracking.participants, participant);
                }
                open_context.participants.clear();
                for (const auto& participant : pipeline_context.participants) {
                    components::table::merge_participant(result_tracking.participants, participant);
                }
                pipeline_context.participants.clear();
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
                auto drive_err = co_await drive_opened_subplan_(plan, &pipeline_context);
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
                            auto fetch_ids = components::table::make_row_id_range(res,
                                                                                  static_cast<int64_t>(gap.row_start),
                                                                                  gap.row_count);
                            auto [_f, ff] =
                                actor_zeta::otterbrix::send(disk_address_,
                                                            &services::disk::manager_disk_t::storage_fetch,
                                                            session,
                                                            oid,
                                                            std::move(fetch_ids),
                                                            std::vector<size_t>{},
                                                            components::table::transaction_data::committed(),
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
            result_tracking.written = plan->written();

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
            if (!pipeline_context.applied_settings.empty()) {
                result_tracking.applied_setting_scope = pipeline_context.applied_setting_scope;
                result_tracking.applied_settings = std::move(pipeline_context.applied_settings);
                pipeline_context.applied_settings.clear();
            }
            if (pipeline_context.prepared_change.kind != components::logical_plan::prepared_change_t::kind_t::none) {
                result_tracking.prepared_change = std::move(pipeline_context.prepared_change);
                pipeline_context.prepared_change = {};
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
            for (auto& rows : pipeline_context.referenced_deletes) {
                result_tracking.referenced_deletes.push_back(std::move(rows));
            }
            pipeline_context.referenced_deletes.clear();
            for (auto& dropped : pipeline_context.dropped_functions) {
                result_tracking.dropped_functions.push_back(std::move(dropped));
            }
            pipeline_context.dropped_functions.clear();
            for (auto& rows : pipeline_context.host_rows) {
                result_tracking.host_rows.push_back(std::move(rows));
            }
            pipeline_context.host_rows.clear();
            if (pipeline_context.committed_id != 0) {
                result_tracking.commit_id = pipeline_context.committed_id;
            }
            // (dml_* fields + cascade vectors were already drained and zeroed by lift_dml_ranges() above.)

            plan_data.sub_plans.pop();
        }

        if (auto open_err = co_await await_opened_(opened, nullptr);
            open_err.contains_error() && cursor->is_success()) {
            cursor = make_cursor(resource(), std::move(open_err));
        }
        // A source the run never reached (an error stopped it first) still holds the cursor its open took.
        for (auto& source : opened) {
            if (source.op->holds_open_cursor()) {
                co_await source.op->release_cursor(&open_context);
            }
        }
        for (const auto& participant : open_context.participants) {
            components::table::merge_participant(result_tracking.participants, participant);
        }

        trace(log_, "executor::execute_sub_plan finished, success: {}", cursor->is_success());
        result_tracking.cursor = std::move(cursor);
        co_return std::move(result_tracking);
    }

    executor_t::unique_future<execute_result_t>
    executor_t::run_commit_pipeline_(components::session::session_id_t session,
                                     components::table::transaction_data txn,
                                     const components::graph_execution_context& settings,
                                     uint64_t lowest_active_start_time,
                                     bool ddl_mode) {
        using components::logical_plan::resolve_direction;
        using components::logical_plan::resolve_entry_t;
        using components::logical_plan::resolve_kind;

        // The drain takes the commit snapshot, which the constraints are resolved under
        services::commit_input_t input;
        if (parent_address_ != actor_zeta::address_t::empty_address()) {
            auto [_dr, drain_future] =
                actor_zeta::otterbrix::send(parent_address_,
                                            &services::dispatcher::manager_dispatcher_t::txn_commit_drain_msg,
                                            session,
                                            txn.transaction_id);
            input.drain = co_await std::move(drain_future);
        }
        const auto& drain = input.drain;
        // Nothing committed since the transaction's snapshot: what its statements checked still holds
        const bool rechecks = drain.txn.transaction_id != 0 && drain.commit_id > 0 &&
                              disk_address_ != actor_zeta::address_t::empty_address() &&
                              !drain.commit_view.snapshot_is_current &&
                              (!drain.base_appends.empty() || !drain.base_delete_tables.empty());
        components::logical_plan::catalog_resolves_t resolves;
        if (rechecks) {
            auto& tables = resolves.ensure(resource(), resolve_kind::table);
            auto& constraints = resolves.ensure(resource(), resolve_kind::constraint);
            // Every written table is resolved: one a commit before this one dropped refuses the commit
            auto request_table = [&](components::catalog::oid_t table_oid) {
                resolve_entry_t table;
                table.relation_oid = table_oid;
                return tables.add(std::move(table));
            };
            auto request_constraints = [&](components::catalog::oid_t table_oid, resolve_direction direction) {
                resolve_entry_t constraint;
                constraint.target = request_table(table_oid);
                constraint.direction = direction;
                constraints.add(std::move(constraint));
            };
            for (const auto& append : drain.base_appends) {
                request_constraints(append.table_oid, resolve_direction::outgoing);
            }
            for (const auto table_oid : drain.base_delete_tables) {
                request_table(table_oid);
            }
            for (const auto& removed : drain.referenced_deletes) {
                request_constraints(removed.table_oid, resolve_direction::referencing);
            }
            auto root = boost::intrusive_ptr<components::logical_plan::node_t>(
                new components::logical_plan::node_sequence_t(resource()));
            root->append_child(resolves.tables);
            if (!constraints.empty()) {
                root->append_child(resolves.constraints);
            }
            services::context_storage_t resolve_storage(resource(), log_.clone(), settings);
            resolve_storage.catalog_resolves = &resolves;
            auto resolved = co_await execute_plan(
                session,
                components::logical_plan::execution_plan_t{resource(),
                                                           std::move(root),
                                                           components::logical_plan::make_parameter_node(resource())},
                std::move(resolve_storage),
                own_registries_(),
                drain.commit_view.snapshot,
                lowest_active_start_time,
                std::pmr::vector<explain_plan_node>{resource()});
            if (resolved.cursor->is_error()) {
                input.resolve_error = resolved.cursor->get_error();
            }
        }

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
        services::context_storage_t cstor(resource(), log_.clone(), settings);
        cstor.catalog_resolves = rechecks ? &resolves : nullptr;
        cstor.commit_input = &input;
        co_return co_await execute_plan(
            session,
            components::logical_plan::execution_plan_t{resource(), std::move(commit_node), std::move(cparams)},
            std::move(cstor),
            own_registries_(),
            txn,
            lowest_active_start_time,
            std::pmr::vector<explain_plan_node>{resource()});
    }

} // namespace services::collection::executor
