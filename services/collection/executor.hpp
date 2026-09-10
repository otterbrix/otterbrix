#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/casts/cast_registry.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/settings.hpp>
#include <components/compute/function.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/context/subplan_runner.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>
#include <optional>
#include <set>

#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/transaction.hpp>
#include <core/date/date_types.hpp>
#include <services/collection/context_storage.hpp>
#include <services/collection/explain/explain_renderer.hpp>
#include <stack>
#include <string>

namespace services::collection::executor {

    // Bumped once per execute_pipeline() entry (streaming vs. legacy materialize path).
#ifdef DEV_MODE
    uint64_t streaming_pipeline_runs() noexcept;

    // Guards the regression where a CHECK/FK failure in autocommit leaves an appended row behind.
    uint64_t dml_appends_reverted() noexcept;

    uint64_t dml_flush_count() noexcept;

    // Fault-injection seam for OID allocation; no file/page/block, so the .otbx/WAL interposers can't reach it.
    struct oid_alloc_interposer_t {
        virtual ~oid_alloc_interposer_t() = default;
        // An empty `allocated` isn't invented — it's what allocate_oids_inline's failure branches already produce.
        virtual std::vector<components::catalog::oid_t>
        substitute(std::size_t requested, std::vector<components::catalog::oid_t> allocated) = 0;
    };

    void dev_set_oid_alloc_interposer(oid_alloc_interposer_t* interposer);
    oid_alloc_interposer_t* dev_oid_alloc_interposer();
#endif

    // Accumulates across sub-plans: FK cascade DELETE emits one range per child table (last-wins would drop entries).
    struct dml_append_range_t {
        components::catalog::oid_t table_oid;
        int64_t row_start;
        uint64_t row_count;
    };
    struct dml_delete_range_t {
        components::catalog::oid_t table_oid;
        uint64_t txn_id;
    };

    struct execute_result_t {
        components::cursor::cursor_t_ptr cursor;
        // Drained by execute_plan_full's commit tail; the dispatcher reads only cursor and applied setting.
        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends{};
        std::set<components::catalog::oid_t> pg_catalog_delete_tables{};
        std::vector<components::pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills{};
        std::vector<dml_append_range_t> dml_appends{};
        std::vector<dml_delete_range_t> dml_deletes{};
        // DROP-GC remap keys off this drained set, not the ddl-commit mode flag.
        std::vector<components::catalog::oid_t> dropped_storage_oids{};
        // CREATE counterpart: COMMIT publishes these, a same-txn ABORT drops the uncommitted artifacts.
        std::vector<components::catalog::oid_t> created_storage_oids{};
        std::vector<components::table::created_index_t> created_indexes{};
        // Non-zero only after a commit ran; CREATE INDEX's backfill tail needs the allocated commit_id.
        uint64_t commit_id{0};
        components::catalog::setting_id applied_setting{};
        std::string applied_setting_value{};
        // Move-only, and everything from here down is appended at the END: a brace initializer that sets
        // only a prefix of the members has to keep compiling.
        std::optional<explain_plan_node> captured_explain_ir{};
        std::optional<std::pair<components::types::complex_logical_type, components::types::complex_logical_type>>
            resolved_cast{};
    };

    using function_result_t = core::result_wrapper_t<components::compute::function_uid>;

    struct plan_t {
        std::stack<components::operators::operator_ptr> sub_plans;
        // Non-owning: points into the execute_plan frame's storage, which outlives execute_sub_plan_.
        const components::logical_plan::storage_parameters* parameters;
        services::context_storage_t context_storage_;
        bool analyze{false};

        explicit plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                        const components::logical_plan::storage_parameters* parameters,
                        services::context_storage_t&& context_storage);
    };

    // Internal only — never crosses an actor boundary; drained from pipeline::context_t::dml_*.
    struct sub_plan_result_t {
        components::cursor::cursor_t_ptr cursor;
        std::vector<dml_append_range_t> dml_appends;
        std::vector<dml_delete_range_t> dml_deletes;
        std::vector<components::catalog::oid_t> dropped_storage_oids;
        std::vector<components::catalog::oid_t> created_storage_oids;
        std::vector<components::table::created_index_t> created_indexes;

        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends;
        std::set<components::catalog::oid_t> pg_catalog_delete_tables;
        std::vector<components::pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;
        uint64_t commit_id{0};
        components::catalog::setting_id applied_setting{};
        std::string applied_setting_value;
    };

    // Implements subplan_runner_t: an operator inside this executor's coroutine drives a child sub-plan via
    // ctx->runner->run_subplan() — not cross-actor sharing, since operator_t runs synchronously inside executor_t.
    class executor_t final
        : public actor_zeta::basic_actor<executor_t>
        , public components::pipeline::subplan_runner_t {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        executor_t(std::pmr::memory_resource* resource,
                   actor_zeta::address_t parent_address,
                   actor_zeta::address_t wal_address,
                   actor_zeta::address_t disk_address,
                   actor_zeta::address_t index_address,
                   log_t&& log,
                   uint64_t dml_flush_row_threshold = 0,
                   planner::create_plan_rule_t create_plan_rule = &planner::no_custom_lowering,
                   components::planner::optimizer_pass_t optimizer_pass = &components::planner::no_op_pass);
        ~executor_t() = default;

        // INTERNAL: called only from execute_plan_full via co_await, never through the mailbox. captured_subplans
        // is by value — a pmr member can't default without re-anchoring to the forbidden get_default_resource().
        unique_future<execute_result_t> execute_plan(components::session::session_id_t session,
                                                     components::logical_plan::execution_plan_t plan,
                                                     services::context_storage_t context_storage,
                                                     components::table::transaction_data txn,
                                                     uint64_t lowest_active_start_time,
                                                     std::pmr::vector<explain_plan_node> captured_subplans);

        // The per-query entry point (the dispatcher's only execute send); txn-state access rides
        // txn_*_msg to the dispatcher, the sole transaction_manager_t owner.
        unique_future<execute_result_t> execute_plan_full(components::session::session_id_t session,
                                                          components::logical_plan::execution_plan_t plan);

        unique_future<std::unique_ptr<function_result_t>> register_udf(components::session::session_id_t session,
                                                                       components::compute::function_ptr function);

        unique_future<bool> unregister_udf(components::session::session_id_t session,
                                           std::string name,
                                           std::pmr::vector<components::types::complex_logical_type> inputs);

        // Compensation for a failed register_udf fan-out; appended last in dispatch_traits (message ids positional).
        unique_future<bool> unregister_udf_uid(components::session::session_id_t session,
                                               components::compute::function_uid uid);

        unique_future<bool> register_cast(components::session::session_id_t session,
                                          components::types::complex_logical_type source,
                                          components::types::complex_logical_type target,
                                          components::casts::cast_entry entry);
        unique_future<bool> unregister_cast(components::session::session_id_t session,
                                            components::types::complex_logical_type source,
                                            components::types::complex_logical_type target);

        // Fanned out from the dispatcher; POD fn-pointers stored per-executor, so there's no shared mutable state.
        unique_future<bool> set_explain_renderer(uint32_t id, explain_render_fn fn);

        // No-op poke target for the dispatcher's lost-wakeup watchdog.
        unique_future<void> poke_msg();

        // Same seam as execute_sub_plan_; not in dispatch_traits, a synchronous in-coroutine call.
        [[nodiscard]] unique_future<core::result_wrapper_t<components::operators::chunks_vector_t>>
        run_subplan(components::operators::operator_ptr root, components::pipeline::context_t* ctx) override;

        using dispatch_traits = actor_zeta::dispatch_traits<&executor_t::execute_plan_full,
                                                            &executor_t::register_udf,
                                                            &executor_t::unregister_udf,
                                                            &executor_t::register_cast,
                                                            &executor_t::unregister_cast,
                                                            &executor_t::set_explain_renderer,
                                                            &executor_t::poke_msg,
                                                            &executor_t::unregister_udf_uid>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        plan_t traverse_plan_(components::operators::operator_ptr&& plan,
                              const components::logical_plan::storage_parameters& parameters,
                              services::context_storage_t&& context_storage);

        unique_future<sub_plan_result_t> execute_sub_plan_(components::session::session_id_t session,
                                                           plan_t plan_data,
                                                           components::table::transaction_data txn,
                                                           uint64_t lowest_active_start_time);

        // Push-based, one batch at a time from source to sink; peak memory is one batch plus active sink state.
        unique_future<core::result_wrapper_t<components::operators::chunks_vector_t>>
        execute_pipeline(components::operators::operator_ptr root, components::pipeline::context_t* ctx);

        // Precondition: caller must have already called root->prepare(); returns the first error, or no_error().
        unique_future<core::error_t> drive_subplan_(components::operators::operator_ptr root,
                                                    components::pipeline::context_t* ctx);

        // Fills a gap run_subplan has: traverse_plan_ pre-splits build sides for the top-level flow, but a single
        // root (e.g. the recursive-CTE's JOIN(scan, cte_scan)) has none, so drive it here via drive_subplan_.
        unique_future<core::error_t> materialize_build_sides_(components::operators::operator_ptr root,
                                                              components::pipeline::context_t* ctx);

        // Fires a non-final incremental flush once chain[dml_idx]'s buffered rows >= dml_flush_row_threshold_; a
        // member coroutine with exactly one co_await (lost-wakeup-safe).
        unique_future<core::error_t> maybe_mid_flush(std::pmr::vector<components::operators::operator_t*>& chain,
                                                     std::size_t dml_idx,
                                                     components::pipeline::context_t* ctx);

        // Unified commit publisher: builds node_transaction_t(commit) — ddl_mode adds the flush/WAL prefix.
        unique_future<execute_result_t> run_commit_pipeline_(components::session::session_id_t session,
                                                             components::table::transaction_data txn,
                                                             const components::graph_execution_context& settings,
                                                             uint64_t lowest_active_start_time,
                                                             bool ddl_mode);

    private:
        // Constructor arguments, never defaults: forgetting one in the init-list does not compile.
        actor_zeta::address_t parent_address_;
        actor_zeta::address_t wal_address_;
        actor_zeta::address_t disk_address_;
        actor_zeta::address_t index_address_;
        log_t log_;
        components::compute::function_registry_t function_registry_;
        components::casts::cast_registry_t cast_registry_;
        // Host-injected (dispatcher -> executor); never null — Null Object defaults.
        planner::create_plan_rule_t create_plan_rule_{&planner::no_custom_lowering};
        components::planner::optimizer_pass_t optimizer_pass_{&components::planner::no_op_pass};
        // Bound on buffered rows before the pump forces an incremental flush; 0 disables the gate.
        uint64_t dml_flush_row_threshold_{0};
        static constexpr uint32_t kExplainRendererSlotLimit = 1024;
        // Slot 0 (built-in postgres) is seeded in the ctor body — can't brace-default a keyed slot.
        std::pmr::vector<explain_render_fn> explain_renderers_;

        [[nodiscard]] bool explain_slot_registered_(uint32_t id) const noexcept {
            return id < explain_renderers_.size() && explain_renderers_[id] != nullptr;
        }

        // Unregistered id resolves to slot 0 as the default, not a silent fallback — pinned by
        // test_explain.cpp's out-of-range cases.
        [[nodiscard]] explain_render_fn resolve_explain_renderer_(uint32_t id) const noexcept {
            if (explain_slot_registered_(id)) {
                return explain_renderers_[id];
            }
            return explain_renderers_.empty() ? &render_postgres : explain_renderers_[0];
        }

        // `cs` non-null resolves scan names live; null reads the pre-collected `names` (ANALYZE).
        [[nodiscard]] components::cursor::cursor_t_ptr
        render_explain_(const components::operators::operator_ptr& explain_root,
                        const explain_name_map_t& names,
                        const services::context_storage_t* cs,
                        uint32_t render_id,
                        bool analyze,
                        std::pmr::vector<explain_plan_node> captured_subplans);
    };

    using executor_ptr = std::unique_ptr<executor_t, actor_zeta::pmr::deleter_t>;
} // namespace services::collection::executor
