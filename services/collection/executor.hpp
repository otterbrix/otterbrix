#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/casts/cast_registry.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/place.hpp>
#include <components/catalog/settings.hpp>
#include <components/compute/function.hpp>
#include <components/configuration/configuration.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/context/subplan_runner.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_prepare.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>
#include <optional>
#include <services/dispatcher/registry_changes.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <set>

#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/host_hooks.hpp>
#include <components/planner/optimizer.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/transaction.hpp>
#include <core/date/date_types.hpp>
#include <services/collection/context_storage.hpp>
#include <services/collection/explain/explain_renderer.hpp>
#include <services/dispatcher/validation/schema.hpp>
#include <span>
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

    // Test seam: the executor is about to await what `source` promised, its open or its next batch.
    using source_await_hook_fn = void (*)(const components::operators::operator_t* source);
    void dev_set_source_await_hook(source_await_hook_fn hook) noexcept; // nullptr = off

    // Test seam: the user functions the registries of every executor of the process hold together.
    std::int64_t dev_user_functions_held() noexcept;
#endif

    // A view the statement reads, as its catalog row described it before the expansion; the body is read back as the
    // first child of `reference`. REFRESH MATERIALIZED VIEW hands its stored body over the same way.
    struct expanded_view_t {
        components::logical_plan::node_ptr reference;
        components::logical_plan::resolved_table_metadata_t view;
    };

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
        // Drained by execute_plan_full's commit tail; the dispatcher reads only cursor and applied settings.
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
        components::catalog::setting_scope_t applied_setting_scope{};
        // Filled only from a SET's sub-plan result, which gives it its resource; empty everywhere else.
        std::pmr::vector<components::catalog::setting_assignment_t> applied_settings{};
        // Move-only, and everything from here down is appended at the END: a brace initializer that sets
        // only a prefix of the members has to keep compiling.
        std::optional<explain_plan_node> captured_explain_ir{};
        std::vector<components::table::referenced_delete_t> referenced_deletes{};
        // written() of the plan root; the cursor reports it only for an INSERT / UPDATE / DELETE.
        uint64_t written{0};
        // Storages that kept state of the statement's transaction (a host's backends it read or wrote).
        std::vector<components::table::participant_t> participants{};
        // A host's rows its DDL replies brought, still to be written in the statement's transaction.
        std::vector<components::host_rows_t> host_rows{};
        // Every pg_proc row the statement's DROPs took with their dependents.
        std::vector<components::pipeline::dropped_function_t> dropped_functions{};
        // Filled only from a PREPARE's or a DEALLOCATE's sub-plan result.
        components::logical_plan::prepared_change_t prepared_change{};
    };

    // The registries a statement sees: the executor's own, or copies of them with its transaction's own changes over
    // them (PostgreSQL 18 inval.c:1409 CommandEndInvalidationMessages).
    struct statement_registries_t {
        const components::compute::function_registry_t* functions;
        const components::casts::cast_registry_t* casts;
    };

    struct plan_t {
        components::operators::operator_ptr root;
        std::stack<components::operators::operator_ptr> sub_plans;
        // Non-owning: points into the execute_plan frame's storage, which outlives execute_sub_plan_.
        const components::logical_plan::storage_parameters* parameters;
        services::context_storage_t context_storage_;
        bool analyze{false};

        explicit plan_t(components::operators::operator_ptr root,
                        std::stack<components::operators::operator_ptr>&& sub_plans,
                        const components::logical_plan::storage_parameters* parameters,
                        services::context_storage_t&& context_storage);
    };

    struct opened_source_t {
        components::operators::operator_t* op;
        actor_zeta::unique_future<core::error_t> ready;
    };
    using opened_sources_t = std::pmr::vector<opened_source_t>;

    // Internal only — never crosses an actor boundary; drained from pipeline::context_t::dml_*.
    struct sub_plan_result_t {
        components::cursor::cursor_t_ptr cursor;
        std::vector<dml_append_range_t> dml_appends;
        std::vector<dml_delete_range_t> dml_deletes;
        std::vector<components::catalog::oid_t> dropped_storage_oids;
        std::vector<components::catalog::oid_t> created_storage_oids;
        std::vector<components::table::created_index_t> created_indexes;
        std::vector<components::table::referenced_delete_t> referenced_deletes;

        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends;
        std::set<components::catalog::oid_t> pg_catalog_delete_tables;
        std::vector<components::pg_attribute_commit_id_backfill_t> pg_attribute_commit_id_backfills;
        uint64_t commit_id{0};
        components::catalog::setting_scope_t applied_setting_scope{};
        std::pmr::vector<components::catalog::setting_assignment_t> applied_settings;
        uint64_t written{0};
        std::vector<components::table::participant_t> participants;
        std::vector<components::host_rows_t> host_rows;
        std::vector<components::pipeline::dropped_function_t> dropped_functions;
        components::logical_plan::prepared_change_t prepared_change;
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
                   const configuration::config_execution& execution,
                   components::planner::primitives_t primitives);
        ~executor_t() = default;

        // INTERNAL: called only from execute_plan_full via co_await, never through the mailbox. captured_subplans
        // is by value — a pmr member can't default without re-anchoring to the forbidden get_default_resource().
        unique_future<execute_result_t> execute_plan(components::session::session_id_t session,
                                                     components::logical_plan::execution_plan_t plan,
                                                     services::context_storage_t context_storage,
                                                     statement_registries_t registries,
                                                     components::table::transaction_data txn,
                                                     uint64_t lowest_active_start_time,
                                                     std::pmr::vector<explain_plan_node> captured_subplans);

        // The per-query entry point (the dispatcher's only execute send); txn-state access rides
        // txn_*_msg to the dispatcher, the sole transaction_manager_t owner.
        // The dispatcher resolves the transaction and hands it down: a statement never opens its own.
        // `own_changes`: what the statement's open transaction registered and dropped before it.
        unique_future<execute_result_t> execute_plan_full(components::session::session_id_t session,
                                                          components::logical_plan::execution_plan_t plan,
                                                          services::dispatcher::txn_session_context_t session_ctx,
                                                          services::dispatcher::registry_changes_t own_changes);

        // What a committed transaction registered and dropped.
        unique_future<core::error_t> apply_registry_changes(services::dispatcher::registry_changes_t changes);

        // Fanned out from the dispatcher; POD fn-pointers stored per-executor, so there's no shared mutable state.
        unique_future<bool> set_explain_renderer(uint32_t id, explain_render_fn fn);

        // Same seam as execute_sub_plan_; not in dispatch_traits, a synchronous in-coroutine call.
        [[nodiscard]] unique_future<core::result_wrapper_t<components::operators::chunks_vector_t>>
        run_subplan(components::operators::operator_ptr root, components::pipeline::context_t* ctx) override;

        using dispatch_traits = actor_zeta::dispatch_traits<&executor_t::execute_plan_full,
                                                            &executor_t::apply_registry_changes,
                                                            &executor_t::set_explain_renderer>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        // `sub_query`: a flattened sub-plan of the statement, whose describe answers its columns only (the
        // statement's $n are the main plan's to list).
        unique_future<execute_result_t> execute_statement_(components::session::session_id_t session,
                                                           components::logical_plan::execution_plan_t plan,
                                                           services::dispatcher::txn_session_context_t session_ctx,
                                                           statement_registries_t registries,
                                                           std::pmr::vector<expanded_view_t> expanded_views,
                                                           bool sub_query = false);

        statement_registries_t own_registries_() const noexcept { return {&function_registry_, &cast_registry_}; }

        // A registration or a drop of a function or a cast, checked against the statement's registries, with the
        // identity the plan is lowered with stamped on its node; the change the statement makes when it succeeds.
        core::result_wrapper_t<services::dispatcher::registry_changes_t>
        prepare_registry_change_(components::logical_plan::execution_plan_t& plan,
                                 statement_registries_t registries,
                                 components::compute::function_uid reserved_uid);

        unique_future<execute_result_t>
        run_resolve_subplan_(components::session::session_id_t session,
                             const services::dispatcher::txn_session_context_t& session_ctx,
                             const services::context_storage_t& statement,
                             components::logical_plan::catalog_resolves_t* resolves,
                             std::pmr::vector<components::logical_plan::node_ptr> resolve_nodes);

        // The tables of connections' databases the DROPs of the planned statement reach (DROP TABLE, SCHEMA,
        // DATABASE, TYPE ... CASCADE), found before it runs: the RESTRICT refusal, and the tables with their storages
        // stamped on the drop. Only in autocommit, and only one connection per transaction.
        unique_future<core::error_t>
        stamp_storage_targets_(components::session::session_id_t session,
                               const services::dispatcher::txn_session_context_t& session_ctx,
                               const services::context_storage_t& statement,
                               components::table::transaction_data txn,
                               components::logical_plan::execution_plan_t& plan);

        // The functions a view body calls are the ones CREATE bound it to (pg_proc read by the 'f' rows' oids).
        unique_future<core::error_t> pin_functions_(components::session::session_id_t session,
                                                    components::table::transaction_data txn,
                                                    const components::compute::function_registry_t& functions,
                                                    const components::logical_plan::resolved_table_metadata_t* view,
                                                    components::logical_plan::node_t* body);

        // The sub-plans of `plan` before its main one, front to back: each runs, or for EXPLAIN and describe is only
        // planned (its IR into `captured_subplans`, a NULL of its type for its value), and fills its slot.
        unique_future<core::error_t> run_sub_queries_(components::session::session_id_t session,
                                                      components::logical_plan::execution_plan_t& plan,
                                                      const services::dispatcher::txn_session_context_t& session_ctx,
                                                      statement_registries_t registries,
                                                      components::logical_plan::explain_type explain,
                                                      std::pmr::vector<explain_plan_node>& captured_subplans);

        // EXECUTE: the statement the session stored under the name runs in place of this one.
        unique_future<execute_result_t> execute_prepared_(components::session::session_id_t session,
                                                          components::logical_plan::execution_plan_t plan,
                                                          services::dispatcher::txn_session_context_t session_ctx,
                                                          statement_registries_t registries);
        // PREPARE: the body's describe, stamped on the node with whether the session has the name already.
        unique_future<core::error_t> describe_prepared_(components::session::session_id_t session,
                                                        components::logical_plan::node_prepare_t& prepare,
                                                        const services::dispatcher::txn_session_context_t& session_ctx,
                                                        statement_registries_t registries);
        // An aggregate or a set-returning function in the plan of an EXECUTE argument (PostgreSQL 18
        // EXPR_KIND_EXECUTE_PARAMETER).
        core::error_t refuse_execute_argument_(const components::logical_plan::node_t* argument,
                                               const components::compute::function_registry_t& functions) const;
        // The value of $`number` as its parameter's type.
        core::result_wrapper_t<components::vector::vector_t>
        coerce_argument_(const components::vector::vector_t& value,
                         const components::types::complex_logical_type& expected,
                         std::size_t number,
                         bool quoted_literal,
                         const components::casts::cast_registry_t& casts,
                         const components::graph_execution_context& settings) const;

        unique_future<execute_result_t> refresh_matview_(components::session::session_id_t session,
                                                         const components::logical_plan::execution_plan_t& plan,
                                                         const services::dispatcher::txn_session_context_t& session_ctx,
                                                         statement_registries_t registries,
                                                         const components::graph_execution_context& settings);

        // The storage of a database of `connection`: the disk for otterbrix's own (no connection), else the backend the
        // host's map gives it; a connection the map lacks has none.
        core::result_wrapper_t<components::table::storage_address_t>
        storage_of_connection_(std::string_view connection) const;

        // Every resolved table gets its storage's address. A table of a connection the host's map lacks keeps an
        // empty one when `catalog_only` (a DROP takes it from the catalog alone), and refuses the statement otherwise.
        core::error_t bind_storages_(components::logical_plan::catalog_resolves_t& resolves, bool catalog_only) const;

        // A write to a connection's database when the transaction already wrote to another one, or a statement that
        // writes to two of them, is refused before any backend gets a message.
        core::error_t check_write_place_(const services::dispatcher::txn_session_context_t& session_ctx,
                                         std::span<const std::string> written_connections) const;

        // The type of each $n of the validated main plan, by number: the lowest one nothing types is 42P18, one given
        // two types 42P08 (PostgreSQL 18 pg_analyze_and_rewrite_varparams, variable_coerce_param_hook).
        core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>
        parameter_types_of_(const components::logical_plan::execution_plan_t& plan) const;
        // The result columns of the validated main plan: a query's own, the RETURNING ones of a write.
        std::pmr::vector<components::types::complex_logical_type>
        result_columns_(const components::logical_plan::execution_plan_t& plan,
                        const services::dispatcher::validation::named_schema& returning) const;
        // A describe's answer: the result columns (the RETURNING ones of a write, QUERY PLAN of an EXPLAIN) and the
        // types of the $n.
        components::cursor::cursor_t_ptr
        describe_(const components::logical_plan::execution_plan_t& plan,
                  const services::dispatcher::validation::named_schema& returning,
                  const std::pmr::vector<components::types::complex_logical_type>& parameters) const;

        // The host's rows its DDL replies brought: written by the statement, in its transaction.
        unique_future<core::error_t> write_host_rows_(components::session::session_id_t session,
                                                      const services::dispatcher::txn_session_context_t& session_ctx,
                                                      statement_registries_t registries,
                                                      std::vector<components::host_rows_t> rows);

        // After optimize: every largest sub-tree that reads only one backend's tables is offered to that backend,
        // all offers at once; what a backend takes becomes a node_remote_t (docs/embedding-host-api.md, "Pushdown").
        // The disk is never offered a block.
        unique_future<core::error_t> offer_blocks_(components::session::session_id_t session,
                                                   components::logical_plan::execution_plan_t& plan,
                                                   components::table::transaction_data txn,
                                                   const components::graph_execution_context& settings);

        core::error_t missing_relation_(const qualified_name_t& name) const;

        enum class target_outcome_t
        {
            act,
            nothing_to_do
        };
        core::result_wrapper_t<target_outcome_t> decide_target_(components::logical_plan::execution_plan_t& plan,
                                                                const qualified_name_t& target);
        core::error_t missing_target_(const components::logical_plan::execution_plan_t& plan,
                                      const qualified_name_t& target) const;
        core::error_t existing_target_(const components::logical_plan::execution_plan_t& plan,
                                       const qualified_name_t& target) const;
        // A target in a connection's database: only a table is created there, and its DDL runs only in autocommit.
        core::error_t check_connection_target_(const components::logical_plan::execution_plan_t& plan,
                                               const qualified_name_t& target) const;

        // A DROP's target that exists, refused for what it is (built-in, system catalog, another relation kind).
        core::error_t check_drop_target_(const components::logical_plan::execution_plan_t& plan,
                                         const qualified_name_t& target) const;

        // How many table and type entries of the resolve are the statement's own, before a view expansion adds any.
        struct own_entries_t {
            std::size_t tables{0};
            std::size_t types{0};
        };
        static own_entries_t own_entries_of(const components::logical_plan::catalog_resolves_t& resolves) noexcept;

        unique_future<core::error_t> validate_create_view_(components::session::session_id_t session,
                                                           components::table::transaction_data txn,
                                                           components::logical_plan::execution_plan_t& plan,
                                                           const components::graph_execution_context& settings,
                                                           statement_registries_t registries,
                                                           own_entries_t own,
                                                           const std::pmr::vector<expanded_view_t>& expanded_views);

        // A column as CREATE TABLE and ALTER TABLE ... ADD COLUMN take it: a type written by its catalog name is that
        // built-in, another name a type main.pg_catalog or the session's place has (a qualified one, its schema's);
        // the type must be persistable and the DEFAULT is cast to it.
        core::error_t prepare_column_(const components::logical_plan::catalog_resolves_t& resolves,
                                      const components::graph_execution_context& settings,
                                      const components::casts::cast_registry_t& casts,
                                      components::table::column_definition_t& column);

        enum class alter_subcommands_t : bool
        {
            remain,
            none_left
        };
        core::result_wrapper_t<alter_subcommands_t>
        check_alter_subcommands_(const components::logical_plan::catalog_resolves_t& resolves,
                                 const components::logical_plan::resolved_table_metadata_t& table,
                                 std::vector<components::logical_plan::alter_table_subcommand_t>& subcommands);

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

        components::pipeline::context_t make_pipeline_context_(components::session::session_id_t session,
                                                               const plan_t& plan_data,
                                                               components::table::transaction_data txn,
                                                               uint64_t lowest_active_start_time);

        // Starts open() on every not-yet-executed source under root without awaiting any, so backend fetches overlap.
        void open_sources_(components::operators::operator_t* root,
                           components::pipeline::context_t* ctx,
                           opened_sources_t& opened);

        // Awaits the open of every source under `piece` (every source when `piece` is null), even after an error, and
        // returns the first error.
        unique_future<core::error_t> await_opened_(opened_sources_t& opened, components::operators::operator_t* piece);

        // Awaits the open of each operator in `wanted`, in that order, and stops at the first error.
        unique_future<core::error_t>
        await_opens_at_start_(opened_sources_t& opened,
                              const std::pmr::vector<components::operators::operator_t*>& wanted);

        // Precondition: every source under root was opened and its open awaited.
        unique_future<core::error_t> drive_opened_subplan_(components::operators::operator_ptr root,
                                                           components::pipeline::context_t* ctx);

        // Fills a gap run_subplan has: traverse_plan_ pre-splits build sides for the top-level flow, but a single
        // root (e.g. the recursive-CTE's JOIN(scan, cte_scan)) has none, so drive it here via drive_opened_subplan_.
        unique_future<core::error_t> materialize_build_sides_(components::operators::operator_ptr root,
                                                              components::pipeline::context_t* ctx);

        // Fires a non-final incremental flush once chain[dml_idx]'s buffered rows reach its flush_row_threshold over
        // dml_flush_row_threshold_.
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
        // The host's map of connections to their backends' actors, this executor's own copy.
        struct backend_t {
            std::pmr::string connection;
            actor_zeta::address_t address;
            std::uint64_t max_rows_per_write{0};
        };
        std::pmr::vector<backend_t> backends_;
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
