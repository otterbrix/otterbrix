#pragma once

#include <core/result_wrapper.hpp>

#include <actor-zeta/detail/future.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/context/context.hpp>
#include <components/log/log.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>

namespace components::expressions {
    class key_t;
}

namespace components::operators {

    // Forward decl: DML operators receive parsed metadata via
    // accept_resolved_metadata(). Defined in resolved_table_metadata.hpp.
    struct resolved_table_metadata_t;

    enum class operator_type
    {
        unused = 0x0,
        empty,
        match,
        full_scan,
        transfer_scan,
        index_scan,
        primary_key_scan,
        insert,
        remove,
        update,
        sort,
        select,
        join,
        // Equi-join fast path. Substituted for `join` by create_plan_join when the
        // ON condition is a single eq(left.key, right.key). Builds a hash table on
        // the right side once and probes with the left; same output layout as `join`.
        hash_join,
        aggregate,
        raw_data,
        union_op,
        recursive_cte,
        cte_scan,
        // Constraint operators
        check_constraint,
        fk_check,
        fk_cascade,
        // DDL sequencing operator
        sequence,
        // DDL primitive write (planner-built pg_catalog row)
        primitive_write,
        // DDL primitive delete (planner-built pg_catalog row delete)
        primitive_delete,
        // DDL create collection (storage + index registration + catalog writes)
        create_collection,
        // ALTER TABLE per-clause primitives
        alter_column_add,
        alter_column_rename,
        alter_column_drop,
        // Universal cascade-delete driver: walks pg_depend at runtime and
        // deletes the dependency closure inline. Replaces the dispatcher BFS
        // duplicated across drop_database/drop_collection/drop_sequence/etc.
        dynamic_cascade_delete,
        // CHECKPOINT — flush indexes, snapshot wal-id, checkpoint_all on disk,
        // truncate WAL segments older than the recovery boundary.
        checkpoint,
        // SET TIMEZONE — validates name, sends pg_settings ('TimeZone',<name>)
        // row to disk; leaf operator. session_catalog_t mutation stays in the
        // dispatcher post-success — operator does not touch shared state.
        set_timezone,
        // VACUUM — cleanup_versions + compact across user tables (relkind 'r'/'g'),
        // cleanup index versions, rebuild and re-populate indexes per table.
        // Iterates pg_class to discover user tables (no dispatcher state).
        vacuum,
        // GET_SCHEMA — self-resolving leaf operator that returns
        // one complex_logical_type per (database, collection) id by reading
        // pg_namespace+pg_class+pg_attribute. Used by
        // manager_dispatcher_t::get_schema.
        get_schema,
        // REGISTER_UDF / UNREGISTER_UDF — operator-pipeline replacements
        // for inline manager_dispatcher_t::{register,unregister}_udf.
        // operator_register_udf_t fans out to per-executor registries, mirrors
        // into function_registry_t::get_default(), and persists pg_proc rows.
        // operator_unregister_udf_t reverses the registry+pg_proc effects.
        register_udf,
        unregister_udf,
        // COMMIT / ROLLBACK — operator-pipeline replacement for inline
        // manager_dispatcher_t::{commit,abort}_transaction. The operator
        // drives txn_manager->{commit,abort}() and (for commit) the
        // pg_catalog MVCC state swap on disk via storage_publish_commits /
        // storage_revert_appends. Invoked directly by the dispatcher
        // (mirrors operator_get_schema_t) since the manager-level state
        // (txn_manager_) lives outside the per-collection executor.
        commit_transaction,
        abort_transaction,
        // BEGIN / START TRANSACTION: ensures an active transaction exists and
        // marks it explicit, so DML accumulates for a batched COMMIT-time publish
        // (see node_begin_transaction.hpp).
        begin_transaction,
        // COMPUTED_FIELD_REGISTER / COMPUTED_FIELD_UNREGISTER —
        // maintain pg_computed_column rows for relkind='g' dynamic-schema
        // tables. register: per-column NEW / SAME-TYPE / TYPE-EVOLUTION
        // classification → append fresh row with attrefcount=1 on NEW or
        // TYPE-EVOLUTION (no-op on SAME-TYPE). unregister: append a
        // refcount=0 tombstone so the resolver hides the column.
        computed_field_register,
        computed_field_unregister,
        // RESOLVE_TABLE — self-resolving catalog read that mirrors
        // manager_disk_t::resolve_table without using the dedicated
        // resolve_table actor message. Scans pg_class for (oid, relkind,
        // relnamespace), then pg_attribute (relkind='r') or pg_computed_column
        // with tombstone/max-version filter (relkind='g') for column metadata.
        // Emits a data_chunk_t with one row per live column:
        // (position int32, attoid oid, attname string, atttypid oid,
        //  atttypspec string). relkind is exposed via the operator instance
        // (resolved_relkind()) for downstream usage that needs to branch on
        // table flavor.
        resolve_table,
        // RESOLVE_NAMESPACE — leaf operator that scans pg_namespace by
        // nspname and emits the resolved namespace_oid as a single-row
        // data_chunk (col 0 = UINTEGER oid). Used by name-resolution
        // pipelines that need to avoid the dispatcher's cached catalog snapshot.
        resolve_namespace,
        // RESOLVE_DATABASE — analogous leaf operator that scans
        // pg_database (well_known_oid=19) by datname and emits the resolved
        // database_oid. Routing key for multi-database WAL workers.
        resolve_database,
        // RESOLVE_TYPE — leaf operator that scans pg_type by
        // (typname, typnamespace) and emits the matching row as a single-row
        // data_chunk_t (cols mirror pg_type_columns: oid, typname,
        // typnamespace, typdefspec). Drives read_rows_by_key only.
        // Composite-type reconstruction (pg_class relkind='c' fallback) is
        // out-of-scope; that path stays on the synchronous resolve_type_sync
        // helper until a separate operator covers it.
        resolve_type,
        // RESOLVE_FUNCTION — leaf operator that scans pg_proc by
        // (proname, pronamespace) and emits matching pg_proc rows as a
        // data_chunk (cols: oid, proname, pronamespace, pronargs, prouid,
        // proargmatchers, prorettype). Mirrors manager_disk_t::resolve_function
        // without the dedicated actor message — drives read_rows_by_key only.
        resolve_function,
        // RESOLVE_CONSTRAINT — pipeline FK + CHECK constraint resolution.
        // Reads pg_constraint by (conrelid|confrelid) +
        // pg_attribute (column-name lookups) + pg_class + pg_namespace
        // (descendant FK reference resolution). Stamps fks() / check_exprs()
        // onto the back-pointed logical node so enrich reads them via the
        // plan_resolve_index.
        resolve_constraint,
        // ALLOCATE_OIDS — sends one allocate-batch request to the disk actor's
        // oid_generator and stamps the resulting vector on the back-pointed node
        // so the DDL planner can read it via oids().
        allocate_oids,
        batch
    };

    inline bool is_scan(operator_type t) {
        return t == operator_type::full_scan || t == operator_type::transfer_scan || t == operator_type::index_scan ||
               t == operator_type::primary_key_scan;
    }

    enum class operator_state
    {
        created,
        running,
        waiting,
        executed,
        cleared,
        failed
    };

    class operator_t : public boost::intrusive_ref_counter<operator_t> {
    public:
        using ptr = boost::intrusive_ptr<operator_t>;

        operator_t() = delete;
        operator_t(const operator_t&) = delete;
        operator_t(operator_t&&) = default;
        operator_t& operator=(const operator_t&) = delete;
        operator_t& operator=(operator_t&&) = default;

        operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type);

        virtual ~operator_t() = default;

        // Prepare the operator tree (connects children) without executing
        void prepare();

        // TODO fwd
        void on_execute(pipeline::context_t* pipeline_context);
        void on_resume(pipeline::context_t* pipeline_context);
        void async_wait();

        virtual actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx);

        bool is_executed() const;
        bool is_wait_sync_disk() const;
        bool is_root() const noexcept;
        void set_as_root() noexcept;

        ptr find_waiting_operator();

        virtual std::pmr::memory_resource* resource() const noexcept;
        log_t& log() noexcept;

        [[nodiscard]] ptr left() const noexcept;
        [[nodiscard]] ptr right() const noexcept;
        [[nodiscard]] operator_state state() const noexcept;
        [[nodiscard]] operator_type type() const noexcept;
        const operator_data_ptr& output() const;
        const operator_write_data_ptr& modified() const;
        const operator_write_data_ptr& no_modified() const;
        void set_children(ptr left, ptr right = nullptr);
        void set_output(operator_data_ptr data);
        void take_output(ptr& src);
        void mark_executed();
        void mark_failed() noexcept { state_ = operator_state::failed; }
        void reset_for_reuse() noexcept {
            state_ = operator_state::created;
            output_ = nullptr;
        }
        void clear(); //todo: replace by copy

        void set_error(const core::error_t& error);
        void set_error(core::error_t&& error);
        bool has_error() const noexcept;
        const core::error_t& get_error() const noexcept;

        // Sibling-resolve plumbing. When an operator_resolve_table_t runs
        // as an upstream step inside a sequence_t (or as a flattened
        // left-child), its output chunk is parsed into
        // resolved_table_metadata_t and handed to the next consumer via this
        // hook. Default no-op; DML operators override (operator_insert /
        // operator_update / operator_delete).
        virtual void accept_resolved_metadata(resolved_table_metadata_t metadata);
        // True when accept_resolved_metadata() may meaningfully be invoked
        // on this operator. Used by operator_sequence_t to route the
        // resolver's output to the appropriate sibling without inspecting
        // operator_type.
        virtual bool wants_resolved_metadata() const noexcept { return false; }

    protected:
        std::pmr::memory_resource* resource_;
        log_t log_;

        ptr left_{nullptr};
        ptr right_{nullptr};
        operator_data_ptr output_{nullptr};
        operator_write_data_ptr modified_{nullptr};
        operator_write_data_ptr no_modified_{nullptr};

    private:
        virtual void on_execute_impl(pipeline::context_t* pipeline_context) = 0;
        virtual void on_resume_impl(pipeline::context_t* pipeline_context);
        virtual void on_prepare_impl();

        operator_type type_;
        operator_state state_{operator_state::created};
        bool root{false};
        bool prepared_{false};
        core::error_t error_;
    };

    class read_only_operator_t : public operator_t {
    public:
        read_only_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type);
    };

    enum class read_write_operator_state
    {
        pending,
        executed,
        conflicted,
        rolledBack,
        committed
    };

    class read_write_operator_t : public operator_t {
    public:
        read_write_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type);
        //todo:
        //void commit();
        //void rollback();

    protected:
        read_write_operator_state state_;
    };

    using operator_ptr = operator_t::ptr;

} // namespace components::operators
