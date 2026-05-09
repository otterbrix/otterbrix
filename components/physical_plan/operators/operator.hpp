#pragma once

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
        join,
        aggregate,
        raw_data,
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
        // VACUUM — cleanup_versions + compact across user tables (relkind 'r'/'g'),
        // cleanup index versions, rebuild and re-populate indexes per table.
        // Iterates pg_class to discover user tables (no dispatcher state).
        vacuum,
        // GET_SCHEMA (Phase 4 #54) — self-resolving leaf operator that returns
        // one complex_logical_type per (database, collection) id by reading
        // pg_namespace+pg_class+pg_attribute. Replaces inline catalog_view_t
        // reads in manager_dispatcher_t::get_schema.
        get_schema,
        // REGISTER_UDF / UNREGISTER_UDF (Phase 4 #55) — operator-pipeline
        // replacements for inline manager_dispatcher_t::{register,unregister}_udf.
        // operator_register_udf_t fans out to per-executor registries, mirrors
        // into function_registry_t::get_default(), and persists pg_proc rows.
        // operator_unregister_udf_t reverses the registry+pg_proc effects.
        register_udf,
        unregister_udf,
        // COMMIT / ROLLBACK (Phase 4 #56) — operator-pipeline replacement for
        // inline manager_dispatcher_t::{commit,abort}_transaction. The
        // operator drives txn_manager->{commit,abort}() and (for commit) the
        // pg_catalog MVCC state swap on disk via storage_commit_appends /
        // storage_revert_appends. Invoked directly by the dispatcher
        // (mirrors operator_get_schema_t) since the manager-level state
        // (txn_manager_) lives outside the per-collection executor.
        commit_transaction,
        abort_transaction,
        // COMPUTED_FIELD_REGISTER / COMPUTED_FIELD_UNREGISTER (Phase 7.1) —
        // maintain pg_computed_column rows for relkind='g' dynamic-schema
        // tables. register: per-column NEW / SAME-TYPE / TYPE-EVOLUTION
        // classification → append fresh row with attrefcount=1 on NEW or
        // TYPE-EVOLUTION (no-op on SAME-TYPE). unregister: append a
        // refcount=0 tombstone so the resolver hides the column.
        computed_field_register,
        computed_field_unregister
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
        executed
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
        void clear(); //todo: replace by copy

        void set_error(std::string msg);
        bool has_error() const noexcept;
        const std::string& error_message() const noexcept;

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
        std::string error_message_;
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
