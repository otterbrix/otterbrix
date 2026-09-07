#pragma once

#include <core/result_wrapper.hpp>

#include <actor-zeta/detail/future.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/log/log.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>

#include <chrono>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::expressions {
    class key_t;
}

namespace components::operators {

    enum class operator_type
    {
        unused = 0x0,
        empty,
        match,
        // Placed above the group operator (never over a scan), unlike `match`; rendered "Having".
        having,
        full_scan,
        transfer_scan,
        index_scan,
        pushed_reduce_scan,
        insert,
        remove,
        update,
        sort,
        select,
        // Whole-row dedup layered outermost (under limit); rendered "Unique", not "Filter".
        distinct,
        // Wraps a terminal whose source can't apply a merged outer limit itself (UNION, pushed GROUP BY).
        limit,
        join,
        // Substituted for `join` by create_plan_join when the ON condition is a single eq(left.key, right.key).
        hash_join,
        aggregate,
        // Coordinator terminal above pushed_reduce_scan; owns the empty-input scalar row a sharded slice lacks.
        group_merge,
        raw_data,
        function,
        union_op,
        recursive_cte,
        cte_scan,
        check_constraint,
        fk_check,
        fk_cascade,
        // Reads the child DML's constraint_input() and errors on a duplicate key.
        unique_constraint,
        sequence,
        create_collection,
        alter_column_add,
        alter_column_rename,
        alter_column_drop,
        // Walks pg_depend at runtime, replacing the dispatcher BFS duplicated across the drop_* handlers.
        dynamic_cascade_delete,
        checkpoint,
        // session_catalog_t mutation stays in the dispatcher post-success; touches no shared state.
        set_timezone,
        vacuum,
        register_udf,
        unregister_udf,
        register_cast,
        unregister_cast,
        // Invoked directly by the dispatcher: the manager-level txn_manager_ lives outside the
        // per-collection executor.
        commit_transaction,
        abort_transaction,
        begin_transaction,
        computed_field_register,
        computed_field_unregister,
        // Self-resolving catalog read mirroring manager_disk_t::resolve_table without the dedicated actor message.
        resolve_table,
        resolve_namespace,
        resolve_database,
        // Composite-type reconstruction (relkind='c') is out of scope; stays on resolve_type_sync.
        resolve_type,
        resolve_constraint,
        allocate_oids,
        extension,
        batch
    };

    inline bool is_scan(operator_type t) {
        return t == operator_type::full_scan || t == operator_type::transfer_scan || t == operator_type::index_scan;
    }

    enum class operator_state
    {
        created,
        executed,
        failed
    };

    // Drives Source -> streaming operators -> Sink one batch at a time, so peak memory is one batch
    // plus active sink state instead of the sum of materialized intermediates.
    enum class pipeline_role
    {
        source,
        streaming,
        sink
    };

    // Raw fn-pointers + void* ctx, not std::function: no IR type crosses into components.
    struct explain_sink {
        void (*on_node)(void*, operator_type, catalog::oid_t, uint64_t, std::chrono::nanoseconds, uint64_t);
        void (*on_end)(void*);
        void* ctx;
        void begin(operator_type t, catalog::oid_t o, uint64_t r, std::chrono::nanoseconds ti, uint64_t l) const {
            on_node(ctx, t, o, r, ti, l);
        }
        void end() const { on_end(ctx); }
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

        void prepare();

        virtual actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx);

        // Default is `sink`; only a SOURCE or a STREAMING operator needs to override this.
        [[nodiscard]] virtual pipeline_role role() const noexcept { return pipeline_role::sink; }

        [[nodiscard]] virtual bool produces_query_rows() const noexcept { return false; }

        // A drained source returns an EMPTY chunk (cardinality 0), never a throw.
        [[nodiscard]] virtual actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx);

        [[nodiscard]] virtual bool holds_open_cursor() const noexcept { return false; }

        // A live, un-released cursor permanently gates compact(); idempotent.
        [[nodiscard]] virtual actor_zeta::unique_future<void> release_cursor(pipeline::context_t* ctx);

        // Synchronous: the executor owns the cross-actor await drive, operators never self-send.
        [[nodiscard]] virtual core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out);

        [[nodiscard]] virtual core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out);

        // The executor drives this operator's await_async_and_resume after the pump completes.
        [[nodiscard]] virtual bool needs_async_finalize() const noexcept { return false; }

        // Catalog-mode DML returns 0 too: its single-shot path must not be mid-flushed.
        [[nodiscard]] virtual uint64_t buffered_rows() const noexcept { return 0; }

        // Covers per-run streaming state reset_for_reuse() doesn't reach (a source's cursor, a
        // sink's built accumulator); the recursive-CTE driver calls both on every node per pass.
        virtual void reset_pipeline_state() noexcept {}

        bool is_executed() const;
        bool is_root() const noexcept;
        void set_as_root() noexcept;

        virtual std::pmr::memory_resource* resource() const noexcept;
        log_t& log() noexcept;

        [[nodiscard]] ptr left() const noexcept;
        [[nodiscard]] ptr right() const noexcept;
        [[nodiscard]] operator_state state() const noexcept;
        [[nodiscard]] operator_type type() const noexcept;
        const operator_data_ptr& output() const;

        // A DML operator snapshots these before overwriting output_ with the RETURNING chunk, so a
        // constraint driven after the DML can still read the written rows.
        const operator_data_ptr& constraint_input() const noexcept { return constraint_input_; }
        void set_children(ptr left, ptr right = nullptr);
        void set_output(operator_data_ptr data);
        void mark_executed();
        void mark_failed() noexcept { state_ = operator_state::failed; }
        void reset_for_reuse() noexcept {
            state_ = operator_state::created;
            output_ = nullptr;
            constraint_input_ = nullptr;
        }
        void clear(); //todo: replace by copy

        // Rebuilds the message on this operator's resource, so an &&-overload could reintroduce
        // producer-arena adoption.
        void set_error(const core::error_t& error);
        bool has_error() const noexcept;
        const core::error_t& get_error() const noexcept;

        // Operators that emit a typed result over zero input rows override this to avoid the NA sentinel.
        virtual void set_output_types(const std::pmr::vector<types::complex_logical_type>& types);

        // Written only by execute_pipeline when ctx->analyze is set; read only by the EXPLAIN renderer.
        void record_analyze(uint64_t rows, std::chrono::nanoseconds dt) noexcept {
            analyze_rows_ += rows;
            analyze_time_ += dt;
        }
        void bump_analyze_loop() noexcept { ++analyze_loops_; }

        // Scans override to add their table oid; lateral/recursive override to recurse into private sub-plans.
        void explain(const explain_sink& s) const { explain_impl(s); }

    protected:
        void explain_begin(const explain_sink& s, catalog::oid_t oid) const {
            s.begin(type(), oid, analyze_rows_, analyze_time_, analyze_loops_);
        }

        std::pmr::memory_resource* resource_;
        log_t log_;

        ptr left_{nullptr};
        ptr right_{nullptr};
        operator_data_ptr output_{nullptr};
        operator_write_data_ptr modified_{nullptr};
        operator_data_ptr constraint_input_{nullptr};

    private:
        // Non-pure: operator_t has concrete leaf subclasses that don't override it.
        virtual void explain_impl(const explain_sink& s) const {
            explain_begin(s, catalog::INVALID_OID);
            if (left_) {
                left_->explain(s);
            }
            if (right_) {
                right_->explain(s);
            }
            s.end();
        }

        operator_type type_;
        operator_state state_{operator_state::created};
        bool root{false};
        bool prepared_{false};
        core::error_t error_;
        uint64_t analyze_rows_{0};
        std::chrono::nanoseconds analyze_time_{std::chrono::nanoseconds::zero()};
        uint64_t analyze_loops_{0};
    };

    class read_only_operator_t : public operator_t {
    public:
        read_only_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type);
    };

    class read_write_operator_t : public operator_t {
    public:
        read_write_operator_t(std::pmr::memory_resource* resource, log_t log, operator_type type);
    };

    using operator_ptr = operator_t::ptr;

} // namespace components::operators
