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
        // Post-aggregation SQL HAVING filter. A dedicated streaming filter operator placed ABOVE
        // the group operator (never over a scan): same per-row predicate machinery as `match`, but
        // structurally always streaming, no LIMIT/read-cap, no row-id propagation. Rendered "Having".
        having,
        full_scan,
        transfer_scan,
        index_scan,
        // Aggregate-pushdown source: ships the POD reduce spec on the dedicated
        // storage_reduce protocol leg and emits the agent's FINAL aggregated rows.
        pushed_reduce_scan,
        insert,
        remove,
        update,
        sort,
        select,
        // SELECT DISTINCT de-duplication (streaming, keep-first). Whole-row dedup layered
        // outermost (under limit). Rendered "Unique" — distinct from a WHERE "Filter".
        distinct,
        // Canonical LIMIT/OFFSET window (streaming). Wraps a terminal whose source
        // cannot apply a merged outer limit itself (UNION, pushed-down GROUP BY). See
        // operator_limit.hpp.
        limit,
        join,
        // Equi-join fast path. Substituted for `join` by create_plan_join when the
        // ON condition is a single eq(left.key, right.key). Builds a hash table on
        // the right side once and probes with the left; same output layout as `join`.
        hash_join,
        aggregate,
        // Coordinator-side terminal of the aggregate pushdown: identity passthrough
        // above a pushed_reduce_scan that OWNS the empty-input scalar row, and the socket a
        // sharded-slice future turns into a real kernel merge (see operator_group_merge.hpp).
        group_merge,
        raw_data,
        function,
        union_op,
        recursive_cte,
        cte_scan,
        // Constraint operators
        check_constraint,
        fk_check,
        fk_cascade,
        // UNIQUE / PRIMARY KEY enforcement. Sourceless streaming sink shaped like
        // fk_check: reads the child DML's constraint_input() and errors on a
        // duplicate key (within-batch typed hash+verify, or an existing table row
        // via scan_by_keys). See operator_unique_constraint.hpp.
        unique_constraint,
        // DDL sequencing operator
        sequence,
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
        // REGISTER_UDF / UNREGISTER_UDF — operator-pipeline replacements
        // for inline manager_dispatcher_t::{register,unregister}_udf.
        // operator_register_udf_t fans out to per-executor registries, mirrors
        // into function_registry_t::get_default(), and persists pg_proc rows.
        // operator_unregister_udf_t reverses the registry+pg_proc effects.
        register_udf,
        unregister_udf,
        register_cast,
        unregister_cast,
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
        // (see node_transaction.hpp).
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
        // RESOLVE_CONSTRAINT — pipeline FK + CHECK constraint resolution.
        // Reads pg_constraint by (conrelid|confrelid) +
        // pg_attribute (column-name lookups) + pg_class + pg_namespace
        // (descendant FK reference resolution). Stamps fks / check_exprs into
        // each entry of the constraint resolve node, where enrich reads them.
        resolve_constraint,
        // ALLOCATE_OIDS — sends one allocate-batch request to the disk actor's
        // oid_generator and stamps the resulting vector on the back-pointed node
        // so the DDL planner can read it via oids().
        allocate_oids,
        // Host-extension operator: an operator_t subclass owned by embedding-host
        // code, delivered into the plan via node_extension_t's payload factory
        // (see logical_plan/node_extension.hpp). The engine drives it purely
        // through the polymorphic operator interface.
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

    // Push-based streaming execution (SELECT read-path). execute_pipeline() drives
    // Source -> streaming operators -> Sink one batch at a time, so peak memory is
    // a single batch plus active sink state instead of the sum of materialized
    // intermediates. An operator's role selects how the driver pulls/pushes it.
    enum class pipeline_role
    {
        source,    // produces batches via source_next() (scans)
        streaming, // 1 batch in -> 0+ out, no accumulation (filter/projection, join probe)
        sink       // accumulates bounded state in push(), emits in finalize() (hash build, group/agg, sort)
    };

    // EXPLAIN: a POD typed-event sink (raw fn-pointers + void* ctx — same idiom as explain_render_fn,
    // NOT std::function). The services-side builder installs ctx + callbacks. No IR type crosses into
    // components — this stays a plain forwarder.
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

        // Prepare the operator tree (connects children) without executing
        void prepare();

        virtual actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx);

        // --- Push-based streaming pipeline interface (SELECT read-path) ---
        // execute_pipeline() inspects role() to drive the operator. The default is
        // `sink`: a leaf/DDL/txn/DML/group/join/sort/distinct operator accumulates
        // bounded state in push() and emits in finalize(), so it needs no override.
        // Only a SOURCE (scans, raw-data carriers) or a STREAMING operator
        // (filter/projection) overrides this.
        [[nodiscard]] virtual pipeline_role role() const noexcept { return pipeline_role::sink; }

        // marker for executor to update sub-queries links
        [[nodiscard]] virtual bool produces_query_rows() const noexcept { return false; }

        // SOURCE: fetch the next batch via an async storage round-trip
        // (storage_fetch_next_batch). A drained source returns an EMPTY chunk
        // (cardinality 0). Buffer-pool OOM / data_corruption ride the result_wrapper,
        // never a throw. Only the executor's nested coroutine drives these awaits
        // (not an agent handler), so N sequential fetches are lost-wakeup-safe.
        [[nodiscard]] virtual actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx);

        // SOURCE: does this operator currently hold an OPEN, un-drained storage cursor?
        // Only a scan source that opened one answers true (A4).
        [[nodiscard]] virtual bool holds_open_cursor() const noexcept { return false; }

        // SOURCE: release an open cursor without draining it. The executor calls this when it
        // stops pumping a source early — an error mid-pump, a satisfied LIMIT, an abandoned
        // sub-plan — because nothing agent-side reclaims an abandoned cursor and a live one
        // permanently gates compact() on its table. Idempotent; default no-op for every
        // operator that owns no cursor.
        [[nodiscard]] virtual actor_zeta::unique_future<void> release_cursor(pipeline::context_t* ctx);

        // STREAMING / SINK: consume exactly one input batch. A streaming operator
        // transforms `input` and appends 0+ chunks to `out`; a sink operator folds
        // `input` into bounded internal state and appends nothing. Synchronous: the
        // executor owns the cross-actor await drive, operators never self-send.
        [[nodiscard]] virtual core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out);

        // SINK: once the upstream pipeline is drained, emit the accumulated result
        // into `out`. Streaming operators have nothing to finalize (default no-op).
        [[nodiscard]] virtual core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out);

        // A SINK whose finalize is an asynchronous cross-actor commit (DML: WAL ->
        // storage -> index -> swap-info). push()/finalize() stay synchronous; the
        // executor drives this operator's await_async_and_resume after the pump (it
        // owns the cross-actor await, so it is lost-wakeup-safe). Default false.
        [[nodiscard]] virtual bool needs_async_finalize() const noexcept { return false; }

        // Rows a buffering DML sink (insert/update/delete) has folded into its
        // accumulator but not yet flushed. The executor's mid-pump flush gate
        // compares this to the flush threshold; every non-DML op (scan / streaming /
        // constraint / DDL sink) keeps it 0 so the gate skips them, and catalog-mode
        // DML returns 0 too (its single-shot path must not be mid-flushed).
        [[nodiscard]] virtual uint64_t buffered_rows() const noexcept { return 0; }

        // Rewind an operator's PRIVATE streaming bookkeeping so it can be re-driven
        // from scratch. reset_for_reuse() clears the generic state_/output_, but the
        // streaming path keeps extra per-run state that reset_for_reuse() does not see:
        //   - a SOURCE's cursor (opened_/drained_/cursor_id_ for a scan; the working-set
        //     walk index for a cte_scan), and
        //   - a SINK's lazily-built accumulator (a hash join's index_built_ / right_index_
        //     / build_matched_).
        // The recursive-CTE fixpoint driver re-runs its recursive-term sub-plan once per
        // iteration, so it walks that subtree calling reset_for_reuse() + this hook on
        // EVERY node before each pass. Default no-op.
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

        // Rows a PARENT constraint operator (fk_check / fk_cascade /
        // check_constraint) must validate against. A DML operator (insert /
        // update / delete) snapshots them at the TOP of its
        // await_async_and_resume — BEFORE it overwrites output_ with the
        // RETURNING / affected-count chunk — so a constraint driven AFTER the DML
        // (the streaming bottom-up async-finalize drive) can still read the
        // written rows even though the scan SOURCE's output_ is empty on the
        // streaming path. Default: null (non-DML operators expose nothing).
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

        // One entry point, by const reference: it rebuilds the message on this operator's
        // resource (core::error_on), so handing it an rvalue would save nothing and an
        // &&-overload could only reintroduce the producer-arena adoption it exists to stop.
        void set_error(const core::error_t& error);
        bool has_error() const noexcept;
        const core::error_t& get_error() const noexcept;

        // Plan-time resolved output column types (one per output column, in output
        // order), forwarded by the physical-plan generator from the logical node's
        // output_types(). Default no-op; operators that emit a typed result (group /
        // select) override it so a column produced over zero input rows stays correctly
        // typed instead of the 0-byte logical_type::NA sentinel.
        virtual void set_output_types(const std::pmr::vector<types::complex_logical_type>& types);

        // --- EXPLAIN ANALYZE per-operator instrumentation ---
        // Written ONLY by execute_pipeline when ctx->analyze is set; read ONLY by the EXPLAIN
        // renderer after the drive completes. Per-instance, driven synchronously inside executor_t
        // (operator_t is an intrusive_ref_counter, not a basic_actor) — no locks/atomics, no
        // cross-actor sharing. Untouched off the ANALYZE path.
        void record_analyze(uint64_t rows, std::chrono::nanoseconds dt) noexcept {
            analyze_rows_ += rows;
            analyze_time_ += dt;
        }
        void bump_analyze_loop() noexcept { ++analyze_loops_; }

        // --- EXPLAIN: NVI public entry (mirrors node_t::to_string()) — the operator describes itself
        // into `s` and recurses into its OWN children. Scans override to add their table oid;
        // lateral/recursive override to recurse into their PRIVATE sub-plans. No static_cast in the walk.
        void explain(const explain_sink& s) const { explain_impl(s); }

    protected:
        // Lets overrides supply just their oid while keeping analyze_* private. Inline — begin() needs no IR.
        void explain_begin(const explain_sink& s, catalog::oid_t oid) const {
            s.begin(type(), oid, analyze_rows_, analyze_time_, analyze_loops_);
        }

        std::pmr::memory_resource* resource_;
        log_t log_;

        ptr left_{nullptr};
        ptr right_{nullptr};
        operator_data_ptr output_{nullptr};
        operator_write_data_ptr modified_{nullptr};
        // Snapshot of the written rows for a parent constraint operator (see
        // constraint_input()). Populated by DML operators only.
        operator_data_ptr constraint_input_{nullptr};

    private:
        // NVI customization point (mirrors node_t::to_string_impl, node.hpp:104) — but NON-pure
        // (default body), because operator_t has concrete leaf subclasses that do not override it.
        // Scans override to pass table_oid_; lateral/recursive override to recurse into private subtrees.
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
        // EXPLAIN ANALYZE counters (see record_analyze). Scalars — no pmr, no re-anchor.
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
