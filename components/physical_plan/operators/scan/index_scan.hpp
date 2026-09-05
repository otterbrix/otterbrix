#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(std::pmr::memory_resource* resource,
                   log_t log,
                   components::catalog::oid_t table_oid,
                   const expressions::key_t& key,
                   const types::logical_value_t& value,
                   expressions::compare_type compare_type,
                   components::logical_plan::index_type preferred_index_type,
                   logical_plan::limit_t limit,
                   std::vector<size_t> projected_cols);

        const expressions::key_t& key() const { return key_; }
        const types::logical_value_t& value() const { return value_; }
        expressions::compare_type compare_type() const { return compare_type_; }
        components::logical_plan::index_type preferred_index_type() const { return preferred_index_type_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        // --- Push-based streaming pipeline source (buffered batch point-fetch) ---
        // The index search is ONE-SHOT — it returns the whole matched row-id set in a single future — so this
        // source materializes the ids ONCE (the FIRST source_next call) into the [pos_, end_) window. It then
        // issues ONE storage_fetch for the whole window; the disk agent batches the reply into
        // <= DEFAULT_VECTOR_CAPACITY chunks (each stamped with the absolute row_ids it actually carries, so a
        // downstream DELETE/UPDATE/index sees the right rows), which this source buffers in batch_ and emits
        // one-chunk-per-call.
        //   The fetch runs under the reader's own snapshot, so a chunk can be SHORTER than its slice of the
        // window: the index answers with a superset of ids and the table decides which of them this transaction
        // may see. The LIMIT head cap therefore travels ON the fetch message, as storage_fetch's post-visibility
        // `limit`, and never touches the matched-id list — the same place full_scan puts it
        // (storage_fetch_next_batch's post-filter matched-row cap).
        //   The FIRST call's sequential cross-actor co_awaits (index search + storage_types + the one window
        // storage_fetch) live in this NESTED operator coroutine (driven by co_await from execute_pipeline), not
        // in a behavior() handler, so the actor-zeta single-slot awaited continuation is republished+cleared
        // between sequential awaits — no lost-wakeup.
        //   index_scan is built ONLY when create_plan_match_ proves an index exists on a real table
        // (can_use_index), so table_oid_ is always valid — there is no no-table shape here, unlike full_scan and
        // transfer_scan, which carry an explicit INVALID_OID sentinel because a no-FROM SELECT lowers onto them.
        // An INVALID_OID reaching this operator is a physgen defect, and the schema read in source_next refuses
        // it rather than draining to an empty guard. role() is unconditionally source.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Rewind the windowed point-fetch cursor so a re-driven sub-plan re-runs the
        // one-shot index search from scratch (recursive-CTE recursive term, per iteration).
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            fetched_ = false;
            drained_ = false;
            emitted_any_ = false;
            pos_ = 0;
            end_ = 0;
            row_ids_vec_.clear();
            guard_types_.clear();
            batch_.clear();
            batch_pos_ = 0;
        }

    private:
        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }

        // Windowing core: run the one-shot index search (txn-aware visibility), store the matched
        // ids in row_ids_vec_, and open the fetch window [pos_=0, end_=row_ids_vec_.size()) over them —
        // OFFSET is operator_limit's, the LIMIT cap rides on the fetch. A search that matched nothing
        // leaves an empty window.
        //
        // Returns the manager's error when the search could not be ANSWERED (no engine
        // for the oid, no index on the predicate key, a failed read in the index's disk
        // agent) — distinct from a search that answered "no rows", which is no_error()
        // over an empty window.
        actor_zeta::unique_future<core::error_t> open_index_window(pipeline::context_t* ctx);

        // Fetch the whole matched window [pos_, end_) in ONE storage_fetch. The disk agent batches the
        // reply into ≤ DEFAULT_VECTOR_CAPACITY chunks (each stamped with its absolute row_ids), which
        // source_next buffers in batch_. An empty window yields an empty vector.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<vector::data_chunk_t>>>
        fetch_matched_window(pipeline::context_t* ctx);

        components::catalog::oid_t table_oid_;
        const expressions::key_t key_;
        const types::logical_value_t value_;
        const expressions::compare_type compare_type_;
        const components::logical_plan::index_type preferred_index_type_;
        const logical_plan::limit_t limit_;
        // Storage chunk indices this scan's consumers actually read; EMPTY means every column.
        // Without it the point-fetch behind this source pulls the whole row — including text
        // columns the statement never names, each of which costs a heap copy per matched row.
        const std::vector<size_t> projected_cols_;

        // Buffered point-fetch state:
        //   opened_   : false until the first source_next runs open_index_window (the one-shot
        //               index search + OFFSET/LIMIT window computation).
        //   fetched_  : false until the first source_next issues the whole-window storage_fetch.
        //   row_ids_vec_ : the materialized matched row-id set (the one-shot search result).
        //   pos_ / end_  : the [pos_, end_) window over row_ids_vec_ AFTER offset/limit (the fetch range).
        //   batch_ / batch_pos_ : the disk-batched chunks of the window + the read cursor over them.
        //   drained_  : batch_ exhausted ⇒ source exhausted.
        //   emitted_any_ / guard_types_: if the scan drains having produced zero rows, emit ONE
        //               schema'd 0-row guard chunk (scalar aggregate COUNT=0 / OUTER-join NULL-pad),
        //               then the 0-column drain sentinel.
        bool opened_{false};
        bool fetched_{false};
        bool drained_{false};
        bool emitted_any_{false};
        size_t pos_{0};
        size_t end_{0};
        std::pmr::vector<int64_t> row_ids_vec_{resource_};
        std::pmr::vector<types::complex_logical_type> guard_types_{resource_};
        // Buffered fetched batches: the single whole-window storage_fetch returns the disk-batched
        // chunks here; source_next emits them one-per-call.
        std::pmr::vector<vector::data_chunk_t> batch_{resource_};
        size_t batch_pos_{0};
    };

} // namespace components::operators
