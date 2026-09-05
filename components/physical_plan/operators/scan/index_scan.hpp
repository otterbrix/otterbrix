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
        // The index search is ONE-SHOT: the first source_next call materializes the whole matched row-id set
        // into [pos_, end_) and issues ONE storage_fetch for it. A chunk can be SHORTER than its window slice
        // — the index answers a superset of ids and the table's snapshot decides visibility, so the LIMIT cap
        // rides on storage_fetch's post-visibility `limit`, not on trimming the id list. table_oid_ is always
        // valid here (index_scan is only built when can_use_index proves a real table).
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

        // Windowing core: run the one-shot index search, store matched ids in row_ids_vec_, and open the
        // fetch window [0, row_ids_vec_.size()) over them (the LIMIT cap rides on the fetch, not here).
        // Returns an error when the search could not be ANSWERED — distinct from answering "no rows".
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
