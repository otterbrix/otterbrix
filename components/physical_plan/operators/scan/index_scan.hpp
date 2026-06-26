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
                   logical_plan::limit_t limit);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        const expressions::key_t& key() const { return key_; }
        const types::logical_value_t& value() const { return value_; }
        expressions::compare_type compare_type() const { return compare_type_; }
        components::logical_plan::index_type preferred_index_type() const { return preferred_index_type_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        // --- Push-based streaming pipeline source (buffered batch point-fetch) ---
        // The index search is ONE-SHOT — it returns the whole matched row-id set in a single
        // future — so this source materializes the ids ONCE (the FIRST source_next call) and applies
        // OFFSET/LIMIT to a [pos_, end_) window over them. It then issues ONE storage_fetch for the
        // whole window; the disk agent batches the reply into ≤ DEFAULT_VECTOR_CAPACITY chunks (each
        // stamped with its absolute row_ids, so a downstream DELETE/UPDATE/index sees the same ids the
        // materialized path produced), which this source buffers in batch_ and emits one-chunk-per-call.
        //   The FIRST call's sequential cross-actor co_awaits (index search + storage_types + the one
        // window storage_fetch) live in this NESTED operator coroutine (driven by co_await from
        // execute_pipeline), not in a behavior() handler, so the actor-zeta single-slot awaited
        // continuation is republished+cleared between sequential awaits — no lost-wakeup.
        //   index_scan is built ONLY when create_plan_match_ proves an index exists on a real table
        // (can_use_index), so table_oid_ is always valid in practice; the INVALID_OID sentinel is a
        // degenerate shape that source_next drains to an empty guard. role() is therefore
        // unconditionally source.
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
        // Windowing core: run the one-shot index search (txn-aware visibility), store the matched
        // ids in row_ids_vec_, and compute the OFFSET/LIMIT window [pos_=start_, end_). If there is
        // no index service or the search matched nothing, leaves an empty window.
        actor_zeta::unique_future<void> open_index_window(pipeline::context_t* ctx);

        // Fetch the whole matched window [pos_, end_) in ONE storage_fetch. The disk agent batches the
        // reply into ≤ DEFAULT_VECTOR_CAPACITY chunks (each stamped with its absolute row_ids), which
        // source_next buffers in batch_. An empty window yields an empty vector.
        actor_zeta::unique_future<std::pmr::vector<vector::data_chunk_t>>
        fetch_matched_window(pipeline::context_t* ctx);

        components::catalog::oid_t table_oid_;
        const expressions::key_t key_;
        const types::logical_value_t value_;
        const expressions::compare_type compare_type_;
        const components::logical_plan::index_type preferred_index_type_;
        const logical_plan::limit_t limit_;

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
