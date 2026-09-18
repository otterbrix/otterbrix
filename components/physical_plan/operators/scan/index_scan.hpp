#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

#ifdef DEV_MODE
    // DEV_MODE hold for test_index_scan_compact_race; one no-op poll per ask, never blocks an actor thread.
    struct index_fetch_gate_t {
        virtual ~index_fetch_gate_t() = default;
        // true = keep holding this fetch; false = let it proceed.
        virtual bool hold(components::catalog::oid_t table_oid) = 0;
    };
    void dev_set_index_fetch_gate(index_fetch_gate_t* gate); // nullptr = off
    index_fetch_gate_t* dev_index_fetch_gate();
#endif

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
                   std::vector<size_t> projected_cols,
                   // Position of the indexed column in a fetched chunk, or -1 when plan-gen could
                   // not resolve it. Feeds the recheck below; -1 leaves the answer on the epoch
                   // gate alone.
                   int64_t key_chunk_col);

        const expressions::key_t& key() const { return key_; }
        const types::logical_value_t& value() const { return value_; }
        expressions::compare_type compare_type() const { return compare_type_; }
        components::logical_plan::index_type preferred_index_type() const { return preferred_index_type_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        // A chunk can be shorter than the window: LIMIT rides on storage_fetch's post-visibility
        // limit, not id trimming.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // A compact between the search and storage_fetch renumbers survivors (test_index_scan_compact_race),
        // so the hold opens before the search and releases once the fetch has the rows.
        [[nodiscard]] bool holds_open_cursor() const noexcept override { return hold_id_ != 0; }
        [[nodiscard]] actor_zeta::unique_future<void> release_cursor(pipeline::context_t* ctx) override;

        // Rewinds for a re-driven sub-plan (recursive-CTE term); hold_id_ is deliberately left
        // alone — zeroing a still-open one leaks the agent-side entry, gating compact forever.
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

        // Returns an error only when the search could not be answered; distinct from an answered "no rows".
        actor_zeta::unique_future<core::error_t> open_index_window(pipeline::context_t* ctx);

        // ONE storage_fetch for the whole window [pos_, end_); each chunk is stamped with its absolute row_ids.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<vector::data_chunk_t>>>
        fetch_matched_window(pipeline::context_t* ctx);

        // The epoch gate proves the row ids were not RENUMBERED; it cannot prove they still name the
        // rows the index meant. A crash that leaves the index ahead of the table (the index is durable
        // per commit, the table only per checkpoint) does not move any epoch, so the ids survive the
        // gate and a later INSERT reuses them. This re-reads the indexed column off the fetched row and
        // re-applies the very comparison the index answered. A row that fails it REFUSES the statement
        // -- it is never filtered out: a silent filter would turn a stale index into a quietly short
        // answer, and correctness here is worth an error the caller can retry.
        [[nodiscard]] core::error_t recheck_answered_rows_() const;

        components::catalog::oid_t table_oid_;
        const expressions::key_t key_;
        const types::logical_value_t value_;
        const expressions::compare_type compare_type_;
        const components::logical_plan::index_type preferred_index_type_;
        const logical_plan::limit_t limit_;
        // Empty means every column; without it, point-fetch pays a heap-copy per matched row
        // for unnamed text columns too.
        const std::vector<size_t> projected_cols_;
        const int64_t key_chunk_col_;
        // projected_cols_ widened by key_chunk_col_ so the recheck has the cell to read. Built once:
        // a per-fetch copy would allocate on the point-lookup path.
        std::vector<size_t> fetch_cols_;

        // If the scan drains having produced zero rows, it emits one schema'd 0-row guard chunk
        // (scalar aggregate COUNT=0 / OUTER-join NULL-pad) before the 0-column drain sentinel.
        bool opened_{false};
        bool fetched_{false};
        bool drained_{false};
        bool emitted_any_{false};
        uint64_t hold_id_{0};
        size_t pos_{0};
        size_t end_{0};
        std::pmr::vector<int64_t> row_ids_vec_{resource_};
        // Rode in with row_ids_vec_ on the search reply: the compact epoch the index was built
        // against. storage_fetch refuses the ids loudly if the table was compacted since.
        uint64_t built_compact_epoch_{0};
        std::pmr::vector<types::complex_logical_type> guard_types_{resource_};
        std::pmr::vector<vector::data_chunk_t> batch_{resource_};
        size_t batch_pos_{0};
    };

} // namespace components::operators
