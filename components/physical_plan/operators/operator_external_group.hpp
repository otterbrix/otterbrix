#pragma once

// Reuse the GROUP/aggregate DTO structs (group_key_t, group_value_t,
// computed_column_t, post_aggregate_column_t) from the in-memory operator —
// they are defined there and shared, never redefined here.
#include <components/physical_plan/operators/operator_group.hpp>

#include <components/table/storage/spill_file.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdint>
#include <memory>

namespace components::operators {

    // Grace aggregate state for partitioned spill. Owned by the external
    // (spill) aggregate operator only — the in-memory operator never spills.
    struct grace_aggregate_state_t {
        uint32_t partition_count{16};          // Number of partitions for spilling
        bool spilled{false};                   // Whether groups were spilled to disk
        std::pmr::string temp_file_path;       // Base path for temp files
        // Real MVCC snapshot stamped into spill headers.
        uint64_t snapshot_horizon{0};
        // Per-query unique id + RAII ownership of spilled partition files so they
        // are removed on every exit path.
        uint64_t query_id{0};
        core::filesystem::local_file_system_t fs;
        std::vector<std::unique_ptr<components::table::storage::spill_file_t>> partition_handles;
        // R10: spill directory resolved from ctx->disk_config in on_execute_impl.
        // Empty until the executor stamps it.
        std::string spill_dir;

        grace_aggregate_state_t(std::pmr::memory_resource* resource)
            : temp_file_path(resource) {}
    };

    /**
     * @brief External (spill) aggregate operator — disk-backed grace aggregate.
     *
     * Proactive spill strategy: on_execute_impl ALWAYS partitions groups and
     * spills per-partition (group keys + row refs) to ctx->disk_config, then
     * re-aggregates partition-by-partition and merges partials via commutative
     * combine. There is no runtime memory check and no in-memory fallback
     * (R6: pure operator, no fallback). The operator trusts the optimizer/physgen
     * decision that selected it.
     *
     * Spill directory and MVCC snapshot are resolved from ctx->disk_config (R10)
     * in on_execute_impl via stamp_spill_context().
     *
     * This operator is fully self-contained: it owns the GROUP BY keys/values,
     * the Phase-1 computed-column evaluation, the in-memory grouping index, the
     * per-group aggregate gather + result builder, the post-aggregate + HAVING
     * columnar passes, the grace spill state and the partition/spill/merge
     * helpers. NOTE: it is a read_write_operator_t (not read_only) — aggregate
     * emits a new derived result set.
     */
    class operator_external_group_t final : public read_write_operator_t {
    public:
        operator_external_group_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  expressions::expression_ptr having = nullptr,
                                  size_t internal_aggregate_count = 0);

        void add_key(group_key_t&& key);
        void add_key(const std::pmr::string& name);
        void add_value(const std::pmr::string& name, aggregate::operator_aggregate_ptr&& aggregator);
        void add_computed_column(computed_column_t&& col);
        void add_post_aggregate(post_aggregate_column_t&& col);

    private:
        // Multi-chunk row reference: (chunk_idx, row_idx_in_chunk).
        // Outer index = group id; inner = list of references that fall into this group.
        using row_ref_t = std::pair<uint32_t, uint32_t>;

        std::pmr::vector<group_key_t> keys_;
        std::pmr::vector<group_value_t> values_;
        std::pmr::vector<computed_column_t> computed_columns_;
        std::pmr::vector<post_aggregate_column_t> post_aggregates_;
        expressions::expression_ptr having_;
        size_t internal_aggregate_count_;

        // In-memory grouping index (populated by create_list_rows).
        std::pmr::vector<std::pmr::vector<row_ref_t>> row_refs_per_group_;
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> group_keys_;
        std::pmr::unordered_map<size_t, std::pmr::vector<size_t>> group_index_;

        // Grace aggregate state for spill-based execution.
        grace_aggregate_state_t grace_state_;

        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        // ---- Phase-1 + per-group + post-pass helpers ----

        bool evaluate_computed_columns(pipeline::context_t* pipeline_context,
                                       chunks_vector_t& in_chunks,
                                       size_t& first_computed_col);
        void strip_computed_columns(chunks_vector_t& in_chunks, size_t first_computed_col) const;
        void create_list_rows(const chunks_vector_t& in_chunks);
        chunks_vector_t calc_aggregate_values(pipeline::context_t* pipeline_context,
                                              chunks_vector_t& in_chunks);
        chunks_vector_t build_result_chunk(size_t num_groups,
                                           size_t key_count,
                                           std::pmr::vector<std::pmr::vector<types::logical_value_t>>& agg_results,
                                           const chunks_vector_t& in_chunks);
        void calc_post_aggregates(pipeline::context_t* pipeline_context, vector::data_chunk_t& result);
        void filter_having(pipeline::context_t* pipeline_context, vector::data_chunk_t& result);
        void clear_grouping_state();
        void finalize_output(pipeline::context_t* pipeline_context,
                             const operator_data_ptr& in,
                             chunks_vector_t& in_chunks,
                             chunks_vector_t& batches,
                             size_t first_computed_col);

        // R10: stamp the real MVCC snapshot + per-query id + resolve the spill
        // dir from ctx->disk_config into grace_state_.
        void stamp_spill_context(pipeline::context_t* pipeline_context);

        // ---- Grace aggregate spill helpers ----

        bool partition_and_spill_groups(std::pmr::string& error_msg);
        bool load_aggregate_partition(uint32_t partition_id,
                                      std::pmr::vector<std::pmr::vector<types::logical_value_t>>& loaded_keys,
                                      std::pmr::vector<std::pmr::vector<row_ref_t>>& loaded_refs,
                                      std::pmr::string& error_msg);
        chunks_vector_t merge_partition_aggregates(pipeline::context_t* pipeline_context,
                                                   const chunks_vector_t& in_chunks,
                                                   std::pmr::string& error_msg);
    };

} // namespace components::operators
