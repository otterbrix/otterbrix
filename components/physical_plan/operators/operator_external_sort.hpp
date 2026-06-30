#pragma once

#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/table/storage/spill_file.hpp>

namespace components::operators {

    /**
     * @brief Grace sort state for external merge sort spill.
     *
     * Sorted runs are spilled to disk and merged via k-way merge:
     * - Run generation: sorted chunks -> serialize_unified -> temp files
     * - K-way merge: deserialize runs -> heap-based merge -> sorted output
     *
     * Owned by operator_external_sort_t. The RAII spill_file_t handles in
     * run_handles keep the temp files alive across the spill -> k-way-merge
     * cycle and are removed on every exit path (success, hard error, early
     * return) by spill_file_t's destructor.
     */
    struct grace_sort_state_t {
        explicit grace_sort_state_t(std::pmr::memory_resource* resource)
            : run_files(resource) {}

        bool spilled{false};
        // Real MVCC snapshot stamped into each spill run header.
        uint64_t snapshot_horizon{0};
        std::pmr::vector<std::string> run_files; // Paths to spilled run files
        // Stateless core::filesystem dispatch object. Held by value; spill_file_t
        // takes it by reference and the operator owns this struct, so the address
        // is stable for the spill -> merge cycle.
        core::filesystem::local_file_system_t fs;
        // RAII ownership of spill temp files. Files stay alive across the
        // spill -> k-way-merge cycle and are removed on every exit path
        // (success, hard error, early return) by spill_file_t's destructor.
        std::vector<std::unique_ptr<components::table::storage::spill_file_t>> run_handles;
        // R10: spill directory resolved from ctx->disk_config in on_execute_impl.
        // Empty until the executor stamps it.
        std::string spill_dir;
    };

    /**
     * @brief External (spill) sort operator — disk-backed grace sort.
     *
     * Proactive spill strategy: on_execute_impl ALWAYS spills sorted runs to disk
     * and merges them back via k-way external merge sort. There is no runtime
     * memory check and no in-memory fallback (R6: pure operator, no fallback). The
     * operator trusts the optimizer/physgen decision that selected it.
     *
     * Spill directory and MVCC snapshot are resolved from ctx->disk_config (R10) in
     * on_execute_impl via stamp_spill_context().
     *
     * This operator is fully self-contained: it owns the sort machinery (sorter_,
     * computed_keys_ Phase-1 evaluation, the k-way merge heap helper, grace_state_)
     * and adds the spill_sorted_runs + external_merge_sort disk I/O on top. It
     * reuses only the computed_sort_key_t DTO from operator_sort.hpp.
     */
    class operator_external_sort_t final : public read_only_operator_t {
    public:
        using order = sort::order;

        operator_external_sort_t(std::pmr::memory_resource* resource, log_t log);

        void add(size_t index, order order_ = order::ascending);
        void add(const std::pmr::vector<size_t>& col_path, order order_ = order::ascending);
        void add_computed(computed_sort_key_t&& key);

        void set_expected_output_count(size_t n) { expected_output_count_ = n; }
        void set_limit(logical_plan::limit_t limit) { limit_ = limit; }

    private:
        sort::columnar_sorter_t sorter_;
        std::pmr::vector<computed_sort_key_t> computed_keys_;
        size_t expected_output_count_{0};
        logical_plan::limit_t limit_;
        grace_sort_state_t grace_state_;

        // ---- Phase-1 + k-way helpers ----

        // Phase 1: for each input chunk evaluate the computed sort keys (mutating
        // the chunk), then locally sort and return the per-chunk sorted index
        // vectors. out_types is seeded from the first chunk's pre-computed schema.
        // first_computed_col receives the original column count (before the
        // temporary computed-key columns are appended). On arithmetic-eval error
        // sets this operator's error and returns false.
        bool
        evaluate_computed_keys_and_local_sort(pipeline::context_t* pipeline_context,
                                              std::pmr::vector<vector::data_chunk_t>& in_chunks,
                                              std::vector<std::vector<uint32_t>>& sorted_indices,
                                              std::pmr::vector<types::complex_logical_type>& out_types,
                                              size_t& first_computed_col);

        // K-way merge heap driven by sorter_.compare_cross. Drains `take` rows
        // (after skipping `skip`) copying `out_cols_effective` columns per row plus
        // row_ids into capacity-batched output chunks.
        void kway_merge_to_output(const std::pmr::vector<vector::data_chunk_t>& chunks,
                                  const std::vector<std::vector<uint32_t>>& sorted_indices,
                                  const std::pmr::vector<types::complex_logical_type>& out_types,
                                  size_t out_cols_effective,
                                  size_t first_computed_col,
                                  chunks_vector_t& out_chunks);

        // Strip the temporary computed-key columns appended during Phase 1 so the
        // upstream's chunks are left in their original schema. No-op when there are
        // no computed keys.
        void strip_computed_keys(std::pmr::vector<vector::data_chunk_t>& in_chunks,
                                 size_t first_computed_col) const;

        // R10: stamp the real MVCC snapshot + resolve the spill dir from
        // ctx->disk_config into grace_state_. Called before touching the spill state.
        void stamp_spill_context(pipeline::context_t* pipeline_context);

        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        // External merge sort spill.
        // Writes each locally-sorted chunk to a run file (serialize_unified).
        // Always spills (no threshold gate): returns false only on I/O error.
        // On failure error_msg carries the real reason (e.g. the serializer's
        // conversion/I-O message) so on_execute_impl's set_error is descriptive.
        bool spill_sorted_runs(const std::pmr::vector<vector::data_chunk_t>& chunks,
                               const std::vector<std::vector<uint32_t>>& sorted_indices,
                               const std::pmr::vector<types::complex_logical_type>& out_types,
                               std::pmr::string& error_msg);

        // Reads the run files back and k-way-merges them into out_chunks.
        // On failure error_msg carries the real reason (e.g. the deserializer's
        // data_corruption message).
        bool external_merge_sort(pipeline::context_t* pipeline_context,
                                 const std::pmr::vector<types::complex_logical_type>& out_types,
                                 size_t out_cols_effective,
                                 chunks_vector_t& out_chunks,
                                 std::pmr::string& error_msg);
    };

} // namespace components::operators
