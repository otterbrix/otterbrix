#pragma once

#include <string>

#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators::aggregate {

    enum class builtin_agg
    {
        SUM,
        MIN,
        MAX,
        COUNT,
        AVG,
        UNKNOWN
    };

    builtin_agg classify(const std::string& func_name);

    struct raw_agg_state_t {
        union {
            int64_t i64;
            uint64_t u64;
            double f64;
        };
        uint64_t count{0};
        bool initialized{false};

        void update_sum(int64_t v);
        void update_sum(uint64_t v);
        void update_sum(double v);

        void update_min(int64_t v);
        void update_min(uint64_t v);
        void update_min(double v);

        void update_max(int64_t v);
        void update_max(uint64_t v);
        void update_max(double v);

        void update_count();

        void update_avg(int64_t v);
        void update_avg(uint64_t v);
        void update_avg(double v);
    };

    // Update all states in a single pass over the column data
    // group_ids[i] is the group index for row i
    // states[group_idx] is the aggregate state for that group
    void update_all(builtin_agg agg,
                    const vector::vector_t& vec,
                    const uint32_t* group_ids,
                    uint64_t count,
                    std::pmr::vector<raw_agg_state_t>& states);

    // GPU counterpart of update_all. The path is enabled by runtime flag and
    // executes grouped aggregation through the OpenCL runtime when available.
    void update_all_gpu(builtin_agg agg,
                        const vector::vector_t& vec,
                        const uint32_t* group_ids,
                        uint64_t count,
                        std::pmr::vector<raw_agg_state_t>& states);

    // GPU helper for COUNT(*) (counts all rows per group).
    void update_count_star_gpu(const uint32_t* group_ids, uint64_t count, std::pmr::vector<raw_agg_state_t>& states);

    // Convert finalized state to logical_value_t
    types::logical_value_t finalize_state(std::pmr::memory_resource* resource,
                                          builtin_agg agg,
                                          const raw_agg_state_t& state,
                                          types::logical_type col_type);

    // GPU counterpart of finalize_state.
    types::logical_value_t finalize_state_gpu(std::pmr::memory_resource* resource,
                                              builtin_agg agg,
                                              const raw_agg_state_t& state,
                                              types::logical_type col_type);

    // Runtime toggle for grouped aggregate GPU path.
    // Enable with: OTTERBRIX_GROUP_AGG_GPU_TEST=1
    bool gpu_group_aggregate_test_enabled();

} // namespace components::operators::aggregate
