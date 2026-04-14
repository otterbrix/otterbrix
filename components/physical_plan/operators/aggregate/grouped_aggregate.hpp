#pragma once

#include <cstddef>
#include <string>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
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
        enum class value_kind_t : uint8_t
        {
            none,
            signed_integer,
            unsigned_integer,
            floating_point
        };

        union {
            int64_t i64;
            uint64_t u64;
            double f64;
        };
        uint64_t count{0};
        bool initialized{false};
        value_kind_t value_kind{value_kind_t::none};

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

        void merge_sum(const raw_agg_state_t& source);
        void merge_min(const raw_agg_state_t& source);
        void merge_max(const raw_agg_state_t& source);
        void merge_count(const raw_agg_state_t& source);
        void merge_avg(const raw_agg_state_t& source);
    };

    // Update all states in a single pass over the column data
    // group_ids[i] is the group index for row i
    // states[group_idx] is the aggregate state for that group
    void update_all(builtin_agg agg,
                    const vector::vector_t& vec,
                    const uint32_t* group_ids,
                    uint64_t count,
                    std::pmr::vector<raw_agg_state_t>& states);

    void update_count_star(const uint32_t* group_ids, uint64_t count, std::pmr::vector<raw_agg_state_t>& states);

    // GPU counterpart of update_all. The path is enabled by runtime flag and
    // executes grouped aggregation through the OpenCL runtime when available.
    void update_all_gpu(builtin_agg agg,
                        const vector::vector_t& vec,
                        const uint32_t* group_ids,
                        uint64_t count,
                        std::pmr::vector<raw_agg_state_t>& states);

    // GPU helper for COUNT(*) (counts all rows per group).
    void update_count_star_gpu(const uint32_t* group_ids, uint64_t count, std::pmr::vector<raw_agg_state_t>& states);

    struct gpu_update_request_t {
        builtin_agg kind{builtin_agg::UNKNOWN};
        const vector::vector_t* vec{nullptr};
        std::pmr::vector<raw_agg_state_t>* states{nullptr};
        uint32_t input_id{0};
        bool is_count_star{false};
    };

    void update_batch_gpu(const gpu_update_request_t* requests,
                          size_t request_count,
                          const uint32_t* group_ids,
                          uint64_t count);

    types::complex_logical_type result_type(builtin_agg agg, types::logical_type col_type);

    void write_finalized_state(vector::vector_t& target,
                               uint64_t row,
                               builtin_agg agg,
                               const raw_agg_state_t& state,
                               types::logical_type col_type);

    void merge_state(builtin_agg agg, raw_agg_state_t& target, const raw_agg_state_t& source);

    // Runtime toggle for grouped aggregate GPU path.
    // Enable with: OTTERBRIX_GROUP_AGG_GPU_TEST=1
    bool gpu_group_aggregate_test_enabled();

} // namespace components::operators::aggregate
