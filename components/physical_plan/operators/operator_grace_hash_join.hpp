#pragma once

#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/table/storage/spill_file.hpp>
#include <components/table/storage/unified_format.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/pmr.hpp>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    // Grace hash join state for spill-based execution.
    //
    // Standalone scratch state for operator_grace_hash_join_t (the in-memory
    // operator_hash_join_t no longer shares it — the two operators are now fully
    // independent). Only the grace operator spills. R6: no runtime fallback —
    // which operator runs is decided at plan time, never at runtime.
    struct grace_hash_join_state_t {
        uint32_t partition_count{16};          // Number of partitions for spilling
        bool spilled{false};                    // Whether build side was spilled
        std::pmr::string temp_file_path;        // Base path for temp files
        // Real MVCC snapshot stamped into spill headers.
        uint64_t snapshot_horizon{0};
        // Per-query unique id so concurrent spill files never collide, plus RAII
        // ownership of the spilled partition files so they are removed on every
        // exit path.
        uint64_t query_id{0};
        // By-value filesystem handle; spill_file_t binds a reference to it.
        core::filesystem::local_file_system_t fs;
        std::vector<std::unique_ptr<components::table::storage::spill_file_t>> partition_handles;
        // R10: spill directory resolved from ctx->disk_config in on_execute_impl.
        // Empty until the executor stamps it.
        std::string spill_dir;

        explicit grace_hash_join_state_t(std::pmr::memory_resource* resource)
            : temp_file_path(resource) {}
    };

    // Grace (spill) hash join — disk-backed equi-join. Same I/O contract as
    // operator_hash_join_t but ALWAYS spills the build side to ctx->disk_config
    // and probes partition-by-partition.
    //
    // Pure spill strategy (R6): no runtime memory check, no fallback to the
    // in-memory path. The optimizer stamps a grace strategy on the logical node
    // and the physical-plan generator instantiates this class; the operator
    // trusts that decision unconditionally.
    //
    // Only inner / left / right / full are ever substituted (cross is not an
    // equi-join); any other join_type is treated as a no-op.
    class operator_grace_hash_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        operator_grace_hash_join_t(std::pmr::memory_resource* resource,
                                   log_t log,
                                   type join_type,
                                   size_t left_col,
                                   size_t right_col);

    private:
        void on_execute_impl(pipeline::context_t* context) override;

        bool probe_from_spilled_partitions(const chunks_vector_t& left_chunks,
                                           const std::pmr::vector<types::complex_logical_type>& out_types,
                                           chunks_vector_t& out_chunks,
                                           std::pmr::string& error_msg);

        type join_type_;
        // Equi-key column index into the left / right input chunks respectively.
        size_t left_col_;
        size_t right_col_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;
        grace_hash_join_state_t grace_state_;
    };

} // namespace components::operators
