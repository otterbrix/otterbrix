#pragma once

#include <cstdint>
#include <memory_resource>

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>
#include "file_buffer.hpp"

namespace components::vector {
    class data_chunk_t;
}

namespace components::table::storage {

/**
 * @brief Unified Format (OTSC1.0) - Single format for spill and HTAP storage
 *
 * Layout:
 * - Header (64 bytes): Magic + Version + Snapshot info
 * - Table metadata: Column names, types, flags
 * - MVCC metadata: Per-row-group timestamps
 * - Null mask: Column-major bitmaps
 * - Column data: Fixed-width or variable-width
 * - Trailer: File size + checksum
 */
struct unified_format_header {
    char magic[8];                  // "OTSC1.0"
    uint32_t version;               // 1
    uint32_t reserved0;
    uint64_t snapshot_horizon;      // MVCC snapshot horizon
    uint64_t min_visible_commit_id; // Minimum visible commit_id
    uint64_t max_visible_commit_id; // Maximum visible commit_id
    uint32_t table_oid;             // Table OID
    uint32_t column_count;          // Number of columns
    uint64_t row_count;              // Total rows
    uint64_t row_group_count;       // Number of row groups
};

enum class mvcc_flags : uint32_t {
    HAS_CONSTANT_INSERT_ID = 1 << 0,
    HAS_CONSTANT_DELETE_ID = 1 << 1
};

constexpr uint64_t NOT_DELETED_ID = UINT64_MAX;
constexpr uint64_t DEFAULT_ROW_GROUP_SIZE = 1024;

/**
 * @brief Serialize data_chunk to unified format
 *
 * @param chunk Source data chunk
 * @param buffer Destination file buffer (must be pre-allocated)
 * @param header Format header with MVCC info
 * @return core::error_t::no_error() on success; conversion_failure when a column
 *         carries a type the codec cannot faithfully round-trip (the message
 *         names the offending logical type); io_error for any other serialize
 *         failure (e.g. an unsupported value reaching the frame writer).
 */
[[nodiscard]] core::error_t serialize_unified(components::vector::data_chunk_t& chunk,
                                              file_buffer_t& buffer,
                                              const unified_format_header& header);

/**
 * @brief Deserialize data_chunk from unified format
 *
 * @param buffer Source file buffer
 * @param resource Memory resource for allocations
 * @param header Output format header (filled on success)
 * @return the deserialized data chunk on success, or core::error_code_t::
 *         data_corruption (with a message) on a magic / version / CRC / bounds /
 *         frame error.
 */
[[nodiscard]] core::result_wrapper_t<components::vector::data_chunk_t>
deserialize_unified(file_buffer_t& buffer,
                    std::pmr::memory_resource* resource,
                    unified_format_header& header);

/**
 * @brief Estimate serialized size for allocation planning
 *
 * @param chunk Source data chunk
 * @param header Format header
 * @return Estimated size in bytes
 */
uint64_t estimate_unified_size(components::vector::data_chunk_t& chunk,
                                const unified_format_header& header);

} // namespace components::table::storage
