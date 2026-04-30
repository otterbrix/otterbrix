#pragma once

#include <components/vector/data_chunk.hpp>
#include <services/wal/base.hpp>

namespace components::vector {

    /// Serialize a data_chunk_t into a compact binary representation and append
    /// the bytes to \p buffer.  The format is architecture-neutral (little-endian).
    ///
    /// Layout:
    ///   [num_columns : 2 LE]
    ///   [num_rows    : 4 LE]
    ///   [null_mask_size : 4 LE]          // 0 when every cell is valid
    ///   [null_mask     : null_mask_size bytes]   // 1-bit-per-cell, row-major
    ///   Per column:
    ///     [physical_type : 1]
    ///     [data_size     : 4 LE]
    ///     [data          : data_size bytes]
    ///       fixed-size types : raw memcpy of column buffer
    ///       STRING           : [(num_rows+1)*4 LE offsets][concatenated string data]
    void serialize_binary(const data_chunk_t& chunk, services::wal::buffer_t& buffer);

    /// Deserialize a data_chunk_t that was previously written by serialize_binary.
    /// \p data / \p len describe the serialised payload (not the surrounding WAL
    /// record framing).
    data_chunk_t deserialize_binary(const char* data, size_t len, std::pmr::memory_resource* resource);

} // namespace components::vector
