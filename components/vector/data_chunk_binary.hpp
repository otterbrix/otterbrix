#pragma once

#include <components/vector/data_chunk.hpp>
#include <services/wal/base.hpp>

namespace components::vector {

    /// Layout:
    ///   [num_columns : 2 LE]
    ///   [num_rows    : 4 LE]
    ///   [null_mask_size : 4 LE]          // 0 when every cell is valid
    ///   [null_mask     : null_mask_size bytes]   // 1-bit-per-cell, row-major, TOP LEVEL only
    ///   Per column:
    ///     [spec_size     : 4 LE]         // canonical types::encode_type_spec; 0 = poison
    ///     [type_spec     : spec_size bytes]
    ///     [data_size     : 4 LE]
    ///     [payload       : data_size bytes]
    /// Recursive (shape follows the type spec, no tag); child order mirrors .otbx, [validity, ...children]:
    ///   fixed-size types : raw memcpy of the column buffer
    ///   STRING           : [(count+1)*4 LE offsets][concatenated string data]
    ///   STRUCT           : per field [validity][payload]      (also TIME_TZ, INTERVAL, UNION)
    ///   ARRAY            : [validity][payload] over count*stride child elements
    ///   LIST             : [count*(offset:8)(length:8)][child_count:8][validity][payload]
    ///                      (also MAP, physically a list of key/value structs)
    ///   NA               : nothing — a NULL-typed column has no payload
    /// [validity] is [mask_size:4 LE][mask bytes], 0 meaning all-valid; nested only, a column's
    /// own validity is the chunk-wide mask above.
    ///
    /// A column this codec has no rule for is poisoned (spec_size 0) rather than written short,
    /// so the reader refuses the record instead of replaying a column of zeroes.
    void serialize_binary(const data_chunk_t& chunk, services::wal::buffer_t& buffer);

    /// Payload only, not WAL framing; on failure \p ok is false (discard the returned chunk), else true.
    data_chunk_t deserialize_binary(const char* data, size_t len, std::pmr::memory_resource* resource, bool& ok);

} // namespace components::vector
