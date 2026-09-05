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
    ///   [null_mask     : null_mask_size bytes]   // 1-bit-per-cell, row-major, TOP LEVEL only
    ///   Per column:
    ///     [spec_size     : 4 LE]         // canonical types::encode_type_spec; 0 = poison
    ///     [type_spec     : spec_size bytes]
    ///     [data_size     : 4 LE]
    ///     [payload       : data_size bytes]
    ///
    /// The payload is RECURSIVE, and both directions derive its shape from the type spec that
    /// precedes it, so there is no tag to keep in sync and a container inside a container is
    /// the same rule applied twice. Child order mirrors the .otbx checkpoint's,
    /// [validity, ...children]:
    ///   fixed-size types : raw memcpy of the column buffer
    ///   STRING           : [(count+1)*4 LE offsets][concatenated string data]
    ///   STRUCT           : per field [validity][payload]      (also TIME_TZ, INTERVAL, UNION)
    ///   ARRAY            : [validity][payload] over count*stride child elements
    ///   LIST             : [count*(offset:8)(length:8)][child_count:8][validity][payload]
    ///                      (also MAP, physically a list of key/value structs)
    ///   NA               : nothing — a NULL-typed column has no payload
    /// where [validity] is [mask_size:4 LE][mask bytes], mask_size 0 meaning all-valid. Only
    /// the levels BELOW a column carry validity here; the column's own stays in the chunk-wide
    /// null mask above.
    ///
    /// A column whose payload this codec has no rule for is POISONED (spec_size 0) rather than
    /// written short, so the reader refuses the record instead of handing replay a column of
    /// zeroes (rule 6).
    void serialize_binary(const data_chunk_t& chunk, services::wal::buffer_t& buffer);

    /// Deserialize a data_chunk_t that was previously written by serialize_binary.
    /// \p data / \p len describe the serialised payload (not the surrounding WAL
    /// record framing). On any buffer-overflow or format violation, sets \p ok to
    /// false and returns an empty (0-column / 0-row) chunk — caller must discard.
    /// Sets \p ok to true on success.
    data_chunk_t deserialize_binary(const char* data, size_t len, std::pmr::memory_resource* resource, bool& ok);

} // namespace components::vector
