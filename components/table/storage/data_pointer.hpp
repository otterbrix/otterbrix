#pragma once

#include <cstdint>
#include <vector>

#include <components/table/compression/compression_type.hpp>

#include "file_buffer.hpp"

namespace components::table {
    class base_statistics_t;
} // namespace components::table

namespace components::table::storage {

    class metadata_writer_t;
    class metadata_reader_t;

    struct data_pointer_t {
        uint64_t row_start{0};
        uint64_t tuple_count{0};
        block_pointer_t block_pointer;
        compression::compression_type compression{compression::compression_type::UNCOMPRESSED};
        uint64_t segment_size{0};
        // Disk blocks holding this segment's BIG-STRING overflow payload (a STRING value
        // >= DEFAULT_STRING_BLOCK_LIMIT does not fit the segment dictionary, which holds a 16-byte
        // marker pointing at a separate block instead). The segment block above carries only the
        // markers, so without this list the payload is unreachable after a reload AND unreclaimable
        // by data_table_t::compact. Empty for every segment with no big strings, so the on-disk cost
        // is one uint32 zero. The format is pre-release; see main_header_t::CURRENT_VERSION.
        std::vector<uint64_t> overflow_blocks;

        void serialize(metadata_writer_t& writer) const;
        static data_pointer_t deserialize(metadata_reader_t& reader);
    };

    // Persistent form of one column NODE, recursive: a nested column (LIST/STRUCT/ARRAY) stores its
    // payload in CHILD column nodes, so the checkpoint must carry the whole tree — a flat one-level
    // list silently drops every child, and a reloaded nested column then scans an empty one. The
    // format is pre-release; see main_header_t::CURRENT_VERSION. `count` is the node's OWN entry
    // count: a LIST child holds SUM(list lengths), not the table's row count, and STRUCT/ARRAY nodes
    // own no segments at all, so it cannot be re-derived from `segments` on load.
    //
    // Child order (v1, fixed): children[0] is ALWAYS the node's VALIDITY bitmap column (standard =
    // [validity]; struct = [validity, field...]; list/array = [validity, element]), mirroring the
    // in-memory child_states[0]. Its segments are ordinary column segments carrying the bitmap (one
    // bit per row, segment-relative, 128 bytes per 1024-row vector); without it the reload
    // manufactured an all-valid bitmap and every checkpointed NULL was silently lost. A record
    // missing its validity child is data_corruption on load.
    struct column_data_pointers_t {
        uint64_t count{0};
        std::vector<data_pointer_t> segments;
        std::vector<column_data_pointers_t> children;

        void serialize(metadata_writer_t& writer) const;
        static column_data_pointers_t deserialize(metadata_reader_t& reader);
    };

    struct row_group_pointer_t {
        uint64_t row_start{0};
        uint64_t tuple_count{0};
        std::vector<column_data_pointers_t> data_pointers; // per-column recursive pointer trees
        std::vector<data_pointer_t> deletes_pointers;

        void serialize(metadata_writer_t& writer) const;
        static row_group_pointer_t deserialize(metadata_reader_t& reader);
    };

} // namespace components::table::storage
