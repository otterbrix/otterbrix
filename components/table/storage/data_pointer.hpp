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

        void serialize(metadata_writer_t& writer) const;
        static data_pointer_t deserialize(metadata_reader_t& reader);
    };

    struct row_group_pointer_t {
        uint64_t row_start{0};
        uint64_t tuple_count{0};
        std::vector<std::vector<data_pointer_t>> data_pointers; // per-column data pointers
        std::vector<data_pointer_t> deletes_pointers;

        void serialize(metadata_writer_t& writer) const;
        static row_group_pointer_t deserialize(metadata_reader_t& reader);
    };

} // namespace components::table::storage
