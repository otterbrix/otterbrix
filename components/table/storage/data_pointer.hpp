#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include <components/table/compression/compression_type.hpp>

#include "file_buffer.hpp"

namespace components::table {
    class base_statistics_t;
} // namespace components::table

namespace components::table::storage {

    class metadata_writer_t;
    class metadata_reader_t;

    enum class row_group_layout_kind : uint8_t
    {
        COLUMNAR = 0,
        PAX_FIXED = 1,
        PAX_GENERIC = 2
    };

    enum class pax_fixed_column_type : uint8_t
    {
        INT8 = 0,
        INT16 = 1,
        INT32 = 2,
        INT64 = 3,
        UINT8 = 4,
        UINT16 = 5,
        UINT32 = 6,
        UINT64 = 7
    };

    enum class pax_fixed_validity_kind : uint8_t
    {
        ALL_VALID = 0,
        ALL_INVALID = 1,
        BITMASK = 2,
        RLE = 3
    };

    enum class pax_generic_slice_kind : uint8_t
    {
        STRING_VALUES = 0,
        VALIDITY = 1
    };

    enum class pax_generic_codec_kind : uint8_t
    {
        STRING_SEGMENT = 0,
        VALIDITY_ALL_VALID = 1,
        VALIDITY_ALL_INVALID = 2,
        VALIDITY_BITMASK = 3
    };

    struct data_pointer_t {
        uint64_t row_start{0};
        uint64_t tuple_count{0};
        block_pointer_t block_pointer;
        compression::compression_type compression{compression::compression_type::UNCOMPRESSED};
        uint64_t segment_size{0};

        void serialize(metadata_writer_t& writer) const;
        static data_pointer_t deserialize(metadata_reader_t& reader);
    };

    struct pax_fixed_slice_t {
        uint32_t column_index{0};
        pax_fixed_column_type column_type{pax_fixed_column_type::INT8};
        data_pointer_t data_pointer;
        pax_fixed_validity_kind validity_kind{pax_fixed_validity_kind::ALL_VALID};
        std::optional<data_pointer_t> validity_data_pointer;

        void serialize(metadata_writer_t& writer, uint16_t version) const;
        static pax_fixed_slice_t deserialize(metadata_reader_t& reader, uint16_t version);
    };

    struct pax_fixed_page_t {
        uint32_t row_offset_in_group{0};
        uint32_t tuple_count{0};
        std::vector<pax_fixed_slice_t> slices;

        void serialize(metadata_writer_t& writer, uint16_t version) const;
        static pax_fixed_page_t deserialize(metadata_reader_t& reader, uint16_t version);
    };

    struct pax_fixed_row_group_layout_t {
        uint16_t version{1};
        uint16_t rows_per_page{0};
        std::vector<pax_fixed_page_t> pages;

        void serialize(metadata_writer_t& writer) const;
        static pax_fixed_row_group_layout_t deserialize(metadata_reader_t& reader);
    };

    struct pax_block_payload_t {
        data_pointer_t main_pointer;
        std::vector<uint32_t> extra_block_ids;

        void serialize(metadata_writer_t& writer) const;
        static pax_block_payload_t deserialize(metadata_reader_t& reader);
    };

    struct pax_generic_slice_t {
        uint32_t column_index{0};
        pax_generic_slice_kind slice_kind{pax_generic_slice_kind::STRING_VALUES};
        pax_generic_codec_kind codec_kind{pax_generic_codec_kind::STRING_SEGMENT};
        std::optional<pax_block_payload_t> payload;

        void serialize(metadata_writer_t& writer, uint16_t version) const;
        static pax_generic_slice_t deserialize(metadata_reader_t& reader, uint16_t version);
    };

    struct pax_generic_page_t {
        uint32_t row_offset_in_group{0};
        uint32_t tuple_count{0};
        std::vector<pax_generic_slice_t> slices;

        void serialize(metadata_writer_t& writer, uint16_t version) const;
        static pax_generic_page_t deserialize(metadata_reader_t& reader, uint16_t version);
    };

    struct pax_generic_row_group_layout_t {
        uint16_t version{1};
        uint16_t rows_per_page{0};
        std::vector<pax_generic_page_t> pages;

        void serialize(metadata_writer_t& writer) const;
        static pax_generic_row_group_layout_t deserialize(metadata_reader_t& reader);
    };

    struct row_group_pointer_t {
        uint64_t row_start{0};
        uint64_t tuple_count{0};
        std::vector<std::vector<data_pointer_t>> columnar_data_pointers; // per-column data pointers
        std::vector<data_pointer_t> deletes_pointers;
        row_group_layout_kind layout_kind{row_group_layout_kind::COLUMNAR};
        std::optional<pax_fixed_row_group_layout_t> pax_fixed_layout;
        std::optional<pax_generic_row_group_layout_t> pax_generic_layout;

        void serialize(metadata_writer_t& writer) const;
        static row_group_pointer_t deserialize(metadata_reader_t& reader);
    };

} // namespace components::table::storage
