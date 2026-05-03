#include "data_pointer.hpp"

#include "metadata_reader.hpp"
#include "metadata_writer.hpp"

namespace components::table::storage {

    void data_pointer_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint64_t>(row_start);
        writer.write<uint64_t>(tuple_count);
        writer.write<uint64_t>(block_pointer.block_id);
        writer.write<uint32_t>(block_pointer.offset);
        writer.write<uint8_t>(static_cast<uint8_t>(compression));
        writer.write<uint64_t>(segment_size);
    }

    data_pointer_t data_pointer_t::deserialize(metadata_reader_t& reader) {
        data_pointer_t result;
        result.row_start = reader.read<uint64_t>();
        result.tuple_count = reader.read<uint64_t>();
        result.block_pointer.block_id = reader.read<uint64_t>();
        result.block_pointer.offset = reader.read<uint32_t>();
        result.compression = static_cast<compression::compression_type>(reader.read<uint8_t>());
        result.segment_size = reader.read<uint64_t>();
        return result;
    }

    void pax_fixed_slice_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint32_t>(column_index);
        writer.write<uint8_t>(static_cast<uint8_t>(column_type));
        data_pointer.serialize(writer);
    }

    pax_fixed_slice_t pax_fixed_slice_t::deserialize(metadata_reader_t& reader) {
        pax_fixed_slice_t result;
        result.column_index = reader.read<uint32_t>();
        result.column_type = static_cast<pax_fixed_column_type>(reader.read<uint8_t>());
        result.data_pointer = data_pointer_t::deserialize(reader);
        return result;
    }

    void pax_fixed_page_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint32_t>(row_offset_in_group);
        writer.write<uint32_t>(tuple_count);
        writer.write<uint32_t>(static_cast<uint32_t>(slices.size()));
        for (const auto& slice : slices) {
            slice.serialize(writer);
        }
    }

    pax_fixed_page_t pax_fixed_page_t::deserialize(metadata_reader_t& reader) {
        pax_fixed_page_t result;
        result.row_offset_in_group = reader.read<uint32_t>();
        result.tuple_count = reader.read<uint32_t>();
        auto slice_count = reader.read<uint32_t>();
        result.slices.resize(slice_count);
        for (uint32_t i = 0; i < slice_count; i++) {
            result.slices[i] = pax_fixed_slice_t::deserialize(reader);
        }
        return result;
    }

    void pax_fixed_row_group_layout_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint16_t>(version);
        writer.write<uint16_t>(rows_per_page);
        writer.write<uint32_t>(static_cast<uint32_t>(pages.size()));
        for (const auto& page : pages) {
            page.serialize(writer);
        }
    }

    pax_fixed_row_group_layout_t pax_fixed_row_group_layout_t::deserialize(metadata_reader_t& reader) {
        pax_fixed_row_group_layout_t result;
        result.version = reader.read<uint16_t>();
        result.rows_per_page = reader.read<uint16_t>();
        auto page_count = reader.read<uint32_t>();
        result.pages.resize(page_count);
        for (uint32_t i = 0; i < page_count; i++) {
            result.pages[i] = pax_fixed_page_t::deserialize(reader);
        }
        return result;
    }

    void row_group_pointer_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint64_t>(row_start);
        writer.write<uint64_t>(tuple_count);

        // column count
        writer.write<uint32_t>(static_cast<uint32_t>(columnar_data_pointers.size()));
        for (const auto& column_ptrs : columnar_data_pointers) {
            // segments per column
            writer.write<uint32_t>(static_cast<uint32_t>(column_ptrs.size()));
            for (const auto& dp : column_ptrs) {
                dp.serialize(writer);
            }
        }

        // deletes
        writer.write<uint32_t>(static_cast<uint32_t>(deletes_pointers.size()));
        for (const auto& dp : deletes_pointers) {
            dp.serialize(writer);
        }
    }

    row_group_pointer_t row_group_pointer_t::deserialize(metadata_reader_t& reader) {
        row_group_pointer_t result;
        result.row_start = reader.read<uint64_t>();
        result.tuple_count = reader.read<uint64_t>();

        auto col_count = reader.read<uint32_t>();
        result.columnar_data_pointers.resize(col_count);
        for (uint32_t i = 0; i < col_count; i++) {
            auto seg_count = reader.read<uint32_t>();
            result.columnar_data_pointers[i].resize(seg_count);
            for (uint32_t j = 0; j < seg_count; j++) {
                result.columnar_data_pointers[i][j] = data_pointer_t::deserialize(reader);
            }
        }

        auto del_count = reader.read<uint32_t>();
        result.deletes_pointers.resize(del_count);
        for (uint32_t i = 0; i < del_count; i++) {
            result.deletes_pointers[i] = data_pointer_t::deserialize(reader);
        }

        return result;
    }

} // namespace components::table::storage
