#include "data_pointer.hpp"

#include <stdexcept>

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

    void pax_fixed_slice_t::serialize(metadata_writer_t& writer, uint16_t version) const {
        writer.write<uint32_t>(column_index);
        writer.write<uint8_t>(static_cast<uint8_t>(column_type));
        data_pointer.serialize(writer);
        if (version >= 2) {
            writer.write<uint8_t>(static_cast<uint8_t>(validity_kind));
            switch (validity_kind) {
                case pax_fixed_validity_kind::ALL_VALID:
                case pax_fixed_validity_kind::ALL_INVALID:
                    break;
                case pax_fixed_validity_kind::BITMASK:
                case pax_fixed_validity_kind::RLE:
                    if (!validity_data_pointer.has_value()) {
                        throw std::logic_error("missing pax_fixed validity pointer");
                    }
                    validity_data_pointer->serialize(writer);
                    break;
                default:
                    throw std::logic_error("unknown pax_fixed validity kind");
            }
        }
    }

    pax_fixed_slice_t pax_fixed_slice_t::deserialize(metadata_reader_t& reader, uint16_t version) {
        pax_fixed_slice_t result;
        result.column_index = reader.read<uint32_t>();
        result.column_type = static_cast<pax_fixed_column_type>(reader.read<uint8_t>());
        result.data_pointer = data_pointer_t::deserialize(reader);
        if (version >= 2) {
            result.validity_kind = static_cast<pax_fixed_validity_kind>(reader.read<uint8_t>());
            switch (result.validity_kind) {
                case pax_fixed_validity_kind::ALL_VALID:
                case pax_fixed_validity_kind::ALL_INVALID:
                    break;
                case pax_fixed_validity_kind::BITMASK:
                case pax_fixed_validity_kind::RLE:
                    result.validity_data_pointer = data_pointer_t::deserialize(reader);
                    break;
                default:
                    throw std::logic_error("unknown pax_fixed validity kind");
            }
        } else {
            result.validity_kind = pax_fixed_validity_kind::ALL_VALID;
            result.validity_data_pointer.reset();
        }
        return result;
    }

    void pax_fixed_page_t::serialize(metadata_writer_t& writer, uint16_t version) const {
        writer.write<uint32_t>(row_offset_in_group);
        writer.write<uint32_t>(tuple_count);
        writer.write<uint32_t>(static_cast<uint32_t>(slices.size()));
        for (const auto& slice : slices) {
            slice.serialize(writer, version);
        }
    }

    pax_fixed_page_t pax_fixed_page_t::deserialize(metadata_reader_t& reader, uint16_t version) {
        pax_fixed_page_t result;
        result.row_offset_in_group = reader.read<uint32_t>();
        result.tuple_count = reader.read<uint32_t>();
        auto slice_count = reader.read<uint32_t>();
        result.slices.resize(slice_count);
        for (uint32_t i = 0; i < slice_count; i++) {
            result.slices[i] = pax_fixed_slice_t::deserialize(reader, version);
        }
        return result;
    }

    void pax_fixed_row_group_layout_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint16_t>(version);
        writer.write<uint16_t>(rows_per_page);
        writer.write<uint32_t>(static_cast<uint32_t>(pages.size()));
        for (const auto& page : pages) {
            page.serialize(writer, version);
        }
    }

    pax_fixed_row_group_layout_t pax_fixed_row_group_layout_t::deserialize(metadata_reader_t& reader) {
        pax_fixed_row_group_layout_t result;
        result.version = reader.read<uint16_t>();
        result.rows_per_page = reader.read<uint16_t>();
        auto page_count = reader.read<uint32_t>();
        result.pages.resize(page_count);
        for (uint32_t i = 0; i < page_count; i++) {
            result.pages[i] = pax_fixed_page_t::deserialize(reader, result.version);
        }
        return result;
    }

    void pax_block_payload_t::serialize(metadata_writer_t& writer) const {
        main_pointer.serialize(writer);
        writer.write<uint32_t>(static_cast<uint32_t>(extra_block_ids.size()));
        for (auto block_id : extra_block_ids) {
            writer.write<uint32_t>(block_id);
        }
    }

    pax_block_payload_t pax_block_payload_t::deserialize(metadata_reader_t& reader) {
        pax_block_payload_t result;
        result.main_pointer = data_pointer_t::deserialize(reader);
        auto block_count = reader.read<uint32_t>();
        result.extra_block_ids.resize(block_count);
        for (uint32_t i = 0; i < block_count; i++) {
            result.extra_block_ids[i] = reader.read<uint32_t>();
        }
        return result;
    }

    void pax_generic_slice_t::serialize(metadata_writer_t& writer, uint16_t version) const {
        writer.write<uint32_t>(column_index);
        writer.write<uint8_t>(static_cast<uint8_t>(slice_kind));
        writer.write<uint8_t>(static_cast<uint8_t>(codec_kind));
        if (version >= 2) {
            writer.write<uint16_t>(static_cast<uint16_t>(field_path.size()));
            for (auto path_entry : field_path) {
                writer.write<uint16_t>(path_entry);
            }
            if (slice_kind == pax_generic_slice_kind::FIXED_VALUES) {
                writer.write<uint8_t>(static_cast<uint8_t>(fixed_logical_type));
            }
        }

        switch (codec_kind) {
            case pax_generic_codec_kind::STRING_SEGMENT:
            case pax_generic_codec_kind::VALIDITY_BITMASK:
            case pax_generic_codec_kind::FIXED_PLAIN:
                if (!payload.has_value()) {
                    throw std::logic_error("missing pax_generic payload");
                }
                payload->serialize(writer);
                break;
            case pax_generic_codec_kind::VALIDITY_ALL_VALID:
            case pax_generic_codec_kind::VALIDITY_ALL_INVALID:
                break;
            default:
                throw std::logic_error("unknown pax_generic codec kind");
        }
    }

    pax_generic_slice_t pax_generic_slice_t::deserialize(metadata_reader_t& reader, uint16_t version) {
        pax_generic_slice_t result;
        result.column_index = reader.read<uint32_t>();
        result.slice_kind = static_cast<pax_generic_slice_kind>(reader.read<uint8_t>());
        result.codec_kind = static_cast<pax_generic_codec_kind>(reader.read<uint8_t>());
        if (version >= 2) {
            auto path_count = reader.read<uint16_t>();
            result.field_path.resize(path_count);
            for (uint16_t i = 0; i < path_count; i++) {
                result.field_path[i] = reader.read<uint16_t>();
            }
            if (result.slice_kind == pax_generic_slice_kind::FIXED_VALUES) {
                result.fixed_logical_type = static_cast<types::logical_type>(reader.read<uint8_t>());
            }
        }

        switch (result.codec_kind) {
            case pax_generic_codec_kind::STRING_SEGMENT:
            case pax_generic_codec_kind::VALIDITY_BITMASK:
            case pax_generic_codec_kind::FIXED_PLAIN:
                result.payload = pax_block_payload_t::deserialize(reader);
                break;
            case pax_generic_codec_kind::VALIDITY_ALL_VALID:
            case pax_generic_codec_kind::VALIDITY_ALL_INVALID:
                result.payload.reset();
                break;
            default:
                throw std::logic_error("unknown pax_generic codec kind");
        }
        return result;
    }

    void pax_generic_page_t::serialize(metadata_writer_t& writer, uint16_t version) const {
        writer.write<uint32_t>(row_offset_in_group);
        writer.write<uint32_t>(tuple_count);
        writer.write<uint32_t>(static_cast<uint32_t>(slices.size()));
        for (const auto& slice : slices) {
            slice.serialize(writer, version);
        }
    }

    pax_generic_page_t pax_generic_page_t::deserialize(metadata_reader_t& reader, uint16_t version) {
        pax_generic_page_t result;
        result.row_offset_in_group = reader.read<uint32_t>();
        result.tuple_count = reader.read<uint32_t>();
        auto slice_count = reader.read<uint32_t>();
        result.slices.resize(slice_count);
        for (uint32_t i = 0; i < slice_count; i++) {
            result.slices[i] = pax_generic_slice_t::deserialize(reader, version);
        }
        return result;
    }

    void pax_generic_row_group_layout_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint16_t>(version);
        writer.write<uint16_t>(rows_per_page);
        writer.write<uint32_t>(static_cast<uint32_t>(pages.size()));
        for (const auto& page : pages) {
            page.serialize(writer, version);
        }
    }

    pax_generic_row_group_layout_t pax_generic_row_group_layout_t::deserialize(metadata_reader_t& reader) {
        pax_generic_row_group_layout_t result;
        result.version = reader.read<uint16_t>();
        result.rows_per_page = reader.read<uint16_t>();
        auto page_count = reader.read<uint32_t>();
        result.pages.resize(page_count);
        for (uint32_t i = 0; i < page_count; i++) {
            result.pages[i] = pax_generic_page_t::deserialize(reader, result.version);
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
