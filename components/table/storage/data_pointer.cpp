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
        writer.write<uint32_t>(static_cast<uint32_t>(overflow_blocks.size()));
        for (uint64_t block_id : overflow_blocks) {
            writer.write<uint64_t>(block_id);
        }
    }

    data_pointer_t data_pointer_t::deserialize(metadata_reader_t& reader) {
        data_pointer_t result;
        result.row_start = reader.read<uint64_t>();
        result.tuple_count = reader.read<uint64_t>();
        result.block_pointer.block_id = reader.read<uint64_t>();
        result.block_pointer.offset = reader.read<uint32_t>();
        result.compression = static_cast<compression::compression_type>(reader.read<uint8_t>());
        result.segment_size = reader.read<uint64_t>();
        auto overflow_count = reader.read<uint32_t>();
        if (reader.has_error()) { // a corrupt count must not size the vector below
            return result;
        }
        result.overflow_blocks.reserve(overflow_count);
        for (uint32_t i = 0; i < overflow_count && !reader.has_error(); i++) {
            result.overflow_blocks.push_back(reader.read<uint64_t>());
        }
        return result;
    }

    void column_data_pointers_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint64_t>(count);
        writer.write<uint32_t>(static_cast<uint32_t>(segments.size()));
        for (const auto& dp : segments) {
            dp.serialize(writer);
        }
        writer.write<uint32_t>(static_cast<uint32_t>(children.size()));
        for (const auto& child : children) {
            child.serialize(writer);
        }
    }

    column_data_pointers_t column_data_pointers_t::deserialize(metadata_reader_t& reader) {
        column_data_pointers_t result;
        result.count = reader.read<uint64_t>();

        auto seg_count = reader.read<uint32_t>();
        if (reader.has_error()) { // a corrupt count must not size the vectors below
            return result;
        }
        result.segments.resize(seg_count);
        for (uint32_t i = 0; i < seg_count && !reader.has_error(); i++) {
            result.segments[i] = data_pointer_t::deserialize(reader);
        }

        auto child_count = reader.read<uint32_t>();
        if (reader.has_error()) {
            return result;
        }
        result.children.reserve(child_count);
        for (uint32_t i = 0; i < child_count && !reader.has_error(); i++) {
            result.children.push_back(column_data_pointers_t::deserialize(reader));
        }
        return result;
    }

    void row_group_pointer_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint64_t>(row_start);
        writer.write<uint64_t>(tuple_count);

        // column count
        writer.write<uint32_t>(static_cast<uint32_t>(data_pointers.size()));
        for (const auto& column_ptrs : data_pointers) {
            column_ptrs.serialize(writer);
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
        if (reader.has_error()) {
            return result;
        }
        result.data_pointers.reserve(col_count);
        for (uint32_t i = 0; i < col_count && !reader.has_error(); i++) {
            result.data_pointers.push_back(column_data_pointers_t::deserialize(reader));
        }

        auto del_count = reader.read<uint32_t>();
        result.deletes_pointers.resize(del_count);
        for (uint32_t i = 0; i < del_count; i++) {
            result.deletes_pointers[i] = data_pointer_t::deserialize(reader);
        }

        return result;
    }

} // namespace components::table::storage