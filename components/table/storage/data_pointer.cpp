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

    void row_group_pointer_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint64_t>(row_start);
        writer.write<uint64_t>(tuple_count);

        // column count
        writer.write<uint32_t>(static_cast<uint32_t>(data_pointers.size()));
        for (const auto& column_ptrs : data_pointers) {
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
        result.data_pointers.resize(col_count);
        for (uint32_t i = 0; i < col_count; i++) {
            auto seg_count = reader.read<uint32_t>();
            result.data_pointers[i].resize(seg_count);
            for (uint32_t j = 0; j < seg_count; j++) {
                result.data_pointers[i][j] = data_pointer_t::deserialize(reader);
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