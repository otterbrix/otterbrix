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
} // namespace components::table::storage