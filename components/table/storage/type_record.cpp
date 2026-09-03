#include "type_record.hpp"

#include "metadata_reader.hpp"
#include "metadata_writer.hpp"

#include <cassert>
#include <vector>

namespace components::table::storage {
    void write_type_record(metadata_writer_t& writer, const types::complex_logical_type& type) {
        const uint32_t size = types::type_binary_size(type);
        std::vector<char> bytes(size);
        [[maybe_unused]] const char* written_end = types::type_binary_write(bytes.data(), type);
        assert(static_cast<uint32_t>(written_end - bytes.data()) == size &&
               "type_binary_write disagrees with type_binary_size");
        writer.write<uint32_t>(size);
        writer.write_data(reinterpret_cast<const std::byte*>(bytes.data()), size);
    }

    core::result_wrapper_t<types::complex_logical_type> read_type_record(metadata_reader_t& reader,
                                                                        std::pmr::memory_resource* resource) {
        const auto size = reader.read<uint32_t>();
        if (reader.has_error()) {
            return reader.error();
        }
        std::vector<char> bytes(size);
        reader.read_data(reinterpret_cast<std::byte*>(bytes.data()), size);
        if (reader.has_error()) {
            return reader.error();
        }
        const char* scan = bytes.data();
        return types::type_binary_read(scan, bytes.data() + size, resource);
    }
} // namespace components::table::storage
