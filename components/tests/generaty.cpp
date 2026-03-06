#include "generaty.hpp"

namespace impl {

    components::types::logical_value_t gen_value(size_t row,
                                                 int start,
                                                 const components::types::complex_logical_type& type,
                                                 std::pmr::memory_resource* resource) {
        using namespace components::types;
        switch (type.type()) {
            case logical_type::NA:
                return logical_value_t{resource, logical_type::NA};
            case logical_type::BOOLEAN:
                return logical_value_t{resource, (static_cast<int64_t>(row) + start) % 2 != 0};
            case logical_type::TINYINT:
                return logical_value_t{resource, static_cast<int8_t>(static_cast<int>(row) + start)};
            case logical_type::SMALLINT:
                return logical_value_t{resource, static_cast<int16_t>(static_cast<int>(row) + start)};
            case logical_type::INTEGER:
                return logical_value_t{resource, static_cast<int32_t>(static_cast<int>(row) + start)};
            case logical_type::BIGINT:
                return logical_value_t{resource, static_cast<int64_t>(static_cast<int>(row) + start)};
            case logical_type::HUGEINT:
                return logical_value_t{resource, static_cast<int128_t>(static_cast<int>(row) + start)};
            case logical_type::FLOAT:
                return logical_value_t{resource, static_cast<float>(static_cast<int64_t>(row) + start) + 0.1f};
            case logical_type::DOUBLE:
                return logical_value_t{resource, static_cast<double>(static_cast<int64_t>(row) + start) + 0.1};
            case logical_type::UTINYINT:
                return logical_value_t{resource, static_cast<uint8_t>(static_cast<int>(row) + start)};
            case logical_type::USMALLINT:
                return logical_value_t{resource, static_cast<uint16_t>(static_cast<int>(row) + start)};
            case logical_type::UINTEGER:
                return logical_value_t{resource, static_cast<uint32_t>(static_cast<int>(row) + start)};
            case logical_type::UBIGINT:
                return logical_value_t{resource, static_cast<uint64_t>(static_cast<int>(row) + start)};
            case logical_type::UHUGEINT:
                return logical_value_t{resource, static_cast<uint128_t>(static_cast<int>(row) + start)};
            case logical_type::STRING_LITERAL:
                return logical_value_t{resource, std::to_string(static_cast<int64_t>(row) + start)};
            case logical_type::ARRAY: {
                auto arr_extension = static_cast<const array_logical_type_extension*>(type.extension());
                std::vector<logical_value_t> arr;
                arr.reserve(arr_extension->size());
                for (size_t j = 0; j < arr_extension->size(); j++) {
                    arr.emplace_back(resource, gen_value(j + 1, 0, arr_extension->internal_type(), resource));
                }
                return logical_value_t::create_array(resource, arr_extension->internal_type(), arr);
            }
            default:
                throw std::logic_error("gen_data_chunk does not implement default value for the given type. Fill free "
                                       "to add your own for the required test!");
        }
    }

} // namespace impl

components::vector::data_chunk_t gen_data_chunk(size_t size, std::pmr::memory_resource* resource) {
    return gen_data_chunk(size, 0, resource);
}

components::vector::data_chunk_t gen_data_chunk(size_t size, int start, std::pmr::memory_resource* resource) {
    using namespace components::types;
    constexpr size_t array_size = 5;

    std::pmr::vector<complex_logical_type> types(resource);

    types.emplace_back(logical_type::BIGINT, "count");
    types.emplace_back(logical_type::STRING_LITERAL, "count_str");
    types.emplace_back(logical_type::DOUBLE, "count_double");
    types.emplace_back(logical_type::BOOLEAN, "count_bool");
    types.emplace_back(complex_logical_type::create_array(logical_type::UBIGINT, array_size, "count_array"));

    return gen_data_chunk(size, start, types, resource);
}

components::vector::data_chunk_t gen_data_chunk(size_t size,
                                                int start,
                                                const std::pmr::vector<components::types::complex_logical_type>& types,
                                                std::pmr::memory_resource* resource) {
    using namespace components::types;

    components::vector::data_chunk_t chunk(resource, types, size);
    chunk.set_cardinality(size);

    for (size_t column = 0; column < types.size(); column++) {
        for (size_t row = 1; row <= size; row++) {
            chunk.set_value(column, row - 1, impl::gen_value(row, start, types[column], resource));
        }
    }

    return chunk;
}
