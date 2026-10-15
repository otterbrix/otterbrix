#include "generaty.hpp"

#include <cassert>

namespace impl {

    void gen_cell(components::vector::vector_t& column, size_t slot, size_t row, int start) {
        using namespace components::types;
        const auto type = column.type();
        const auto n = static_cast<int>(row) + start;
        const auto n64 = static_cast<int64_t>(row) + start;
        switch (type.type()) {
            case logical_type::NA:
                column.set_null(slot, true);
                return;
            case logical_type::BOOLEAN:
                column.set_value(slot, n64 % 2 != 0);
                return;
            case logical_type::TINYINT:
                column.set_value(slot, static_cast<int8_t>(n));
                return;
            case logical_type::SMALLINT:
                column.set_value(slot, static_cast<int16_t>(n));
                return;
            case logical_type::INTEGER:
                column.set_value(slot, static_cast<int32_t>(n));
                return;
            case logical_type::BIGINT:
                column.set_value(slot, static_cast<int64_t>(n));
                return;
            case logical_type::HUGEINT:
                column.set_value(slot, static_cast<int128_t>(n));
                return;
            case logical_type::FLOAT:
                column.set_value(slot, static_cast<float>(n64) + 0.1f);
                return;
            case logical_type::DOUBLE:
                column.set_value(slot, static_cast<double>(n64) + 0.1);
                return;
            case logical_type::UTINYINT:
                column.set_value(slot, static_cast<uint8_t>(n));
                return;
            case logical_type::USMALLINT:
                column.set_value(slot, static_cast<uint16_t>(n));
                return;
            case logical_type::UINTEGER:
                column.set_value(slot, static_cast<uint32_t>(n));
                return;
            case logical_type::UBIGINT:
                column.set_value(slot, static_cast<uint64_t>(n));
                return;
            case logical_type::UHUGEINT:
                column.set_value(slot, static_cast<uint128_t>(n));
                return;
            case logical_type::STRING_LITERAL: {
                const std::string text = std::to_string(n64);
                column.set_value(slot, std::string_view{text});
                return;
            }
            case logical_type::DECIMAL:
                switch (type.to_physical_type()) {
                    case physical_type::INT16:
                        column.set_value(slot, static_cast<int16_t>(n));
                        return;
                    case physical_type::INT32:
                        column.set_value(slot, static_cast<int32_t>(n));
                        return;
                    case physical_type::INT64:
                        column.set_value(slot, static_cast<int64_t>(n));
                        return;
                    default:
                        column.set_value(slot, static_cast<int128_t>(n));
                        return;
                }
            case logical_type::ARRAY: {
                auto arr_extension = static_cast<const array_logical_type_extension*>(type.extension());
                auto& elements = column.entry();
                for (size_t j = 0; j < arr_extension->size(); j++) {
                    gen_cell(elements, slot * arr_extension->size() + j, j + 1, 0);
                }
                return;
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
    auto count_decimal = complex_logical_type::create_decimal(resource, 15, 7, "count_decimal");
    assert(!count_decimal.has_error() && "generaty: DECIMAL(15,7) is inside the DECIMAL window");
    types.emplace_back(std::move(count_decimal.value()));

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
            impl::gen_cell(chunk.data[column], row - 1, row, start);
        }
    }

    return chunk;
}
