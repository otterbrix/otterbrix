#include "generaty.hpp"

std::string gen_id(int num) {
    auto res = std::to_string(num);
    while (res.size() < 24) {
        res = "0" + res;
    }
    return res;
}

std::pmr::string gen_id(int num, std::pmr::memory_resource* resource) {
    std::pmr::string res{std::to_string(num), resource};
    while (res.size() < 24) {
        res = "0" + res;
    }
    return res;
}

components::vector::data_chunk_t gen_data_chunk(size_t size, std::pmr::memory_resource* resource) {
    return gen_data_chunk(size, 0, resource);
}

components::vector::data_chunk_t gen_data_chunk(size_t size, int num, std::pmr::memory_resource* resource) {
    using namespace components::types;
    constexpr size_t array_size = 5;

    std::pmr::vector<complex_logical_type> types(resource);

    types.emplace_back(logical_type::BIGINT, "count");
    types.emplace_back(logical_type::STRING_LITERAL, "_id");
    types.emplace_back(logical_type::STRING_LITERAL, "count_str");
    types.emplace_back(logical_type::DOUBLE, "count_double");
    types.emplace_back(logical_type::BOOLEAN, "count_bool");
    types.emplace_back(complex_logical_type::create_array(logical_type::UBIGINT, array_size, "count_array"));

    components::vector::data_chunk_t chunk(resource, types, size);
    chunk.set_cardinality(size);

    for (size_t i = 1; i <= size; i++) {
        chunk.set_value(0, i - 1, logical_value_t{static_cast<int64_t>(i) + num});
        chunk.set_value(1, i - 1, logical_value_t{resource, gen_id(static_cast<int>(i) + num)});
        chunk.set_value(2, i - 1, logical_value_t{resource, std::to_string(static_cast<int64_t>(i) + num)});
        chunk.set_value(3, i - 1, logical_value_t{static_cast<double>(static_cast<int64_t>(i) + num) + 0.1});
        chunk.set_value(4, i - 1, logical_value_t{(static_cast<int64_t>(i) + num) % 2 != 0});
        {
            std::vector<logical_value_t> arr;
            arr.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                arr.emplace_back(uint64_t{j + 1});
            }
            chunk.set_value(5, i - 1, logical_value_t::create_array(resource, logical_type::UBIGINT, arr));
        }
    }

    return chunk;
}
