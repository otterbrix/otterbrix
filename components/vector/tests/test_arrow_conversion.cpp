#include "arrow/arrow_converter.hpp"

#include <catch2/catch.hpp>

#include <components/vector/arrow/arrow_appender.hpp>
#include <components/vector/arrow/arrow_converter.hpp>

using namespace components::vector::arrow;
using namespace components::vector;
using namespace components::types;

TEST_CASE("data_chunk to arrow") {
    constexpr size_t chunk_size = 2048;
    constexpr size_t array_size = 5;
    constexpr size_t max_list_size = 128;
    auto list_length = [&](size_t i) { return i - (i / max_list_size) * max_list_size; };

    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::vector<complex_logical_type> types(&resource);

    types.emplace_back(logical_type::BIGINT, "fixed_size");
    types.emplace_back(logical_type::STRING_LITERAL, "string");
    types.emplace_back(logical_type::DOUBLE, "double");
    types.emplace_back(logical_type::BOOLEAN, "bool");
    types.emplace_back(complex_logical_type::create_array(logical_type::UBIGINT, array_size, "array_fixed"));
    types.emplace_back(complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size, "array_string"));
    types.emplace_back(complex_logical_type::create_list(logical_type::UINTEGER, "list_fixed"));
    types.emplace_back(complex_logical_type::create_list(logical_type::STRING_LITERAL, "list_string"));
    {
        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BOOLEAN, "flag");
        fields.emplace_back(logical_type::INTEGER, "number");
        fields.emplace_back(logical_type::STRING_LITERAL, "string");
        fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT, "array"));
        types.emplace_back(complex_logical_type::create_struct(fields, "struct"));
    }

    data_chunk_t chunk(&resource, types, chunk_size);
    chunk.set_cardinality(chunk_size);

    for (size_t i = 0; i < chunk_size; i++) {
        // fixed
        { chunk.set_value(0, i, logical_value_t{int64_t(i)}); }
        // string
        { chunk.set_value(1, i, logical_value_t{std::string{"long_string_with_index_" + std::to_string(i)}}); }
        // double
        { chunk.set_value(2, i, logical_value_t{double(i) + 0.1}); }
        // bool
        { chunk.set_value(3, i, logical_value_t{(bool) (i % 2)}); }
        // array_fixed
        {
            std::vector<logical_value_t> arr;
            arr.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                arr.emplace_back(uint64_t{i * array_size + j});
            }
            chunk.set_value(4, i, logical_value_t::create_array(logical_type::UBIGINT, arr));
        }
        // array_string
        {
            std::vector<logical_value_t> arr;
            arr.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                arr.emplace_back(std::string{"long_string_with_index_" + std::to_string(i * array_size + j)});
            }
            chunk.set_value(5, i, logical_value_t::create_array(logical_type::STRING_LITERAL, arr));
        }
        // list_fixed
        {
            std::vector<logical_value_t> list;
            // test that each list entry can be a different length
            list.reserve(list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                list.emplace_back(static_cast<uint32_t>(i * list_length(i) + j));
            }
            chunk.set_value(6, i, logical_value_t::create_list(logical_type::UINTEGER, list));
        }
        // list_string
        {
            std::vector<logical_value_t> list;
            // test that each list entry can be a different length
            list.reserve(list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                list.emplace_back(std::string{"long_string_with_index_" + std::to_string(i * list_length(i) + j)});
            }
            chunk.set_value(7, i, logical_value_t::create_list(logical_type::STRING_LITERAL, list));
        }
        // struct
        {
            std::vector<logical_value_t> arr;
            arr.reserve(i);
            for (size_t j = 0; j < i; j++) {
                arr.emplace_back(j);
            }
            std::vector<logical_value_t> value_fiels;
            value_fiels.emplace_back(logical_value_t{(bool) (i % 2)});
            value_fiels.emplace_back(logical_value_t{static_cast<int32_t>(i)});
            value_fiels.emplace_back(logical_value_t{std::string{"long_string_with_index_" + std::to_string(i)}});
            value_fiels.emplace_back(logical_value_t::create_list(logical_type::USMALLINT, arr));
            logical_value_t value = logical_value_t::create_struct(types.back(), value_fiels);
            chunk.set_value(8, i, value);
        }
    }

    ArrowSchema schema;
    ArrowArray arrow_array;
    to_arrow_schema(&schema, types);
    to_arrow_array(chunk, &arrow_array);
    auto res = data_chunk_from_arrow(&resource, &arrow_array, schema_from_arrow(&schema));
    REQUIRE(chunk.column_count() == res.column_count());
    REQUIRE(chunk.size() == res.size());
    for (size_t i = 0; i < chunk.column_count(); i++) {
        for (size_t j = 0; j < chunk.size(); j++) {
            REQUIRE(chunk.value(i, j) == res.value(i, j));
        }
    }
}