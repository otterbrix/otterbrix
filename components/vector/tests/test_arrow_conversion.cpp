#include "arrow/arrow_converter.hpp"

#include <catch2/catch.hpp>

#include <components/vector/arrow/arrow_appender.hpp>
#include <components/vector/arrow/arrow_converter.hpp>
#include <core/date/date_types.hpp>

using namespace components::vector::arrow;
using namespace components::vector;
using namespace components::types;

TEST_CASE("components::vector::data_chunk_to_arrow") {
    {
        constexpr size_t chunk_size = DEFAULT_VECTOR_CAPACITY;
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
        types.emplace_back(
            complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size, "array_string"));
        types.emplace_back(complex_logical_type::create_list(logical_type::UINTEGER, "list_fixed"));
        types.emplace_back(complex_logical_type::create_list(logical_type::STRING_LITERAL, "list_string"));
        {
            std::pmr::vector<complex_logical_type> fields(&resource);
            fields.emplace_back(logical_type::BOOLEAN, "flag");
            fields.emplace_back(logical_type::INTEGER, "number");
            fields.emplace_back(logical_type::STRING_LITERAL, "string");
            fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT, "array"));
            types.emplace_back(complex_logical_type::create_struct("struct", fields));
        }

        data_chunk_t chunk(&resource, types, chunk_size);
        chunk.set_cardinality(chunk_size);

        for (size_t i = 0; i < chunk_size; i++) {
            // fixed
            { chunk.set_value(0, i, int64_t(i)); }
            // string
            { chunk.set_value(1, i, std::string_view{"long_string_with_index_" + std::to_string(i)}); }
            // double
            { chunk.set_value(2, i, double(i) + 0.1); }
            // bool
            { chunk.set_value(3, i, i % 2 != 0); }
            // array_fixed
            {
                std::vector<std::optional<uint64_t>> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(uint64_t{i * array_size + j});
                }
                chunk.set_value(4, i, arr);
            }
            // array_string
            {
                std::vector<std::string> storage;
                storage.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    storage.push_back("long_string_with_index_" + std::to_string(i * array_size + j));
                }
                std::vector<std::optional<std::string_view>> arr;
                arr.reserve(array_size);
                for (const auto& s : storage) {
                    arr.emplace_back(std::string_view{s});
                }
                chunk.set_value(5, i, arr);
            }
            // list_fixed
            {
                // test that each list entry can be a different length
                std::vector<std::optional<uint32_t>> list;
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(static_cast<uint32_t>(i * list_length(i) + j));
                }
                chunk.set_value(6, i, list);
            }
            // list_string
            {
                // test that each list entry can be a different length
                std::vector<std::string> storage;
                storage.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    storage.push_back("long_string_with_index_" + std::to_string(i * list_length(i) + j));
                }
                std::vector<std::optional<std::string_view>> list;
                list.reserve(list_length(i));
                for (const auto& s : storage) {
                    list.emplace_back(std::string_view{s});
                }
                chunk.set_value(7, i, list);
            }
            // struct
            {
                std::vector<std::optional<uint16_t>> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(static_cast<uint16_t>(j));
                }
                std::string name{"long_string_with_index_" + std::to_string(i)};
                chunk.set_value(8,
                                i,
                                std::tuple{std::optional<bool>(i % 2 != 0),
                                           std::optional<int32_t>(static_cast<int32_t>(i)),
                                           std::optional<std::string_view>(name),
                                           std::optional<std::vector<std::optional<uint16_t>>>(std::move(arr))});
            }
        }

        ArrowSchema schema;
        ArrowArray arrow_array;
        to_arrow_schema(&schema, types);
        to_arrow_array(chunk, &arrow_array);
        auto schema_res = schema_from_arrow(&resource, &schema);
        REQUIRE(!schema_res.has_error());
        auto res_w = data_chunk_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
        REQUIRE(!res_w.has_error());
        auto& res = res_w.value();
        REQUIRE(chunk.column_count() == res.column_count());
        REQUIRE(chunk.size() == res.size());
        for (size_t i = 0; i < chunk.column_count(); i++) {
            for (size_t j = 0; j < chunk.size(); j++) {
                REQUIRE(chunk.value(i, j) == res.value(i, j));
            }
        }
        schema.release(&schema);
    }
    {
        constexpr size_t chunk_size = DEFAULT_VECTOR_CAPACITY;
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
        types.emplace_back(
            complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size, "array_string"));
        types.emplace_back(complex_logical_type::create_list(logical_type::UINTEGER, "list_fixed"));
        types.emplace_back(complex_logical_type::create_list(logical_type::STRING_LITERAL, "list_string"));
        {
            std::pmr::vector<complex_logical_type> fields(&resource);
            fields.emplace_back(logical_type::BOOLEAN, "flag");
            fields.emplace_back(logical_type::INTEGER, "number");
            fields.emplace_back(logical_type::STRING_LITERAL, "string");
            fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT, "array"));
            types.emplace_back(complex_logical_type::create_struct("struct", fields));
        }

        data_chunk_t chunk(&resource, types, chunk_size);
        chunk.set_cardinality(chunk_size);

        for (size_t i = 0; i < chunk_size; i++) {
            // fixed
            { chunk.set_value(0, i, int64_t(i)); }
            // string
            { chunk.set_value(1, i, std::string_view{"long_string_with_index_" + std::to_string(i)}); }
            // double
            { chunk.set_value(2, i, double(i) + 0.1); }
            // bool
            { chunk.set_value(3, i, i % 2 != 0); }
            // array_fixed
            {
                std::vector<std::optional<uint64_t>> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(uint64_t{i * array_size + j});
                }
                chunk.set_value(4, i, arr);
            }
            // array_string
            {
                std::vector<std::string> storage;
                storage.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    storage.push_back("long_string_with_index_" + std::to_string(i * array_size + j));
                }
                std::vector<std::optional<std::string_view>> arr;
                arr.reserve(array_size);
                for (const auto& s : storage) {
                    arr.emplace_back(std::string_view{s});
                }
                chunk.set_value(5, i, arr);
            }
            // list_fixed
            {
                // test that each list entry can be a different length
                std::vector<std::optional<uint32_t>> list;
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(static_cast<uint32_t>(i * list_length(i) + j));
                }
                chunk.set_value(6, i, list);
            }
            // list_string
            {
                // test that each list entry can be a different length
                std::vector<std::string> storage;
                storage.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    storage.push_back("long_string_with_index_" + std::to_string(i * list_length(i) + j));
                }
                std::vector<std::optional<std::string_view>> list;
                list.reserve(list_length(i));
                for (const auto& s : storage) {
                    list.emplace_back(std::string_view{s});
                }
                chunk.set_value(7, i, list);
            }
            // struct
            {
                std::vector<std::optional<uint16_t>> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(static_cast<uint16_t>(j));
                }
                std::string name{"long_string_with_index_" + std::to_string(i)};
                chunk.set_value(8,
                                i,
                                std::tuple{std::optional<bool>(i % 2 != 0),
                                           std::optional<int32_t>(static_cast<int32_t>(i)),
                                           std::optional<std::string_view>(name),
                                           std::optional<std::vector<std::optional<uint16_t>>>(std::move(arr))});
            }
        }

        ArrowSchema schema;
        ArrowArray arrow_array;
        to_arrow_schema(&schema, types);
        to_arrow_array(chunk, &arrow_array);
        auto schema_res = schema_from_arrow(&resource, &schema);
        REQUIRE(!schema_res.has_error());
        auto res_w = data_chunk_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
        REQUIRE(!res_w.has_error());
        auto& res = res_w.value();
        REQUIRE(chunk.column_count() == res.column_count());
        REQUIRE(chunk.size() == res.size());
        for (size_t i = 0; i < chunk.column_count(); i++) {
            for (size_t j = 0; j < chunk.size(); j++) {
                REQUIRE(chunk.value(i, j) == res.value(i, j));
            }
        }
        schema.release(&schema);
    }
}

TEST_CASE("components::vector::data_chunk_to_arrow::datetime") {
    constexpr size_t chunk_size = 64;
    using namespace core::date;

    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::vector<complex_logical_type> types(&resource);

    types.emplace_back(logical_type::DATE, "date_col");
    types.emplace_back(logical_type::TIME, "time_col");
    types.emplace_back(logical_type::TIMESTAMP, "ts_col");
    types.emplace_back(logical_type::TIMESTAMP_TZ, "tstz_col");
    types.emplace_back(logical_type::INTERVAL, "interval_col");

    data_chunk_t chunk(&resource, types, chunk_size);
    chunk.set_cardinality(chunk_size);

    for (size_t i = 0; i < chunk_size; i++) {
        chunk.set_value(0, i, logical_value_t{&resource, date_t{days{static_cast<int32_t>(i) - 100}}});
        chunk.set_value(
            1,
            i,
            logical_value_t{&resource, core::date::time_t{microseconds{static_cast<int64_t>(i) * 1000000LL}}});
        chunk.set_value(
            2,
            i,
            logical_value_t{&resource, timestamp_t{microseconds{static_cast<int64_t>(i) * 1000000LL - 86400000000LL}}});
        chunk.set_value(3,
                        i,
                        logical_value_t{&resource, timestamptz_t{microseconds{static_cast<int64_t>(i) * 1000000LL}}});
        chunk.set_value(4,
                        i,
                        logical_value_t{&resource,
                                        interval_t{microseconds{static_cast<int64_t>(i) * 1000LL},
                                                   days{static_cast<int32_t>(i)},
                                                   months{static_cast<int32_t>(i % 12)}}});
    }

    ArrowSchema schema;
    ArrowArray arrow_array;
    to_arrow_schema(&schema, types);
    to_arrow_array(chunk, &arrow_array);
    auto schema_res = schema_from_arrow(&resource, &schema);
    REQUIRE(!schema_res.has_error());
    auto res_w = data_chunk_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
    REQUIRE(!res_w.has_error());
    auto& res = res_w.value();

    REQUIRE(chunk.column_count() == res.column_count());
    REQUIRE(chunk.size() == res.size());
    for (size_t i = 0; i < chunk.column_count(); i++) {
        for (size_t j = 0; j < chunk.size(); j++) {
            REQUIRE(chunk.value(i, j) == res.value(i, j));
        }
    }
    schema.release(&schema);
}
