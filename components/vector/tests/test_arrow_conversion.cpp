#include "arrow/arrow_converter.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/vector/arrow/arrow_appender.hpp>
#include <components/vector/arrow/arrow_converter.hpp>
#include <components/vector/arrow/scaner/arrow_conversion.hpp>
#include <components/vector/cell_equal.hpp>
#include <core/date/date_types.hpp>

using namespace components::vector::arrow;
using namespace components::vector;
using namespace components::types;

data_chunk_t& only_single_chunk(std::pmr::vector<data_chunk_t>& chunks) {
    REQUIRE(chunks.size() == 1);
    return chunks.front();
}

// An Arrow record batch is as long as its producer made it; it comes back as chunks no larger than a vector.
TEST_CASE("components::vector::arrow_batch_longer_than_a_vector") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.emplace_back(logical_type::STRING_LITERAL, "text");
    types.emplace_back(logical_type::DOUBLE, "nullable");
    types.emplace_back(complex_logical_type::create_list(logical_type::UINTEGER, "list"));

    const auto total_rows = static_cast<size_t>(DEFAULT_VECTOR_CAPACITY * 3.4);
    std::pmr::vector<data_chunk_t> source(&resource);
    for (size_t first = 0; first < total_rows; first += DEFAULT_VECTOR_CAPACITY) {
        const size_t count = std::min<size_t>(DEFAULT_VECTOR_CAPACITY, total_rows - first);
        data_chunk_t chunk(&resource, types, count);
        chunk.set_cardinality(count);
        for (size_t row = 0; row < count; row++) {
            const size_t global_row = first + row;
            chunk.set_value(0, row, static_cast<int64_t>(global_row));
            const std::string text{"row_" + std::to_string(global_row)};
            chunk.set_value(1, row, std::string_view{text});
            if (global_row % 7 == 0) {
                chunk.data[2].set_null(row, true);
            } else {
                chunk.set_value(2, row, static_cast<double>(global_row) / 2);
            }
            std::vector<uint32_t> list(global_row % 5, static_cast<uint32_t>(global_row));
            chunk.set_value(3, row, list);
        }
        source.push_back(std::move(chunk));
    }

    arrow_appender_t appender(types, total_rows);
    for (auto& chunk : source) {
        appender.append(chunk, 0, chunk.size(), chunk.size());
    }
    ArrowArray arrow_array = appender.finalize();
    ArrowSchema schema;
    to_arrow_schema(&schema, types);
    auto schema_res = schema_from_arrow(&resource, &schema);
    REQUIRE(!schema_res.has_error());

    auto res_w = data_chunks_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
    REQUIRE(!res_w.has_error());
    auto& converted = res_w.value();
    REQUIRE(converted.size() == source.size());
    for (size_t index = 0; index < converted.size(); index++) {
        REQUIRE(converted[index].size() <= DEFAULT_VECTOR_CAPACITY);
        REQUIRE(converted[index].size() == source[index].size());
        for (size_t column = 0; column < types.size(); column++) {
            for (size_t row = 0; row < source[index].size(); row++) {
                REQUIRE(cells_equal(source[index].data[column], row, converted[index].data[column], row));
            }
        }
    }
    schema.release(&schema);
}

TEST_CASE("components::vector::data_chunk_to_arrow") {
    {
        constexpr size_t chunk_size = DEFAULT_VECTOR_CAPACITY;
        constexpr size_t array_size = 5;
        constexpr size_t max_list_size = 128;
        auto list_length = [&](size_t i) { return i - (i / max_list_size) * max_list_size; };

        auto resource = core::pmr::otterbrix_resource();
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
                std::vector<uint64_t> arr;
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
                std::vector<std::string_view> arr;
                arr.reserve(array_size);
                for (const auto& s : storage) {
                    arr.emplace_back(std::string_view{s});
                }
                chunk.set_value(5, i, arr);
            }
            // list_fixed
            {
                // test that each list entry can be a different length
                std::vector<uint32_t> list;
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
                std::vector<std::string_view> list;
                list.reserve(list_length(i));
                for (const auto& s : storage) {
                    list.emplace_back(std::string_view{s});
                }
                chunk.set_value(7, i, list);
            }
            // struct
            {
                std::vector<uint16_t> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(static_cast<uint16_t>(j));
                }
                const std::string name{"long_string_with_index_" + std::to_string(i)};
                auto& fields = chunk.data[8].entries();
                fields[0]->set_value(i, i % 2 != 0);
                fields[1]->set_value(i, static_cast<int32_t>(i));
                fields[2]->set_value(i, std::string_view{name});
                fields[3]->set_value(i, arr);
            }
        }

        ArrowSchema schema;
        ArrowArray arrow_array;
        to_arrow_schema(&schema, types);
        to_arrow_array(chunk, &arrow_array);
        auto schema_res = schema_from_arrow(&resource, &schema);
        REQUIRE(!schema_res.has_error());
        auto res_w = data_chunks_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
        REQUIRE(!res_w.has_error());
        auto& res = only_single_chunk(res_w.value());
        REQUIRE(chunk.column_count() == res.column_count());
        REQUIRE(chunk.size() == res.size());
        for (size_t i = 0; i < chunk.column_count(); i++) {
            for (size_t j = 0; j < chunk.size(); j++) {
                REQUIRE(cells_equal(chunk.data[i], j, res.data[i], j));
            }
        }
        schema.release(&schema);
    }
    {
        constexpr size_t chunk_size = DEFAULT_VECTOR_CAPACITY;
        constexpr size_t array_size = 5;
        constexpr size_t max_list_size = 128;
        auto list_length = [&](size_t i) { return i - (i / max_list_size) * max_list_size; };

        auto resource = core::pmr::otterbrix_resource();
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
                std::vector<uint64_t> arr;
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
                std::vector<std::string_view> arr;
                arr.reserve(array_size);
                for (const auto& s : storage) {
                    arr.emplace_back(std::string_view{s});
                }
                chunk.set_value(5, i, arr);
            }
            // list_fixed
            {
                // test that each list entry can be a different length
                std::vector<uint32_t> list;
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
                std::vector<std::string_view> list;
                list.reserve(list_length(i));
                for (const auto& s : storage) {
                    list.emplace_back(std::string_view{s});
                }
                chunk.set_value(7, i, list);
            }
            // struct
            {
                std::vector<uint16_t> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(static_cast<uint16_t>(j));
                }
                const std::string name{"long_string_with_index_" + std::to_string(i)};
                auto& fields = chunk.data[8].entries();
                fields[0]->set_value(i, i % 2 != 0);
                fields[1]->set_value(i, static_cast<int32_t>(i));
                fields[2]->set_value(i, std::string_view{name});
                fields[3]->set_value(i, arr);
            }
        }

        ArrowSchema schema;
        ArrowArray arrow_array;
        to_arrow_schema(&schema, types);
        to_arrow_array(chunk, &arrow_array);
        auto schema_res = schema_from_arrow(&resource, &schema);
        REQUIRE(!schema_res.has_error());
        auto res_w = data_chunks_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
        REQUIRE(!res_w.has_error());
        auto& res = only_single_chunk(res_w.value());
        REQUIRE(chunk.column_count() == res.column_count());
        REQUIRE(chunk.size() == res.size());
        for (size_t i = 0; i < chunk.column_count(); i++) {
            for (size_t j = 0; j < chunk.size(); j++) {
                REQUIRE(cells_equal(chunk.data[i], j, res.data[i], j));
            }
        }
        schema.release(&schema);
    }
}

TEST_CASE("components::vector::data_chunk_to_arrow::datetime") {
    constexpr size_t chunk_size = 64;
    using namespace core::date;

    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> types(&resource);

    types.emplace_back(logical_type::DATE, "date_col");
    types.emplace_back(logical_type::TIME, "time_col");
    types.emplace_back(logical_type::TIMESTAMP, "ts_col");
    types.emplace_back(logical_type::TIMESTAMP_TZ, "tstz_col");
    types.emplace_back(logical_type::INTERVAL, "interval_col");

    data_chunk_t chunk(&resource, types, chunk_size);
    chunk.set_cardinality(chunk_size);

    for (size_t i = 0; i < chunk_size; i++) {
        chunk.data[0].set_value(i, date_t{days{static_cast<int32_t>(i) - 100}});
        chunk.data[1].set_value(i, core::date::time_t{microseconds{static_cast<int64_t>(i) * 1000000LL}});
        chunk.data[2].set_value(i, timestamp_t{microseconds{static_cast<int64_t>(i) * 1000000LL - 86400000000LL}});
        chunk.data[3].set_value(i, timestamptz_t{microseconds{static_cast<int64_t>(i) * 1000000LL}});
        chunk.data[4].set_value(i,
                                interval_t{microseconds{static_cast<int64_t>(i) * 1000LL},
                                           days{static_cast<int32_t>(i)},
                                           months{static_cast<int32_t>(i % 12)}});
    }

    ArrowSchema schema;
    ArrowArray arrow_array;
    to_arrow_schema(&schema, types);
    to_arrow_array(chunk, &arrow_array);
    auto schema_res = schema_from_arrow(&resource, &schema);
    REQUIRE(!schema_res.has_error());
    auto res_w = data_chunks_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
    REQUIRE(!res_w.has_error());
    auto& res = only_single_chunk(res_w.value());

    REQUIRE(chunk.column_count() == res.column_count());
    REQUIRE(chunk.size() == res.size());
    for (size_t i = 0; i < chunk.column_count(); i++) {
        for (size_t j = 0; j < chunk.size(); j++) {
            REQUIRE(cells_equal(chunk.data[i], j, res.data[i], j));
        }
    }
    schema.release(&schema);
}

TEST_CASE("components::vector::arrow_scan::sparse_union_fills_tag_and_active_member") {
    core::pmr::otterbrix_resource resource;

    ArrowSchema child_a{};
    child_a.format = "i";
    child_a.name = "a";
    ArrowSchema child_b{};
    child_b.format = "l";
    child_b.name = "b";
    ArrowSchema* schema_children[] = {&child_a, &child_b};
    ArrowSchema schema{};
    schema.format = "+us:0,1";
    schema.name = "u";
    schema.n_children = 2;
    schema.children = schema_children;

    // rows: a=7, b=9, a=NULL
    const int8_t type_ids[] = {0, 1, 0};
    const int32_t a_data[] = {7, 0, 0};
    const uint8_t a_validity[] = {0b011};
    const int64_t b_data[] = {0, 9, 0};
    const void* a_buffers[] = {a_validity, a_data};
    const void* b_buffers[] = {nullptr, b_data};
    ArrowArray a_array{};
    a_array.length = 3;
    a_array.null_count = 1;
    a_array.n_buffers = 2;
    a_array.buffers = a_buffers;
    ArrowArray b_array{};
    b_array.length = 3;
    b_array.n_buffers = 2;
    b_array.buffers = b_buffers;
    ArrowArray* array_children[] = {&a_array, &b_array};
    const void* union_buffers[] = {nullptr, type_ids};
    ArrowArray array{};
    array.length = 3;
    array.n_buffers = 2;
    array.buffers = union_buffers;
    array.n_children = 2;
    array.children = array_children;

    auto arrow_type = arrow_logical_type(&resource, schema);
    REQUIRE_FALSE(arrow_type.has_error());
    vector_t vec(&resource, arrow_type.value()->type(), 3);
    arrow_array_scan_state state;
    auto err = scaner::arrow_column_to_vector(vec, array, 0, state, 3, *arrow_type.value());
    REQUIRE_FALSE(err.contains_error());

    auto& tag = *vec.entries()[0];
    auto& member_a = *vec.entries()[1];
    auto& member_b = *vec.entries()[2];

    CHECK_FALSE(tag.is_null(0));
    CHECK(tag.get_value<uint8_t>(0) == 0);
    CHECK_FALSE(member_a.is_null(0));
    CHECK(member_a.get_value<int32_t>(0) == 7);
    CHECK(member_b.is_null(0));

    CHECK_FALSE(tag.is_null(1));
    CHECK(tag.get_value<uint8_t>(1) == 1);
    CHECK(member_a.is_null(1));
    CHECK_FALSE(member_b.is_null(1));
    CHECK(member_b.get_value<int64_t>(1) == 9);

    CHECK(tag.is_null(2));
    CHECK_FALSE(vec.is_null(0));
    CHECK_FALSE(vec.is_null(1));
    CHECK(vec.is_null(2));
}
