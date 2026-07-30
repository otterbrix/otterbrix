#include "arrow/arrow_converter.hpp"

#include <catch2/catch_test_macros.hpp>

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
                std::vector<logical_value_t> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(&resource, static_cast<uint16_t>(j));
                }
                std::vector<logical_value_t> value_fiels;
                value_fiels.emplace_back(&resource, i % 2 != 0);
                value_fiels.emplace_back(&resource, static_cast<int32_t>(i));
                value_fiels.emplace_back(
                    logical_value_t{&resource, std::string{"long_string_with_index_" + std::to_string(i)}});
                value_fiels.emplace_back(logical_value_t::create_list(&resource, logical_type::USMALLINT, arr));
                logical_value_t value = logical_value_t::create_struct(&resource, types.back(), value_fiels);
                chunk.set_value(8, i, value);
            }
        }

        ArrowSchema schema;
        ArrowArray arrow_array;
        REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
        REQUIRE_FALSE(to_arrow_array(chunk, &arrow_array).contains_error());
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
                std::vector<logical_value_t> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(&resource, static_cast<uint16_t>(j));
                }
                std::vector<logical_value_t> value_fiels;
                value_fiels.emplace_back(&resource, i % 2 != 0);
                value_fiels.emplace_back(&resource, static_cast<int32_t>(i));
                value_fiels.emplace_back(
                    logical_value_t{&resource, std::string{"long_string_with_index_" + std::to_string(i)}});
                value_fiels.emplace_back(logical_value_t::create_list(&resource, logical_type::USMALLINT, arr));
                logical_value_t value = logical_value_t::create_struct(&resource, types.back(), value_fiels);
                chunk.set_value(8, i, value);
            }
        }

        ArrowSchema schema;
        ArrowArray arrow_array;
        REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
        REQUIRE_FALSE(to_arrow_array(chunk, &arrow_array).contains_error());
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
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
    REQUIRE_FALSE(to_arrow_array(chunk, &arrow_array).contains_error());
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

// The Arrow export path has no exceptions left: every failure it can reach answers a
// core::error_t. An exception here would escape into an actor coroutine whose
// unhandled_exception() is empty under NDEBUG -- the batch would be dropped while the
// caller was told the export succeeded.

TEST_CASE("components::vector::to_arrow_array reports a column type it cannot append") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> types(&resource);
    // NA is the live asymmetry: to_arrow_schema writes the "n" format string for it, but the
    // appender switch has no NA arm -- so a schema is produced for a column whose data cannot
    // be appended. This used to throw std::runtime_error out of to_arrow_array.
    types.emplace_back(logical_type::NA, "na_col");
    data_chunk_t chunk(&resource, types, 4);
    chunk.set_cardinality(4);

    ArrowSchema schema;
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());

    ArrowArray arrow_array;
    auto error = to_arrow_array(chunk, &arrow_array);
    REQUIRE(error.contains_error());
    REQUIRE(error.type == core::error_code_t::unimplemented_yet);
    schema.release(&schema);
}

namespace {
    // A MAP vector is physically a LIST whose single child is a struct<key, value>; rows are
    // list_entry_t offset/length pairs into that struct. Filled through that layout directly so
    // these cases pin the exporter against the storage shape itself, independent of how
    // logical_value_t::create_map spells a MAP value.
    void fill_map_rows(components::vector::vector_t& map_vec,
                       std::pmr::memory_resource* resource,
                       const std::vector<std::vector<std::pair<int32_t, int32_t>>>& rows) {
        uint64_t total = 0;
        for (const auto& row : rows) {
            total += row.size();
        }
        map_vec.reserve(total);
        auto& entries_vec = map_vec.entry();
        auto& key_vec = *entries_vec.entries()[0];
        auto& value_vec = *entries_vec.entries()[1];
        auto* offsets = map_vec.data<list_entry_t>();

        uint64_t cursor = 0;
        for (size_t row = 0; row < rows.size(); row++) {
            offsets[row].offset = cursor;
            offsets[row].length = rows[row].size();
            for (size_t j = 0; j < rows[row].size(); j++) {
                REQUIRE_FALSE(
                    key_vec.set_value(cursor + j, logical_value_t(resource, rows[row][j].first)).contains_error());
                REQUIRE_FALSE(
                    value_vec.set_value(cursor + j, logical_value_t(resource, rows[row][j].second)).contains_error());
            }
            cursor += rows[row].size();
        }
    }
} // namespace

TEST_CASE("components::vector::to_arrow_array round-trips a MAP column") {
    auto resource = core::pmr::otterbrix_resource();
    auto key_type = complex_logical_type{logical_type::INTEGER};
    auto value_type = complex_logical_type{logical_type::INTEGER};
    auto map_type = complex_logical_type::create_map(&resource, key_type, value_type, "map_col");

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(map_type);

    std::vector<std::vector<std::pair<int32_t, int32_t>>> rows{
        {{1, 100}},
        {{2, 200}, {3, 300}},
        {{4, 400}, {5, 500}, {6, 600}},
        {{7, 700}},
    };
    data_chunk_t chunk(&resource, types, rows.size());
    chunk.set_cardinality(rows.size());
    fill_map_rows(chunk.data[0], &resource, rows);

    ArrowSchema schema;
    ArrowArray arrow_array;
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
    // The appender used to read entries() off the map vector itself -- a list buffer, which has
    // no struct entries -- and aborted on the assert inside vector_t::entries() before it could
    // append anything. The key and value vectors live one level down, on the list's struct child.
    REQUIRE_FALSE(to_arrow_array(chunk, &arrow_array).contains_error());

    auto schema_res = schema_from_arrow(&resource, &schema);
    REQUIRE(!schema_res.has_error());
    auto res_w = data_chunk_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
    REQUIRE(!res_w.has_error());
    auto& res = res_w.value();
    REQUIRE(chunk.column_count() == res.column_count());
    REQUIRE(chunk.size() == res.size());
    for (size_t j = 0; j < chunk.size(); j++) {
        REQUIRE(chunk.value(0, j) == res.value(0, j));
    }
    schema.release(&schema);
}

TEST_CASE("components::vector::to_arrow_array round-trips a MAP written via logical_value_t::create_map") {
    auto resource = core::pmr::otterbrix_resource();
    auto key_type = complex_logical_type{logical_type::BIGINT};
    auto value_type = complex_logical_type{logical_type::BIGINT};
    auto map_type = complex_logical_type::create_map(&resource, key_type, value_type, "map_col");

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(map_type);

    // The same rows as the case above, but written through the value API rather than laid into
    // the buffers by hand. This is the join between the two spellings of a MAP: create_map's
    // children have to be exactly what the exporter reads off the entry-struct vector, or the
    // export sees the storage the writer never filled.
    std::vector<std::vector<std::pair<int64_t, int64_t>>> rows{
        {{1, 100}},
        {{2, 200}, {3, 300}},
        {{4, 400}, {5, 500}, {6, 600}},
    };
    data_chunk_t chunk(&resource, types, rows.size());
    chunk.set_cardinality(rows.size());
    for (size_t row = 0; row < rows.size(); row++) {
        std::vector<logical_value_t> keys;
        std::vector<logical_value_t> values;
        for (const auto& [k, v] : rows[row]) {
            keys.emplace_back(&resource, k);
            values.emplace_back(&resource, v);
        }
        auto map_value = logical_value_t::create_map(&resource, key_type, value_type, keys, values);
        REQUIRE_FALSE(chunk.set_value(0, row, map_value).contains_error());
    }

    auto& entries_vec = chunk.data[0].entry();
    REQUIRE(entries_vec.entries()[0]->get_value<int64_t>(0) == 1);
    REQUIRE(entries_vec.entries()[1]->get_value<int64_t>(0) == 100);
    REQUIRE(entries_vec.entries()[0]->get_value<int64_t>(5) == 6);
    REQUIRE(entries_vec.entries()[1]->get_value<int64_t>(5) == 600);

    ArrowSchema schema;
    ArrowArray arrow_array;
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
    REQUIRE_FALSE(to_arrow_array(chunk, &arrow_array).contains_error());

    auto schema_res = schema_from_arrow(&resource, &schema);
    REQUIRE(!schema_res.has_error());
    auto res_w = data_chunk_from_arrow(&resource, &arrow_array, std::move(schema_res.value()));
    REQUIRE(!res_w.has_error());
    auto& res = res_w.value();
    REQUIRE(chunk.size() == res.size());
    for (size_t j = 0; j < chunk.size(); j++) {
        REQUIRE(chunk.value(0, j) == res.value(0, j));
    }
    schema.release(&schema);
}

TEST_CASE("components::vector::to_arrow_array reports a NULL key on a MAP") {
    auto resource = core::pmr::otterbrix_resource();
    auto key_type = complex_logical_type{logical_type::INTEGER};
    auto value_type = complex_logical_type{logical_type::INTEGER};
    auto map_type = complex_logical_type::create_map(&resource, key_type, value_type, "map_col");

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(map_type);

    std::vector<std::vector<std::pair<int32_t, int32_t>>> rows{{{1, 100}, {2, 200}}};
    data_chunk_t chunk(&resource, types, rows.size());
    chunk.set_cardinality(rows.size());
    fill_map_rows(chunk.data[0], &resource, rows);

    // Arrow's MAP layout requires non-null keys; otterbrix imposes no such rule, so a NULL key
    // reaches the exporter. It used to throw std::runtime_error out of finalize.
    chunk.data[0].entry().entries()[0]->set_null(1, true);

    ArrowArray arrow_array;
    auto error = to_arrow_array(chunk, &arrow_array);
    REQUIRE(error.contains_error());
    REQUIRE(error.type == core::error_code_t::conversion_failure);
}

TEST_CASE("components::vector::to_arrow_array round-trips every DECIMAL storage width") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> types(&resource);
    // decimal_logical_type_extension pins stored_as() to INT16/INT32/INT64/INT128 by width, so
    // the appender's DECIMAL switch has an arm for every value it can ever see -- its default
    // arm is unreachable. One column per storage class pins that.
    types.emplace_back(complex_logical_type::create_decimal(4, 2, "dec16"));
    types.emplace_back(complex_logical_type::create_decimal(9, 2, "dec32"));
    types.emplace_back(complex_logical_type::create_decimal(18, 2, "dec64"));
    types.emplace_back(complex_logical_type::create_decimal(38, 2, "dec128"));

    constexpr size_t chunk_size = 4;
    data_chunk_t chunk(&resource, types, chunk_size);
    chunk.set_cardinality(chunk_size);
    for (size_t row = 0; row < chunk_size; row++) {
        for (size_t col = 0; col < types.size(); col++) {
            chunk.set_value(
                col,
                row,
                logical_value_t::create_decimal(&resource, types[col], static_cast<int64_t>(row * 100 + col)));
        }
    }

    ArrowSchema schema;
    ArrowArray arrow_array;
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
    REQUIRE_FALSE(to_arrow_array(chunk, &arrow_array).contains_error());
    arrow_array.release(&arrow_array);
    schema.release(&schema);
}

// M3-B2 characterization, carried through B5: the column names an Arrow consumer sees. They
// are the ONLY user-visible product of the export that comes from column identity rather than
// from the values, so they are what a reader migration has to leave untouched.
//
// B2 could only pin that the exported name of column i already EQUALLED the chunk's own
// record for column i, because to_arrow_schema took a bare type list and read the name out of
// the type. B5 made that the definition: the export takes the schema, so the pin below is now
// the contract rather than a coincidence the migration had to preserve.
//
// Note what is NOT a column name here: the child names inside a STRUCT column
// (arrow_converter.cpp:202) are STRUCT FIELD names, a different role that stays on the type.
// Both are pinned so the two cannot be confused.
TEST_CASE("components::vector::arrow export names columns as the chunk's schema does") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BOOLEAN, "flag");
    fields.emplace_back(logical_type::INTEGER, "number");

    schema_t columns(&resource);
    const char* const column_names[] = {"id", "label", "payload"};
    complex_logical_type column_types[] = {complex_logical_type{logical_type::BIGINT},
                                           complex_logical_type{logical_type::STRING_LITERAL},
                                           // create_struct's FIRST argument is the struct
                                           // TYPE's own name; the COLUMN's name is the
                                           // schema record's, and the two are now different
                                           // slots rather than the same one.
                                           complex_logical_type::create_struct("payload_t", fields)};
    for (size_t col = 0; col < 3; ++col) {
        column_schema_t record{&resource};
        record.name = column_names[col];
        record.type = column_types[col];
        columns.push_back(std::move(record));
    }

    data_chunk_t chunk = make_chunk(&resource, columns, 2);
    chunk.set_cardinality(0);

    ArrowSchema schema;
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());

    REQUIRE(schema.n_children == 3);
    REQUIRE(std::string{schema.children[0]->name} == "id");
    REQUIRE(std::string{schema.children[1]->name} == "label");
    REQUIRE(std::string{schema.children[2]->name} == "payload");
    // The exported name of column i is the chunk's own record for column i.
    for (size_t col = 0; col < chunk.column_count(); ++col) {
        REQUIRE(std::string{schema.children[col]->name} == std::string{chunk.schema()[col].name});
    }
    // STRUCT field names: a separate role, still carried by the type.
    REQUIRE(schema.children[2]->n_children == 2);
    REQUIRE(std::string{schema.children[2]->children[0]->name} == "flag");
    REQUIRE(std::string{schema.children[2]->children[1]->name} == "number");

    schema.release(&schema);
}

// A STRUCT column built from a type alone has no column name: naming the TYPE never named
// the COLUMN, which is exactly the confusion the two separate slots removed. Arrow exports it
// nameless and the chunk's schema record says the same thing.
TEST_CASE("components::vector::arrow export leaves a STRUCT type name out of the column name") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::INTEGER, "number");

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(complex_logical_type::create_struct("payload_t", fields));

    data_chunk_t chunk(&resource, types, 1);
    chunk.set_cardinality(0);

    ArrowSchema schema;
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
    REQUIRE(schema.n_children == 1);
    REQUIRE(std::string{schema.children[0]->name}.empty());
    REQUIRE(std::string{chunk.schema()[0].name}.empty());
    schema.release(&schema);
}

// RED before the fix, and it stays a pin. complex_logical_type::alias() used to assert on
// extension_ and dereference null in release, so exporting a chunk with any unnamed column
// aborted in a debug build and read through a null pointer in a release one. An unnamed
// column is not exotic: an expression output need not be aliased at all, and the one
// production caller (arrow_export_utils.cpp) names only the first names.size() columns.
TEST_CASE("components::vector::arrow export gives an unnamed column an empty Arrow name") {
    auto resource = core::pmr::otterbrix_resource();
    schema_t columns(&resource);
    {
        column_schema_t unnamed{&resource}; // no name at all
        unnamed.type = complex_logical_type{logical_type::BIGINT};
        columns.push_back(std::move(unnamed));
        column_schema_t named{&resource};
        named.name = "named";
        named.type = complex_logical_type{logical_type::INTEGER};
        columns.push_back(std::move(named));
    }

    data_chunk_t chunk = make_chunk(&resource, columns, 1);
    chunk.set_cardinality(0);

    ArrowSchema schema;
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
    REQUIRE(schema.n_children == 2);
    REQUIRE(std::string{schema.children[0]->name}.empty());
    REQUIRE(std::string{schema.children[1]->name} == "named");
    REQUIRE(std::string{chunk.schema()[0].name}.empty());
    schema.release(&schema);
}

TEST_CASE("components::vector::arrow export gives an unnamed STRUCT field an empty Arrow name") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BOOLEAN); // unnamed field
    fields.emplace_back(logical_type::INTEGER, "number");

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(complex_logical_type::create_struct("payload", fields));
    data_chunk_t chunk(&resource, types, 1);
    chunk.set_cardinality(0);

    ArrowSchema schema;
    REQUIRE_FALSE(to_arrow_schema(&schema, chunk.schema()).contains_error());
    REQUIRE(schema.n_children == 1);
    REQUIRE(schema.children[0]->n_children == 2);
    REQUIRE(std::string{schema.children[0]->children[0]->name}.empty());
    REQUIRE(std::string{schema.children[0]->children[1]->name} == "number");
    schema.release(&schema);
}
