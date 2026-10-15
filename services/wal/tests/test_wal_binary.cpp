#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <components/tests/generaty.hpp>
#include <components/vector/data_chunk_binary.hpp>
#include <core/pmr.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal.hpp>
#include <services/wal/wal_binary.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>
#include <array>
#include <cstring>

#include "wal_test_attoids.hpp"

using namespace services::wal;
using wal_test::attoids_for;
using namespace components::types;
using namespace components::vector;

static std::pmr::vector<components::vector::data_chunk_t>
to_chunk_batch(const components::vector::data_chunk_t& chunk) {
    std::pmr::vector<components::vector::data_chunk_t> batch(chunk.resource());
    components::vector::data_chunk_t copy(chunk.resource(), chunk.types(), chunk.size() == 0 ? 1 : chunk.size());
    chunk.copy(copy, 0);
    batch.emplace_back(std::move(copy));
    return batch;
}

static void put(const vector_t& cell, vector_t& target, uint64_t row) {
    vector_ops::copy(cell, target, 1, 0, row);
}

static uint64_t len(const vector_t& v, uint64_t row) {
    return sequence_of(v, row).length;
}

template<typename T>
static T elem(const vector_t& v, uint64_t row, uint64_t i) {
    const auto seq = sequence_of(v, row);
    return seq.child->get_value<T>(seq.offset + i);
}

static bool elem_null(const vector_t& v, uint64_t row, uint64_t i) {
    const auto seq = sequence_of(v, row);
    return seq.child->is_null(seq.offset + i);
}

template<typename T>
static T fld(const vector_t& v, uint64_t row, uint64_t k) {
    const auto cell = resolve_cell(v, row);
    return cell.vec->entries()[k]->get_value<T>(cell.index);
}

static constexpr std::string_view kGarbageMarker = "GARBAGE-UNDER-NULL";

static bool buffer_contains(const buffer_t& buffer, std::string_view needle) {
    std::string_view haystack(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    return haystack.find(needle) != std::string_view::npos;
}

static std::pmr::vector<components::types::complex_logical_type> wal_test_types(std::pmr::memory_resource* r) {
    using namespace components::types;
    std::pmr::vector<complex_logical_type> types(r);
    types.emplace_back(logical_type::BIGINT, "count");
    types.emplace_back(logical_type::STRING_LITERAL, "count_str");
    types.emplace_back(logical_type::DOUBLE, "count_double");
    types.emplace_back(logical_type::BOOLEAN, "count_bool");
    return types;
}

// WAL records key by table_oid (4 bytes), not (database, collection) strings.
constexpr components::catalog::oid_t kTestTableOid = 16500;

TEST_CASE("wal_binary::encode_decode_insert") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto chunk = gen_data_chunk(10, 0, wal_test_types(&resource), &resource);

    buffer_t buffer(&resource);
    auto attoids = attoids_for(chunk);
    encode_insert(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/1,
                  /*txn_id=*/100,
                  /*sub_id=*/0,
                  kTestTableOid,
                  attoids,
                  to_chunk_batch(chunk),
                  /*row_start=*/0,
                  /*row_count=*/10);

    REQUIRE(buffer.size() > 0);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.physical_attoids == attoids);
    REQUIRE(record.id == 1);
    REQUIRE(record.transaction_id == 100);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_INSERT);
    REQUIRE(record.table_oid == kTestTableOid);
    REQUIRE(record.physical_row_start == 0);
    REQUIRE(record.physical_row_count == 10);
    REQUIRE(!record.physical_data.empty());
    REQUIRE(record.physical_data.front().column_count() == chunk.column_count());
    REQUIRE(record.physical_data.front().size() == chunk.size());

    for (uint64_t col = 0; col < chunk.column_count(); col++) {
        for (uint64_t row = 0; row < chunk.size(); row++) {
            REQUIRE(record.physical_data.front().data[col].type() == chunk.data[col].type());
            REQUIRE(cells_equal(record.physical_data.front().data[col], row, chunk.data[col], row));
        }
    }
}

TEST_CASE("wal_binary::encode_decode_delete") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    std::vector<int64_t> row_ids = {1, 3, 5, 7, 9};

    buffer_t buffer(&resource);
    encode_delete(buffer,
                  /*last_crc32=*/0,
                  /*wal_id=*/2,
                  /*txn_id=*/101,
                  /*sub_id=*/0,
                  kTestTableOid,
                  row_ids.data(),
                  /*count=*/5);

    REQUIRE(buffer.size() > 0);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.id == 2);
    REQUIRE(record.transaction_id == 101);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_DELETE);
    REQUIRE(record.table_oid == kTestTableOid);
    REQUIRE(record.physical_row_ids.size() == 5);

    for (size_t i = 0; i < row_ids.size(); i++) {
        REQUIRE(record.physical_row_ids[i] == row_ids[i]);
    }
}

TEST_CASE("wal_binary::encode_decode_update") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto new_data = gen_data_chunk(5, 0, wal_test_types(&resource), &resource);
    std::vector<int64_t> row_ids = {0, 2, 4, 6, 8};

    buffer_t buffer(&resource);
    auto attoids = attoids_for(new_data);
    encode_update(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/3,
                  /*txn_id=*/102,
                  /*sub_id=*/0,
                  kTestTableOid,
                  row_ids.data(),
                  attoids,
                  to_chunk_batch(new_data),
                  /*count=*/5);

    REQUIRE(buffer.size() > 0);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.physical_attoids == attoids);
    REQUIRE(record.id == 3);
    REQUIRE(record.transaction_id == 102);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_UPDATE);
    REQUIRE(record.table_oid == kTestTableOid);
    REQUIRE(record.physical_row_ids.size() == 5);

    for (size_t i = 0; i < row_ids.size(); i++) {
        REQUIRE(record.physical_row_ids[i] == row_ids[i]);
    }

    REQUIRE(!record.physical_data.empty());
    REQUIRE(record.physical_data.front().column_count() == new_data.column_count());
    REQUIRE(record.physical_data.front().size() == new_data.size());

    for (uint64_t col = 0; col < new_data.column_count(); col++) {
        for (uint64_t row = 0; row < new_data.size(); row++) {
            REQUIRE(record.physical_data.front().data[col].type() == new_data.data[col].type());
            REQUIRE(cells_equal(record.physical_data.front().data[col], row, new_data.data[col], row));
        }
    }
}

TEST_CASE("wal_binary::encode_decode_commit") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    buffer_t buffer(&resource);
    encode_commit(buffer, /*last_crc32=*/0, /*wal_id=*/4, /*txn_id=*/103, /*commit_id=*/0);

    REQUIRE(buffer.size() == 37);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.id == 4);
    REQUIRE(record.transaction_id == 103);
    REQUIRE(record.record_type == wal_record_type::COMMIT);
    REQUIRE(record.is_commit_marker());
}

TEST_CASE("wal_binary::crc32_corruption") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto chunk = gen_data_chunk(10, &resource);

    buffer_t buffer(&resource);
    encode_insert(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/1,
                  /*txn_id=*/100,
                  /*sub_id=*/0,
                  kTestTableOid,
                  attoids_for(chunk),
                  to_chunk_batch(chunk),
                  /*row_start=*/0,
                  /*row_count=*/10);

    REQUIRE(buffer.size() > 29);

    size_t flip_pos = buffer.size() / 2;
    buffer[flip_pos] ^= static_cast<char>(0xFF);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_corrupt);
}

TEST_CASE("wal_binary::truncated_input") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto chunk = gen_data_chunk(10, &resource);

    buffer_t buffer(&resource);
    encode_insert(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/1,
                  /*txn_id=*/100,
                  /*sub_id=*/0,
                  kTestTableOid,
                  attoids_for(chunk),
                  to_chunk_batch(chunk),
                  /*row_start=*/0,
                  /*row_count=*/10);

    buffer_t truncated(buffer.data(), buffer.size() / 2, &resource);

    auto record = decode_record(truncated, &resource);
    REQUIRE((record.is_corrupt || record.size == 0));
}

TEST_CASE("wal_binary::data_chunk_binary_mixed_types") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.emplace_back(logical_type::DOUBLE, "score");
    types.emplace_back(logical_type::STRING_LITERAL, "name");
    types.emplace_back(logical_type::BOOLEAN, "active");

    auto chunk = gen_data_chunk(8, 0, types, &resource);

    REQUIRE(chunk.column_count() == 4);
    REQUIRE(chunk.size() == 8);

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    REQUIRE(buffer.size() > 0);

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);

    REQUIRE(ok);
    REQUIRE(result.column_count() == chunk.column_count());
    REQUIRE(result.size() == chunk.size());

    for (uint64_t col = 0; col < chunk.column_count(); col++) {
        for (uint64_t row = 0; row < chunk.size(); row++) {
            REQUIRE(result.data[col].type() == chunk.data[col].type());
            REQUIRE(cells_equal(result.data[col], row, chunk.data[col], row));
        }
    }
}

TEST_CASE("wal_binary::data_chunk_binary_with_nulls") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.emplace_back(logical_type::DOUBLE, "value");

    auto chunk = gen_data_chunk(10, 0, types, &resource);

    for (auto& vec : chunk.data) {
        vec.validity().set_invalid(1);
        vec.validity().set_invalid(4);
        vec.validity().set_invalid(7);
    }

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    REQUIRE(buffer.size() > 0);

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);

    REQUIRE(ok);
    REQUIRE(result.column_count() == chunk.column_count());
    REQUIRE(result.size() == chunk.size());

    for (uint64_t col = 0; col < result.column_count(); col++) {
        REQUIRE_FALSE(result.data[col].validity().row_is_valid(1));
        REQUIRE_FALSE(result.data[col].validity().row_is_valid(4));
        REQUIRE_FALSE(result.data[col].validity().row_is_valid(7));

        REQUIRE(result.data[col].validity().row_is_valid(0));
        REQUIRE(result.data[col].validity().row_is_valid(2));
        REQUIRE(result.data[col].validity().row_is_valid(3));
        REQUIRE(result.data[col].validity().row_is_valid(5));
        REQUIRE(result.data[col].validity().row_is_valid(6));
        REQUIRE(result.data[col].validity().row_is_valid(8));
        REQUIRE(result.data[col].validity().row_is_valid(9));
    }
}

// The STRUCT-less type header walked missing field types through a garbage pointer; now uses the canonical type spec.
TEST_CASE("wal_binary::data_chunk_binary_struct_type_roundtrip") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(logical_type::BIGINT, "b");
    auto pair_type = complex_logical_type::create_struct("np_pair", fields);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.push_back(pair_type);

    data_chunk_t chunk(&resource, types, 3);
    chunk.set_cardinality(3);
    for (uint64_t row = 0; row < 3; row++) {
        chunk.set_value(0, row, static_cast<int64_t>(row + 1));
        if (row == 1) {
            put(make_null(&resource, types[1]), chunk.data[1], row);
        } else {
            std::vector<vector_t> members;
            members.push_back(make_scalar(&resource, static_cast<int64_t>(row * 10)));
            members.push_back(make_scalar(&resource, static_cast<int64_t>(row * 20)));
            put(make_struct(&resource, pair_type, members), chunk.data[1], row);
        }
    }

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);
    REQUIRE(buffer.size() > 0);

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);

    REQUIRE(ok);
    REQUIRE(result.column_count() == 2);
    REQUIRE(result.size() == 3);

    const auto& decoded_type = result.data[1].type();
    REQUIRE(decoded_type.type() == logical_type::STRUCT);
    REQUIRE(decoded_type.child_types().size() == 2);
    REQUIRE(decoded_type.child_types()[0].type() == logical_type::BIGINT);
    REQUIRE(decoded_type.child_types()[1].type() == logical_type::BIGINT);

    REQUIRE(result.data[1].validity().row_is_valid(0));
    REQUIRE_FALSE(result.data[1].validity().row_is_valid(1));
    REQUIRE(result.data[1].validity().row_is_valid(2));

    for (uint64_t row = 0; row < 3; row++) {
        REQUIRE(result.data[0].get_value<int64_t>(row) == static_cast<int64_t>(row + 1));
    }
}

// fixed_type_size() is 0 for LIST/STRUCT/ARRAY, so a naive codec silently rebuilt an all-zero shape; now recursive.

TEST_CASE("wal_binary::data_chunk_binary_array_payload_roundtrip") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    auto element_type = complex_logical_type{logical_type::BIGINT};
    auto array_type = complex_logical_type::create_array(element_type, 3);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.push_back(array_type);

    data_chunk_t chunk(&resource, types, 4);
    chunk.set_cardinality(4);
    for (uint64_t row = 0; row < 4; row++) {
        chunk.set_value(0, row, static_cast<int64_t>(row + 1));
    }
    for (uint64_t row : {uint64_t{0}, uint64_t{1}}) {
        std::vector<vector_t> elements;
        for (uint64_t i = 0; i < 3; i++) {
            elements.push_back(make_scalar(&resource, static_cast<int64_t>(row * 10 + i)));
        }
        put(make_array(&resource, element_type, elements), chunk.data[1], row);
    }
    put(make_null(&resource, types[1]), chunk.data[1], 2);
    {
        std::vector<vector_t> elements;
        elements.push_back(make_scalar(&resource, static_cast<int64_t>(70)));
        elements.push_back(make_null(&resource, element_type));
        elements.push_back(make_scalar(&resource, static_cast<int64_t>(90)));
        put(make_array(&resource, element_type, elements), chunk.data[1], 3);
    }

    REQUIRE(elem_null(chunk.data[1], 3, 1));

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);

    REQUIRE(ok);
    REQUIRE(result.column_count() == 2);
    REQUIRE(result.size() == 4);
    REQUIRE(result.data[1].type().type() == logical_type::ARRAY);

    for (uint64_t row = 0; row < 4; row++) {
        INFO("row " << row);
        REQUIRE(result.data[0].get_value<int64_t>(row) == static_cast<int64_t>(row + 1));
    }

    for (uint64_t row : {uint64_t{0}, uint64_t{1}}) {
        INFO("array row " << row);
        REQUIRE(len(result.data[1], row) == 3);
        for (uint64_t i = 0; i < 3; i++) {
            INFO("element " << i);
            REQUIRE(elem<int64_t>(result.data[1], row, i) == static_cast<int64_t>(row * 10 + i));
        }
    }

    INFO("the whole-cell NULL is still NULL");
    REQUIRE_FALSE(result.data[1].validity().row_is_valid(2));

    INFO("the interior NULL survives beside its present neighbours");
    {
        REQUIRE(len(result.data[1], 3) == 3);
        REQUIRE(elem<int64_t>(result.data[1], 3, 0) == 70);
        REQUIRE(elem_null(result.data[1], 3, 1));
        REQUIRE(elem<int64_t>(result.data[1], 3, 2) == 90);
    }
}

TEST_CASE("wal_binary::data_chunk_binary_list_payload_roundtrip") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    auto element_type = complex_logical_type{logical_type::BIGINT};
    auto list_type = complex_logical_type::create_list(element_type);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.push_back(list_type);

    data_chunk_t chunk(&resource, types, 4);
    chunk.set_cardinality(4);
    for (uint64_t row = 0; row < 4; row++) {
        chunk.set_value(0, row, static_cast<int64_t>(row + 1));
    }

    // Deliberately ragged and longer than the row count, to catch sizing the child from row count, not the span.
    const std::vector<std::vector<int64_t>> rows = {{10, 20}, {}, {30, 40, 50, 60, 70}, {80}};
    for (uint64_t row = 0; row < rows.size(); row++) {
        std::vector<vector_t> elements;
        for (auto value : rows[row]) {
            elements.push_back(make_scalar(&resource, value));
        }
        put(make_list(&resource, element_type, elements), chunk.data[1], row);
    }

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);

    REQUIRE(ok);
    REQUIRE(result.column_count() == 2);
    REQUIRE(result.size() == 4);
    REQUIRE(result.data[1].type().type() == logical_type::LIST);

    for (uint64_t row = 0; row < rows.size(); row++) {
        INFO("list row " << row << " expected length " << rows[row].size());
        REQUIRE(len(result.data[1], row) == rows[row].size());
        for (uint64_t i = 0; i < rows[row].size(); i++) {
            INFO("element " << i);
            REQUIRE(elem<int64_t>(result.data[1], row, i) == rows[row][i]);
        }
    }
}

TEST_CASE("wal_binary::data_chunk_binary_null_string_payload_is_not_read") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::STRING_LITERAL, "s");

    data_chunk_t chunk(&resource, types, 3);
    chunk.set_cardinality(3);
    chunk.set_value(0, 0, std::string_view{std::string("a")});
    chunk.set_value(0, 1, std::string_view{std::string("b")});
    chunk.set_value(0, 2, std::string_view{std::string("c")});
    chunk.data[0].validity().set_invalid(1);
    const std::string marker(kGarbageMarker);
    reinterpret_cast<std::string_view*>(chunk.data[0].data())[1] = marker;

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    CHECK_FALSE(buffer_contains(buffer, kGarbageMarker));

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);
    REQUIRE(ok);
    REQUIRE(result.size() == 3);
    CHECK(result.data[0].get_value<std::string_view>(0) == "a");
    CHECK_FALSE(result.data[0].validity().row_is_valid(1));
    CHECK(result.data[0].get_value<std::string_view>(2) == "c");
}

TEST_CASE("wal_binary::data_chunk_binary_null_list_element_string_payload_is_not_read") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto element_type = complex_logical_type{logical_type::STRING_LITERAL};
    auto list_type = complex_logical_type::create_list(element_type);
    std::pmr::vector<complex_logical_type> types(&resource);
    types.push_back(list_type);

    data_chunk_t chunk(&resource, types, 2);
    chunk.set_cardinality(2);
    const std::vector<std::vector<std::string>> rows = {{"a", "b"}, {"c", "d", "e"}};
    for (uint64_t row = 0; row < rows.size(); row++) {
        std::vector<vector_t> elements;
        for (const auto& value : rows[row]) {
            elements.push_back(make_scalar(&resource, value));
        }
        put(make_list(&resource, element_type, elements), chunk.data[0], row);
    }
    auto& child = chunk.data[0].entry();
    child.validity().set_invalid(3);
    const std::string marker(kGarbageMarker);
    reinterpret_cast<std::string_view*>(child.data())[3] = marker;

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    CHECK_FALSE(buffer_contains(buffer, kGarbageMarker));

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);
    REQUIRE(ok);
    REQUIRE(result.size() == 2);
    REQUIRE(len(result.data[0], 1) == 3);
    CHECK(elem<std::string_view>(result.data[0], 1, 0) == "c");
    CHECK(elem_null(result.data[0], 1, 1));
    CHECK(elem<std::string_view>(result.data[0], 1, 2) == "e");
}

TEST_CASE("wal_binary::data_chunk_binary_null_list_row_entry_is_not_read") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto element_type = complex_logical_type{logical_type::BIGINT};
    auto list_type = complex_logical_type::create_list(element_type);
    std::pmr::vector<complex_logical_type> types(&resource);
    types.push_back(list_type);

    auto make_chunk = [&](uint64_t garbage_length) {
        data_chunk_t chunk(&resource, types, 8);
        chunk.set_cardinality(3);
        const std::vector<std::vector<int64_t>> rows = {{10, 20}, {}, {30}};
        for (uint64_t row = 0; row < rows.size(); row++) {
            std::vector<vector_t> elements;
            for (auto value : rows[row]) {
                elements.push_back(make_scalar(&resource, value));
            }
            put(make_list(&resource, element_type, elements), chunk.data[0], row);
        }
        chunk.data[0].validity().set_invalid(1);
        reinterpret_cast<list_entry_t*>(chunk.data[0].data())[1] = list_entry_t{0, garbage_length};
        return chunk;
    };

    auto clean = make_chunk(0);
    auto dirty = make_chunk(5);
    buffer_t clean_buffer(&resource);
    buffer_t dirty_buffer(&resource);
    serialize_binary(clean, clean_buffer);
    serialize_binary(dirty, dirty_buffer);

    CHECK(dirty_buffer.size() == clean_buffer.size());

    bool ok = false;
    auto result = deserialize_binary(dirty_buffer.data(), dirty_buffer.size(), &resource, ok);
    REQUIRE(ok);
    REQUIRE(result.size() == 3);
    CHECK(len(result.data[0], 0) == 2);
    CHECK_FALSE(result.data[0].validity().row_is_valid(1));
    REQUIRE(len(result.data[0], 2) == 1);
    CHECK(elem<int64_t>(result.data[0], 2, 0) == 30);
}

TEST_CASE("wal_binary::data_chunk_binary_null_fixed_width_payload_is_not_read") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "b");
    types.emplace_back(logical_type::INTEGER, "i");
    types.emplace_back(logical_type::DOUBLE, "d");

    data_chunk_t chunk(&resource, types, 3);
    chunk.set_cardinality(3);
    for (uint64_t row = 0; row < 3; row++) {
        chunk.set_value(0, row, static_cast<int64_t>(row + 1));
        chunk.set_value(1, row, static_cast<int32_t>(row + 1));
        chunk.set_value(2, row, static_cast<double>(row + 1));
    }
    static constexpr std::string_view kBytes = "\xDE\xAD\xBE\xEF\xDE\xAD\xBE\xEF";
    for (uint64_t column = 0; column < 3; column++) {
        chunk.data[column].validity().set_invalid(1);
        const size_t width = column == 1 ? 4 : 8;
        std::memcpy(chunk.data[column].data() + width, kBytes.data(), width);
    }

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    CHECK_FALSE(buffer_contains(buffer, kBytes.substr(0, 4)));

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);
    REQUIRE(ok);
    REQUIRE(result.size() == 3);
    CHECK(result.data[0].get_value<int64_t>(0) == 1);
    CHECK(result.data[1].get_value<int32_t>(2) == 3);
    CHECK_THAT(result.data[2].get_value<double>(2), Catch::Matchers::WithinULP(3.0, 0));
    for (uint64_t column = 0; column < 3; column++) {
        CHECK_FALSE(result.data[column].validity().row_is_valid(1));
    }
}

TEST_CASE("wal_binary::data_chunk_binary_null_struct_row_children_are_not_read") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::STRING_LITERAL, "s");
    fields.emplace_back(logical_type::BIGINT, "n");
    auto struct_type = complex_logical_type::create_struct("np_pair", fields);
    std::pmr::vector<complex_logical_type> types(&resource);
    types.push_back(struct_type);

    data_chunk_t chunk(&resource, types, 3);
    chunk.set_cardinality(3);
    const std::string marker(kGarbageMarker);
    constexpr int64_t kBigintMarker = 0x7A6B5C4D3E2F1A09;
    const std::string_view bigint_marker(reinterpret_cast<const char*>(&kBigintMarker), sizeof(kBigintMarker));
    for (uint64_t row = 0; row < 3; row++) {
        std::vector<vector_t> members;
        members.push_back(make_scalar(&resource, row == 1 ? marker : std::string("ok")));
        members.push_back(make_scalar(&resource, row == 1 ? kBigintMarker : static_cast<int64_t>(row)));
        put(make_struct(&resource, struct_type, members), chunk.data[0], row);
    }
    chunk.data[0].validity().set_invalid(1);
    REQUIRE(chunk.data[0].entries()[0]->validity().row_is_valid(1));
    REQUIRE(chunk.data[0].entries()[1]->validity().row_is_valid(1));

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    CHECK_FALSE(buffer_contains(buffer, kGarbageMarker));
    CHECK_FALSE(buffer_contains(buffer, bigint_marker));

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);
    REQUIRE(ok);
    REQUIRE(result.size() == 3);
    CHECK_FALSE(result.data[0].validity().row_is_valid(1));
    CHECK(result.data[0].entries()[0]->get_value<std::string_view>(0) == "ok");
    CHECK(result.data[0].entries()[0]->get_value<std::string_view>(2) == "ok");
}

TEST_CASE("wal_binary::data_chunk_binary_null_array_row_elements_are_not_read") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto element_type = complex_logical_type{logical_type::STRING_LITERAL};
    auto array_type = complex_logical_type::create_array(element_type, 2);
    std::pmr::vector<complex_logical_type> types(&resource);
    types.push_back(array_type);

    data_chunk_t chunk(&resource, types, 3);
    chunk.set_cardinality(3);
    const std::string marker(kGarbageMarker);
    for (uint64_t row = 0; row < 3; row++) {
        std::vector<vector_t> elements;
        elements.push_back(make_scalar(&resource, row == 1 ? marker : std::string("ok")));
        elements.push_back(make_scalar(&resource, row == 1 ? marker : std::string("ok")));
        put(make_array(&resource, element_type, elements), chunk.data[0], row);
    }
    chunk.data[0].validity().set_invalid(1);
    REQUIRE(chunk.data[0].entry().validity().row_is_valid(2));

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    CHECK_FALSE(buffer_contains(buffer, kGarbageMarker));

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);
    REQUIRE(ok);
    REQUIRE(result.size() == 3);
    CHECK_FALSE(result.data[0].validity().row_is_valid(1));
    CHECK(result.data[0].entry().get_value<std::string_view>(0) == "ok");
    CHECK(result.data[0].entry().get_value<std::string_view>(4) == "ok");
}

TEST_CASE("wal_binary::data_chunk_binary_struct_payload_roundtrip") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(logical_type::BIGINT, "b");
    auto pair_type = complex_logical_type::create_struct("np_pair", fields);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.push_back(pair_type);

    data_chunk_t chunk(&resource, types, 3);
    chunk.set_cardinality(3);
    for (uint64_t row = 0; row < 3; row++) {
        chunk.set_value(0, row, static_cast<int64_t>(row + 1));
    }
    {
        std::vector<vector_t> members;
        members.push_back(make_scalar(&resource, static_cast<int64_t>(11)));
        members.push_back(make_scalar(&resource, static_cast<int64_t>(12)));
        put(make_struct(&resource, pair_type, members), chunk.data[1], 0);
    }
    put(make_null(&resource, types[1]), chunk.data[1], 1);
    {
        std::vector<vector_t> members;
        members.push_back(make_scalar(&resource, static_cast<int64_t>(31)));
        members.push_back(make_null(&resource, complex_logical_type{logical_type::BIGINT}));
        put(make_struct(&resource, pair_type, members), chunk.data[1], 2);
    }

    REQUIRE_FALSE(chunk.data[1].entries()[1]->validity().row_is_valid(2));

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);

    REQUIRE(ok);
    REQUIRE(result.column_count() == 2);
    REQUIRE(result.size() == 3);

    INFO("both fields of the populated row carry their values");
    {
        REQUIRE(result.data[1].entries().size() == 2);
        REQUIRE(fld<int64_t>(result.data[1], 0, 0) == 11);
        REQUIRE(fld<int64_t>(result.data[1], 0, 1) == 12);
    }

    INFO("the whole-struct NULL is still NULL");
    REQUIRE_FALSE(result.data[1].validity().row_is_valid(1));

    INFO("a present field beside a NULL one");
    {
        REQUIRE(result.data[1].entries().size() == 2);
        REQUIRE(result.data[1].entries()[0]->get_value<int64_t>(2) == 31);
        REQUIRE_FALSE(result.data[1].entries()[1]->validity().row_is_valid(2));
    }
}

// The payload codec descends by physical type, so nesting reenters the same path, not three flat LIST/STRUCT/ARRAY legs
TEST_CASE("wal_binary::data_chunk_binary_second_level_nesting_roundtrip") {
    std::pmr::monotonic_buffer_resource resource(1024 * 128);

    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(logical_type::STRING_LITERAL, "s");
    auto pair_type = complex_logical_type::create_struct("np_pair", fields);
    auto list_of_structs = complex_logical_type::create_list(pair_type);

    auto inner_list_type = complex_logical_type::create_list(complex_logical_type{logical_type::BIGINT});
    std::pmr::vector<complex_logical_type> holder_fields(&resource);
    holder_fields.push_back(inner_list_type);
    holder_fields.back().set_alias("l");
    auto struct_of_list = complex_logical_type::create_struct("np_holder", holder_fields);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.push_back(list_of_structs);
    types.push_back(struct_of_list);

    data_chunk_t chunk(&resource, types, 2);
    chunk.set_cardinality(2);
    for (uint64_t row = 0; row < 2; row++) {
        chunk.set_value(0, row, static_cast<int64_t>(row + 1));

        std::vector<vector_t> members;
        for (uint64_t i = 0; i < 2 - row; i++) {
            std::vector<vector_t> pair;
            pair.push_back(make_scalar(&resource, static_cast<int64_t>(row * 100 + i)));
            pair.push_back(make_scalar(&resource, std::string("s") + std::to_string(row * 100 + i)));
            members.push_back(make_struct(&resource, pair_type, pair));
        }
        put(make_list(&resource, pair_type, members), chunk.data[1], row);

        std::vector<vector_t> inner;
        for (uint64_t i = 0; i <= row; i++) {
            inner.push_back(make_scalar(&resource, static_cast<int64_t>(row * 1000 + i)));
        }
        std::vector<vector_t> holder;
        holder.push_back(make_list(&resource, complex_logical_type{logical_type::BIGINT}, inner));
        put(make_struct(&resource, struct_of_list, holder), chunk.data[2], row);
    }

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);

    REQUIRE(ok);
    REQUIRE(result.column_count() == 3);
    REQUIRE(result.size() == 2);

    for (uint64_t row = 0; row < 2; row++) {
        INFO("row " << row);
        REQUIRE(result.data[0].get_value<int64_t>(row) == static_cast<int64_t>(row + 1));

        INFO("list of structs");
        REQUIRE(len(result.data[1], row) == 2 - row);
        const auto members = sequence_of(result.data[1], row);
        REQUIRE(members.child->entries().size() == 2);
        for (uint64_t i = 0; i < 2 - row; i++) {
            INFO("member " << i);
            REQUIRE(fld<int64_t>(*members.child, members.offset + i, 0) == static_cast<int64_t>(row * 100 + i));
            REQUIRE(fld<std::string_view>(*members.child, members.offset + i, 1) ==
                    std::string_view(std::string("s") + std::to_string(row * 100 + i)));
        }

        INFO("struct holding a list");
        const auto holder = resolve_cell(result.data[2], row);
        REQUIRE(holder.vec->entries().size() == 1);
        const auto& inner_list = *holder.vec->entries()[0];
        REQUIRE(len(inner_list, holder.index) == row + 1);
        for (uint64_t i = 0; i <= row; i++) {
            INFO("inner element " << i);
            REQUIRE(elem<int64_t>(inner_list, holder.index, i) == static_cast<int64_t>(row * 1000 + i));
        }
    }
}

// Exercises full WAL record framing, not just the chunk codec, since that's what replay calls.
TEST_CASE("wal_binary::encode_decode_insert_carries_nested_payload") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    auto element_type = complex_logical_type{logical_type::BIGINT};
    auto array_type = complex_logical_type::create_array(element_type, 4);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.push_back(array_type);

    data_chunk_t chunk(&resource, types, 3);
    chunk.set_cardinality(3);
    for (uint64_t row = 0; row < 3; row++) {
        chunk.set_value(0, row, static_cast<int64_t>(row));
        std::vector<vector_t> elements;
        for (uint64_t i = 0; i < 4; i++) {
            elements.push_back(make_scalar(&resource, static_cast<int64_t>(row * 100 + i)));
        }
        put(make_array(&resource, element_type, elements), chunk.data[1], row);
    }

    buffer_t buffer(&resource);
    encode_insert(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/7,
                  /*txn_id=*/42,
                  /*sub_id=*/0,
                  kTestTableOid,
                  attoids_for(chunk),
                  to_chunk_batch(chunk),
                  /*row_start=*/0,
                  /*row_count=*/3);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.physical_data.size() == 1);

    const auto& decoded = record.physical_data[0];
    REQUIRE(decoded.column_count() == 2);
    REQUIRE(decoded.size() == 3);
    for (uint64_t row = 0; row < 3; row++) {
        INFO("row " << row);
        REQUIRE(decoded.data[0].get_value<int64_t>(row) == static_cast<int64_t>(row));
        REQUIRE(len(decoded.data[1], row) == 4);
        for (uint64_t i = 0; i < 4; i++) {
            INFO("element " << i);
            REQUIRE(elem<int64_t>(decoded.data[1], row, i) == static_cast<int64_t>(row * 100 + i));
        }
    }
}

// Sizing the row-id vector as payload_size/8 while memcpy'ing payload_size bytes overruns for
// any non-multiple-of-8 length; UPDATE's own bounds check also wraps in 32-bit arithmetic near
// UINT32_MAX. Records below carry valid CRCs on purpose: the checksum doesn't catch this.

#include <absl/crc/crc32c.h>

namespace {

    void put_le32(std::vector<char>& out, uint32_t v) {
        char b[4];
        std::memcpy(b, &v, 4);
        out.insert(out.end(), b, b + 4);
    }
    void put_le64(std::vector<char>& out, uint64_t v) {
        char b[8];
        std::memcpy(b, &v, 8);
        out.insert(out.end(), b, b + 8);
    }

    std::vector<char> craft_dml_record(wal_record_type type, const std::vector<char>& payload) {
        std::vector<char> body;
        put_le32(body, 0);
        put_le64(body, 7);
        put_le64(body, 100);
        body.push_back(static_cast<char>(type));
        put_le32(body, static_cast<uint32_t>(kTestTableOid));
        put_le64(body, 0); // sub_id
        put_le64(body, 0);
        put_le64(body, payload.size());                        // row_count (decode does not cross-check it)
        put_le32(body, static_cast<uint32_t>(payload.size())); // payload_size
        body.insert(body.end(), payload.begin(), payload.end());

        std::vector<char> record;
        put_le32(record, static_cast<uint32_t>(body.size()));
        record.insert(record.end(), body.begin(), body.end());
        const auto crc = static_cast<uint32_t>(absl::ComputeCrc32c(absl::string_view(body.data(), body.size())));
        put_le32(record, crc);
        return record;
    }

} // namespace

TEST_CASE("wal_binary::a_delete_payload_that_is_not_whole_row_ids_is_corrupt") {
    core::pmr::otterbrix_resource resource;

    std::vector<char> payload(12, '\x5a');
    auto record_bytes = craft_dml_record(wal_record_type::PHYSICAL_DELETE, payload);

    auto rec = decode_record(record_bytes.data(), record_bytes.size(), &resource);
    INFO("a PHYSICAL_DELETE payload of 12 bytes is not a row-id array; it must be corrupt");
    REQUIRE(rec.is_corrupt);
}

TEST_CASE("wal_binary::an_update_row_id_block_that_is_not_whole_row_ids_is_corrupt") {
    core::pmr::otterbrix_resource resource;

    // UPDATE payload: [row_ids_bytes = 12][12 bytes], no chunk batch behind it.
    std::vector<char> payload;
    put_le32(payload, 12);
    payload.insert(payload.end(), 12, '\x5a');
    auto record_bytes = craft_dml_record(wal_record_type::PHYSICAL_UPDATE, payload);

    auto rec = decode_record(record_bytes.data(), record_bytes.size(), &resource);
    INFO("a PHYSICAL_UPDATE row-id block of 12 bytes is not a row-id array; it must be corrupt");
    REQUIRE(rec.is_corrupt);
}

TEST_CASE("wal_binary::an_update_row_id_length_near_uint32_max_is_corrupt_not_a_giant_copy") {
    core::pmr::otterbrix_resource resource;

    std::vector<char> payload;
    put_le32(payload, 0xFFFFFFFCu);
    payload.insert(payload.end(), 12, '\x5a');
    auto record_bytes = craft_dml_record(wal_record_type::PHYSICAL_UPDATE, payload);

    auto rec = decode_record(record_bytes.data(), record_bytes.size(), &resource);
    INFO("a row-id length the payload cannot hold must be corrupt, whatever 32-bit addition says");
    REQUIRE(rec.is_corrupt);
}

TEST_CASE("wal_binary::savepoints") {
    core::pmr::otterbrix_resource resource;
    std::vector<int64_t> row_ids = {1, 2, 3};
    constexpr uint64_t txn_id = 100;

    auto delete_record = [&](services::wal::id_t wal_id, uint64_t txn, uint64_t sub_id) {
        buffer_t buffer(&resource);
        encode_delete(buffer, 0, wal_id, txn, sub_id, kTestTableOid, row_ids.data(), row_ids.size());
        return decode_record(buffer, &resource);
    };
    auto rollback_to_record = [&](services::wal::id_t wal_id, uint64_t from_sub_id) {
        buffer_t buffer(&resource);
        encode_rollback_to(buffer, 0, wal_id, txn_id, from_sub_id);
        return decode_record(buffer, &resource);
    };
    auto commit_record = [&](services::wal::id_t wal_id) {
        buffer_t buffer(&resource);
        encode_commit(buffer, 0, wal_id, txn_id, /*commit_id=*/wal_id);
        return decode_record(buffer, &resource);
    };

    SECTION("a physical record keeps its savepoint level") {
        const auto record = delete_record(1, txn_id, 3);
        REQUIRE(record.is_valid());
        REQUIRE(record.sub_id == 3);
        REQUIRE(record.physical_row_ids.size() == row_ids.size());
    }

    SECTION("a rollback to marker keeps its level") {
        const auto record = rollback_to_record(1, 2);
        REQUIRE(record.is_valid());
        REQUIRE(record.is_rollback_to_marker());
        REQUIRE(record.sub_id == 2);
    }

    SECTION("replay drops what a rollback to killed before its commit") {
        std::vector<record_t> records;
        records.push_back(delete_record(1, txn_id, 0));
        records.push_back(delete_record(2, txn_id, 2));
        records.push_back(rollback_to_record(3, 2));
        records.push_back(delete_record(4, txn_id, 3));
        records.push_back(commit_record(5));
        // The same id recycled by a later transaction: the earlier marker is outside its commit window.
        records.push_back(delete_record(6, txn_id, 2));
        records.push_back(commit_record(7));

        std::vector<services::wal::id_t> kept;
        for (const auto& record : filter_committed_records(std::move(records))) {
            kept.push_back(record.id);
        }
        REQUIRE(kept == std::vector<services::wal::id_t>{1, 4, 5, 6, 7});
    }
}
