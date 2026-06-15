#include <catch2/catch.hpp>
#include <components/tests/generaty.hpp>
#include <components/vector/data_chunk_binary.hpp>
#include <core/pmr.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_binary.hpp>

using namespace services::wal;
using namespace components::types;
using namespace components::vector;

// Fixed-size and STRING columns only.
static std::pmr::vector<components::types::complex_logical_type> wal_test_types(std::pmr::memory_resource* r) {
    using namespace components::types;
    std::pmr::vector<complex_logical_type> types(r);
    types.emplace_back(logical_type::BIGINT, "count");
    types.emplace_back(logical_type::STRING_LITERAL, "count_str");
    types.emplace_back(logical_type::DOUBLE, "count_double");
    types.emplace_back(logical_type::BOOLEAN, "count_bool");
    return types;
}

static data_chunk_t make_pax_generic_chunk(std::pmr::memory_resource* r) {
    constexpr uint64_t row_count = 4;

    std::pmr::vector<complex_logical_type> types(r);
    types.emplace_back(logical_type::BIGINT, "id");
    types.emplace_back(complex_logical_type::create_array(logical_type::UBIGINT, 3, "scores"));
    types.emplace_back(complex_logical_type::create_list(logical_type::STRING_LITERAL, "tags"));

    std::pmr::vector<complex_logical_type> struct_fields(r);
    struct_fields.emplace_back(logical_type::BOOLEAN, "flag");
    struct_fields.emplace_back(logical_type::INTEGER, "count");
    struct_fields.emplace_back(complex_logical_type::create_list(logical_type::STRING_LITERAL, "notes"));
    auto struct_type = complex_logical_type::create_struct("payload", struct_fields, "payload");
    types.emplace_back(struct_type);

    std::pmr::vector<complex_logical_type> union_fields(r);
    union_fields.emplace_back(logical_type::BOOLEAN, "as_bool");
    union_fields.emplace_back(logical_type::INTEGER, "as_int");
    union_fields.emplace_back(logical_type::STRING_LITERAL, "as_text");
    auto union_type = complex_logical_type::create_union(union_fields, "choice");
    types.emplace_back(union_type);

    std::vector<logical_value_t> enum_entries;
    {
        logical_value_t ready(r, int32_t{1});
        ready.set_alias("ready");
        enum_entries.emplace_back(std::move(ready));
        logical_value_t paused(r, int32_t{2});
        paused.set_alias("paused");
        enum_entries.emplace_back(std::move(paused));
    }
    auto enum_type = complex_logical_type::create_enum("status_t", std::move(enum_entries), "status");
    types.emplace_back(enum_type);

    data_chunk_t chunk(r, types, row_count);
    chunk.set_cardinality(row_count);

    for (uint64_t row = 0; row < row_count; ++row) {
        chunk.set_value(0, row, logical_value_t(r, static_cast<int64_t>(row + 10)));

        if (row == 2) {
            chunk.set_value(1, row, logical_value_t(r, logical_type::NA));
        } else {
            std::vector<logical_value_t> values;
            values.reserve(3);
            values.emplace_back(r, static_cast<uint64_t>(row * 10 + 1));
            values.emplace_back(r, static_cast<uint64_t>(row * 10 + 2));
            values.emplace_back(r, static_cast<uint64_t>(row * 10 + 3));
            chunk.set_value(1, row, logical_value_t::create_array(r, logical_type::UBIGINT, values));
        }

        {
            std::vector<logical_value_t> values;
            values.emplace_back(r, std::string{"tag_" + std::to_string(row)});
            if (row == 1) {
                values.emplace_back(r, logical_type::NA);
            } else {
                values.emplace_back(r, std::string{"tail_" + std::to_string(row)});
            }
            chunk.set_value(2, row, logical_value_t::create_list(r, logical_type::STRING_LITERAL, values));
        }

        if (row == 3) {
            chunk.set_value(3, row, logical_value_t(r, logical_type::NA));
        } else {
            std::vector<logical_value_t> notes;
            notes.emplace_back(r, std::string{"note_" + std::to_string(row)});
            if (row == 1) {
                notes.emplace_back(r, logical_type::NA);
            }

            std::vector<logical_value_t> fields;
            fields.emplace_back(r, row % 2 == 0);
            fields.emplace_back(r, static_cast<int32_t>(row * 100));
            fields.emplace_back(logical_value_t::create_list(r, logical_type::STRING_LITERAL, notes));
            chunk.set_value(3, row, logical_value_t::create_struct(r, struct_type, fields));
        }

        switch (row) {
            case 0:
                chunk.set_value(4,
                                row,
                                logical_value_t::create_union(r,
                                                              union_fields,
                                                              0,
                                                              logical_value_t(r, true)));
                break;
            case 1:
                chunk.set_value(4,
                                row,
                                logical_value_t::create_union(r,
                                                              union_fields,
                                                              1,
                                                              logical_value_t(r, int32_t{42})));
                break;
            case 2:
                chunk.set_value(4,
                                row,
                                logical_value_t::create_union(r,
                                                              union_fields,
                                                              2,
                                                              logical_value_t(r, std::string{"union_text"})));
                break;
            default:
                chunk.set_value(4, row, logical_value_t(r, logical_type::NA));
                break;
        }

        chunk.set_value(5, row, logical_value_t::create_enum(r, enum_type, row % 2 == 0 ? 1 : 2));
    }

    return chunk;
}

// WAL records carry table_oid (4 bytes) instead of (database, collection)
// strings. Tests pass arbitrary oids to verify the round-trip; production code uses
// the actual catalog OIDs.
constexpr components::catalog::oid_t kTestTableOid = 16500;

TEST_CASE("wal_binary::encode_decode_insert") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto chunk = gen_data_chunk(10, 0, wal_test_types(&resource), &resource);

    buffer_t buffer(&resource);
    encode_insert(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/1,
                  /*txn_id=*/100,
                  kTestTableOid,
                  chunk,
                  /*row_start=*/0,
                  /*row_count=*/10);

    REQUIRE(buffer.size() > 0);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.id == 1);
    REQUIRE(record.transaction_id == 100);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_INSERT);
    REQUIRE(record.table_oid == kTestTableOid);
    REQUIRE(record.physical_row_start == 0);
    REQUIRE(record.physical_row_count == 10);
    REQUIRE(record.physical_data != nullptr);
    REQUIRE(record.physical_data->column_count() == chunk.column_count());
    REQUIRE(record.physical_data->size() == chunk.size());

    for (uint64_t col = 0; col < chunk.column_count(); col++) {
        for (uint64_t row = 0; row < chunk.size(); row++) {
            REQUIRE(record.physical_data->value(col, row) == chunk.value(col, row));
        }
    }
}

TEST_CASE("wal_binary::encode_decode_insert_pax_generic_nested") {
    std::pmr::monotonic_buffer_resource resource(1024 * 128);
    auto chunk = make_pax_generic_chunk(&resource);

    buffer_t buffer(&resource);
    encode_insert(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/10,
                  /*txn_id=*/110,
                  kTestTableOid,
                  chunk,
                  /*row_start=*/0,
                  /*row_count=*/chunk.size());

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_INSERT);
    REQUIRE(record.physical_data != nullptr);
    REQUIRE(record.physical_data->column_count() == chunk.column_count());
    REQUIRE(record.physical_data->size() == chunk.size());

    for (uint64_t col = 0; col < chunk.column_count(); col++) {
        REQUIRE(record.physical_data->data[col].type() == chunk.data[col].type());
        for (uint64_t row = 0; row < chunk.size(); row++) {
            REQUIRE(record.physical_data->value(col, row) == chunk.value(col, row));
        }
    }
}

TEST_CASE("wal_binary::encode_decode_delete") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    std::vector<int64_t> row_ids = {1, 3, 5, 7, 9};

    buffer_t buffer(&resource);
    encode_delete(buffer, /*last_crc32=*/0, /*wal_id=*/2, /*txn_id=*/101, kTestTableOid, row_ids.data(), /*count=*/5);

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
    encode_update(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/3,
                  /*txn_id=*/102,
                  kTestTableOid,
                  row_ids.data(),
                  new_data,
                  /*count=*/5);

    REQUIRE(buffer.size() > 0);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.id == 3);
    REQUIRE(record.transaction_id == 102);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_UPDATE);
    REQUIRE(record.table_oid == kTestTableOid);
    REQUIRE(record.physical_row_ids.size() == 5);

    for (size_t i = 0; i < row_ids.size(); i++) {
        REQUIRE(record.physical_row_ids[i] == row_ids[i]);
    }

    REQUIRE(record.physical_data != nullptr);
    REQUIRE(record.physical_data->column_count() == new_data.column_count());
    REQUIRE(record.physical_data->size() == new_data.size());

    for (uint64_t col = 0; col < new_data.column_count(); col++) {
        for (uint64_t row = 0; row < new_data.size(); row++) {
            REQUIRE(record.physical_data->value(col, row) == new_data.value(col, row));
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
                  kTestTableOid,
                  chunk,
                  /*row_start=*/0,
                  /*row_count=*/10);

    REQUIRE(buffer.size() > 29);

    // Flip a byte in the payload area (somewhere in the middle of the record)
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
                  kTestTableOid,
                  chunk,
                  /*row_start=*/0,
                  /*row_count=*/10);

    // Truncate to half size
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
            REQUIRE(result.value(col, row) == chunk.value(col, row));
        }
    }
}

TEST_CASE("wal_binary::data_chunk_binary_with_nulls") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(logical_type::BIGINT, "id");
    types.emplace_back(logical_type::DOUBLE, "value");

    auto chunk = gen_data_chunk(10, 0, types, &resource);

    // Set some values to null by invalidating rows in the validity mask
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

    // Verify null mask is preserved
    for (uint64_t col = 0; col < result.column_count(); col++) {
        REQUIRE_FALSE(result.data[col].validity().row_is_valid(1));
        REQUIRE_FALSE(result.data[col].validity().row_is_valid(4));
        REQUIRE_FALSE(result.data[col].validity().row_is_valid(7));

        // Non-null rows should remain valid
        REQUIRE(result.data[col].validity().row_is_valid(0));
        REQUIRE(result.data[col].validity().row_is_valid(2));
        REQUIRE(result.data[col].validity().row_is_valid(3));
        REQUIRE(result.data[col].validity().row_is_valid(5));
        REQUIRE(result.data[col].validity().row_is_valid(6));
        REQUIRE(result.data[col].validity().row_is_valid(8));
        REQUIRE(result.data[col].validity().row_is_valid(9));
    }
}

TEST_CASE("wal_binary::data_chunk_binary_pax_generic_nested_types") {
    std::pmr::monotonic_buffer_resource resource(1024 * 128);
    auto chunk = make_pax_generic_chunk(&resource);

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);

    REQUIRE(buffer.size() > 0);

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);

    REQUIRE(ok);
    REQUIRE(result.column_count() == chunk.column_count());
    REQUIRE(result.size() == chunk.size());

    for (uint64_t col = 0; col < chunk.column_count(); col++) {
        REQUIRE(result.data[col].type() == chunk.data[col].type());
        for (uint64_t row = 0; row < chunk.size(); row++) {
            REQUIRE(result.value(col, row) == chunk.value(col, row));
        }
    }
}
