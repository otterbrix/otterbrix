#include <catch2/catch.hpp>
#include <core/pmr.hpp>
#include <services/wal/wal_binary.hpp>
#include <components/vector/data_chunk_binary.hpp>
#include <components/tests/generaty.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

using namespace services::wal;
using namespace components::types;
using namespace components::vector;

// WAL binary serialization supports fixed-size and STRING types.
// ARRAY/LIST not yet supported in binary format — use explicit types.
static std::pmr::vector<components::types::complex_logical_type>
wal_test_types(std::pmr::memory_resource* r) {
    using namespace components::types;
    std::pmr::vector<complex_logical_type> types(r);
    types.emplace_back(logical_type::BIGINT, "count");
    types.emplace_back(logical_type::STRING_LITERAL, "count_str");
    types.emplace_back(logical_type::DOUBLE, "count_double");
    types.emplace_back(logical_type::BOOLEAN, "count_bool");
    return types;
}

TEST_CASE("wal_binary::encode_decode_insert") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto chunk = gen_data_chunk(10, 0, wal_test_types(&resource), &resource);

    buffer_t buffer(&resource);
    encode_insert(buffer, &resource, /*last_crc32=*/0, /*wal_id=*/1, /*txn_id=*/100,
                  "testdb", "users", chunk, /*row_start=*/0, /*row_count=*/10);

    REQUIRE(buffer.size() > 0);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.id == 1);
    REQUIRE(record.transaction_id == 100);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_INSERT);
    REQUIRE(record.collection_name.database == "testdb");
    REQUIRE(record.collection_name.collection == "users");
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

TEST_CASE("wal_binary::encode_decode_delete") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    std::vector<int64_t> row_ids = {1, 3, 5, 7, 9};

    buffer_t buffer(&resource);
    encode_delete(buffer, /*last_crc32=*/0, /*wal_id=*/2, /*txn_id=*/101,
                  "testdb", "users", row_ids.data(), /*count=*/5);

    REQUIRE(buffer.size() > 0);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.id == 2);
    REQUIRE(record.transaction_id == 101);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_DELETE);
    REQUIRE(record.collection_name.database == "testdb");
    REQUIRE(record.collection_name.collection == "users");
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
    encode_update(buffer, &resource, /*last_crc32=*/0, /*wal_id=*/3, /*txn_id=*/102,
                  "testdb", "users", row_ids.data(), new_data, /*count=*/5);

    REQUIRE(buffer.size() > 0);

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_valid());
    REQUIRE_FALSE(record.is_corrupt);
    REQUIRE(record.id == 3);
    REQUIRE(record.transaction_id == 102);
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_UPDATE);
    REQUIRE(record.collection_name.database == "testdb");
    REQUIRE(record.collection_name.collection == "users");
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
    encode_commit(buffer, /*last_crc32=*/0, /*wal_id=*/4, /*txn_id=*/103);

    REQUIRE(buffer.size() == 29);

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
    encode_insert(buffer, &resource, /*last_crc32=*/0, /*wal_id=*/1, /*txn_id=*/100,
                  "testdb", "users", chunk, /*row_start=*/0, /*row_count=*/10);

    REQUIRE(buffer.size() > 29);

    // Flip a byte in the payload area (somewhere in the middle of the record)
    size_t flip_pos = buffer.size() / 2;
    buffer[flip_pos] ^= 0xFF;

    auto record = decode_record(buffer, &resource);
    REQUIRE(record.is_corrupt);
}

TEST_CASE("wal_binary::truncated_input") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto chunk = gen_data_chunk(10, &resource);

    buffer_t buffer(&resource);
    encode_insert(buffer, &resource, /*last_crc32=*/0, /*wal_id=*/1, /*txn_id=*/100,
                  "testdb", "users", chunk, /*row_start=*/0, /*row_count=*/10);

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

    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource);

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

    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource);

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
