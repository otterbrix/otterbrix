#include <catch2/catch_test_macros.hpp>
#include <components/tests/generaty.hpp>
#include <components/vector/data_chunk_binary.hpp>
#include <core/pmr.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_binary.hpp>

using namespace services::wal;
using namespace components::types;
using namespace components::vector;

// encode_insert/encode_update now take a chunk batch; wrap a single chunk (deep copy).
static std::pmr::vector<components::vector::data_chunk_t>
to_chunk_batch(const components::vector::data_chunk_t& chunk) {
    std::pmr::vector<components::vector::data_chunk_t> batch(chunk.resource());
    components::vector::data_chunk_t copy(chunk.resource(), chunk.types(), chunk.size() == 0 ? 1 : chunk.size());
    chunk.copy(copy, 0);
    batch.emplace_back(std::move(copy));
    return batch;
}

// WAL binary serialization supports fixed-size and STRING types.
// ARRAY/LIST not yet supported in binary format — use explicit types.
static components::vector::schema_t wal_test_schema(std::pmr::memory_resource* r) {
    using namespace components::types;
    components::vector::schema_t schema(r);
    schema.push_back(gen_column(r, "count", complex_logical_type{logical_type::BIGINT}));
    schema.push_back(gen_column(r, "count_str", complex_logical_type{logical_type::STRING_LITERAL}));
    schema.push_back(gen_column(r, "count_double", complex_logical_type{logical_type::DOUBLE}));
    schema.push_back(gen_column(r, "count_bool", complex_logical_type{logical_type::BOOLEAN}));
    return schema;
}

// WAL records carry table_oid (4 bytes) instead of (database, collection)
// strings. Tests pass arbitrary oids to verify the round-trip; production code uses
// the actual catalog OIDs.
constexpr components::catalog::oid_t kTestTableOid = 16500;

TEST_CASE("wal_binary::encode_decode_insert") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);
    auto chunk = gen_data_chunk(10, 0, wal_test_schema(&resource), &resource);

    buffer_t buffer(&resource);
    encode_insert(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/1,
                  /*txn_id=*/100,
                  kTestTableOid,
                  to_chunk_batch(chunk),
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
    REQUIRE(!record.physical_data.empty());
    REQUIRE(record.physical_data.front().column_count() == chunk.column_count());
    REQUIRE(record.physical_data.front().size() == chunk.size());

    for (uint64_t col = 0; col < chunk.column_count(); col++) {
        for (uint64_t row = 0; row < chunk.size(); row++) {
            REQUIRE(record.physical_data.front().value(col, row) == chunk.value(col, row));
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
    auto new_data = gen_data_chunk(5, 0, wal_test_schema(&resource), &resource);
    std::vector<int64_t> row_ids = {0, 2, 4, 6, 8};

    buffer_t buffer(&resource);
    encode_update(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/3,
                  /*txn_id=*/102,
                  kTestTableOid,
                  row_ids.data(),
                  to_chunk_batch(new_data),
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

    REQUIRE(!record.physical_data.empty());
    REQUIRE(record.physical_data.front().column_count() == new_data.column_count());
    REQUIRE(record.physical_data.front().size() == new_data.size());

    for (uint64_t col = 0; col < new_data.column_count(); col++) {
        for (uint64_t row = 0; row < new_data.size(); row++) {
            REQUIRE(record.physical_data.front().value(col, row) == new_data.value(col, row));
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
                  to_chunk_batch(chunk),
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
                  to_chunk_batch(chunk),
                  /*row_start=*/0,
                  /*row_count=*/10);

    // Truncate to half size
    buffer_t truncated(buffer.data(), buffer.size() / 2, &resource);

    auto record = decode_record(truncated, &resource);
    REQUIRE((record.is_corrupt || record.size == 0));
}

TEST_CASE("wal_binary::data_chunk_binary_mixed_types") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    components::vector::schema_t schema(&resource);
    schema.push_back(gen_column(&resource, "id", complex_logical_type{logical_type::BIGINT}));
    schema.push_back(gen_column(&resource, "score", complex_logical_type{logical_type::DOUBLE}));
    schema.push_back(gen_column(&resource, "name", complex_logical_type{logical_type::STRING_LITERAL}));
    schema.push_back(gen_column(&resource, "active", complex_logical_type{logical_type::BOOLEAN}));

    auto chunk = gen_data_chunk(8, 0, schema, &resource);

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

// M3-B2 characterization: the COLUMN NAMES the WAL codec puts on disk.
//
// data_chunk_binary's per-column type header is [logical_type][name_length][name]...
// (data_chunk_binary.cpp), so a column's name is part of the on-disk format and survives a
// restart. The codec has no version field, which is exactly why M3 does not put attoid in
// it — so the bytes pinned here must not move. Both halves of the codec read and write the
// name through the chunk's schema record; M3-B5 moved where that record's name is STORED
// (onto the column), and this pin is what says the bytes did not move with it.
TEST_CASE("wal_binary::data_chunk_binary_round_trips_column_names") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    components::vector::schema_t columns(&resource);
    const char* const names[] = {"id", "label", /*unnamed: written as name_length 0*/ "", "active"};
    const logical_type column_types[] = {logical_type::BIGINT,
                                         logical_type::STRING_LITERAL,
                                         logical_type::DOUBLE,
                                         logical_type::BOOLEAN};
    for (size_t col = 0; col < 4; col++) {
        components::vector::column_schema_t record{&resource};
        record.name = names[col];
        record.type = complex_logical_type{column_types[col]};
        columns.push_back(std::move(record));
    }

    auto chunk = components::vector::make_chunk(&resource, columns, 4);
    chunk.set_cardinality(4);
    for (uint64_t row = 0; row < 4; row++) {
        chunk.set_value(0, row, static_cast<int64_t>(row));
        chunk.set_value(1, row, std::string_view{"x"});
        chunk.set_value(2, row, static_cast<double>(row));
        chunk.set_value(3, row, row % 2 == 0);
    }

    const std::vector<std::string> expected{"id", "label", "", "active"};
    std::vector<std::string> before;
    for (const auto& record : chunk.schema()) {
        before.emplace_back(record.name);
    }
    REQUIRE(before == expected);

    buffer_t buffer(&resource);
    serialize_binary(chunk, buffer);
    const size_t encoded_size = buffer.size();

    bool ok = false;
    auto result = deserialize_binary(buffer.data(), buffer.size(), &resource, ok);
    REQUIRE(ok);
    REQUIRE(result.column_count() == chunk.column_count());

    std::vector<std::string> after;
    for (const auto& record : result.schema()) {
        after.emplace_back(record.name);
    }
    REQUIRE(after == expected);

    // Re-encoding the decoded chunk must produce the same number of bytes: the name is part
    // of the payload, so a lost or added name would change the length.
    buffer_t reencoded(&resource);
    serialize_binary(result, reencoded);
    REQUIRE(reencoded.size() == encoded_size);
}

TEST_CASE("wal_binary::data_chunk_binary_with_nulls") {
    std::pmr::monotonic_buffer_resource resource(1024 * 64);

    components::vector::schema_t schema(&resource);
    schema.push_back(gen_column(&resource, "id", complex_logical_type{logical_type::BIGINT}));
    schema.push_back(gen_column(&resource, "value", complex_logical_type{logical_type::DOUBLE}));

    auto chunk = gen_data_chunk(10, 0, schema, &resource);

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
