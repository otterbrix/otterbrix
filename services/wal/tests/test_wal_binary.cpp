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

// The flat column set the framing cases below use: gen_data_chunk fills fixed-size and STRING
// columns, so these types keep the record-level cases about the RECORD rather than about the
// column codec. Nested columns are covered by the payload cases at the end of this file.
static std::pmr::vector<components::types::complex_logical_type> wal_test_types(std::pmr::memory_resource* r) {
    using namespace components::types;
    std::pmr::vector<complex_logical_type> types(r);
    types.emplace_back(logical_type::BIGINT, "count");
    types.emplace_back(logical_type::STRING_LITERAL, "count_str");
    types.emplace_back(logical_type::DOUBLE, "count_double");
    types.emplace_back(logical_type::BOOLEAN, "count_bool");
    return types;
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

// The WAL chunk codec's hand-rolled type header had no STRUCT leg: the writer emitted
// "no extension" and the reader rebuilt a bare STRUCT type; constructing the decoded
// chunk then walked the missing struct extension's field types through a garbage
// pointer — a FLAKY SIGSEGV at STARTUP (manager_wal_replicate's read_all_records) for
// ANY log containing an insert into a table with a struct column. The header now
// carries the canonical type spec (types::encode_type_spec / decode_type_spec), which
// round-trips every persistable type exactly — struct fields, decimal width/scale,
// nested children and aliases — so a new type can never again decode into a
// crash-shaped half-type. This case gates the TYPE and the null mask on their own; the
// nested column PAYLOAD has its own cases further down.
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
        chunk.set_value(0, row, logical_value_t{&resource, static_cast<int64_t>(row + 1)});
        if (row == 1) {
            chunk.set_value(1, row, logical_value_t{&resource, nullptr}); // whole-struct NULL
        } else {
            std::vector<logical_value_t> members;
            members.emplace_back(&resource, static_cast<int64_t>(row * 10));
            members.emplace_back(&resource, static_cast<int64_t>(row * 20));
            chunk.set_value(1, row, logical_value_t::create_struct(&resource, pair_type, members));
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

    // The struct TYPE must round-trip completely: STRUCT tag, field count, field types.
    const auto& decoded_type = result.data[1].type();
    REQUIRE(decoded_type.type() == logical_type::STRUCT);
    REQUIRE(decoded_type.child_types().size() == 2);
    REQUIRE(decoded_type.child_types()[0].type() == logical_type::BIGINT);
    REQUIRE(decoded_type.child_types()[1].type() == logical_type::BIGINT);

    // The null mask covers every column, struct included: the whole-struct NULL survives.
    REQUIRE(result.data[1].validity().row_is_valid(0));
    REQUIRE_FALSE(result.data[1].validity().row_is_valid(1));
    REQUIRE(result.data[1].validity().row_is_valid(2));

    // The scalar column's data still round-trips alongside the struct column.
    for (uint64_t row = 0; row < 3; row++) {
        REQUIRE(result.value(0, row).value<int64_t>() == static_cast<int64_t>(row + 1));
    }
}

// ---------------------------------------------------------------------------
// NESTED COLUMN PAYLOAD.
//
// The codec sized every column through fixed_type_size(), which answers 0 for LIST, STRUCT and
// ARRAY. Writer and reader agreed on that zero — `data_size = 0`, no bytes written, no bytes
// read — so a replayed insert rebuilt a correctly-SHAPED nested column in which every element
// was the zero the constructor left behind. Nothing reported a failure; the type round-tripped,
// the top-level null mask round-tripped, and only the CONTENT was gone.
//
// The payload is now written recursively, in the checkpoint's own child order —
// [validity, ...children] — so the cases below gate the CELL CONTENTS of each of the three
// nested shapes, plus the interior validity that rides with them.
// ---------------------------------------------------------------------------

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
        chunk.set_value(0, row, logical_value_t{&resource, static_cast<int64_t>(row + 1)});
    }
    // row 0/1/3 carry values; row 2 is a whole-cell NULL; row 3 holds an interior NULL.
    for (uint64_t row : {uint64_t{0}, uint64_t{1}}) {
        std::vector<logical_value_t> elements;
        for (uint64_t i = 0; i < 3; i++) {
            elements.emplace_back(&resource, static_cast<int64_t>(row * 10 + i));
        }
        chunk.set_value(1, row, logical_value_t::create_array(&resource, element_type, elements));
    }
    chunk.set_value(1, 2, logical_value_t{&resource, nullptr});
    {
        std::vector<logical_value_t> elements;
        elements.emplace_back(&resource, static_cast<int64_t>(70));
        elements.emplace_back(&resource, nullptr); // NULL element
        elements.emplace_back(&resource, static_cast<int64_t>(90));
        chunk.set_value(1, 3, logical_value_t::create_array(&resource, element_type, elements));
    }

    // Precondition, asserted rather than assumed: the chunk going IN carries the interior
    // NULL. Without this the round-trip claim below could pass for the wrong reason.
    REQUIRE(chunk.value(1, 3).children()[1].is_null());

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
        REQUIRE(result.value(0, row).value<int64_t>() == static_cast<int64_t>(row + 1));
    }

    for (uint64_t row : {uint64_t{0}, uint64_t{1}}) {
        INFO("array row " << row);
        auto cell = result.value(1, row);
        REQUIRE(cell.children().size() == 3);
        for (uint64_t i = 0; i < 3; i++) {
            INFO("element " << i);
            REQUIRE(cell.children()[i].value<int64_t>() == static_cast<int64_t>(row * 10 + i));
        }
    }

    INFO("the whole-cell NULL is still NULL");
    REQUIRE_FALSE(result.data[1].validity().row_is_valid(2));

    INFO("the interior NULL survives beside its present neighbours");
    {
        auto cell = result.value(1, 3);
        REQUIRE(cell.children().size() == 3);
        REQUIRE(cell.children()[0].value<int64_t>() == 70);
        REQUIRE(cell.children()[1].is_null());
        REQUIRE(cell.children()[2].value<int64_t>() == 90);
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
        chunk.set_value(0, row, logical_value_t{&resource, static_cast<int64_t>(row + 1)});
    }

    // Deliberately RAGGED, and longer than the row count: a reader that sizes the child from
    // the number of rows instead of from the written span comes up short on row 2.
    const std::vector<std::vector<int64_t>> rows = {{10, 20}, {}, {30, 40, 50, 60, 70}, {80}};
    for (uint64_t row = 0; row < rows.size(); row++) {
        std::vector<logical_value_t> elements;
        for (auto value : rows[row]) {
            elements.emplace_back(&resource, value);
        }
        chunk.set_value(1, row, logical_value_t::create_list(&resource, element_type, elements));
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
        auto cell = result.value(1, row);
        REQUIRE(cell.children().size() == rows[row].size());
        for (uint64_t i = 0; i < rows[row].size(); i++) {
            INFO("element " << i);
            REQUIRE(cell.children()[i].value<int64_t>() == rows[row][i]);
        }
    }
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
        chunk.set_value(0, row, logical_value_t{&resource, static_cast<int64_t>(row + 1)});
    }
    {
        std::vector<logical_value_t> members;
        members.emplace_back(&resource, static_cast<int64_t>(11));
        members.emplace_back(&resource, static_cast<int64_t>(12));
        chunk.set_value(1, 0, logical_value_t::create_struct(&resource, pair_type, members));
    }
    chunk.set_value(1, 1, logical_value_t{&resource, nullptr}); // whole-struct NULL
    {
        std::vector<logical_value_t> members;
        members.emplace_back(&resource, static_cast<int64_t>(31));
        members.emplace_back(&resource, nullptr); // NULL field
        chunk.set_value(1, 2, logical_value_t::create_struct(&resource, pair_type, members));
    }

    // Precondition, asserted rather than assumed: the chunk going IN carries the NULL field.
    // Read off the FIELD vector, not through data_chunk_t::value(): reconstructing a whole
    // struct value derives its type from the field VALUES, so a NULL field yields a struct
    // typed <BIGINT, NA> and trips vector_t::value()'s type-identity assert. That is a
    // pre-existing limitation of the struct read-back and has nothing to do with the codec —
    // going through the field vector keeps this case about the payload.
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
        auto cell = result.value(1, 0);
        REQUIRE(cell.children().size() == 2);
        REQUIRE(cell.children()[0].value<int64_t>() == 11);
        REQUIRE(cell.children()[1].value<int64_t>() == 12);
    }

    INFO("the whole-struct NULL is still NULL");
    REQUIRE_FALSE(result.data[1].validity().row_is_valid(1));

    INFO("a present field beside a NULL one");
    {
        REQUIRE(result.data[1].entries().size() == 2);
        REQUIRE(result.data[1].entries()[0]->value(2).value<int64_t>() == 31);
        REQUIRE_FALSE(result.data[1].entries()[1]->validity().row_is_valid(2));
    }
}

// SECOND-LEVEL NESTING. The payload codec descends by physical type rather than enumerating the
// shapes it knows, so a container inside a container is the same code path taken twice. This is
// the case that would fail on any "handle LIST, ARRAY and STRUCT" fix written as three flat
// legs: a list of structs and a struct holding a list each need the recursion to re-enter.
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
        chunk.set_value(0, row, logical_value_t{&resource, static_cast<int64_t>(row + 1)});

        // list-of-structs: row 0 gets two members, row 1 gets one.
        std::vector<logical_value_t> members;
        for (uint64_t i = 0; i < 2 - row; i++) {
            std::vector<logical_value_t> pair;
            pair.emplace_back(&resource, static_cast<int64_t>(row * 100 + i));
            pair.emplace_back(&resource, std::string("s") + std::to_string(row * 100 + i));
            members.push_back(logical_value_t::create_struct(&resource, pair_type, pair));
        }
        chunk.set_value(1, row, logical_value_t::create_list(&resource, pair_type, members));

        // struct-of-list: the inner list length varies with the row.
        std::vector<logical_value_t> inner;
        for (uint64_t i = 0; i <= row; i++) {
            inner.emplace_back(&resource, static_cast<int64_t>(row * 1000 + i));
        }
        std::vector<logical_value_t> holder;
        holder.push_back(
            logical_value_t::create_list(&resource, complex_logical_type{logical_type::BIGINT}, inner));
        chunk.set_value(2, row, logical_value_t::create_struct(&resource, struct_of_list, holder));
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
        REQUIRE(result.value(0, row).value<int64_t>() == static_cast<int64_t>(row + 1));

        INFO("list of structs");
        auto list_cell = result.value(1, row);
        REQUIRE(list_cell.children().size() == 2 - row);
        for (uint64_t i = 0; i < 2 - row; i++) {
            INFO("member " << i);
            const auto& member = list_cell.children()[i];
            REQUIRE(member.children().size() == 2);
            REQUIRE(member.children()[0].value<int64_t>() == static_cast<int64_t>(row * 100 + i));
            REQUIRE(member.children()[1].value<std::string_view>() ==
                    std::string_view(std::string("s") + std::to_string(row * 100 + i)));
        }

        INFO("struct holding a list");
        auto holder_cell = result.value(2, row);
        REQUIRE(holder_cell.children().size() == 1);
        const auto& inner_list = holder_cell.children()[0];
        REQUIRE(inner_list.children().size() == row + 1);
        for (uint64_t i = 0; i <= row; i++) {
            INFO("inner element " << i);
            REQUIRE(inner_list.children()[i].value<int64_t>() == static_cast<int64_t>(row * 1000 + i));
        }
    }
}

// The full WAL record framing, not just the bare chunk codec: an INSERT carrying a nested
// column has to come back out of decode_record with its payload, because that is the call
// replay actually makes.
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
        chunk.set_value(0, row, logical_value_t{&resource, static_cast<int64_t>(row)});
        std::vector<logical_value_t> elements;
        for (uint64_t i = 0; i < 4; i++) {
            elements.emplace_back(&resource, static_cast<int64_t>(row * 100 + i));
        }
        chunk.set_value(1, row, logical_value_t::create_array(&resource, element_type, elements));
    }

    buffer_t buffer(&resource);
    encode_insert(buffer,
                  &resource,
                  /*last_crc32=*/0,
                  /*wal_id=*/7,
                  /*txn_id=*/42,
                  kTestTableOid,
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
        REQUIRE(decoded.value(0, row).value<int64_t>() == static_cast<int64_t>(row));
        auto cell = decoded.value(1, row);
        REQUIRE(cell.children().size() == 4);
        for (uint64_t i = 0; i < 4; i++) {
            INFO("element " << i);
            REQUIRE(cell.children()[i].value<int64_t>() == static_cast<int64_t>(row * 100 + i));
        }
    }
}

// ===========================================================================
// ROW-ID LENGTHS THAT ARE NOT WHOLE ROW IDS ARE CORRUPTION, NOT ARITHMETIC.
//
// Sizing the row-id vector as payload_size / 8 and then memcpy'ing payload_size BYTES into it
// writes, for any length that is not a multiple of 8, up to 7 bytes past the heap allocation,
// silently, on both the PHYSICAL_DELETE payload and the row-id half of PHYSICAL_UPDATE. The
// UPDATE bounds check `4 + row_ids_bytes > payload_size` additionally wraps in 32-bit
// arithmetic, so a row_ids_bytes near UINT32_MAX slips past the check and drives a
// multi-gigabyte resize+memcpy from a 16-byte buffer.
//
// The records below are byte-crafted with VALID CRCs: the checksum is precisely the guard
// that does NOT protect against these lengths, because a legitimately-CRC'd record with a
// ragged length is exactly what a flipped length byte upstream of the CRC computation — or a
// crafted journal — produces. A ragged length must come back is_corrupt.
// ===========================================================================

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

    // Craft a whole DML record with a correct trailing CRC over the body.
    std::vector<char> craft_dml_record(wal_record_type type, const std::vector<char>& payload) {
        std::vector<char> body;
        put_le32(body, 0);                     // last_crc32
        put_le64(body, 7);                     // wal_id
        put_le64(body, 100);                   // txn_id
        body.push_back(static_cast<char>(type));
        put_le32(body, static_cast<uint32_t>(kTestTableOid)); // table_oid
        put_le64(body, 0);                     // row_start
        put_le64(body, payload.size());        // row_count (decode does not cross-check it)
        put_le32(body, static_cast<uint32_t>(payload.size())); // payload_size
        body.insert(body.end(), payload.begin(), payload.end());

        std::vector<char> record;
        put_le32(record, static_cast<uint32_t>(body.size())); // size field
        record.insert(record.end(), body.begin(), body.end());
        const auto crc = static_cast<uint32_t>(absl::ComputeCrc32c(absl::string_view(body.data(), body.size())));
        put_le32(record, crc);
        return record;
    }

} // namespace

TEST_CASE("wal_binary::a_delete_payload_that_is_not_whole_row_ids_is_corrupt") {
    core::pmr::otterbrix_resource resource;

    // 12 bytes: one and a half row ids. count = 12/8 = 1 allocates 8 bytes, so a memcpy of 12
    // bytes into it overruns the allocation by 4.
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

    // row_ids_bytes = 0xFFFFFFFC: `4 + row_ids_bytes` wraps to 0 in 32-bit arithmetic, so the
    // old bounds check passed and the decoder resized to half a billion row ids and memcpy'd
    // 4 GiB from a 16-byte buffer.
    std::vector<char> payload;
    put_le32(payload, 0xFFFFFFFCu);
    payload.insert(payload.end(), 12, '\x5a');
    auto record_bytes = craft_dml_record(wal_record_type::PHYSICAL_UPDATE, payload);

    auto rec = decode_record(record_bytes.data(), record_bytes.size(), &resource);
    INFO("a row-id length the payload cannot hold must be corrupt, whatever 32-bit addition says");
    REQUIRE(rec.is_corrupt);
}
