// record_t used to pin both pmr vectors with `{std::pmr::get_default_resource()}` default
// member initialisers. decode_record() then default-constructed a record_t and ASSIGNED the
// decoded payload in — but pmr move-assignment does NOT take the source's allocator
// (propagate_on_container_move_assignment is false), so every replayed WAL record was allocated
// on the process-global arena regardless of the resource the caller handed decode_record.
//
// Two probes, because one alone can lie: get_allocator().resource() names the arena the vectors
// report, while a counting resource installed AS the process default counts what decoding
// actually took from the global arena. A correct decode answers "&arena" and "0".

#include <catch2/catch_test_macros.hpp>

#include <components/tests/generaty.hpp>
#include <core/counting_resource.hpp>
#include <core/pmr.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_binary.hpp>

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <vector>

using namespace services::wal;

namespace {

    using core::pmr::default_resource_window_t;
    using core::pmr::process_default_probe;

    std::pmr::vector<components::types::complex_logical_type> residency_types(std::pmr::memory_resource* r) {
        using namespace components::types;
        std::pmr::vector<complex_logical_type> types(r);
        types.emplace_back(logical_type::BIGINT, "count");
        types.emplace_back(logical_type::STRING_LITERAL, "count_str");
        return types;
    }

    std::pmr::vector<components::vector::data_chunk_t> one_chunk_batch(const components::vector::data_chunk_t& chunk) {
        std::pmr::vector<components::vector::data_chunk_t> batch(chunk.resource());
        components::vector::data_chunk_t copy(chunk.resource(), chunk.types(), chunk.size());
        chunk.copy(copy, 0);
        batch.emplace_back(std::move(copy));
        return batch;
    }

    constexpr components::catalog::oid_t kResidencyTableOid = 16700;

} // namespace

TEST_CASE("services::wal::decode_record::an INSERT payload lives on the arena the caller named") {
    core::pmr::otterbrix_resource arena;

    auto chunk = gen_data_chunk(8, 0, residency_types(&arena), &arena);
    buffer_t buffer(&arena);
    encode_insert(buffer,
                  &arena,
                  /*last_crc32=*/0,
                  /*wal_id=*/1,
                  /*txn_id=*/100,
                  kResidencyTableOid,
                  one_chunk_batch(chunk),
                  /*row_start=*/0,
                  /*row_count=*/8);
    REQUIRE(buffer.size() > 0);

    auto& probe = process_default_probe();
    probe.reset();

    std::optional<record_t> record;
    {
        default_resource_window_t window{&probe};
        record.emplace(decode_record(buffer, &arena));
    }

    REQUIRE(record.has_value());
    REQUIRE(record->is_valid());
    REQUIRE(record->record_type == wal_record_type::PHYSICAL_INSERT);
    REQUIRE(record->physical_data.size() == 1);
    CHECK(record->physical_data.front().size() == 8);

    CHECK(record->physical_data.get_allocator().resource() == &arena);
    CHECK(record->physical_row_ids.get_allocator().resource() == &arena);

    INFO("allocations taken from the process-global default resource while decoding an INSERT: "
         << probe.allocations() << " (" << probe.allocated_bytes() << " bytes)");
    CHECK(probe.allocations() == 0);
}

TEST_CASE("services::wal::decode_record::a DELETE row-id list lives on the arena the caller named") {
    core::pmr::otterbrix_resource arena;

    const std::vector<int64_t> row_ids = {1, 3, 5, 7, 9, 11, 13, 15};
    buffer_t buffer(&arena);
    encode_delete(buffer,
                  /*last_crc32=*/0,
                  /*wal_id=*/2,
                  /*txn_id=*/101,
                  kResidencyTableOid,
                  row_ids.data(),
                  row_ids.size());
    REQUIRE(buffer.size() > 0);

    auto& probe = process_default_probe();
    probe.reset();

    std::optional<record_t> record;
    {
        default_resource_window_t window{&probe};
        record.emplace(decode_record(buffer, &arena));
    }

    REQUIRE(record.has_value());
    REQUIRE(record->is_valid());
    REQUIRE(record->record_type == wal_record_type::PHYSICAL_DELETE);
    REQUIRE(record->physical_row_ids.size() == row_ids.size());
    for (size_t i = 0; i < row_ids.size(); i++) {
        CHECK(record->physical_row_ids[i] == row_ids[i]);
    }

    CHECK(record->physical_row_ids.get_allocator().resource() == &arena);
    CHECK(record->physical_data.get_allocator().resource() == &arena);

    INFO("allocations taken from the process-global default resource while decoding a DELETE: "
         << probe.allocations() << " (" << probe.allocated_bytes() << " bytes)");
    CHECK(probe.allocations() == 0);
}

TEST_CASE("services::wal::decode_record::an UPDATE carries both payloads on the caller's arena") {
    core::pmr::otterbrix_resource arena;

    auto new_data = gen_data_chunk(4, 0, residency_types(&arena), &arena);
    const std::vector<int64_t> row_ids = {0, 2, 4, 6};
    buffer_t buffer(&arena);
    encode_update(buffer,
                  &arena,
                  /*last_crc32=*/0,
                  /*wal_id=*/3,
                  /*txn_id=*/102,
                  kResidencyTableOid,
                  row_ids.data(),
                  one_chunk_batch(new_data),
                  row_ids.size());
    REQUIRE(buffer.size() > 0);

    auto& probe = process_default_probe();
    probe.reset();

    std::optional<record_t> record;
    {
        default_resource_window_t window{&probe};
        record.emplace(decode_record(buffer, &arena));
    }

    REQUIRE(record.has_value());
    REQUIRE(record->is_valid());
    REQUIRE(record->record_type == wal_record_type::PHYSICAL_UPDATE);
    REQUIRE(record->physical_row_ids.size() == row_ids.size());
    REQUIRE(record->physical_data.size() == 1);

    CHECK(record->physical_row_ids.get_allocator().resource() == &arena);
    CHECK(record->physical_data.get_allocator().resource() == &arena);

    INFO("allocations taken from the process-global default resource while decoding an UPDATE: "
         << probe.allocations() << " (" << probe.allocated_bytes() << " bytes)");
    CHECK(probe.allocations() == 0);
}

// A COMMIT record carries no payload, so nothing assigns into the two vectors: the arena they
// report is the one record_t itself was built on, and nothing else can put it right later.
TEST_CASE("services::wal::decode_record::a payload-free COMMIT record still names the caller's arena") {
    core::pmr::otterbrix_resource arena;

    buffer_t buffer(&arena);
    encode_commit(buffer, /*last_crc32=*/0, /*wal_id=*/4, /*txn_id=*/103, /*commit_id=*/9);
    REQUIRE(buffer.size() > 0);

    auto& probe = process_default_probe();
    probe.reset();

    std::optional<record_t> record;
    {
        default_resource_window_t window{&probe};
        record.emplace(decode_record(buffer, &arena));
    }

    REQUIRE(record.has_value());
    REQUIRE(record->is_commit_marker());
    REQUIRE(record->commit_id == 9);
    CHECK(record->physical_data.get_allocator().resource() == &arena);
    CHECK(record->physical_row_ids.get_allocator().resource() == &arena);

    INFO("allocations taken from the process-global default resource while decoding a COMMIT: "
         << probe.allocations() << " (" << probe.allocated_bytes() << " bytes)");
    CHECK(probe.allocations() == 0);
}
