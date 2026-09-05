// A DEFAULT MEMBER INITIALISER THAT NAMES THE PROCESS DEFAULT IS THE DEFECT, NOT THE FIX.
//
// record_t pinned BOTH of its pmr vectors with `{std::pmr::get_default_resource()}` default
// member initialisers — rule 14's forbidden construct, spelled out. decode_record() then
// default-constructed a record_t and ASSIGNED the decoded payload into those members. A pmr
// move-assignment does NOT take the source's allocator (propagate_on_container_move_assignment
// is false), so every byte of every replayed WAL record was allocated by the TARGET's allocator
// — the process-global arena — no matter which resource the caller handed decode_record. Replay
// is the one path where the arena matters most, and it was the one path that never used it.
//
// Two probes, because one alone can lie. get_allocator().resource() names the arena the vectors
// report; the counting resource installed AS the process default counts what decoding actually
// took from the global arena. A correct decode answers "&arena" and "0".

#include <catch2/catch_test_macros.hpp>

#include <components/tests/generaty.hpp>
#include <core/pmr.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_binary.hpp>

#include <atomic>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <vector>

using namespace services::wal;

namespace {

    class counting_resource_t final : public std::pmr::memory_resource {
    public:
        std::atomic<uint64_t> allocations{0};
        std::atomic<uint64_t> bytes{0};

        void reset() noexcept {
            allocations.store(0, std::memory_order_relaxed);
            bytes.store(0, std::memory_order_relaxed);
        }

    private:
        void* do_allocate(size_t size, size_t align) override {
            allocations.fetch_add(1, std::memory_order_relaxed);
            bytes.fetch_add(size, std::memory_order_relaxed);
            return std::pmr::new_delete_resource()->allocate(size, align);
        }
        void do_deallocate(void* p, size_t size, size_t align) override {
            std::pmr::new_delete_resource()->deallocate(p, size, align);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
    };

    // Immortal on purpose: it is the process default for the duration of one decode, and what it
    // hands out in that window may be freed long after the window closes.
    counting_resource_t& process_default_probe() {
        static counting_resource_t* probe = new counting_resource_t();
        return *probe;
    }

    struct default_resource_window_t final {
        std::pmr::memory_resource* previous;

        explicit default_resource_window_t(std::pmr::memory_resource* probe)
            : previous(std::pmr::set_default_resource(probe)) {}

        ~default_resource_window_t() { std::pmr::set_default_resource(previous); }
    };

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
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);
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
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);
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
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);
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
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);
}
