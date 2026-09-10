#include <array>
#include <catch2/catch_test_macros.hpp>
#include <components/index/logical_value_binary_codec.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>
#include <filesystem>
#include <memory_resource>
#include <random>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/disk_hash_table.hpp>
#include <set>
#include <string_view>
#include <unordered_set>

#include "index_fixture_path.hpp"

using services::index::tests::index_fixture_path;
using services::index::tests::index_fixture_root;

using components::types::logical_value_t;
using services::index::bitcask_index_disk_t;

namespace {
    // bitcask's find() answers with a core::result_wrapper_t (a keydir walk that met an
    // unreadable page refuses rather than returning partial rows); btree's find() still
    // answers with the row list directly. None of the cases below test that refusal, so
    // each just asserts it didn't happen and unwraps.
    template<typename found_t>
    auto rows_of(found_t&& found) {
        if constexpr (core::detail::result_like<std::remove_reference_t<found_t>>) {
            // FAIL prints the refusal's segment/offset/reason instead of a bare "!true" from
            // REQUIRE_FALSE. Not redundant: Catch2's FAIL throws, so it's the assertion on the
            // refusing path, and REQUIRE_FALSE is the assertion on every call that doesn't refuse.
            if (found.has_error()) {
                FAIL("find refused: " << std::string_view{found.error().what});
            }
            REQUIRE_FALSE(found.has_error());
            return std::move(found.value());
        } else {
            return std::forward<found_t>(found);
        }
    }
} // namespace

namespace {
    constexpr uint64_t test_flush_threshold = 1000;
    constexpr uint64_t test_segment_record_limit = 100;

    // Empty committed set: the segment-only fixtures below never recover a
    // txn-log, so the recover gate is never consulted — an empty set is the
    // correct value, not a fallback (a fresh dir has no txn-log to gate).
    bitcask_index_disk_t
    make_test_index(const std::filesystem::path& path,
                    std::pmr::memory_resource* resource,
                    std::pmr::set<std::uint64_t> commit_ids = std::pmr::set<std::uint64_t>{}) {
        return bitcask_index_disk_t(path,
                                    resource,
                                    test_flush_threshold,
                                    test_segment_record_limit,
                                    std::move(commit_ids));
    }

} // namespace

// A long randomized insert/remove/find run against the store, single-threaded on purpose: the
// store is a by-value member reached only through its agent's mailbox and carries no lock of its
// own, so a multi-threaded fixture here would test a data race, not production. Checks that a
// key's row list never contains a duplicate, and that closing and reopening the store answers
// exactly what it answered before.
//
// The concurrency claim belongs to integration/cpp/test/test_index_concurrent_merge.cpp instead,
// which also drives enough traffic to rotate/merge segments -- this test's segment limit is set
// high so the insert/remove/find mixture, not the merger, is what's under test.
TEST_CASE("services::index::bitcask_index_disk::randomized_insert_remove_find_stress", "[stress][long]") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{index_fixture_path("bitcask_randomized_stress")};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr size_t key_count = 64;
    constexpr size_t worker_count = 8;
    constexpr size_t operations_per_worker = 40000;
    static_assert(key_count % worker_count == 0);
    constexpr size_t keys_per_worker = key_count / worker_count;

    size_t find_count = 0;
    size_t duplicate_row_violations = 0;
    std::array<std::unordered_set<size_t>, key_count> expected_after_stress;

    auto snapshot = [&](bitcask_index_disk_t& from) {
        std::array<std::unordered_set<size_t>, key_count> state;
        for (size_t key = 0; key < key_count; ++key) {
            const auto logical_key = logical_value_t(&resource, static_cast<int64_t>(key));
            const auto actual_rows = rows_of(from.find(logical_key));

            std::unordered_set<size_t> actual_set;
            actual_set.reserve(actual_rows.size());
            for (auto row : actual_rows) {
                actual_set.insert(row);
            }
            REQUIRE(actual_set.size() == actual_rows.size());
            state[key] = std::move(actual_set);
        }
        return state;
    };

    {
        auto index = bitcask_index_disk_t(path, &resource, 128, 10'000'000, std::pmr::set<std::uint64_t>{});
        // The eight "workers" are kept as eight independent random streams over disjoint
        // key ranges -- the same eight sequences the threaded version produced, run one
        // after another instead of at the same time.
        for (size_t worker_id = 0; worker_id < worker_count; ++worker_id) {
            std::mt19937_64 rng(0xB17CA5ULL + worker_id * 7919ULL);
            const size_t key_begin = worker_id * keys_per_worker;
            std::uniform_int_distribution<size_t> key_dist(key_begin, key_begin + keys_per_worker - 1);
            std::uniform_int_distribution<size_t> row_dist(0, 1999);
            std::uniform_int_distribution<int> op_dist(0, 99);

            for (size_t i = 0; i < operations_per_worker; ++i) {
                const auto key = key_dist(rng);
                const auto row = worker_id * 100000 + row_dist(rng);
                const auto op = op_dist(rng);
                const auto logical_key = logical_value_t(&resource, static_cast<int64_t>(key));

                if (op < 45) {
                    index.insert(logical_key, row);
                } else if (op < 80) {
                    index.remove(logical_key, row);
                } else {
                    auto rows = rows_of(index.find(logical_key));
                    if (!rows.empty()) {
                        std::unordered_set<size_t> seen;
                        seen.reserve(rows.size());
                        for (auto r : rows) {
                            seen.insert(r);
                        }
                        if (seen.size() != rows.size()) {
                            ++duplicate_row_violations;
                        }
                    }
                    ++find_count;
                }
            }
        }

        REQUIRE(duplicate_row_violations == 0);
        REQUIRE(find_count > 0);
        REQUIRE(index.force_flush().type == core::error_code_t::none);
        expected_after_stress = snapshot(index);
    }

    {
        auto reopened = make_test_index(path, &resource);
        const auto actual_after_reopen = snapshot(reopened);
        for (size_t key = 0; key < key_count; ++key) {
            REQUIRE(actual_after_reopen[key].size() == expected_after_stress[key].size());
            for (auto row : expected_after_stress[key]) {
                REQUIRE(actual_after_reopen[key].contains(row));
            }
        }
    }
}
