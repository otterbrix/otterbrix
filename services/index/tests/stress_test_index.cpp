#include <atomic>
#include <catch2/catch.hpp>
#include <charconv>
#include <components/index/logical_value_binary_codec.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>
#include <fstream>
#include <limits>
#include <memory_resource>
#include <mutex>
#include <random>
#include <services/index/bitcask_hash_key_loader.hpp>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/btree_index_disk.hpp>
#include <services/index/disk_hash_table.hpp>
#include <set>
#include <thread>
#include <unordered_set>

using components::types::logical_value_t;
using services::index::bitcask_index_disk_t;
using services::index::btree_index_disk_t;

namespace {
    constexpr uint64_t test_flush_threshold = 1000;
    constexpr uint64_t test_segment_record_limit = 100;

    // Empty committed set: the segment-only fixtures below never recover a
    // txn-log, so the recover gate is never consulted — an empty set is the
    // correct value, not a fallback (a fresh dir has no txn-log to gate).
    bitcask_index_disk_t
    make_test_index(const std::filesystem::path& path,
                    std::pmr::memory_resource* resource,
                    std::pmr::set<std::uint64_t> committed_txn_ids = std::pmr::set<std::uint64_t>{}) {
        return bitcask_index_disk_t(path,
                                    resource,
                                    test_flush_threshold,
                                    test_segment_record_limit,
                                    std::move(committed_txn_ids));
    }

} // namespace

TEST_CASE("services::index::bitcask_index_disk::concurrent_insert_remove_find_stress", "[stress][long]") {
    auto resource = core::pmr::otterbrix_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_concurrent_stress"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr size_t key_count = 64;
    constexpr size_t thread_count = 8;
    constexpr size_t operations_per_thread = 40000;
    static_assert(key_count % thread_count == 0);
    constexpr size_t keys_per_thread = key_count / thread_count;

    std::atomic<size_t> find_count{0};
    // Catch2's RunContext is not thread-safe — REQUIRE must not run on
    // worker threads (TSAN flags the shared assertion counters/message
    // scopes). Workers record violations here; the main thread REQUIREs
    // zero after join.
    std::atomic<size_t> duplicate_row_violations{0};
    std::array<std::unordered_set<size_t>, key_count> expected_after_stress;

    auto snapshot = [&](bitcask_index_disk_t& from) {
        std::array<std::unordered_set<size_t>, key_count> state;
        for (size_t key = 0; key < key_count; ++key) {
            const auto logical_key = logical_value_t(&resource, static_cast<int64_t>(key));
            const auto actual_rows = from.find(logical_key);

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
        auto worker = [&](size_t worker_id) {
            std::mt19937_64 rng(0xB17CA5ULL + worker_id * 7919ULL);
            const size_t key_begin = worker_id * keys_per_thread;
            const size_t key_end = key_begin + keys_per_thread - 1;
            std::uniform_int_distribution<size_t> key_dist(key_begin, key_end);
            std::uniform_int_distribution<size_t> row_dist(0, 1999);
            std::uniform_int_distribution<int> op_dist(0, 99);

            for (size_t i = 0; i < operations_per_thread; ++i) {
                const auto key = key_dist(rng);
                const auto row = worker_id * 100000 + row_dist(rng);
                const auto op = op_dist(rng);
                const auto logical_key = logical_value_t(&resource, static_cast<int64_t>(key));

                if (op < 45) {
                    index.insert(logical_key, row);
                } else if (op < 80) {
                    index.remove(logical_key, row);
                } else {
                    auto rows = index.find(logical_key);
                    if (!rows.empty()) {
                        std::unordered_set<size_t> seen;
                        seen.reserve(rows.size());
                        for (auto r : rows) {
                            seen.insert(r);
                        }
                        if (seen.size() != rows.size()) {
                            duplicate_row_violations.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    find_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(thread_count);
        for (size_t t = 0; t < thread_count; ++t) {
            threads.emplace_back(worker, t);
        }
        for (auto& thread : threads) {
            thread.join();
        }

        REQUIRE(duplicate_row_violations.load(std::memory_order_relaxed) == 0);
        REQUIRE(find_count.load(std::memory_order_relaxed) > 0);
        index.force_flush();
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