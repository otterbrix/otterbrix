#include <atomic>
#include <catch2/catch.hpp>
#include <charconv>
#include <core/result_wrapper.hpp>
#include <fstream>
#include <limits>
#include <memory_resource>
#include <mutex>
#include <random>
#include <core/result_wrapper.hpp>
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

    // Build a committed set from an initializer list using the given resource.
    std::pmr::set<std::uint64_t> committed_set(std::pmr::memory_resource* resource,
                                               std::initializer_list<std::uint64_t> ids) {
        std::pmr::set<std::uint64_t> out(resource);
        for (auto id : ids) {
            out.insert(id);
        }
        return out;
    }

    // Simulate the crash window: the durable txn-log frames survive, but the
    // eagerly-applied segment state and the applied-offset checkpoint do not.
    // Removing everything except bitcask.txn.log forces the next reopen to
    // replay the log from offset 0, so the recover gate alone decides which
    // frames are applied.
    void wipe_all_but_txn_log(const std::filesystem::path& path) {
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (entry.path().filename() == "bitcask.txn.log") {
                continue;
            }
            std::filesystem::remove_all(entry.path());
        }
    }

    size_t count_bitcask_data_files(const std::filesystem::path& path) {
        if (!std::filesystem::exists(path)) {
            return 0;
        }
        size_t data_file_count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (entry.is_regular_file() && entry.path().extension() == ".data") {
                ++data_file_count;
            }
        }
        return data_file_count;
    }

    std::filesystem::path latest_bitcask_data_file(const std::filesystem::path& path) {
        if (!std::filesystem::exists(path)) {
            return {};
        }
        std::filesystem::path latest;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".data") {
                continue;
            }
            if (latest.empty() || entry.path().filename().string() > latest.filename().string()) {
                latest = entry.path();
            }
        }
        return latest;
    }

    uint64_t max_bitcask_segment_id(const std::filesystem::path& path) {
        uint64_t max_id = 0;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".data") {
                continue;
            }
            const auto filename = entry.path().filename().string();
            constexpr std::string_view prefix = "bitcask.";
            constexpr std::string_view suffix = ".data";
            const std::string_view name_sv{filename};
            if (!name_sv.starts_with(prefix) || !name_sv.ends_with(suffix)) {
                continue;
            }
            const auto digits = name_sv.substr(prefix.size(), name_sv.size() - prefix.size() - suffix.size());
            uint64_t segment_id = 0;
            const auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), segment_id);
            if (ec == std::errc() && ptr == digits.data() + digits.size()) {
                max_id = std::max(max_id, segment_id);
            }
        }
        return max_id;
    }

    std::vector<std::byte> read_file_bytes(const std::filesystem::path& file_path) {
        auto input = std::ifstream(file_path, std::ios::binary);
        if (!input.good()) {
            return {};
        }
        input.seekg(0, std::ios::end);
        const auto size = input.tellg();
        input.seekg(0, std::ios::beg);
        if (size <= 0) {
            return {};
        }
        std::vector<std::byte> bytes(static_cast<size_t>(size));
        input.read(reinterpret_cast<char*>(bytes.data()), size);
        return bytes;
    }
} // namespace

TEST_CASE("services::index::bitcask_index_disk::int64_basic") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_int64"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = make_test_index(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }

    REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 100l)).front() == 100);
    REQUIRE(index.find(logical_value_t(&resource, 101l)).empty());

    for (int i = 2; i <= 100; i += 2) {
        index.remove(logical_value_t(&resource, int64_t(i)));
    }

    { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

    REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
}

TEST_CASE("services::index::bitcask_index_disk::persist_close_reopen") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_persist_reopen"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        // Segment-only fixture: no txn-log is written, so an empty committed
        // set is the correct value for this recover.
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 1000, std::pmr::set<std::uint64_t>{});
        for (int i = 1; i <= 100; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        for (int i = 2; i <= 100; i += 2) {
            index.remove(logical_value_t(&resource, int64_t(i)));
        }
        index.force_flush();
    }

    { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

    REQUIRE(count_bitcask_data_files(path) == 1);

    {
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 1000, std::pmr::set<std::uint64_t>{});

        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 99l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 99l)).front() == 99);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).empty());
    }
}

TEST_CASE("services::index::bitcask_index_disk::persist_close_reopen_large_dataset") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_persist_reopen_large"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 1000, std::pmr::set<std::uint64_t>{});
        for (int i = 1; i <= 2500; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        index.force_flush();
    }

    {
        auto reopened = bitcask_index_disk_t(path, &resource, test_flush_threshold, 1000, std::pmr::set<std::uint64_t>{});
        for (int key : {1, 42, 872, 1500, 2499, 2500}) {
            auto rows = reopened.find(logical_value_t(&resource, int64_t(key)));
            REQUIRE(rows.size() == 1);
            REQUIRE(rows.front() == static_cast<size_t>(key));
        }
        REQUIRE(reopened.find(logical_value_t(&resource, int64_t(2600))).empty());
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_immutable_segments") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_segments"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        index.force_flush();
    }

    { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).front() == 100);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).front() == 250);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_keeps_latest_snapshot_for_key") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_latest_snapshot"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);

        index.insert(logical_value_t(&resource, 777l), 1);
        for (int i = 1; i < 100; ++i) {
            index.insert(logical_value_t(&resource, 10000l + i), static_cast<size_t>(i));
        }

        index.insert(logical_value_t(&resource, 777l), 2);
        for (int i = 1; i < 100; ++i) {
            index.insert(logical_value_t(&resource, 20000l + i), static_cast<size_t>(100 + i));
        }

        index.insert(logical_value_t(&resource, 30001l), 30001);
        index.force_flush();
    }

    { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = make_test_index(path, &resource);
        const auto rows = index.find(logical_value_t(&resource, 777l));
        REQUIRE(rows.size() == 2);
        REQUIRE(rows[0] == 1);
        REQUIRE(rows[1] == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_drops_tombstoned_keys") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_tombstone"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);

        index.insert(logical_value_t(&resource, 555l), 55);
        for (int i = 1; i < 100; ++i) {
            index.insert(logical_value_t(&resource, 40000l + i), static_cast<size_t>(i));
        }

        index.remove(logical_value_t(&resource, 555l));
        for (int i = 1; i < 100; ++i) {
            index.insert(logical_value_t(&resource, 50000l + i), static_cast<size_t>(100 + i));
        }

        index.insert(logical_value_t(&resource, 60001l), 60001);
        index.force_flush();
    }

    { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

    REQUIRE(count_bitcask_data_files(path) == 2);

    auto index = make_test_index(path, &resource);
    REQUIRE(index.find(logical_value_t(&resource, 555l)).empty());
    REQUIRE(index.find(logical_value_t(&resource, 60001l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 60001l)).front() == 60001);
}

TEST_CASE("services::index::bitcask_index_disk::merge_preserves_active_segment_entries") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_active_segment"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);

        for (int i = 1; i <= 200; ++i) {
            index.insert(logical_value_t(&resource, 70000l + i), static_cast<size_t>(i));
        }

        index.insert(logical_value_t(&resource, 888l), 888);
        index.insert(logical_value_t(&resource, 889l), 889);
        index.force_flush();
    }

    { std::this_thread::sleep_for(std::chrono::milliseconds(100)); }

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 888l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 888l)).front() == 888);
        REQUIRE(index.find(logical_value_t(&resource, 889l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 889l)).front() == 889);
        REQUIRE(index.find(logical_value_t(&resource, 70001l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 70200l)).size() == 1);
    }
}

TEST_CASE("services::index::bitcask_index_disk::remove_specific_row_id") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_remove_specific_row"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);

        index.insert(logical_value_t(&resource, 42l), 100);
        index.insert(logical_value_t(&resource, 42l), 101);
        index.insert(logical_value_t(&resource, 42l), 102);
        index.insert(logical_value_t(&resource, 43l), 200);

        index.remove(logical_value_t(&resource, 42l), 101);
        const auto after_first_remove = index.find(logical_value_t(&resource, 42l));
        REQUIRE(after_first_remove.size() == 2);
        REQUIRE(after_first_remove[0] == 100);
        REQUIRE(after_first_remove[1] == 102);

        index.remove(logical_value_t(&resource, 42l), 999); // no-op
        const auto after_noop_remove = index.find(logical_value_t(&resource, 42l));
        REQUIRE(after_noop_remove.size() == 2);
        REQUIRE(after_noop_remove[0] == 100);
        REQUIRE(after_noop_remove[1] == 102);

        index.remove(logical_value_t(&resource, 42l), 100);
        index.remove(logical_value_t(&resource, 42l), 102); // transitions to tombstone
        REQUIRE(index.find(logical_value_t(&resource, 42l)).empty());

        index.force_flush();
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 42l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 43l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 43l)).front() == 200);
    }
}

TEST_CASE("services::index::bitcask_index_disk::deduplicates_same_row_for_key") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_deduplicate_rows"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 10l), 7);
        index.insert(logical_value_t(&resource, 10l), 7); // duplicate must be ignored
        index.insert(logical_value_t(&resource, 10l), 8);
        index.force_flush();
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = index.find(logical_value_t(&resource, 10l));
        REQUIRE(rows.size() == 2);
        REQUIRE(rows[0] == 7);
        REQUIRE(rows[1] == 8);
    }
}

TEST_CASE("services::index::bitcask_index_disk::load_entries_reflects_current_state") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_load_entries"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    index.insert(logical_value_t(&resource, 1l), 11);
    index.insert(logical_value_t(&resource, 1l), 12);
    index.insert(logical_value_t(&resource, 2l), 21);
    index.insert(logical_value_t(&resource, 3l), 31);
    index.remove(logical_value_t(&resource, 1l), 11);
    index.remove(logical_value_t(&resource, 3l));

    bitcask_index_disk_t::entries_t entries(&resource);
    index.load_entries(entries);

    REQUIRE(entries.size() == 2);
    REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 12);
    REQUIRE(index.find(logical_value_t(&resource, 2l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 2l)).front() == 21);
    REQUIRE(index.find(logical_value_t(&resource, 3l)).empty());
}

TEST_CASE("services::index::bitcask_index_disk::drop_removes_storage_and_recreate_is_empty") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_drop_recreate"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 99l), 999);
        index.force_flush();
        REQUIRE(std::filesystem::exists(path));
        REQUIRE(count_bitcask_data_files(path) == 1);

        index.drop();
        REQUIRE_FALSE(std::filesystem::exists(path));
    }

    {
        auto recreated = make_test_index(path, &resource);
        REQUIRE(std::filesystem::exists(path));
        REQUIRE(recreated.find(logical_value_t(&resource, 99l)).empty());

        recreated.insert(logical_value_t(&resource, 100l), 1000);
        REQUIRE(recreated.find(logical_value_t(&resource, 100l)).size() == 1);
        REQUIRE(recreated.find(logical_value_t(&resource, 100l)).front() == 1000);
    }
}

TEST_CASE("services::index::bitcask_index_disk::empty_index_operations_are_noop") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_empty_noop"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto index = make_test_index(path, &resource);
    index.remove(logical_value_t(&resource, 111l));      // no-op
    index.remove(logical_value_t(&resource, 111l), 222); // no-op

    REQUIRE(index.find(logical_value_t(&resource, 111l)).empty());

    bitcask_index_disk_t::entries_t entries(&resource);
    index.load_entries(entries);
    REQUIRE(entries.empty());
}

TEST_CASE("services::index::bitcask_index_disk::string_keys_persist_and_range_queries") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_string_keys"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, std::string("alpha")), 1);
        index.insert(logical_value_t(&resource, std::string("beta")), 2);
        index.insert(logical_value_t(&resource, std::string("gamma")), 3);
        index.force_flush();
    }

    {
        auto index = make_test_index(path, &resource);
        auto beta = index.find(logical_value_t(&resource, std::string("beta")));
        REQUIRE(beta.size() == 1);
        REQUIRE(beta.front() == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::flush_threshold_persists_without_explicit_force_flush") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_flush_threshold"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        // flush_threshold = 3, so third operation should trigger flush_if_needed.
        // Segment-only fixture: empty committed set is correct (no txn-log).
        auto index = bitcask_index_disk_t(path, &resource, 3, 1000, std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, 1l), 10);
        index.insert(logical_value_t(&resource, 2l), 20);
        index.insert(logical_value_t(&resource, 3l), 30);
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 2l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 3l)).size() == 1);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_fs_error_does_not_lose_data") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_fs_error"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        index.force_flush();
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    const auto blocking_segment_id = max_bitcask_segment_id(path) + 1;
    std::ostringstream blocking_name;
    blocking_name << "bitcask." << std::setw(6) << std::setfill('0') << blocking_segment_id << ".data";
    const auto blocking_path = path / blocking_name.str();
    std::filesystem::create_directory(blocking_path);

    {
        auto index = make_test_index(path, &resource);
        index.force_flush(); // enqueue merge; publish should fail because target path is a directory
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::filesystem::remove_all(blocking_path);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).front() == 100);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).front() == 250);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recovery_ignores_corrupted_tail_record") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_corrupted_tail"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 1l), 11);
        index.insert(logical_value_t(&resource, 2l), 22);
        index.force_flush();
    }

    const auto file_path = latest_bitcask_data_file(path);
    REQUIRE_FALSE(file_path.empty());

    const auto original_size = std::filesystem::file_size(file_path);
    std::filesystem::resize_file(file_path, original_size + 5); // append incomplete/trash tail

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 11);
        REQUIRE(index.find(logical_value_t(&resource, 2l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 2l)).front() == 22);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recovery_throws_on_crc_mismatch") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_crc_mismatch"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = bitcask_index_disk_t(path, &resource, test_flush_threshold, 2, std::pmr::set<std::uint64_t>{});
        index.insert(logical_value_t(&resource, 1l), 11);
        index.insert(logical_value_t(&resource, 2l), 22);
        index.insert(logical_value_t(&resource, 100l), 100);
        index.insert(logical_value_t(&resource, 200l), 200);
        index.force_flush();
    }

    const auto file_path = latest_bitcask_data_file(path);
    REQUIRE_FALSE(file_path.empty());
    const auto backup_path = file_path.string() + ".bak";
    std::filesystem::copy_file(file_path, backup_path, std::filesystem::copy_options::overwrite_existing);

    {
        auto file = std::fstream(file_path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.good());
        file.seekp(0, std::ios::beg);
        char byte = 0;
        file.read(&byte, 1);
        REQUIRE(file.good());
        byte ^= static_cast<char>(0xFF);
        file.seekp(0, std::ios::beg);
        file.write(&byte, 1);
        REQUIRE(file.good());
    }

    {
        auto res =
            bitcask_index_disk_t::create(path, &resource, test_flush_threshold, 2, std::pmr::set<std::uint64_t>{});
        REQUIRE(res.has_error());
        REQUIRE(res.error().type == core::error_code_t::index_create_fail);
    }

    std::filesystem::copy_file(backup_path, file_path, std::filesystem::copy_options::overwrite_existing);
    std::filesystem::remove(backup_path);
    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 2l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 200l)).size() == 1);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recovery_crc_mismatch_does_not_damage_other_segments") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_crc_mismatch_segments_intact"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        index.force_flush();
    }

    std::vector<std::filesystem::path> segment_files;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        if (entry.is_regular_file() && entry.path().extension() == ".data") {
            segment_files.push_back(entry.path());
        }
    }
    std::sort(segment_files.begin(), segment_files.end());
    REQUIRE(segment_files.size() >= 2);

    const auto corrupted_segment = segment_files.front();
    const auto intact_segment = segment_files.back();
    const auto corrupted_backup = corrupted_segment.string() + ".bak";

    std::filesystem::copy_file(corrupted_segment, corrupted_backup, std::filesystem::copy_options::overwrite_existing);

    const auto intact_before = read_file_bytes(intact_segment);
    REQUIRE_FALSE(intact_before.empty());

    {
        auto file = std::fstream(corrupted_segment, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.good());
        file.seekp(0, std::ios::beg);
        char byte = 0;
        file.read(&byte, 1);
        REQUIRE(file.good());
        byte ^= static_cast<char>(0xFF);
        file.seekp(0, std::ios::beg);
        file.write(&byte, 1);
        REQUIRE(file.good());
    }

    {
        auto res = bitcask_index_disk_t::create(path,
                                                &resource,
                                                test_flush_threshold,
                                                test_segment_record_limit,
                                                std::pmr::set<std::uint64_t>{});
        REQUIRE(res.has_error());
        REQUIRE(res.error().type == core::error_code_t::index_create_fail);
    }

    const auto intact_after_failed_recovery = read_file_bytes(intact_segment);
    REQUIRE(intact_after_failed_recovery == intact_before);

    std::filesystem::copy_file(corrupted_backup, corrupted_segment, std::filesystem::copy_options::overwrite_existing);
    std::filesystem::remove(corrupted_backup);

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).front() == 250);
    }
}

TEST_CASE("services::index::bitcask_index_disk::recovery_with_invalid_current_file_uses_latest_segment") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_invalid_current"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        index.force_flush();
    }

    const auto current_file = path / "CURRENT";
    {
        auto out = std::ofstream(current_file, std::ios::trunc);
        REQUIRE(out.good());
        out << "broken-current";
        out.flush();
        REQUIRE(out.good());
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).size() == 1);
        index.insert(logical_value_t(&resource, 9999l), 9999);
        index.force_flush();
    }

    {
        auto index = make_test_index(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 9999l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 9999l)).front() == 9999);
    }
}

TEST_CASE("services::index::bitcask_index_disk::tombstone_then_reinsert_persists_latest_state") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_tombstone_reinsert"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 77l), 1);
        index.remove(logical_value_t(&resource, 77l));
        index.insert(logical_value_t(&resource, 77l), 2);
        index.force_flush();
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = index.find(logical_value_t(&resource, 77l));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::string_key_with_embedded_null_persists") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_string_embedded_null"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string key_with_null{"abc\0def", 7};

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, key_with_null), 77);
        index.force_flush();
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = index.find(logical_value_t(&resource, key_with_null));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 77);
    }
}

TEST_CASE("services::index::bitcask_index_disk::find_invokes_key_loader_for_truncated_key") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_find_loader_invoked"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    auto shared_table = boost::intrusive_ptr(new services::index::disk_hash_table_t(
        path / "hash_index.bin",
        services::index::disk_hash_table_t::default_bucket_count,
        &resource));

    bitcask_index_disk_t index(path,
                               &resource,
                               test_flush_threshold,
                               test_segment_record_limit,
                               std::pmr::set<std::uint64_t>{&resource},
                               shared_table);

    const auto real_loader = services::index::make_bitcask_hash_key_loader(index);
    size_t loader_calls = 0;
    shared_table->set_full_key_loader([&](uint32_t segment_id, uint64_t value_offset, std::string& out_key, bool lock_bitcask) {
        ++loader_calls;
        return real_loader(segment_id, value_offset, out_key, lock_bitcask);
    });

    const std::string long_key(200, 'q');
    index.insert(logical_value_t(&resource, long_key), 4242);
    index.force_flush();
    loader_calls = 0;

    const auto rows = index.find(logical_value_t(&resource, long_key));
    REQUIRE(rows.size() == 1);
    REQUIRE(rows.front() == 4242);
    REQUIRE(loader_calls >= 1);
}

TEST_CASE("services::index::bitcask_index_disk::very_long_string_key_persists") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_very_long_string_key"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const std::string long_key(1U << 20U, 'x');

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, long_key), 12345);
        index.force_flush();
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = index.find(logical_value_t(&resource, long_key));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 12345);
    }
}

TEST_CASE("services::index::bitcask_index_disk::txn_log_recovery_replays_committed_batch") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_txn_recovery"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> inserts;
        inserts.emplace_back(logical_value_t(&resource, 1001l), 11);
        inserts.emplace_back(logical_value_t(&resource, 1002l), 22);
        REQUIRE(!index.apply_txn_inserts(5001, inserts).contains_error());
    }

    {
        // txn 5001 is committed: its frame must replay if the gate is consulted.
        auto index = make_test_index(path, &resource, committed_set(&resource, {5001}));
        REQUIRE(index.find(logical_value_t(&resource, 1001l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1001l)).front() == 11);
        REQUIRE(index.find(logical_value_t(&resource, 1002l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1002l)).front() == 22);
    }
}

TEST_CASE("services::index::bitcask_index_disk::txn_log_applied_checkpoint_prevents_replay_duplicates") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_txn_recovery_idempotent"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> inserts;
        inserts.emplace_back(logical_value_t(&resource, 2001l), 77);
        REQUIRE(!index.apply_txn_inserts(6001, inserts).contains_error());
    }

    {
        // txn 6001 is committed: replays once, never duplicated across reopens.
        auto index = make_test_index(path, &resource, committed_set(&resource, {6001}));
        auto rows = index.find(logical_value_t(&resource, 2001l));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 77);
    }
}

TEST_CASE("services::index::bitcask_index_disk::txn_log_recovery_is_order_independent_by_txn_id") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_txn_recovery_out_of_order_txn_id"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = make_test_index(path, &resource);
        std::vector<std::pair<logical_value_t, size_t>> first;
        first.emplace_back(logical_value_t(&resource, 3001l), 1);
        REQUIRE(!index.apply_txn_inserts(9002, first).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> second;
        second.emplace_back(logical_value_t(&resource, 3002l), 2);
        // lower txn_id, committed later
        REQUIRE(!index.apply_txn_inserts(9001, second).contains_error());
    }

    {
        // Both txns committed regardless of txn_id order.
        auto index = make_test_index(path, &resource, committed_set(&resource, {9001, 9002}));
        REQUIRE(index.find(logical_value_t(&resource, 3001l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 3001l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 3002l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 3002l)).front() == 2);
    }
}

TEST_CASE("services::index::bitcask_index_disk::max_size_t_row_id_persists") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_max_row_id"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    const auto max_row_id = std::numeric_limits<size_t>::max();

    {
        auto index = make_test_index(path, &resource);
        index.insert(logical_value_t(&resource, 999l), max_row_id);
        index.force_flush();
    }

    {
        auto index = make_test_index(path, &resource);
        const auto rows = index.find(logical_value_t(&resource, 999l));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == max_row_id);
    }
}

TEST_CASE("services::index::bitcask_index_disk::concurrent_insert_remove_find_stress", "[stress][long]") {
    auto resource = std::pmr::synchronized_pool_resource();

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

// Recover gate. apply_txn_inserts writes a durable txn-log frame AND
// eagerly applies the entries to the active segment. wipe_all_but_txn_log
// reproduces the crash window: only the durable txn-log survives, so the next
// reopen replays the log from offset 0 and the committed_txn_ids gate alone
// decides which frames are applied.
TEST_CASE("services::index::bitcask_index_disk::recover_gates_uncommitted_txn_frames") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_recover_gate"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr std::uint64_t txn_a = 7001;
    constexpr std::uint64_t txn_b = 7002;

    {
        auto index = make_test_index(path, &resource);

        std::vector<std::pair<logical_value_t, size_t>> a_inserts;
        a_inserts.emplace_back(logical_value_t(&resource, 4001l), 41);
        a_inserts.emplace_back(logical_value_t(&resource, 4002l), 42);
        REQUIRE(!index.apply_txn_inserts(txn_a, a_inserts).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> b_inserts;
        b_inserts.emplace_back(logical_value_t(&resource, 5001l), 51);
        b_inserts.emplace_back(logical_value_t(&resource, 5002l), 52);
        REQUIRE(!index.apply_txn_inserts(txn_b, b_inserts).contains_error());
    }

    // Crash window: keep only the durable txn-log; drop the eagerly-applied
    // segment state and the applied-offset checkpoint.
    wipe_all_but_txn_log(path);
    REQUIRE(std::filesystem::exists(path / "bitcask.txn.log"));

    {
        // Only txn B committed: A's frame must be skipped, B's applied.
        auto index = make_test_index(path, &resource, committed_set(&resource, {txn_b}));

        REQUIRE(index.find(logical_value_t(&resource, 4001l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 4002l)).empty());

        const auto b_first = index.find(logical_value_t(&resource, 5001l));
        REQUIRE(b_first.size() == 1);
        REQUIRE(b_first.front() == 51);
        const auto b_second = index.find(logical_value_t(&resource, 5002l));
        REQUIRE(b_second.size() == 1);
        REQUIRE(b_second.front() == 52);
    }
}

// A skipped frame still advances write_applied_log_offset past its end,
// so it is consumed permanently. Even if a later reopen reports the previously
// uncommitted txn as committed, its frame is never replayed again.
TEST_CASE("services::index::bitcask_index_disk::recover_skipped_frames_advance_applied_offset") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_recover_skip_offset"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    constexpr std::uint64_t txn_a = 8001;
    constexpr std::uint64_t txn_b = 8002;

    {
        auto index = make_test_index(path, &resource);

        std::vector<std::pair<logical_value_t, size_t>> a_inserts;
        a_inserts.emplace_back(logical_value_t(&resource, 6001l), 61);
        REQUIRE(!index.apply_txn_inserts(txn_a, a_inserts).contains_error());

        std::vector<std::pair<logical_value_t, size_t>> b_inserts;
        b_inserts.emplace_back(logical_value_t(&resource, 7001l), 71);
        REQUIRE(!index.apply_txn_inserts(txn_b, b_inserts).contains_error());
    }

    wipe_all_but_txn_log(path);

    {
        // First reopen gates A out; recover advances the applied offset past
        // every frame, including A's skipped one.
        auto index = make_test_index(path, &resource, committed_set(&resource, {txn_b}));
        REQUIRE(index.find(logical_value_t(&resource, 6001l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 7001l)).size() == 1);
    }

    {
        // Second reopen now reports A committed too, but A's frame was already
        // consumed (offset advanced past it) — it must NOT come back.
        auto index = make_test_index(path, &resource, committed_set(&resource, {txn_a, txn_b}));
        REQUIRE(index.find(logical_value_t(&resource, 6001l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 7001l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 7001l)).front() == 71);
    }
}

// A fresh runtime instance receives an EMPTY committed set (correct value, not a
// fallback): with no txn-log to gate, normal insert/find works.
TEST_CASE("services::index::bitcask_index_disk::fresh_instance_with_empty_set_works") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_fresh_empty_set"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    REQUIRE_FALSE(std::filesystem::exists(path / "bitcask.txn.log"));

    auto index = make_test_index(path, &resource, std::pmr::set<std::uint64_t>{});
    index.insert(logical_value_t(&resource, 8001l), 81);
    index.insert(logical_value_t(&resource, 8002l), 82);

    const auto first = index.find(logical_value_t(&resource, 8001l));
    REQUIRE(first.size() == 1);
    REQUIRE(first.front() == 81);
    const auto second = index.find(logical_value_t(&resource, 8002l));
    REQUIRE(second.size() == 1);
    REQUIRE(second.front() == 82);
    REQUIRE(index.find(logical_value_t(&resource, 9999l)).empty());
}
