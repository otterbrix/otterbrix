#include <catch2/catch.hpp>
#include <services/index/bitcask_index_disk.hpp>
#include <services/index/btree_index_disk.hpp>

using components::types::logical_value_t;
using services::index::bitcask_index_disk_t;
using services::index::btree_index_disk_t;

namespace {
    size_t count_bitcask_data_files(const std::filesystem::path& path) {
        size_t data_file_count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            if (entry.is_regular_file() && entry.path().extension() == ".data") {
                ++data_file_count;
            }
        }
        return data_file_count;
    }
} // namespace


TEST_CASE("services::index::bitcask_index_disk::int64_basic") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_int64"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    auto index = bitcask_index_disk_t(path, &resource);

    for (int i = 1; i <= 100; ++i) {
        index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
    }

    REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 1);
    REQUIRE(index.find(logical_value_t(&resource, 100l)).front() == 100);
    REQUIRE(index.find(logical_value_t(&resource, 101l)).empty());

    REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 9);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 10);

    for (int i = 2; i <= 100; i += 2) {
        index.remove(logical_value_t(&resource, int64_t(i)));
    }

    REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
    REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 5);
    REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 5);
}

TEST_CASE("services::index::bitcask_index_disk::persist_close_reopen") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_persist_reopen"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = bitcask_index_disk_t(path, &resource);
        for (int i = 1; i <= 100; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        for (int i = 2; i <= 100; i += 2) {
            index.remove(logical_value_t(&resource, int64_t(i)));
        }
        index.force_flush();
    }

    REQUIRE(count_bitcask_data_files(path) >= 2);

    {
        auto index = bitcask_index_disk_t(path, &resource);

        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 2l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 99l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 99l)).front() == 99);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).empty());

        REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 5);
        REQUIRE(index.upper_bound(logical_value_t(&resource, 90l)).size() == 5);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_immutable_segments") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_segments"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = bitcask_index_disk_t(path, &resource);
        for (int i = 1; i <= 250; ++i) {
            index.insert(logical_value_t(&resource, int64_t(i)), static_cast<size_t>(i));
        }
        index.force_flush();
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = bitcask_index_disk_t(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 1l)).front() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 100l)).front() == 100);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 250l)).front() == 250);
        REQUIRE(index.lower_bound(logical_value_t(&resource, 10l)).size() == 9);
        REQUIRE(index.upper_bound(logical_value_t(&resource, 240l)).size() == 10);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_keeps_latest_snapshot_for_key") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_latest_snapshot"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = bitcask_index_disk_t(path, &resource);

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

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = bitcask_index_disk_t(path, &resource);
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
        auto index = bitcask_index_disk_t(path, &resource);

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

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = bitcask_index_disk_t(path, &resource);
        REQUIRE(index.find(logical_value_t(&resource, 555l)).empty());
        REQUIRE(index.find(logical_value_t(&resource, 60001l)).size() == 1);
        REQUIRE(index.find(logical_value_t(&resource, 60001l)).front() == 60001);
    }
}

TEST_CASE("services::index::bitcask_index_disk::merge_preserves_active_segment_entries") {
    auto resource = std::pmr::synchronized_pool_resource();

    std::filesystem::path path{"/tmp/index_disk/bitcask_merge_active_segment"};
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);

    {
        auto index = bitcask_index_disk_t(path, &resource);

        for (int i = 1; i <= 200; ++i) {
            index.insert(logical_value_t(&resource, 70000l + i), static_cast<size_t>(i));
        }

        index.insert(logical_value_t(&resource, 888l), 888);
        index.insert(logical_value_t(&resource, 889l), 889);
        index.force_flush();
    }

    REQUIRE(count_bitcask_data_files(path) == 2);

    {
        auto index = bitcask_index_disk_t(path, &resource);
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
        auto index = bitcask_index_disk_t(path, &resource);

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
        auto index = bitcask_index_disk_t(path, &resource);
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
        auto index = bitcask_index_disk_t(path, &resource);
        index.insert(logical_value_t(&resource, 10l), 7);
        index.insert(logical_value_t(&resource, 10l), 7); // duplicate must be ignored
        index.insert(logical_value_t(&resource, 10l), 8);
        index.force_flush();
    }

    {
        auto index = bitcask_index_disk_t(path, &resource);
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

    auto index = bitcask_index_disk_t(path, &resource);
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
        auto index = bitcask_index_disk_t(path, &resource);
        index.insert(logical_value_t(&resource, 99l), 999);
        index.force_flush();
        REQUIRE(std::filesystem::exists(path));
        REQUIRE(count_bitcask_data_files(path) >= 1);

        index.drop();
        REQUIRE_FALSE(std::filesystem::exists(path));
    }

    {
        auto recreated = bitcask_index_disk_t(path, &resource);
        REQUIRE(std::filesystem::exists(path));
        REQUIRE(recreated.find(logical_value_t(&resource, 99l)).empty());

        recreated.insert(logical_value_t(&resource, 100l), 1000);
        REQUIRE(recreated.find(logical_value_t(&resource, 100l)).size() == 1);
        REQUIRE(recreated.find(logical_value_t(&resource, 100l)).front() == 1000);
    }
}
