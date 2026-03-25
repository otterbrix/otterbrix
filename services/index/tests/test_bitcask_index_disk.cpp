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
