#include <catch2/catch.hpp>
#include <disk/disk.hpp>

const core::filesystem::path_t test_folder = "/tmp/databases";

using namespace services::disk;

TEST_CASE("services::disk::sync_database_from_disk") {
    auto resource = std::pmr::synchronized_pool_resource();
    core::filesystem::local_file_system_t fs = core::filesystem::local_file_system_t();
    if (directory_exists(fs, test_folder)) {
        remove_directory(fs, test_folder);
    }
    create_directory(fs, test_folder);

    INFO("save databases into disk") {
        disk_t disk(test_folder, &resource);
        for (int num = 1; num <= 10; ++num) {
            REQUIRE(disk.append_database("database_" + std::to_string(num)));
        }
        auto databases = disk.databases();
        REQUIRE(databases.size() == 10);
    }

    INFO("load list databases from disk") {
        disk_t disk(test_folder, &resource);
        auto databases = disk.databases();
        REQUIRE(databases.size() == 10);
    }

    INFO("delete database from disk") {
        disk_t disk(test_folder, &resource);
        for (int num = 1; num <= 10; num += 2) {
            REQUIRE(disk.remove_database("database_" + std::to_string(num)));
        }
        auto databases = disk.databases();
        REQUIRE(databases.size() == 5);
    }

    remove_directory(fs, test_folder);
}