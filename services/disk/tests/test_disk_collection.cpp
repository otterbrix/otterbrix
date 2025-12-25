#include <catch2/catch.hpp>
#include <disk/disk.hpp>

const core::filesystem::path_t test_folder = "/tmp/collections";
const std::string database_name1 = "test_database1";
const std::string database_name2 = "test_database2";

using namespace services::disk;

TEST_CASE("services::disk::sync_collection_from_disk") {
    auto resource = std::pmr::synchronized_pool_resource();
    core::filesystem::local_file_system_t fs = core::filesystem::local_file_system_t();
    if (directory_exists(fs, test_folder)) {
        remove_directory(fs, test_folder);
    }
    create_directory(fs, test_folder);

    INFO("save collection into disk") {
        disk_t disk(test_folder, &resource);
        REQUIRE(disk.append_database(database_name1));
        REQUIRE(disk.append_database(database_name2));
        for (int num = 1; num <= 100; ++num) {
            if (num % 2 == 0) {
                REQUIRE(disk.append_collection(database_name1, "collection_" + std::to_string(num)));
            } else {
                REQUIRE(disk.append_collection(database_name2, "collection_" + std::to_string(num)));
            }
        }
    }

    INFO("load list collections from disk") {
        disk_t disk(test_folder, &resource);
        REQUIRE(disk.collections(database_name1).size() == 50);
        REQUIRE(disk.collections(database_name2).size() == 50);
    }

    INFO("delete collection from disk") {
        disk_t disk(test_folder, &resource);
        for (int num = 1; num <= 100; ++num) {
            REQUIRE(disk.remove_collection(database_name1, "collection_" + std::to_string(num)) == (num % 2 == 0));
            REQUIRE(disk.remove_collection(database_name2, "collection_" + std::to_string(num)) != (num % 2 == 0));
        }
        REQUIRE(disk.collections(database_name1).empty());
        REQUIRE(disk.collections(database_name2).empty());
    }

    remove_directory(fs, test_folder);
}