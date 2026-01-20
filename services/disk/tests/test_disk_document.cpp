#include <catch2/catch.hpp>
#include <components/document/document.hpp>
#include <components/tests/generaty.hpp>
#include <disk/disk.hpp>

const core::filesystem::path_t test_folder = "/tmp/documents";
const std::string database_name = "test_database";
const std::string collection_name = "test_collection";

using namespace services::disk;

TEST_CASE("services::disk::sync_documents_from_disk") {
    auto resource = std::pmr::synchronized_pool_resource();
    core::filesystem::local_file_system_t fs = core::filesystem::local_file_system_t();
    if (directory_exists(fs, test_folder)) {
        remove_directory(fs, test_folder);
    }
    create_directory(fs, test_folder);

    INFO("save document into disk") {
        disk_t disk(test_folder, &resource);
        for (int num = 1; num <= 100; ++num) {
            disk.save_document(database_name, collection_name, gen_doc(num, &resource));
        }
        REQUIRE(disk.load_list_documents(database_name, collection_name).size() == 100);
    }

    INFO("load document from disk") {
        disk_t disk(test_folder, &resource);
        for (int num = 1; num <= 100; ++num) {
            auto doc = disk.load_document(database_name, collection_name, document_id_t(gen_id(num, &resource)));
            REQUIRE(doc != nullptr);
            REQUIRE(doc->get_string("_id") == gen_id(num, &resource));
            REQUIRE(doc->get_long("count") == num);
            auto str1 = doc->get_string("count_str");
            auto str2 = core::pmr::to_pmr_string(&resource, num);
            REQUIRE(str1 == str2);
            REQUIRE(doc->get_double("count_double") == Approx(float(num) + 0.1));
            REQUIRE(doc->get_bool("count_bool") == (num % 2 != 0));
            REQUIRE(doc->get_array("count_array")->count() == 5);
            REQUIRE(doc->get_dict("count_dict")->count() == 4);
        }
    }

    INFO("delete document from disk") {
        disk_t disk(test_folder, &resource);
        for (int num = 1; num <= 100; num += 2) {
            disk.remove_document(database_name, collection_name, document_id_t(gen_id(num, &resource)));
        }
        for (int num = 1; num <= 100; ++num) {
            auto doc = disk.load_document(database_name, collection_name, document_id_t(gen_id(num, &resource)));
            if (num % 2 == 0) {
                REQUIRE(doc != nullptr);
                REQUIRE(doc->get_string("_id") == gen_id(num, &resource));
                REQUIRE(doc->get_long("count") == num);
                REQUIRE(doc->get_string("count_str") == core::pmr::to_pmr_string(&resource, num));
                REQUIRE(doc->get_double("count_double") == Approx(float(num) + 0.1));
                REQUIRE(doc->get_bool("count_bool") == (num % 2 != 0));
                REQUIRE(doc->get_array("count_array")->count() == 5);
                REQUIRE(doc->get_dict("count_dict")->count() == 4);
            } else {
                REQUIRE(doc == nullptr);
            }
        }
    }

    INFO("load list documents from disk") {
        disk_t disk(test_folder, &resource);
        auto id_documents = disk.load_list_documents(database_name, collection_name);
        REQUIRE(id_documents.size() == 50);
        for (const auto& id : id_documents) {
            auto doc = disk.load_document(database_name, collection_name, id);
            REQUIRE(doc != nullptr);
        }
    }

    remove_directory(fs, test_folder);
}