#include <catch2/catch.hpp>
#include <disk/catalog_storage.hpp>
#include <filesystem>
#include <fstream>
#include <unistd.h>

using namespace services::disk;

namespace {
    std::string test_dir() {
        static std::string path = "/tmp/test_otterbrix_catalog_" + std::to_string(::getpid());
        return path;
    }
    void cleanup_test_dir() { std::filesystem::remove_all(test_dir()); }
} // namespace

TEST_CASE("services::disk::catalog_storage::create_and_drop_database") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.append_database("db1");
        cs.append_database("db2");
        REQUIRE(cs.databases().size() == 2);
        REQUIRE(cs.database_exists("db1"));
        REQUIRE(cs.database_exists("db2"));
    }

    // Reload from disk
    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.load();
        REQUIRE(cs.databases().size() == 2);
        REQUIRE(cs.database_exists("db1"));
        REQUIRE(cs.database_exists("db2"));

        cs.remove_database("db1");
        REQUIRE(cs.databases().size() == 1);
        REQUIRE_FALSE(cs.database_exists("db1"));
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::create_and_drop_table") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
    cs.append_database("testdb");

    // In-memory table (no columns)
    catalog_table_entry_t im_table;
    im_table.name = "im_coll";
    im_table.storage_mode = table_storage_mode_t::IN_MEMORY;
    cs.append_table("testdb", im_table);

    // Disk table (3 columns)
    catalog_table_entry_t disk_table;
    disk_table.name = "disk_coll";
    disk_table.storage_mode = table_storage_mode_t::DISK;
    disk_table.columns = {
        {"id", components::types::logical_type::BIGINT},
        {"name", components::types::logical_type::STRING_LITERAL},
        {"value", components::types::logical_type::DOUBLE},
    };
    cs.append_table("testdb", disk_table);

    auto tables = cs.tables("testdb");
    REQUIRE(tables.size() == 2);

    auto* found_im = cs.find_table("testdb", "im_coll");
    REQUIRE(found_im != nullptr);
    REQUIRE(found_im->storage_mode == table_storage_mode_t::IN_MEMORY);
    REQUIRE(found_im->columns.empty());

    auto* found_disk = cs.find_table("testdb", "disk_coll");
    REQUIRE(found_disk != nullptr);
    REQUIRE(found_disk->storage_mode == table_storage_mode_t::DISK);
    REQUIRE(found_disk->columns.size() == 3);
    REQUIRE(found_disk->columns[0].name == "id");
    REQUIRE(found_disk->columns[0].type == components::types::logical_type::BIGINT);

    cs.remove_table("testdb", "im_coll");
    REQUIRE(cs.tables("testdb").size() == 1);

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::storage_mode_distinction") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.append_database("db");

        catalog_table_entry_t im_entry;
        im_entry.name = "mem_table";
        im_entry.storage_mode = table_storage_mode_t::IN_MEMORY;
        cs.append_table("db", im_entry);

        catalog_table_entry_t disk_entry;
        disk_entry.name = "disk_table";
        disk_entry.storage_mode = table_storage_mode_t::DISK;
        disk_entry.columns = {{"col1", components::types::logical_type::INTEGER}};
        cs.append_table("db", disk_entry);
    }

    // Reload and verify modes preserved
    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.load();
        auto* im = cs.find_table("db", "mem_table");
        auto* dk = cs.find_table("db", "disk_table");
        REQUIRE(im != nullptr);
        REQUIRE(dk != nullptr);
        REQUIRE(im->storage_mode == table_storage_mode_t::IN_MEMORY);
        REQUIRE(dk->storage_mode == table_storage_mode_t::DISK);
        REQUIRE(dk->columns.size() == 1);
        REQUIRE(dk->columns[0].name == "col1");
        REQUIRE(dk->columns[0].type == components::types::logical_type::INTEGER);
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::save_and_load_round_trip") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.append_database("db1");
        cs.append_database("db2");

        // db1: 2 tables
        catalog_table_entry_t t1;
        t1.name = "users";
        t1.storage_mode = table_storage_mode_t::DISK;
        t1.columns = {
            {"id", components::types::logical_type::BIGINT},
            {"name", components::types::logical_type::STRING_LITERAL},
        };
        cs.append_table("db1", t1);

        catalog_table_entry_t t2;
        t2.name = "logs";
        t2.storage_mode = table_storage_mode_t::IN_MEMORY;
        cs.append_table("db1", t2);

        // db2: 2 tables
        catalog_table_entry_t t3;
        t3.name = "events";
        t3.storage_mode = table_storage_mode_t::DISK;
        t3.columns = {
            {"ts", components::types::logical_type::TIMESTAMP_MS},
            {"data", components::types::logical_type::BLOB},
            {"count", components::types::logical_type::UINTEGER},
        };
        cs.append_table("db2", t3);

        catalog_table_entry_t t4;
        t4.name = "cache";
        t4.storage_mode = table_storage_mode_t::IN_MEMORY;
        cs.append_table("db2", t4);
    }

    // Reload and verify all data identical
    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.load();

        REQUIRE(cs.databases().size() == 2);
        REQUIRE(cs.database_exists("db1"));
        REQUIRE(cs.database_exists("db2"));

        REQUIRE(cs.tables("db1").size() == 2);
        REQUIRE(cs.tables("db2").size() == 2);

        auto* users = cs.find_table("db1", "users");
        REQUIRE(users != nullptr);
        REQUIRE(users->storage_mode == table_storage_mode_t::DISK);
        REQUIRE(users->columns.size() == 2);
        REQUIRE(users->columns[0].name == "id");
        REQUIRE(users->columns[1].name == "name");

        auto* events = cs.find_table("db2", "events");
        REQUIRE(events != nullptr);
        REQUIRE(events->columns.size() == 3);
        REQUIRE(events->columns[0].type == components::types::logical_type::TIMESTAMP_MS);
        REQUIRE(events->columns[1].type == components::types::logical_type::BLOB);
        REQUIRE(events->columns[2].type == components::types::logical_type::UINTEGER);

        auto* logs = cs.find_table("db1", "logs");
        REQUIRE(logs != nullptr);
        REQUIRE(logs->storage_mode == table_storage_mode_t::IN_MEMORY);
        REQUIRE(logs->columns.empty());
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::empty_catalog_load") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
    cs.load(); // file doesn't exist
    REQUIRE(cs.databases().empty());

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::checksum_validation") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    auto path = test_dir() + "/catalog.otbx";
    {
        catalog_storage_t cs(fs, path);
        cs.append_database("testdb");
    }

    // Corrupt 1 byte in file
    {
        std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.is_open());
        file.seekp(10); // somewhere in the payload
        char bad = 0xFF;
        file.write(&bad, 1);
        file.close();
    }

    // Load should throw due to checksum mismatch
    catalog_storage_t cs2(fs, path);
    REQUIRE_THROWS(cs2.load());

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::constraint_round_trip") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.append_database("db");

        catalog_table_entry_t tbl;
        tbl.name = "constrained";
        tbl.storage_mode = table_storage_mode_t::DISK;
        tbl.columns = {
            {"id", components::types::logical_type::BIGINT, true, false},
            {"name", components::types::logical_type::STRING_LITERAL, false, true},
            {"score", components::types::logical_type::DOUBLE, false, false},
        };
        tbl.primary_key_columns = {"id"};
        cs.append_table("db", tbl);
    }

    // Reload and verify constraints preserved
    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.load();
        auto* found = cs.find_table("db", "constrained");
        REQUIRE(found != nullptr);
        REQUIRE(found->columns.size() == 3);
        REQUIRE(found->columns[0].not_null == true);
        REQUIRE(found->columns[0].has_default == false);
        REQUIRE(found->columns[1].not_null == false);
        REQUIRE(found->columns[1].has_default == true);
        REQUIRE(found->columns[2].not_null == false);
        REQUIRE(found->columns[2].has_default == false);
        REQUIRE(found->primary_key_columns.size() == 1);
        REQUIRE(found->primary_key_columns[0] == "id");
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::sequence_crud") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.append_database("db");

        catalog_sequence_entry_t seq1;
        seq1.name = "seq1";
        seq1.start_value = 10;
        seq1.increment = 2;
        seq1.current_value = 10;
        seq1.min_value = 1;
        seq1.max_value = 1000;
        cs.append_sequence("db", seq1);

        catalog_sequence_entry_t seq2;
        seq2.name = "seq2";
        cs.append_sequence("db", seq2);

        REQUIRE(cs.sequences("db").size() == 2);
    }

    // Reload and verify
    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.load();
        auto seqs = cs.sequences("db");
        REQUIRE(seqs.size() == 2);
        REQUIRE(seqs[0].name == "seq1");
        REQUIRE(seqs[0].start_value == 10);
        REQUIRE(seqs[0].increment == 2);
        REQUIRE(seqs[0].max_value == 1000);

        cs.remove_sequence("db", "seq1");
        REQUIRE(cs.sequences("db").size() == 1);
        REQUIRE(cs.sequences("db")[0].name == "seq2");
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::view_crud") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.append_database("db");

        catalog_view_entry_t v1;
        v1.name = "my_view";
        v1.query_sql = "SELECT * FROM db.tbl WHERE id > 0";
        cs.append_view("db", v1);

        REQUIRE(cs.views("db").size() == 1);
    }

    // Reload and verify
    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.load();
        auto views = cs.views("db");
        REQUIRE(views.size() == 1);
        REQUIRE(views[0].name == "my_view");
        REQUIRE(views[0].query_sql == "SELECT * FROM db.tbl WHERE id > 0");

        cs.remove_view("db", "my_view");
        REQUIRE(cs.views("db").empty());
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::catalog_storage::macro_crud") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    core::filesystem::local_file_system_t fs;

    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.append_database("db");

        catalog_macro_entry_t m1;
        m1.name = "add_one";
        m1.parameters = {"x"};
        m1.body_sql = "x + 1";
        cs.append_macro("db", m1);

        catalog_macro_entry_t m2;
        m2.name = "add_two";
        m2.parameters = {"a", "b"};
        m2.body_sql = "a + b";
        cs.append_macro("db", m2);

        REQUIRE(cs.macros("db").size() == 2);
    }

    // Reload and verify
    {
        catalog_storage_t cs(fs, test_dir() + "/catalog.otbx");
        cs.load();
        auto macros = cs.macros("db");
        REQUIRE(macros.size() == 2);
        REQUIRE(macros[0].name == "add_one");
        REQUIRE(macros[0].parameters.size() == 1);
        REQUIRE(macros[0].parameters[0] == "x");
        REQUIRE(macros[0].body_sql == "x + 1");
        REQUIRE(macros[1].name == "add_two");
        REQUIRE(macros[1].parameters.size() == 2);

        cs.remove_macro("db", "add_one");
        REQUIRE(cs.macros("db").size() == 1);
    }

    cleanup_test_dir();
}
