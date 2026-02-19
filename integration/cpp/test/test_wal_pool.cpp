#include "test_config.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/tests/generaty.hpp>

#include <catch2/catch.hpp>
#include <filesystem>

using components::expressions::compare_type;
using components::expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;
using namespace components::types;

// NOTE: SQL parser lowercases identifiers, so API names must be lowercase
static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";
static const collection_name_t collection_name_2 = "testcollection2";

#define INIT_COLLECTION_WAL(DB, COLL)                                                                                  \
    do {                                                                                                               \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->create_database(session, DB);                                                                  \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            auto types = gen_data_chunk(0, dispatcher->resource()).types();                                            \
            dispatcher->create_collection(session, DB, COLL, types);                                                   \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION_WAL(DB, COLL, COUNT)                                                                           \
    do {                                                                                                               \
        auto chunk = gen_data_chunk(COUNT, dispatcher->resource());                                                    \
        auto ins = components::logical_plan::make_node_insert(dispatcher->resource(),                                  \
                                                              {DB, COLL},                                              \
                                                              std::move(chunk));                                        \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->execute_plan(session, ins);                                                                    \
        }                                                                                                              \
    } while (false)

#define CHECK_FIND_WAL(DB, COLL, KEY, COMPARE, VALUE, COUNT)                                                           \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto plan =                                                                                                    \
            components::logical_plan::make_node_aggregate(dispatcher->resource(), {DB, COLL});                         \
        auto expr = components::expressions::make_compare_expression(dispatcher->resource(),                           \
                                                                     COMPARE,                                          \
                                                                     key{dispatcher->resource(), KEY, side_t::left},   \
                                                                     id_par{1});                                       \
        plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),                           \
                                                                     {DB, COLL},                                       \
                                                                     std::move(expr)));                                \
        auto params = components::logical_plan::make_parameter_node(dispatcher->resource());                           \
        params->add_parameter(id_par{1}, VALUE);                                                                       \
        auto c = dispatcher->find(session, plan, params);                                                              \
        REQUIRE(c->size() == COUNT);                                                                                   \
    } while (false)

#define CHECK_FIND_SQL_WAL(QUERY, COUNT)                                                                               \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto cur = dispatcher->execute_sql(session, QUERY);                                                            \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == COUNT);                                                                                 \
    } while (false)


TEST_CASE("integration::cpp::test_wal_pool::per_worker_files_created") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_wal_pool/per_worker_files");
    test_clear_directory(config);

    INFO("insert data to trigger WAL writes") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION_WAL(database_name, collection_name);
        FILL_COLLECTION_WAL(database_name, collection_name, 100);
    }

    INFO("verify per-worker WAL segment files exist") {
        // With agent=2, should have .wal_0_000000 and .wal_1_000000
        auto wal_path_0 = config.wal.path / ".wal_0_000000";
        auto wal_path_1 = config.wal.path / ".wal_1_000000";
        REQUIRE(std::filesystem::exists(wal_path_0));
        REQUIRE(std::filesystem::exists(wal_path_1));

        // Legacy single .wal should NOT exist
        auto legacy_wal_path = config.wal.path / ".wal";
        REQUIRE_FALSE(std::filesystem::exists(legacy_wal_path));

        // At least one WAL file should have non-zero size (data was written)
        auto size_0 = std::filesystem::file_size(wal_path_0);
        auto size_1 = std::filesystem::file_size(wal_path_1);
        REQUIRE((size_0 > 0 || size_1 > 0));
    }
}


TEST_CASE("integration::cpp::test_wal_pool::recovery_after_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_wal_pool/recovery");
    test_clear_directory(config);

    constexpr int kDocuments = 100;

    INFO("phase 1: create and fill data") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION_WAL(database_name, collection_name);
        FILL_COLLECTION_WAL(database_name, collection_name, kDocuments);

        // Verify data exists before restart
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1;", 1);
    }

    INFO("phase 2: restart and verify data recovered from WAL") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 100;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count > 90;", 10);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count <= 5;", 5);
    }
}


TEST_CASE("integration::cpp::test_wal_pool::index_durability") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_wal_pool/index_durability");
    test_clear_directory(config);

    constexpr int kDocuments = 100;

    INFO("phase 1: create collection, index, and fill data") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION_WAL(database_name, collection_name);

        // Create index via SQL (this should now go through WAL)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
            REQUIRE(cur->is_success());
        }

        FILL_COLLECTION_WAL(database_name, collection_name, kDocuments);

        // Verify index works before restart
        CHECK_FIND_WAL(database_name, collection_name, "count", compare_type::eq,
                       logical_value_t(dispatcher->resource(), 50), 1);
    }

    INFO("phase 2: restart and verify index survived") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_WAL(database_name, collection_name, "count", compare_type::eq,
                       logical_value_t(dispatcher->resource(), 1), 1);
        CHECK_FIND_WAL(database_name, collection_name, "count", compare_type::eq,
                       logical_value_t(dispatcher->resource(), 50), 1);
        CHECK_FIND_WAL(database_name, collection_name, "count", compare_type::gt,
                       logical_value_t(dispatcher->resource(), 90), 10);
    }
}


TEST_CASE("integration::cpp::test_wal_pool::multiple_collections_routing") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_wal_pool/multi_coll_routing");
    test_clear_directory(config);

    constexpr int kDocuments = 50;

    INFO("insert into two collections") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Collection 1
        INIT_COLLECTION_WAL(database_name, collection_name);
        FILL_COLLECTION_WAL(database_name, collection_name, kDocuments);

        // Collection 2
        {
            auto session = otterbrix::session_id_t();
            auto types = gen_data_chunk(0, dispatcher->resource()).types();
            dispatcher->create_collection(session, database_name, collection_name_2, types);
        }
        FILL_COLLECTION_WAL(database_name, collection_name_2, kDocuments);

        // Both should be queryable
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 25;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection2 WHERE count = 25;", 1);
    }

    INFO("verify both WAL segment files have data") {
        auto wal_path_0 = config.wal.path / ".wal_0_000000";
        auto wal_path_1 = config.wal.path / ".wal_1_000000";
        REQUIRE(std::filesystem::exists(wal_path_0));
        REQUIRE(std::filesystem::exists(wal_path_1));
    }

    INFO("restart and verify both collections recovered") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection2 WHERE count = 1;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection2 WHERE count = 50;", 1);
    }
}


TEST_CASE("integration::cpp::test_wal_pool::update_wal_recovery") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_wal_pool/update_recovery");
    test_clear_directory(config);

    constexpr int kDocuments = 100;

    INFO("phase 1: insert, update, and verify") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION_WAL(database_name, collection_name);
        FILL_COLLECTION_WAL(database_name, collection_name, kDocuments);

        // UPDATE count=999 WHERE count=50
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "UPDATE TestDatabase.TestCollection SET count = 999 WHERE count = 50;");
            REQUIRE(cur->is_success());
        }

        // Updated value should exist
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
        // Old value should be gone
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);
    }

    INFO("phase 2: restart and verify WAL replayed UPDATE") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Updated value should survive restart
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
        // Old value should still be gone
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);
    }
}


TEST_CASE("integration::cpp::test_wal_pool::sql_dml_full_cycle") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_wal_pool/sql_dml_cycle");
    test_clear_directory(config);

    constexpr int kDocuments = 100;

    INFO("phase 1: insert, delete, update via SQL with index") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION_WAL(database_name, collection_name);

        // Create index on count column — makes index path real
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
            REQUIRE(cur->is_success());
        }

        // INSERT 100 rows via SQL (count = 0..99)
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < kDocuments; ++i) {
                query << "('name_" << i << "', " << i << ")" << (i == kDocuments - 1 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kDocuments);
        }

        // Verify insert: total + exact match + range + boundary
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", kDocuments);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;", 9);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 1;", 1);

        // DELETE WHERE count > 90 (deletes 9 rows: 91..99)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        // Verify delete: deleted gone + boundary intact
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 91);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 95;", 0);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;", 0);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 90;", 1);

        // UPDATE SET count = 999 WHERE count = 50
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "UPDATE TestDatabase.TestCollection SET count = 999 WHERE count = 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        // Verify update: old gone, new present, total unchanged
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 91);
    }

    INFO("phase 2: restart and verify durability") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Total rows: 91 (100 - 9 deleted)
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 91);

        // Deleted rows stay deleted, only count=999 survives above 90
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;", 1);

        // Updated row persisted
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);

        // Original value gone after update
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);

        // Deleted rows should not reappear
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE count = 95;", 0);
    }
}


TEST_CASE("integration::cpp::test_wal_pool::sql_constraint_enforcement") {
    auto config = test_create_config(
        "/tmp/otterbrix/integration/test_wal_pool/constraint_enforce");
    test_clear_directory(config);

    INFO("phase 1: create table with NOT NULL, test enforcement") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Create database
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }

        // Create table with NOT NULL on a string column
        // (SQL integer literals produce BIGINT, not INTEGER — use string to avoid type mismatch)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "CREATE TABLE TestDatabase.TestCollection "
                "(name string, tag string NOT NULL);");
            REQUIRE(cur->is_success());
        }

        // INSERT valid data
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.TestCollection (name, tag) VALUES "
                "('alice', 'red'), ('bob', 'green'), ('charlie', 'blue');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE tag = 'red';", 1);

        // Attempt INSERT with NULL in NOT NULL column — rejected (0 rows)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.TestCollection (name, tag) "
                "VALUES ('dave', NULL);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }

        // Only original 3 rows exist (violation didn't corrupt state)
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 3);

        // INSERT more valid data after violation (system not broken)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.TestCollection (name, tag) VALUES "
                "('eve', 'yellow'), ('frank', 'white');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 5);
    }

    INFO("phase 2: restart and verify constraint state persisted") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // All 5 valid rows survived restart
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE tag = 'red';", 1);
        CHECK_FIND_SQL_WAL(
            "SELECT * FROM TestDatabase.TestCollection WHERE tag = 'white';", 1);

        // NOT NULL constraint still enforced after restart
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "INSERT INTO TestDatabase.TestCollection (name, tag) "
                "VALUES ('ghost', NULL);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }

        // Still 5 rows
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 5);
    }
}

TEST_CASE("integration::cpp::test_wal_pool::constant_data_checkpoint_restart") {
    auto config = test_create_config(
        "/tmp/otterbrix/integration/test_wal_pool/constant_checkpoint");
    test_clear_directory(config);

    INFO("phase 1: create table, insert 100 constant-value rows, checkpoint") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }

        // Create table with a typed schema
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "CREATE TABLE TestDatabase.TestCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }

        // INSERT 100 rows all with count=42
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 100; ++i) {
                query << "('const_" << i << "', 42)" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 100);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 42;", 100);

        // CHECKPOINT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify data recovered from checkpoint") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 100);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 42;", 100);
    }
}

TEST_CASE("integration::cpp::test_wal_pool::insert_delete_checkpoint_restart") {
    auto config = test_create_config(
        "/tmp/otterbrix/integration/test_wal_pool/insert_delete_checkpoint");
    test_clear_directory(config);

    INFO("phase 1: insert 100 rows, delete where count < 50, checkpoint") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "CREATE TABLE TestDatabase.TestCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }

        // INSERT 100 rows with count = 0..99
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 100; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 100);

        // DELETE WHERE count < 50 (removes 50 rows: 0..49)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                "DELETE FROM TestDatabase.TestCollection WHERE count < 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 50);

        // CHECKPOINT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify correct rows survive") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection;", 50);
        // Deleted rows should not reappear
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 0);
        // Surviving rows should be queryable
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL_WAL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}
