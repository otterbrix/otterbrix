#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <components/catalog/catalog_oids.hpp>
#include <cstdlib>
#include <filesystem>
#include <set>
#include <sstream>
#include <thread>

using namespace components::types;

static const database_name_t database_name = "testdatabase";

#define CHECK_FIND_SQL(QUERY, COUNT)                                                                                   \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto cur = dispatcher->execute_sql(session, QUERY);                                                            \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == static_cast<std::size_t>(COUNT));                                                       \
    } while (false)

TEST_CASE("integration::cpp::test_persistence::wal_recovery_mixed_batch") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/wal_mixed_batch");
    test_clear_directory(config);

    INFO("phase 1: insert two batches (no checkpoint)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }

        // INSERT first 50 rows (count = 0..49)
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 50; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 49 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);

        // INSERT 50 more rows (count = 50..99)
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 50; i < 100; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
    }

    INFO("phase 2: restart — all 100 rows from WAL");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::wal_recovery_multi_type") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/wal_multi_type");
    test_clear_directory(config);

    constexpr int kDocuments = 50;

    INFO("phase 1: create table with multiple types, insert");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection (id bigint, name string, score double);");
            REQUIRE(cur->is_success());
        }

        // INSERT rows with all 3 types
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (id, name, score) VALUES ";
            for (int i = 0; i < kDocuments; ++i) {
                query << "(" << i << ", 'item_" << i << "', " << (i + 0.5) << ")" << (i == kDocuments - 1 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kDocuments);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", kDocuments);
    }

    INFO("phase 2: restart and verify all types recovered from WAL");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", kDocuments);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 25;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 49;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'item_10';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'item_40';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 0.5;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 25.5;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::wal_recovery_not_null") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/wal_not_null");
    test_clear_directory(config);

    INFO("phase 1: create table with NOT NULL, insert valid data");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
    }

    INFO("phase 2: restart and verify data + NOT NULL constraint enforced");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE tag = 'red';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE tag = 'green';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE tag = 'blue';", 1);

        // NOT NULL constraint must still be enforced after restart
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, tag) "
                                               "VALUES ('ghost', NULL);");
            REQUIRE(cur->is_error());
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);

        // Valid insert still works after restart
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name, tag) VALUES ('dave', 'yellow');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 4);
    }
}

TEST_CASE("integration::cpp::test_persistence::wal_recovery_dml_full_cycle") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/wal_dml_cycle");
    test_clear_directory(config);

    INFO("phase 1: insert, delete, update (no checkpoint)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);

        // DELETE WHERE count > 90 (removes 9 rows: 91..99)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

        // UPDATE SET count=999 WHERE count=50
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection SET count = 999 WHERE count = 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);
    }

    INFO("phase 2: restart and verify full DML cycle survived WAL recovery");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);
        // Deleted rows stay gone
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 95;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count > 90;", 1);
        // Updated value persisted
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
        // Original updated value gone
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);
        // Boundary rows intact
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 90;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::default_application_in_session") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/default_application");
    test_clear_directory(config);

    INFO("verify DEFAULT values are applied during INSERT within a single session");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "CREATE TABLE TestDatabase.TestCollection "
                                        "(name string, status string DEFAULT 'active', count bigint DEFAULT 0);");
            REQUIRE(cur->is_success());
        }

        // INSERT omitting all defaulted columns — only provide name
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name) VALUES "
                                               "('alice'), ('bob'), ('charlie');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }

        // Verify defaults applied: status='active', count=0
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 3);

        // INSERT omitting only one defaulted column — provide name + count
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, count) VALUES "
                                               "('dave', 10), ('eve', 20);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        // dave and eve have status='active' (default), count explicit
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 10;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 20;", 1);

        // INSERT with all columns — override defaults
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, status, count) VALUES "
                                               "('frank', 'inactive', 99);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 6);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'inactive';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::partial_insert_consistent_wal_recovery") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/partial_insert_wal");
    test_clear_directory(config);

    INFO("phase 1: insert with consistent partial columns (only name)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "CREATE TABLE TestDatabase.TestCollection "
                                        "(name string, status string DEFAULT 'active', count bigint DEFAULT 0);");
            REQUIRE(cur->is_success());
        }

        // All INSERTs use only (name) — WAL records all have 1 column
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name) VALUES "
                                               "('alice'), ('bob'), ('charlie'), ('dave'), ('eve');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
        }

        // Verify defaults applied in session
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'eve';", 1);
    }

    INFO("phase 2: restart — WAL replay with the full post-default chunk");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // The PHYSICAL_INSERT carries the chunk AFTER the disk agent's default-expansion
        // stage, so status='active'/count=0 are baked into the WAL record (not just the
        // supplied 'name'). On restart the storage is synthesised from the WAL chunk's
        // column types, so the defaulted columns and their values survive replay.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'bob';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'eve';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 5);
    }
}

TEST_CASE("integration::cpp::test_persistence::wal_recovery_not_null_with_default") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/wal_not_null_default");
    test_clear_directory(config);

    INFO("phase 1: create table with NOT NULL + DEFAULT, test enforcement + defaults");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection "
                                               "(name string NOT NULL, status string NOT NULL DEFAULT 'pending');");
            REQUIRE(cur->is_success());
        }

        // INSERT providing all columns
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, status) VALUES "
                                               "('alice', 'pending'), ('bob', 'approved'), ('charlie', 'pending');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'pending';", 2);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'approved';", 1);

        // NOT NULL on name: INSERT with NULL name should be rejected
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name, status) VALUES (NULL, 'test');");
            REQUIRE(cur->is_error());
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
    }

    INFO("phase 2: restart and verify NOT NULL + DEFAULT constraints");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'pending';", 2);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'approved';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);

        // NOT NULL still enforced after restart
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name, status) VALUES (NULL, 'test');");
            REQUIRE(cur->is_error());
        }

        // Valid insert still works
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name, status) VALUES ('dave', 'rejected');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 4);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'rejected';", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::partial_insert_two_columns_wal") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/partial_two_cols_wal");
    test_clear_directory(config);

    INFO("phase 1: insert providing 2 of 3 columns (consistent partial)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection "
                                               "(name string, score bigint, tag string DEFAULT 'untagged');");
            REQUIRE(cur->is_success());
        }

        // All INSERTs provide (name, score) — 2 columns consistently; tag uses default
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, score) VALUES "
                                               "('alice', 100), ('bob', 200), ('charlie', 300);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 100;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 200;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE tag = 'untagged';", 3);
    }

    INFO("phase 2: restart — 2-column WAL records replayed consistently");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // name and score columns survive (both in WAL records)
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 100;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 200;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 300;", 1);
        // The PHYSICAL_INSERT carries the default-expanded chunk, so the unsupplied 'tag'
        // column (DEFAULT 'untagged') is durable across restart too.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE tag = 'untagged';", 3);
    }
}

// Dynamic-schema (relkind='g' computed) tables grow their column set per-INSERT
// (stage 1b in agent_disk::storage_append_inner). Every table is disk-backed and the
// computed flag is itself durable, so growth still works after restart.
// The growth is made durable by emitting a PHYSICAL_ADD_COLUMN WAL record (the new
// columns as a 0-row alias-tagged chunk) BEFORE the dependent PHYSICAL_INSERT, and
// replaying it on restart (base_spaces replay loop -> direct_add_column_sync) so the
// grown schema is reconstructed ahead of the rows that reference it.
//
// Scenario: a computing table is created with WAL ON, a first INSERT introduces (id,
// name), a second INSERT introduces an ADDITIONAL new column (value) — triggering stage-1b
// growth and a PHYSICAL_ADD_COLUMN record — then the engine is restarted and ALL columns +
// rows must survive.
//
// Computed tables have a file behind them: a disk-backed .otbx whose computed flag is
// restored from pg_class.relkind on load.
TEST_CASE("integration::cpp::test_persistence::computed_schema_growth_wal_recovery") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/computed_schema_growth_wal");
    test_clear_directory(config);

    INFO("phase 1: computing table, two INSERTs growing the schema, WAL ON");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        // CREATE TABLE with no columns => relkind='g' (computed, dynamic schema).
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection ();");
            REQUIRE(cur->is_success());
        }

        // First INSERT: introduces columns (id bigint, name string). Schema adopted.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (id, name) VALUES "
                                               "(1, 'alice'), (2, 'bob');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        // Second INSERT: introduces an ADDITIONAL new column 'value' alongside the
        // existing (id, name). This is stage-1b schema growth and emits a
        // PHYSICAL_ADD_COLUMN WAL record before the PHYSICAL_INSERT.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (id, name, value) VALUES "
                                               "(3, 'charlie', 100);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        // In-session sanity: 3 rows, 3 columns (id, name, value); rows 1-2 NULL for value.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            REQUIRE(cur->column_count() == 3);
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE value = 100;", 1);
    }

    INFO("phase 2: restart — PHYSICAL_ADD_COLUMN replay reconstructs the grown schema");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // All three rows survive WAL replay.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);

        // The dynamically-added 'value' column survives: its schema was reconstructed by
        // replaying PHYSICAL_ADD_COLUMN ahead of the PHYSICAL_INSERT that filled it.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            REQUIRE(cur->column_count() == 3);
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE value = 100;", 1);

        // The first-INSERT columns are still queryable and bind to the right rows.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'charlie';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 3;", 1);
    }
}

// End-to-end gate, clean-restart leg: the computed (relkind='g') flag must
// survive a restart, proven by the one behavior only the flag enables — merging a
// NEW type variant of an existing field name into its OWN physical column. A table
// that lost is_computed on reload would match the post-restart {a:bool} chunk by
// name alone and glue the boolean vector into the bigint 'a' column.
//
// {a:int}, {a:string}, restart, {a:bool}: SELECT * must return all three rows with
// each variant in its own column.
TEST_CASE("integration::cpp::test_persistence::computed_type_variants_survive_restart") {
    auto config = test_create_config(integration_fixture_path("test_persistence/computed_variants_restart"));
    test_clear_directory(config);

    INFO("phase 1: computed table, two type variants of 'a' (bigint, string)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection ();");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id, a) VALUES (1, 10);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id, a) VALUES (2, 'str');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->column_count() == 3); // id, a:bigint, a:string
        }
        // In-session '::?type' variant selection (a plain column ref, not a jsonb
        // chain) — the transformer must mark the key variant_select, not lower it
        // to a cast, or validation refuses the multi-type name as ambiguous.
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT id, a::?bigint FROM TestDatabase.TestCollection ORDER BY id;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 2);
            REQUIRE(c2->value(1, 0).value<int64_t>() == 10);
            REQUIRE(c2->value(1, 1).is_null());
        }
        // Pushed-down WHERE on the multi-type table (in-session baseline).
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE id = 1;", 1);
    }

    INFO("phase 2: restart, add the THIRD type variant (bool), all three merge");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Both pre-restart variants replayed.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);

        // Pushed-down WHERE and the '::?' selector still work on the reloaded
        // multi-type table BEFORE any new write.
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE id = 1;", 1);
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT a::?bigint FROM TestDatabase.TestCollection WHERE id = 1;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 1);
            REQUIRE(c2->value(0, 0).value<int64_t>() == 10);
        }

        // The variant added AFTER the restart is the proof the flag survived:
        // stage-1b growth must key on (name, type), which only runs computed.
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id, a) VALUES (3, true);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->column_count() == 4); // id + three 'a' variants

        // Locate the three 'a' variants by physical type — each in its OWN column.
        const auto& chunk = cur->chunks().front();
        int a_bigint = -1, a_string = -1, a_bool = -1;
        for (size_t c = 0; c < chunk.column_count(); ++c) {
            if (std::string(chunk.data[c].type().alias()) != "a") {
                continue;
            }
            switch (chunk.data[c].type().type()) {
                case logical_type::BIGINT:
                    a_bigint = static_cast<int>(c);
                    break;
                case logical_type::STRING_LITERAL:
                    a_string = static_cast<int>(c);
                    break;
                case logical_type::BOOLEAN:
                    a_bool = static_cast<int>(c);
                    break;
                default:
                    break;
            }
        }
        REQUIRE(a_bigint >= 0);
        REQUIRE(a_string >= 0);
        REQUIRE(a_bool >= 0);

        // bigint variant: only row id=1.
        REQUIRE(chunk.get_value<int64_t>(static_cast<size_t>(a_bigint), 0) == 10);
        REQUIRE(chunk.value(static_cast<size_t>(a_bigint), 1).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(a_bigint), 2).is_null());
        // string variant: only row id=2.
        REQUIRE(chunk.value(static_cast<size_t>(a_string), 0).is_null());
        REQUIRE(chunk.get_value<std::string_view>(static_cast<size_t>(a_string), 1) == "str");
        REQUIRE(chunk.value(static_cast<size_t>(a_string), 2).is_null());
        // bool variant: only row id=3 (the post-restart insert).
        REQUIRE(chunk.value(static_cast<size_t>(a_bool), 0).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(a_bool), 1).is_null());
        REQUIRE(chunk.get_value<bool>(static_cast<size_t>(a_bool), 2) == true);

        // The ::?type variant selector agrees.
        // Each '::?type' selector picks its own variant after the growth.
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT a::?bigint FROM TestDatabase.TestCollection WHERE id = 1;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 1);
            REQUIRE(c2->value(0, 0).value<int64_t>() == 10);
        }
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT a::?string FROM TestDatabase.TestCollection WHERE id = 2;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 1);
            REQUIRE(c2->value(0, 0).value<std::string_view>() == "str");
        }
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT a::?bool FROM TestDatabase.TestCollection;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 3);
        }
        // Pushed-down WHERE still matches AFTER the post-restart growth. This pinned
        // the reopened-oid-generator defect: the (a, bool) registration re-minted an
        // attoid already taken by a persisted computed column, the duplicate broke
        // resolve_table's attoid order against the storage order, and every pushed
        // filter on the table matched zero rows.
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE id = 3;", 1);
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT a::?bool FROM TestDatabase.TestCollection WHERE id = 3;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 1);
            REQUIRE(c2->value(0, 0).value<bool>() == true);
        }
    }
}

// End-to-end gate, crash leg: the computed flag must survive WAL replay
// SYNTHESIS, not only a clean reload of the .otbx. Crash model (same kill -9
// simulation as test_persistence_gaps::create_then_kill_before_checkpoint): copy
// the LIVE data directory, then delete the user table's storage directory from the
// COPY — a freshly created .otbx's directory entry is not fsynced, so a real crash
// durably keeps the pg_class row and the WAL while losing the file (the exact
// scenario base_spaces' replay-synthesis branch exists for). On reopen the storage
// is synthesised from the WAL chunks' column types; the synthesised entry must
// still be computed (from pg_class.relkind), or the post-recovery {a:bool} insert
// would be glued into the bigint 'a' column instead of a new variant column.
TEST_CASE("integration::cpp::test_persistence::computed_type_variants_survive_crash_replay_synthesis") {
    auto config = test_create_config(integration_fixture_path("test_persistence/computed_variants_crash_src"));
    test_clear_directory(config);

    const std::filesystem::path crash_dir =
        integration_fixture_path("test_persistence/computed_variants_crash_copy");

    INFO("phase 1: computed table, two type variants; copy the live directory (crash image)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection ();");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id, a) VALUES (1, 10);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id, a) VALUES (2, 'str');");
            REQUIRE(cur->is_success());
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);

        std::filesystem::remove_all(crash_dir);
        std::filesystem::create_directories(crash_dir.parent_path());
        std::filesystem::copy(config.main_path, crash_dir, std::filesystem::copy_options::recursive);
    }

    // Model the lost directory entry: drop every USER table's storage directory from
    // the crash image (oid >= FIRST_USER_OID; system tables keep theirs). The WAL —
    // which carries the PHYSICAL_INSERT / PHYSICAL_ADD_COLUMN records — survives.
    {
        std::vector<std::filesystem::path> user_table_dirs;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(crash_dir)) {
            if (!entry.is_regular_file() || entry.path().filename() != "table.otbx") {
                continue;
            }
            const std::string oid_dir = entry.path().parent_path().filename().string();
            char* end = nullptr;
            const unsigned long oid = std::strtoul(oid_dir.c_str(), &end, 10);
            if (end && *end == '\0' && oid >= components::catalog::FIRST_USER_OID) {
                user_table_dirs.push_back(entry.path().parent_path());
            }
        }
        INFO("the crash image must contain exactly the one user table's storage dir");
        REQUIRE(user_table_dirs.size() == 1);
        std::filesystem::remove_all(user_table_dirs.front());
    }

    INFO("phase 2: reopen the crash image — replay synthesis rebuilds the computed storage");
    {
        auto crash_config = test_create_config(crash_dir);
        test_spaces space(crash_config);
        auto* dispatcher = space.dispatcher();

        // Both pre-crash rows come back through the synthesised storage.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);

        // Third type variant AFTER the crash recovery: only a synthesised entry that
        // kept is_computed grows a NEW (a, bool) column instead of gluing by name.
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id, a) VALUES (3, true);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->column_count() == 4); // id + three 'a' variants

        const auto& chunk = cur->chunks().front();
        int a_bigint = -1, a_string = -1, a_bool = -1;
        for (size_t c = 0; c < chunk.column_count(); ++c) {
            if (std::string(chunk.data[c].type().alias()) != "a") {
                continue;
            }
            switch (chunk.data[c].type().type()) {
                case logical_type::BIGINT:
                    a_bigint = static_cast<int>(c);
                    break;
                case logical_type::STRING_LITERAL:
                    a_string = static_cast<int>(c);
                    break;
                case logical_type::BOOLEAN:
                    a_bool = static_cast<int>(c);
                    break;
                default:
                    break;
            }
        }
        REQUIRE(a_bigint >= 0);
        REQUIRE(a_string >= 0);
        REQUIRE(a_bool >= 0);

        REQUIRE(chunk.get_value<int64_t>(static_cast<size_t>(a_bigint), 0) == 10);
        REQUIRE(chunk.value(static_cast<size_t>(a_bigint), 1).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(a_bigint), 2).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(a_string), 0).is_null());
        REQUIRE(chunk.get_value<std::string_view>(static_cast<size_t>(a_string), 1) == "str");
        REQUIRE(chunk.value(static_cast<size_t>(a_string), 2).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(a_bool), 0).is_null());
        REQUIRE(chunk.value(static_cast<size_t>(a_bool), 1).is_null());
        REQUIRE(chunk.get_value<bool>(static_cast<size_t>(a_bool), 2) == true);

        // Pushed-down WHERE and the '::?' selector across replay synthesis + growth
        // (same attoid-frontier and variant-column pins as the clean-restart test).
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE id = 3;", 1);
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT a::?bool FROM TestDatabase.TestCollection WHERE id = 3;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 1);
            REQUIRE(c2->value(0, 0).value<bool>() == true);
        }
    }
}

// WAL-replay synthesis must put the recreated `.otbx` where the table's own
// resolve will look for it — under its pg_class.relnamespace.
//
// A table's file lives at ${disk_root}/${relnamespace}/${table_oid}/table.otbx: that is what
// manager_disk_t::create_storage_disk builds from the namespace oid the planner resolved
// (create_plan_sequence passes node_create_collection_t::namespace_oid as the create's
// "database_oid"). Every recovery path that has to REBUILD that path — replay synthesis here,
// the deferred-DROP GC sweep, the rehydrate of a lost file — used to substitute
// well_known_oid::main_database == 4 instead. 4 is a pg_database oid, not a namespace oid, and
// no user table can carry it: CREATE DATABASE allocates its namespace from FIRST_USER_OID
// upward. So the synthesised file landed in a directory nothing ever opens for this table.
//
// It is not merely a misplaced file. The next restart's directory walk (which accepts any
// numeric directory) loads THAT file for the oid, so a table can come back with the rows a
// stale synthesis left under 4 rather than the ones its real file holds — and the GC sweep,
// aimed at the same wrong directory, never removes a dropped table's .otbx at all.
//
// Crash model is the one the synthesis branch exists for and is copied from
// computed_type_variants_survive_crash_replay_synthesis: copy the LIVE directory, then delete
// the user table's storage directory from the COPY (a freshly created .otbx's directory entry
// is not fsynced, so a real crash keeps the pg_class row and the WAL while losing the file).
// The table is computed (relkind='g') on purpose: rehydrate_missing_user_storages_sync skips
// 'g' at the source, so the ONLY thing that can rebuild this file is replay synthesis.
TEST_CASE("integration::cpp::test_persistence::replay_synthesis_places_otbx_under_its_namespace") {
    auto config = test_create_config(integration_fixture_path("test_persistence/replay_ns_src"));
    test_clear_directory(config);

    const std::filesystem::path crash_dir = integration_fixture_path("test_persistence/replay_ns_copy");

    // Returns every ${ns}/${oid} pair under the disk root that holds a table.otbx and whose
    // BOTH components are user oids.
    auto user_table_dirs = [](const std::filesystem::path& root) {
        std::vector<std::pair<unsigned long, unsigned long>> found;
        if (!std::filesystem::exists(root)) {
            return found;
        }
        auto numeric = [](const std::filesystem::path& dir) -> unsigned long {
            const auto name = dir.filename().string();
            char* end = nullptr;
            const unsigned long v = std::strtoul(name.c_str(), &end, 10);
            return (end != nullptr && *end == '\0' && !name.empty()) ? v : 0;
        };
        for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
            if (!entry.is_regular_file() || entry.path().filename() != "table.otbx") {
                continue;
            }
            const auto tbl = numeric(entry.path().parent_path());
            const auto ns = numeric(entry.path().parent_path().parent_path());
            if (tbl >= components::catalog::FIRST_USER_OID && ns >= components::catalog::FIRST_USER_OID) {
                found.emplace_back(ns, tbl);
            }
        }
        return found;
    };

    unsigned long live_ns = 0;
    unsigned long live_tbl = 0;

    INFO("phase 1: computed table with rows; copy the live directory as the crash image");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection ();");
            REQUIRE(cur->is_success());
        }
        for (int i = 1; i <= 2; ++i) {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (id, a) VALUES (" +
                                                   std::to_string(i) + ", " + std::to_string(i * 10) + ");");
            REQUIRE(cur->is_success());
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);

        // The live layout is the ANSWER the recovery has to reproduce: one user table, under a
        // user namespace oid. Read it off the disk rather than assuming a value.
        const auto live = user_table_dirs(config.disk.path);
        INFO("the live directory must hold exactly the one user table");
        REQUIRE(live.size() == 1);
        live_ns = live.front().first;
        live_tbl = live.front().second;

        std::filesystem::remove_all(crash_dir);
        std::filesystem::create_directories(crash_dir.parent_path());
        std::filesystem::copy(config.main_path, crash_dir, std::filesystem::copy_options::recursive);
    }

    // Model the lost directory entry: remove the table's storage directory from the copy. The
    // WAL, which carries the pg_class row and the PHYSICAL_INSERTs, survives.
    {
        auto crash_config = test_create_config(crash_dir);
        auto victim = crash_config.disk.path / std::to_string(live_ns) / std::to_string(live_tbl);
        REQUIRE(std::filesystem::exists(victim / "table.otbx"));
        std::filesystem::remove_all(victim);
    }

    INFO("phase 2: reopen the crash image — synthesis must rebuild the file under live_ns");
    {
        auto crash_config = test_create_config(crash_dir);
        {
            test_spaces space(crash_config);
            auto* dispatcher = space.dispatcher();
            // Functional consequence: a storage the table can actually find holds the rows.
            CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);
        }

        // Structural gate, and the one a hardwired namespace oid fails: the recreated
        // file sits under the table's own namespace...
        REQUIRE(std::filesystem::exists(crash_config.disk.path / std::to_string(live_ns) /
                                        std::to_string(live_tbl) / "table.otbx"));
        // ...and nothing was manufactured under the main-database oid (4), which is where the
        // hardwired path put it.
        REQUIRE_FALSE(std::filesystem::exists(
            crash_config.disk.path /
            std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::main_database)) /
            std::to_string(live_tbl) / "table.otbx"));
        // Nothing else moved either: still exactly the one user table, at the same coordinates.
        const auto after = user_table_dirs(crash_config.disk.path);
        REQUIRE(after.size() == 1);
        REQUIRE(after.front().first == live_ns);
        REQUIRE(after.front().second == live_tbl);
    }

    std::filesystem::remove_all(crash_dir);
}

// A ZERO-COLUMN REGULAR table (relkind='r' whose only column was
// dropped) must NOT come back computed after a restart. Its pg_attribute schema is
// empty — exactly the shape the load path once used as the computed heuristic — so
// only the relkind check keeps it regular. Regular vs computed is observable
// through INSERT: a computed table adopts arbitrary per-document columns, a regular
// zero-column table refuses them.
TEST_CASE("integration::cpp::test_persistence::zero_column_regular_table_stays_regular") {
    auto config = test_create_config(integration_fixture_path("test_persistence/zero_col_regular"));
    test_clear_directory(config);

    INFO("phase 1: regular one-column table, DROP the only column");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.OneCol (x BIGINT);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "ALTER TABLE TestDatabase.OneCol DROP COLUMN x;");
            REQUIRE(cur->is_success());
        }
        // In-session: still a regular table — refuses arbitrary document columns.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.OneCol (id, a) VALUES (1, 10);");
            REQUIRE_FALSE(cur->is_success());
        }
    }

    INFO("phase 2: restart — the empty-schema regular table must NOT become computed");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // A computed table would adopt (id, a) and take the row; the regular
        // zero-column table must still refuse it.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.OneCol (id, a) VALUES (1, 10);");
            REQUIRE_FALSE(cur->is_success());
        }
    }
}

TEST_CASE("integration::cpp::test_persistence::double_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/double_restart");
    test_clear_directory(config);

    INFO("phase 1: create table, insert first 50 rows");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }

        // INSERT 50 rows with count = 0..49
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 50; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 49 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
    }

    INFO("phase 2: first restart, verify, insert 50 more rows");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Verify first batch survived
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);

        // INSERT 50 more rows with count = 50..99
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 50; i < 100; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
    }

    INFO("phase 3: second restart, verify all 100 rows accumulated");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
        // Rows from phase 1
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
        // Rows from phase 2
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}

// ---- Real DISK checkpoint tests ----

TEST_CASE("integration::cpp::test_persistence::disk_checkpoint_basic") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_basic");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert 50 rows, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        // INSERT 50 rows
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 50; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 49 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);

        // CHECKPOINT — writes data to table.otbx
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify 50 rows loaded from table.otbx");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 25;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_checkpoint_after_update") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_update");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert, update, delete, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        // INSERT 100 rows
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

        // DELETE WHERE count > 90 (removes 9 rows: 91..99)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

        // UPDATE SET count=999 WHERE count=50
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection SET count = 999 WHERE count = 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

        // CHECKPOINT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify DML changes survived checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 95;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 90;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_checkpoint_plus_wal") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_plus_wal");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert 50, checkpoint, insert 50 more (no second checkpoint)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        // INSERT first 50 rows
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 50; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 49 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        // CHECKPOINT — first 50 go to table.otbx
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }

        // INSERT 50 more rows (no checkpoint — these stay in WAL only)
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 50; i < 100; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
    }

    INFO("phase 2: restart — 50 from table.otbx + 50 from WAL");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
        // From checkpoint
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
        // From WAL
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}

// ---- DISK partial insert, constraints, WAL-only recovery, double restart, DML cycle ----

TEST_CASE("integration::cpp::test_persistence::disk_partial_insert") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_partial_insert");
    test_clear_directory(config);

    INFO("phase 1: create DISK table with 3 cols, partial INSERT, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection "
                "(name string, score bigint, tag string DEFAULT 'untagged') ;");
            REQUIRE(cur->is_success());
        }

        // Partial INSERT: only (name, score) — tag uses default
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, score) VALUES "
                                               "('alice', 100), ('bob', 200), ('charlie', 300);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 100;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE tag = 'untagged';", 3);

        // Partial INSERT: only (name) — score NULL, tag default
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "INSERT INTO TestDatabase.TestCollection (name) VALUES ('dave'), ('eve');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);

        // CHECKPOINT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify partial inserts survived");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 100;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 200;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 300;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'dave';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'eve';", 1);

        // A restart must not turn the DEFAULT off. Reading the rows written BEFORE the
        // restart says nothing about that (their tag was materialised in the creating
        // session); only a NEW partial INSERT does.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name, score) VALUES ('frank', 400);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 6);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE tag = 'untagged';", 6);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_not_null_default") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_not_null_default");
    test_clear_directory(config);

    INFO("phase 1: create DISK table with NOT NULL + DEFAULT, test enforcement");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection "
                "(name string NOT NULL, status string NOT NULL DEFAULT 'pending') ;");
            REQUIRE(cur->is_success());
        }

        // INSERT with all columns
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, status) VALUES "
                                               "('alice', 'active'), ('bob', 'pending');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        // NOT NULL violation — rejected
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name, status) VALUES (NULL, 'test');");
            REQUIRE(cur->is_error());
        }

        // Partial INSERT: only (name) — status gets DEFAULT 'pending'
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (name) VALUES ('charlie');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'pending';", 2);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 1);

        // CHECKPOINT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify constraints + defaults persisted");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'pending';", 2);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'charlie';", 1);

        // The NOT NULL DEFAULT column must still be filled for a NEW partial INSERT.
        // Re-reading pre-restart rows cannot show this: their status was materialised
        // in the creating session.
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (name) VALUES ('dave');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 4);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'pending';", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'dave';", 1);
    }
}

// SOURCE CONVERGENCE (1/2). What a diverged DEFAULT breaks is not the value but
// the CONSTRAINT. `CHECK (c IS NOT NULL)` on a column with a DEFAULT is compiled against
// the PLAN's copy of the default (pg_attribute.attdefspec, which survives a restart) and
// therefore PASSES an INSERT that omits `c` — "the stored row will carry 5". The value
// actually written came from the storage-layer column list, which after a restart has no
// defaults, so NULL was stored. The constraint admitted exactly what it exists to reject.
// This test requires the check's verdict and the stored value to agree.
TEST_CASE("integration::cpp::test_persistence::default_check_constraint_agrees_after_restart") {
    auto config = test_create_config(integration_fixture_path("test_persistence/default_check_agrees"));
    test_clear_directory(config);

    INFO("phase 1: c INT DEFAULT 5 with CHECK (c IS NOT NULL); verdict and value agree in-session");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(
                dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (id bigint, c int DEFAULT 5);")
                    ->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "ALTER TABLE TestDatabase.TestCollection "
                                      "ADD CONSTRAINT chk_c_not_null CHECK (c IS NOT NULL);")
                        ->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id) VALUES (1);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT c FROM TestDatabase.TestCollection WHERE id = 1;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE_FALSE(cur->value(0, 0).is_null());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CHECKPOINT;")->is_success());
        }
    }

    INFO("phase 2: restart — the CHECK verdict and the STORED value must still agree");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto ins = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id) VALUES (2);");
            INFO("the CHECK passed this row believing the DEFAULT would be stored");
            REQUIRE(ins->is_success());
            REQUIRE(ins->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT c FROM TestDatabase.TestCollection WHERE id = 2;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            INFO("CHECK (c IS NOT NULL) admitted the row, so the stored c must satisfy it");
            CHECK_FALSE(cur->value(0, 0).is_null());
        }
        // Stated as the constraint itself: no row in the table may violate the CHECK the
        // engine claims to enforce.
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE c IS NULL;", 0);
    }
}

// SOURCE CONVERGENCE (2/2). Uniqueness diverges symmetrically: an omitted key
// column is compared as its DEFAULT (from the catalog, which survives) while NULL is what
// lands on disk. Before the fix two inserts omitting the column both succeed after a
// restart — the duplicate-key decision was made about 5 and NULL was stored twice. The
// decision must be about what is really written.
TEST_CASE("integration::cpp::test_persistence::default_unique_constraint_agrees_after_restart") {
    auto config = test_create_config(integration_fixture_path("test_persistence/default_unique_agrees"));
    test_clear_directory(config);

    INFO("phase 1: code bigint DEFAULT 5 UNIQUE; one omitted-column row lands with 5");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "CREATE TABLE TestDatabase.TestCollection (id bigint, code bigint DEFAULT 5);")
                        ->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "ALTER TABLE TestDatabase.TestCollection "
                                      "ADD CONSTRAINT uq_code UNIQUE (code);")
                        ->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id) VALUES (1);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE code = 5;", 1);
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CHECKPOINT;")->is_success());
        }
    }

    INFO("phase 2: restart — the duplicate-key decision must be about the STORED value");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto ins = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (id) VALUES (2);");
            INFO("a second row omitting the UNIQUE column takes the same DEFAULT key as row 1");
            CHECK(ins->is_error());
        }
        // Whatever the verdict, the table must not end up holding two rows that share the
        // key the constraint compared them by.
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE code = 5;", 1);
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE code IS NULL;", 0);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_wal_only_recovery") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_wal_only");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert 50 rows, NO checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 50; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 49 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
        // No CHECKPOINT — all data in WAL only
    }

    INFO("phase 2: restart — verify WAL recovery for DISK table");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 25;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_double_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_double_restart");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert 50 rows, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 50; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 49 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: first restart, verify, insert 50 more, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);

        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 50; i < 100; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 3: second restart, verify all 100 rows");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_dml_full_cycle") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_dml_cycle");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, INSERT 100, DELETE 10, UPDATE 1, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        // INSERT 100 rows
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

        // DELETE WHERE count > 90 (removes 9 rows: 91..99)
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

        // UPDATE SET count=999 WHERE count=50
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection SET count = 999 WHERE count = 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);

        // CHECKPOINT
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify final state");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 95;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 90;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_drop_table_survives_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_drop_table");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert, checkpoint, DROP TABLE, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 20; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 19 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 20);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DROP TABLE TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart — table must be gone, re-create must succeed");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_error());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (val bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.TestCollection (val) VALUES (42);");
            REQUIRE(cur->is_success());
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 1);
    }
}

// Recursive scan for every storage payload file under the disk root. The GC
// sweep removes table.otbx + sidecars of dropped tables; comparing the scan
// before/after pins the exact file set the sweep must reclaim.
static std::set<std::filesystem::path> scan_otbx_files(const std::filesystem::path& disk_root) {
    std::set<std::filesystem::path> files;
    std::error_code ec;
    for (auto it = std::filesystem::recursive_directory_iterator(disk_root, ec);
         !ec && it != std::filesystem::recursive_directory_iterator();
         it.increment(ec)) {
        if (it->is_regular_file(ec) && it->path().filename() == "table.otbx") {
            files.insert(it->path());
        }
    }
    return files;
}

TEST_CASE("integration::cpp::test_persistence::disk_drop_gc_removes_storage_files") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_drop_gc");
    test_clear_directory(config);

    // End-to-end DROP-GC through the unified commit channel. Two nets:
    //   PRIMARY — drop_storage during the DROP statement removes .otbx +
    //   sidecars immediately (a surviving file would let WAL replay
    //   synthesise a phantom storage);
    //   SECONDARY — mark_storage_dropped_many parks a tombstone keyed by the
    //   dropping TXN-ID, the commit operator remaps it to the real commit_id
    //   (storage_dropped_committed), and the next commit's horizon broadcast
    //   (on_horizon_advanced, commit-id space) drains the queue. The drain is
    //   internal state; what this test pins is that the whole chain runs
    //   without touching any OTHER table's storage.
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "CREATE TABLE TestDatabase.GcSurvivor (val bigint) "
                                           ";");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.GcSurvivor (val) VALUES (42);");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
        REQUIRE(cur->is_success());
    }
    const auto baseline_files = scan_otbx_files(config.disk.path);

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "CREATE TABLE TestDatabase.GcVictim (name string, count bigint) "
                                           ";");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO TestDatabase.GcVictim (name, count) VALUES ";
        for (int i = 0; i < 20; ++i) {
            query << "('row_" << i << "', " << i << ")" << (i == 19 ? ";" : ", ");
        }
        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
        REQUIRE(cur->is_success());
    }

    // The victim's payload file = exactly what appeared since the baseline.
    auto with_victim_files = scan_otbx_files(config.disk.path);
    std::set<std::filesystem::path> victim_files;
    for (const auto& f : with_victim_files) {
        if (baseline_files.find(f) == baseline_files.end()) {
            victim_files.insert(f);
        }
    }
    REQUIRE(victim_files.size() == 1);
    const auto victim_otbx = *victim_files.begin();
    REQUIRE(std::filesystem::exists(victim_otbx));

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DROP TABLE TestDatabase.GcVictim;");
        REQUIRE(cur->is_success());
    }
    // PRIMARY net: drop_storage ran inside the DROP statement — the payload
    // file (and its per-oid directory) must already be gone when the
    // statement's cursor returns.
    REQUIRE_FALSE(std::filesystem::exists(victim_otbx));
    REQUIRE_FALSE(std::filesystem::exists(victim_otbx.parent_path()));

    // SECONDARY net: the next commit advances the published horizon past the
    // DROP's commit_id; the dispatcher broadcast walks the (remapped)
    // tombstone queue. Asynchronous fire-and-forget — give it a bounded
    // window, then pin that it disturbed nothing else.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.GcSurvivor (val) VALUES (43);");
        REQUIRE(cur->is_success());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Every baseline file (the survivor's storage + system tables) must be
    // untouched by both nets.
    auto after_gc_files = scan_otbx_files(config.disk.path);
    for (const auto& f : baseline_files) {
        REQUIRE(after_gc_files.find(f) != after_gc_files.end());
    }
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.GcSurvivor;", 2);
}

// A DROP TABLE inside an explicit transaction must be fully revertible until
// COMMIT.
//   - Same txn: the pg_class row delete is MVCC-visible to the dropping session
//     (self-write), so SELECT from the table in that SAME session no longer
//     resolves -> error cursor.
//   - The storage drop (drop_storage / unregister_collection) is DEFERRED to the
//     post-publish commit tail rather than run during the DROP plan. So on
//     ROLLBACK the catalog delete is reverted, the storage was never dropped,
//     and a fresh session sees the table alive with every row — and its
//     table.otbx payload file is still on disk, untouched.
//   - Only after COMMIT does the deferred drop run: the table disappears from
//     the catalog and its storage payload file is reclaimed.
// Statements share one session_id_t (active txns are keyed by session.data()).
TEST_CASE("integration::cpp::test_persistence::drop_rollback") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/drop_rollback");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    std::set<std::filesystem::path> baseline_files;
    INFO("setup: DISK table with rows, checkpointed so its payload file exists");
    {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;")->is_success());
        }
        // Snapshot the .otbx files that exist BEFORE DropVictim — these are the
        // system / catalog tables, which a single-table DROP must NEVER remove.
        // The DropVictim-specific file is then the delta against this baseline,
        // isolating the assertions to the dropped table's own storage (a DROP of
        // one user table cannot reclaim the shared catalog heaps).
        baseline_files = scan_otbx_files(config.disk.path);
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.DropVictim (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.DropVictim (name, count) VALUES "
                                               "('alice', 10), ('bob', 20), ('charlie', 30);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CHECKPOINT;")->is_success());
        }
    }

    // The DropVictim payload file is the delta over the pre-CREATE baseline: only
    // these files belong to the dropped table and must disappear at COMMIT.
    std::set<std::filesystem::path> victim_files;
    for (const auto& f : scan_otbx_files(config.disk.path)) {
        if (baseline_files.find(f) == baseline_files.end()) {
            victim_files.insert(f);
        }
    }
    REQUIRE_FALSE(victim_files.empty());

    INFO("BEGIN; DROP TABLE; same-session SELECT fails to resolve; ROLLBACK — one shared session");
    {
        auto session = otterbrix::session_id_t();
        auto begin_cur = dispatcher->execute_sql(session, "BEGIN;");
        REQUIRE(begin_cur->is_success());

        auto drop_cur = dispatcher->execute_sql(session, "DROP TABLE TestDatabase.DropVictim;");
        REQUIRE(drop_cur->is_success());

        // Same txn: the catalog delete is visible to this session (self-write),
        // so the table no longer resolves for the dropping session.
        auto sel_cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.DropVictim;");
        REQUIRE(sel_cur->is_error());

        auto rollback_cur = dispatcher->execute_sql(session, "ROLLBACK;");
        REQUIRE(rollback_cur->is_success());
    }

    INFO("after ROLLBACK: a fresh session sees the table alive with all rows");
    {
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.DropVictim;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.DropVictim WHERE count = 20;", 1);
    }

    INFO("after ROLLBACK: the storage payload file was never dropped");
    {
        // The deferred drop_storage only runs at COMMIT; an aborted DROP must
        // leave the DropVictim payload file intact (and the catalog files too).
        auto after_rollback_files = scan_otbx_files(config.disk.path);
        for (const auto& f : victim_files) {
            REQUIRE(std::filesystem::exists(f));
            REQUIRE(after_rollback_files.find(f) != after_rollback_files.end());
        }
        for (const auto& f : baseline_files) {
            REQUIRE(std::filesystem::exists(f));
        }
    }

    INFO("BEGIN; DROP TABLE; COMMIT — the deferred drop runs at commit time");
    {
        auto session = otterbrix::session_id_t();
        auto begin_cur = dispatcher->execute_sql(session, "BEGIN;");
        REQUIRE(begin_cur->is_success());

        auto drop_cur = dispatcher->execute_sql(session, "DROP TABLE TestDatabase.DropVictim;");
        REQUIRE(drop_cur->is_success());

        auto commit_cur = dispatcher->execute_sql(session, "COMMIT;");
        REQUIRE(commit_cur->is_success());
    }

    INFO("after COMMIT: the table is gone and its storage payload file is removed");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.DropVictim;");
            REQUIRE(cur->is_error());
        }
        // The committed DROP's deferred drop_storage reclaimed the DropVictim
        // payload file (and its per-oid directory). The shared catalog files
        // (baseline) must survive — a single-table DROP never touches them.
        for (const auto& f : victim_files) {
            REQUIRE_FALSE(std::filesystem::exists(f));
            REQUIRE_FALSE(std::filesystem::exists(f.parent_path()));
        }
        for (const auto& f : baseline_files) {
            REQUIRE(std::filesystem::exists(f));
        }
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_add_column_survives_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_add_column");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert, checkpoint, ADD COLUMN, insert, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 10; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == 9 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 10);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "ALTER TABLE TestDatabase.TestCollection ADD COLUMN score double;");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection "
                                               "(name, count, score) VALUES ('new_row', 99, 1.5);");
            REQUIRE(cur->is_success());
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 11);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart — schema change and new rows must survive");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 11);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection WHERE count = 99;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection "
                                               "(name, count, score) VALUES ('post_restart', 100, 2.0);");
            REQUIRE(cur->is_success());
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 12);
    }
}

// REGRESSION — MVCC commit-clock restore on reopen (the durable WAL-COMMIT-marker
// frontier → published_horizon_ + current_timestamp_). What the restore guards:
//
//   * Committed DML stamps each row version with a real commit-id (insert_id for an
//     INSERT, delete_id for a DELETE), drawn from the prior session's commit clock.
//     CHECKPOINT folds those stamps into the .otbx.
//   * On reopen the transaction_manager restarts at {current_timestamp_=1,
//     published_horizon_=0}, and nothing on the recovery path calls publish() (it runs
//     only in the live commit pipeline). So WITHOUT the restore a fresh post-reopen
//     reader snapshots published_horizon_=0 and the MVCC filter (row_version_manager:
//     id > snapshot_horizon ⇒ not visible) judges every committed DELETE as "deleted
//     after my snapshot" — the deleted rows REAPPEAR, and the phase-2 count comes back
//     100 instead of 50.
//   * The restore raises published_horizon_ to the durable frontier (max of persisted
//     pg_attribute commit-ids and the max WAL COMMIT-marker commit-id).
//
// Sibling tests test_wal_pool::insert_delete_checkpoint_restart and
// test_persistence::wal_recovery_dml_full_cycle give equivalent coverage; this case
// states the intent explicitly so the regression is unmistakable.
//
// (The restore's OTHER input is manager_disk_t::max_persisted_commit_id_sync, the max
// over pg_attribute added_at_commit_id / dropped_at_commit_id. That branch USED to be
// dead through real DDL, because ALTER ADD COLUMN left added_at at the placeholder 0:
// the commit backfill, agent_disk_t::update_pg_attribute_commit_id_field_inner, scanned
// with a transaction that could not see the row it was asked to patch. It is live now,
// so a reopened engine can raise its frontier off pg_attribute alone. This case still
// exercises the WAL-frontier half; the added_at half is pinned in
// test_catalog_delete_refusal.cpp — an_added_columns_commit_id_survives_a_restart.)
TEST_CASE("integration::cpp::test_persistence::reopen_keeps_committed_deletes_invisible") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/reopen_keeps_committed_deletes");
    test_clear_directory(config);

    INFO("phase 1: WAL-backed table, INSERT 100, DELETE 50, CHECKPOINT");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        // Default storage (WAL-recovered, no .otbx): reopen rebuilds the table from
        // WAL + the committed MVCC stamps, so delete visibility depends on the
        // restored published_horizon_ (a disk-backed table's CHECKPOINT compaction
        // would mask the bug by physically dropping committed-deleted rows).
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }
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
        // DELETE WHERE count < 50 (removes 50 rows: 0..49). Each tombstone gets
        // delete_id = this txn's committed commit-id (> 0).
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count < 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: REOPEN — committed deletes MUST stay deleted (no resurrection)");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Decisive: without the commit-clock restore published_horizon_=0, the
        // committed delete tombstones (delete_id > 0) are judged not-yet-applied
        // and the 50 deleted rows reappear → 100 here instead of 50.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_index_mixed_ops_checkpoint_restart") {
    auto config =
        test_create_config("/tmp/otterbrix/integration/test_persistence/disk_index_mixed_ops_checkpoint_restart");
    test_clear_directory(config);

    INFO("phase 1: create disk table + index, apply mixed DML, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            std::stringstream q;
            q << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 200; ++i) {
                q << "('row_" << i << "', " << i << ")" << (i == 199 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 200);
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 200);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count % 2 = 0;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "UPDATE TestDatabase.TestCollection SET count = count + 1000 WHERE count > 150;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 25);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 10;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 151;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1151;", 1);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify index-backed predicates remain correct");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 10;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 151;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1151;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count > 1000;", 25);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_index_long_keys_survive_checkpoint_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_index_long_keys");
    test_clear_directory(config);

    const std::string long_a(220, 'a');
    const std::string long_b(220, 'b');

    INFO("phase 1: insert long keys and checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE INDEX idx_name ON TestDatabase.TestCollection (name);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ('" +
                                                   long_a + "', 1), ('" + long_b + "', 2);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = '" + long_a + "';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = '" + long_b + "';", 1);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart and verify long-key lookup");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = '" + long_a + "';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = '" + long_b + "';", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_index_massive_checkpoint_cycle") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_index_massive_checkpoint_cycle");
    test_clear_directory(config);

    INFO("phase 1: many batches with periodic checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
            REQUIRE(cur->is_success());
        }

        int inserted = 0;
        for (int batch = 0; batch < 10; ++batch) {
            auto session = otterbrix::session_id_t();
            std::stringstream q;
            q << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 100; ++i) {
                const int v = batch * 100 + i;
                q << "('row_" << v << "', " << v << ")" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            inserted += 100;

            if ((batch + 1) % 2 == 0) {
                auto cp_session = otterbrix::session_id_t();
                auto cp = dispatcher->execute_sql(cp_session, "CHECKPOINT;");
                REQUIRE(cp->is_success());
            }
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", inserted);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
    }

    INFO("phase 2: restart and verify all data present");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 1000);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count > 950;", 49);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count < 10;", 10);
    }
}

// Restart recovery of an on-disk user index via bootstrap_indexes_sync, over a
// clean shutdown (base_otterbrix_t dtor CHECKPOINTs, no explicit CHECKPOINT).
// On restart bootstrap_indexes_sync must re-mint the engine and respawn the
// disk agent from pg_index alone, so post-restart email lookups stay correct.
TEST_CASE("integration::cpp::test_persistence::index_recovery_phase4_catalog_driven_bootstrap") {
    auto config = test_create_config(
        "/tmp/otterbrix/integration/test_persistence/index_recovery_phase4_catalog_driven_bootstrap");
    test_clear_directory(config);

    INFO("phase 1: create users(id, email) + email index, insert 10 rows, dtor checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.users (id INT, email TEXT) "
                                               ";");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE INDEX users_email_idx ON TestDatabase.users (email);");
            REQUIRE(cur->is_success());
        }

        // Stable emails ("user_0@x" … "user_9@x") so post-restart lookups can
        // probe both an existing and a missing value unambiguously.
        {
            auto session = otterbrix::session_id_t();
            std::stringstream q;
            q << "INSERT INTO TestDatabase.users (id, email) VALUES ";
            for (int i = 0; i < 10; ++i) {
                q << "(" << i << ", 'user_" << i << "@x')" << (i == 9 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users;", 10);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_0@x';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_9@x';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'missing@x';", 0);
    }

    INFO("phase 2: restart — bootstrap rewires the email index from pg_index");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Structural witness: the disk agent's b+tree dir at
        // ${disk.path}/${users_oid}/${indexrelid} exists, proving bootstrap
        // respawned it. The layout is oid-keyed and carries no index name, so the
        // dir is found by content: the ordered b+tree backend owns a `metadata`
        // file in its directory (bitcask would own CURRENT instead).
        bool found = false;
        if (std::filesystem::exists(config.disk.path)) {
            for (const auto& d : std::filesystem::recursive_directory_iterator(config.disk.path)) {
                if (d.is_directory() && std::filesystem::exists(d.path() / "metadata")) {
                    found = true;
                    break;
                }
            }
        }
        REQUIRE(found);

        // Functional witness: equality lookups on the indexed column return
        // correct rows. "Index was used" isn't observable from SQL, so dir
        // existence + correct results together stand in for it.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users;", 10);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_0@x';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_5@x';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_9@x';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'missing@x';", 0);

        // A fresh INSERT + lookup proves the rewired engine takes runtime
        // writes, not just read-only replay.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.users (id, email) VALUES (10, 'user_10@x');");
            REQUIRE(cur->is_success());
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users;", 11);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_10@x';", 1);
    }
}

// SET TIMEZONE writes a 'TimeZone' row into the pg_settings system table, which
// the disk agent persists like any other catalog table; on restart the dispatcher
// refreshes its default_tz_cat_ from that row. pg_settings is not queryable via
// SELECT (no SHOW / no pg_catalog read path is wired into the SQL pipeline), so
// the persisted value cannot be asserted directly. Instead we assert indirectly:
// phase 1 sets the timezone alongside real table data; phase 2 confirms the
// catalog/WAL still recover cleanly after the SET (the table data survives) and a
// fresh SET TIMEZONE applies post-restart. Limitation: this characterizes that the
// SET TIMEZONE write does not corrupt persistence and the path stays usable across
// restart; the exact stored value is not observable from SQL.
TEST_CASE("integration::cpp::test_persistence::set_timezone_survives_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/set_timezone_survives_restart");
    test_clear_directory(config);

    INFO("phase 1: SET TIMEZONE, then create + populate a table");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SET TIMEZONE TO 'Asia/Tokyo';");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, count) VALUES "
                                               "('alice', 1), ('bob', 2), ('charlie', 3);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
    }

    INFO("phase 2: restart — persistence recovered cleanly, SET TIMEZONE still works");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // The SET TIMEZONE row in pg_settings did not corrupt catalog/WAL recovery:
        // user table data survives the restart intact.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 3;", 1);

        // The SET TIMEZONE path remains usable after restart: a fresh valid SET
        // applies, and an unknown timezone is still rejected.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SET TIMEZONE TO 'Europe/London';");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SET TIMEZONE TO 'not_a_real_timezone';");
            REQUIRE(cur->is_error());
        }
    }
}

// An indexed disk table whose rows are DELETE'd > 30% in a committed txn, then
// CHECKPOINT'd, must survive a restart with index-path queries still exact.
// Commit-path compaction is GATED for indexed tables (tables_without_indexes),
// so the commit itself does NOT shift ids — but the result set must already be
// correct (deleted rows invisible via the live index). The CHECKPOINT
// repopulates the on-disk index against compacted ids, and on restart bootstrap
// repopulate (txn_id=0) + the replay gate must reconstruct a consistent, visible
// index.
TEST_CASE("integration::cpp::test_persistence::indexed_table_compact_survives_restart") {
    auto config =
        test_create_config("/tmp/otterbrix/integration/test_persistence/indexed_table_compact_survives_restart");
    test_clear_directory(config);

    INFO("phase 1: disk table + index, insert, delete >30%, commit, checkpoint");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE TestDatabase.TestCollection (name string, count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
            REQUIRE(cur->is_success());
        }

        // INSERT 100 rows, count = 0..99, inside an explicit txn that commits.
        {
            auto session = otterbrix::session_id_t();
            std::stringstream q;
            q << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 100; ++i) {
                q << "('row_" << i << "', " << i << ")" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }

        // DELETE > 30% (count < 40 → 40 rows) in a committed statement; the
        // commit-path compact is gated for this indexed table, but the live
        // index must already hide the deleted rows.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count < 40;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 40);
        }

        // Correct results even though commit-path compact is gated.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 60);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 39;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 40;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);

        // CHECKPOINT compacts ids and repopulates the on-disk index.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 2: restart — bootstrap repopulate keeps index-path queries exact");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 60);
        // Deleted values stay gone through the rebuilt index.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 39;", 0);
        // Surviving values resolve to exactly their one row (no stale id hit).
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 40;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 70;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count >= 40;", 60);
    }
}

// Regression guard for the SSB-reopen bug: with disk AND wal OFF (the SSB
// benchmark configuration), pg_class is still persisted unconditionally.
// User tables are ALWAYS disk-backed, so the phase-1 rows are durable across the
// reopen (the shutdown checkpoint seals the .otbx) and phase 2 sees the union of old
// + re-inserted rows (200); a file-less table would lose its rows and see only the
// re-inserted 100. The load-bearing core: after a reopen the storage must exist
// again, the re-run CREATE TABLE IF NOT EXISTS must be a clean no-op, and the
// re-INSERT must land and be visible (the SSB "4ms / 0 rows" regression, where
// storage_append silently no-opped against a catalog-only table).
TEST_CASE("integration::cpp::test_persistence::reopen_reinsert_visible") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/reopen_in_memory_reinsert");
    // SSB benchmark config: WAL persistence OFF. Tables are disk-backed
    // regardless; durability across a clean shutdown comes from the shutdown
    // checkpoint.
    config.wal.on = false;
    test_clear_directory(config);

    INFO("phase 1: create table + insert 100 rows");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE IF NOT EXISTS TestDatabase.TestCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }
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
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 100);
    }

    INFO("phase 2: reopen, re-run setup + re-insert, the fresh rows must be visible");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Mirror the benchmark runner, which re-runs CREATE TABLE IF NOT EXISTS and
        // re-INSERTs on every reopen. The catalog still knows the table, so this is
        // a no-op DDL — but the storage shell must exist for the inserts to land.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE IF NOT EXISTS TestDatabase.TestCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
            for (int i = 0; i < 100; ++i) {
                query << "('reopen_" << i << "', " << i << ")" << (i == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            // The bug: storage_append no-ops, so the cursor reports 0 affected rows.
            REQUIRE(cur->size() == 100);
        }

        // Decisive gate: a REGULAR snapshot scan must see the re-inserted rows.
        // With the bug this returns 100 (only the durable phase-1 rows; the SSB
        // "4ms / 0 rows" symptom shape). Phase-1 rows survive the reopen,
        // so the union is 200 and each count value now matches two rows (row_N
        // and reopen_N).
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 200);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 2);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 2);
    }
}

// ---------------------------------------------------------------------------
// Disk is the ONLY storage mode.
//   1. A plain CREATE TABLE (no WITH clause) produces a .otbx under
//      ${db_oid}/${table_oid}/ — no opt-in required.
//   2. Its pg_class row carries relstoragemode == 'd' (the column stays,
//      write-only, always 'd').
//   3. create → insert → restart → read back round-trips end to end through
//      SQL with no storage clause anywhere.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_persistence::b1a_disk_is_default") {
    auto config = test_create_config(integration_fixture_path("test_persistence/b1a_disk_default"));
    test_clear_directory(config);

    const auto disk_root = config.disk.path;

    INFO("phase 1: plain CREATE TABLE must create a .otbx and a 'd' pg_class row");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.B1aDefault (name string, count bigint);");
            REQUIRE(cur->is_success());
        }

        // Gate 1: the table's .otbx exists at ${namespace_oid}/${table_oid}/
        // under the disk root — both components are user oids (>= FIRST_USER_OID).
        // (The relstoragemode == 'd' half of the gate is asserted at the write
        // site — catalog::ddl::create_table_writes tests — because SQL cannot
        // project pg_class columns today: bare `SELECT oid FROM pg_class` fails
        // with "path: 'oid' was not found"; system tables' own columns have no
        // pg_attribute rows.)
        {
            auto numeric_oid = [](const std::filesystem::path& dir) -> unsigned long {
                const auto name = dir.filename().string();
                char* end = nullptr;
                const unsigned long oid = std::strtoul(name.c_str(), &end, 10);
                return (end != nullptr && *end == '\0' && !name.empty()) ? oid : 0;
            };
            bool found = false;
            for (const auto& ns_entry : std::filesystem::directory_iterator(disk_root)) {
                if (!ns_entry.is_directory() || numeric_oid(ns_entry.path()) < components::catalog::FIRST_USER_OID) {
                    continue;
                }
                for (const auto& tbl_entry : std::filesystem::directory_iterator(ns_entry.path())) {
                    if (tbl_entry.is_directory() &&
                        numeric_oid(tbl_entry.path()) >= components::catalog::FIRST_USER_OID &&
                        std::filesystem::exists(tbl_entry.path() / "table.otbx")) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    break;
                }
            }
            REQUIRE(found);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.B1aDefault (name, count) VALUES ('a', 1), ('b', 2), ('c', 3);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
    }

    INFO("phase 2: restart — rows come back with no storage clause anywhere");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.B1aDefault;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.B1aDefault WHERE count = 2;", 1);
    }
}

// WAL SEALING, end to end. A checkpoint truncates the WAL by DELETING whole segment
// files at or below the floor checkpoint_all reports, and the restart that follows replays
// what is left on top of the checkpointed files. Two ways for that to be wrong, and this
// test fails on either:
//   * the floor reached too far and a segment still holding un-checkpointed rows was
//     deleted  -> the restart comes back with FEWER rows;
//   * a segment describing rows already folded into table.otbx survived and was replayed
//     on top of them -> the restart comes back with MORE rows.
// Both are "!= exactly what was inserted", so the row counts below are the whole assertion.
//
// Truncation only happens on the SECOND checkpoint: the floor is min(prev_checkpoint_wal_id)
// and prev is 0 for every table until a round supersedes a root. Segments are also never
// deleted while the writer is still on them, hence the deliberately small max_segment_size —
// without it the whole test fits in one live segment and nothing is retired.
//
// Between the two checkpoints only TruncCollection is written to, so it is the only table
// the second round rewrites; every system table is unchanged and merely advances its
// wal-id chain to the same prev. min(prev) therefore still lands the floor on checkpoint
// #1's wal id, and the segments below it still go.
TEST_CASE("integration::cpp::test_persistence::wal_truncate_restart_no_double_replay") {
    auto config = test_create_config(integration_fixture_path("test_persistence/wal_truncate_no_double_replay"));
    test_clear_directory(config);
    config.wal.max_segment_size = 8 * 1024;

    // Segment files are `<wal path>/<db oid>/wal_<db oid>_<index>`.
    auto count_wal_segments = [&]() {
        std::size_t n = 0;
        if (!std::filesystem::exists(config.wal.path)) {
            return n;
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(config.wal.path)) {
            if (entry.is_regular_file() && entry.path().filename().string().rfind("wal_", 0) == 0) {
                ++n;
            }
        }
        return n;
    };

    constexpr int kBatch = 50;
    constexpr int kBeforeCheckpoint = 200; // rows 0..199
    constexpr int kAfterCheckpoint = 200;  // rows 200..399
    constexpr int kAfterTruncate = 50;     // rows 400..449

    std::size_t segments_before_truncate = 0;
    std::size_t segments_after_truncate = 0;

    INFO("phase 1: fill several WAL segments, checkpoint twice so the second one truncates");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TruncCollection (name string, count bigint);");
            REQUIRE(cur->is_success());
        }

        auto insert_range = [&](int from, int to) {
            for (int base = from; base < to; base += kBatch) {
                auto session = otterbrix::session_id_t();
                std::stringstream query;
                query << "INSERT INTO TestDatabase.TruncCollection (name, count) VALUES ";
                const int last = std::min(base + kBatch, to) - 1;
                for (int i = base; i <= last; ++i) {
                    query << "('row_" << i << "', " << i << ")" << (i == last ? ";" : ", ");
                }
                auto cur = dispatcher->execute_sql(session, query.str());
                REQUIRE(cur->is_success());
            }
        };

        insert_range(0, kBeforeCheckpoint);

        // Checkpoint #1: every table's prev_checkpoint_wal_id is still 0, so the reported
        // floor is 0 and nothing is truncated. This is the round that gives the tables a
        // superseded root for the next one to seal against.
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CHECKPOINT;")->is_success());
        }

        insert_range(kBeforeCheckpoint, kBeforeCheckpoint + kAfterCheckpoint);

        segments_before_truncate = count_wal_segments();
        REQUIRE(segments_before_truncate > 1);

        // Checkpoint #2: the floor is now the wal id checkpoint #1 was taken at, and the
        // segments lying entirely below it are removed.
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CHECKPOINT;")->is_success());
        }

        segments_after_truncate = count_wal_segments();
        INFO("segments before truncate: " << segments_before_truncate
                                          << ", after: " << segments_after_truncate);
        REQUIRE(segments_after_truncate < segments_before_truncate);

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection;", kBeforeCheckpoint + kAfterCheckpoint);
    }

    INFO("phase 2: restart on the truncated WAL — every row exactly once");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection;", kBeforeCheckpoint + kAfterCheckpoint);
        // One row per key: a replayed-twice segment shows up here as 2.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 199;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 200;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 399;", 1);

        // Write past the truncation point without checkpointing: these rows live only in the
        // segments that survived, so the next restart has to replay them.
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TruncCollection (name, count) VALUES ";
            const int first = kBeforeCheckpoint + kAfterCheckpoint;
            const int last = first + kAfterTruncate - 1;
            for (int i = first; i <= last; ++i) {
                query << "('row_" << i << "', " << i << ")" << (i == last ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
        }
    }

    INFO("phase 3: restart again — checkpointed rows plus the WAL-only tail, still exactly once");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection;",
                       kBeforeCheckpoint + kAfterCheckpoint + kAfterTruncate);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 400;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 449;", 1);
    }
}
