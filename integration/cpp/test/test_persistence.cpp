#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <filesystem>
#include <sstream>

using namespace components::types;

static const database_name_t database_name = "testdatabase";

#define CHECK_FIND_SQL(QUERY, COUNT)                                                                                   \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto cur = dispatcher->execute_sql(session, QUERY);                                                            \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == static_cast<std::size_t>(COUNT));                                                      \
    } while (false)

TEST_CASE("integration::cpp::test_persistence::wal_recovery_mixed_batch") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/wal_mixed_batch");
    test_clear_directory(config);

    INFO("phase 1: insert two batches (no checkpoint)") {
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

    INFO("phase 2: restart — all 100 rows from WAL") {
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

    INFO("phase 1: create table with multiple types, insert") {
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

    INFO("phase 2: restart and verify all types recovered from WAL") {
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

    INFO("phase 1: create table with NOT NULL, insert valid data") {
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

    INFO("phase 2: restart and verify data + NOT NULL constraint enforced") {
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

    INFO("phase 1: insert, delete, update (no checkpoint)") {
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

    INFO("phase 2: restart and verify full DML cycle survived WAL recovery") {
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

    INFO("verify DEFAULT values are applied during INSERT within a single session") {
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

    INFO("phase 1: insert with consistent partial columns (only name)") {
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

    INFO("phase 2: restart — WAL replay with consistent 1-column records") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Name column survives WAL replay (it's the only column in WAL records).
        // After restart, computing table schema is derived from WAL chunk (1 column).
        // Defaulted columns (status, count) are NOT preserved — their schema is lost.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'bob';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'eve';", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::wal_recovery_not_null_with_default") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/wal_not_null_default");
    test_clear_directory(config);

    INFO("phase 1: create table with NOT NULL + DEFAULT, test enforcement + defaults") {
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

    INFO("phase 2: restart and verify NOT NULL + DEFAULT constraints") {
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

    INFO("phase 1: insert providing 2 of 3 columns (consistent partial)") {
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

    INFO("phase 2: restart — 2-column WAL records replayed consistently") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // name and score columns survive (both in WAL records)
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 100;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 200;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 300;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::double_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/double_restart");
    test_clear_directory(config);

    INFO("phase 1: create table, insert first 50 rows") {
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

    INFO("phase 2: first restart, verify, insert 50 more rows") {
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

    INFO("phase 3: second restart, verify all 100 rows accumulated") {
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

    INFO("phase 1: create DISK table, insert 50 rows, checkpoint") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("phase 2: restart and verify 50 rows loaded from table.otbx") {
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

    INFO("phase 1: create DISK table, insert, update, delete, checkpoint") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("phase 2: restart and verify DML changes survived checkpoint") {
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

    INFO("phase 1: create DISK table, insert 50, checkpoint, insert 50 more (no second checkpoint)") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("phase 2: restart — 50 from table.otbx + 50 from WAL") {
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

    INFO("phase 1: create DISK table with 3 cols, partial INSERT, checkpoint") {
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
                "(name string, score bigint, tag string DEFAULT 'untagged') WITH (storage = 'disk');");
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

    INFO("phase 2: restart and verify partial inserts survived") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 100;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 200;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 300;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'dave';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'eve';", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_not_null_default") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_not_null_default");
    test_clear_directory(config);

    INFO("phase 1: create DISK table with NOT NULL + DEFAULT, test enforcement") {
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
                "(name string NOT NULL, status string NOT NULL DEFAULT 'pending') WITH (storage = 'disk');");
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

    INFO("phase 2: restart and verify constraints + defaults persisted") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'pending';", 2);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'charlie';", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_wal_only_recovery") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_wal_only");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert 50 rows, NO checkpoint") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("phase 2: restart — verify WAL recovery for DISK table") {
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

    INFO("phase 1: create DISK table, insert 50 rows, checkpoint") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("phase 2: first restart, verify, insert 50 more, checkpoint") {
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

    INFO("phase 3: second restart, verify all 100 rows") {
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

    INFO("phase 1: create DISK table, INSERT 100, DELETE 10, UPDATE 1, checkpoint") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("phase 2: restart and verify final state") {
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

    INFO("phase 1: create DISK table, insert, checkpoint, DROP TABLE, checkpoint") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("phase 2: restart — table must be gone, re-create must succeed") {
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
                                               "WITH (storage = 'disk');");
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

TEST_CASE("integration::cpp::test_persistence::disk_add_column_survives_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_add_column");
    test_clear_directory(config);

    INFO("phase 1: create DISK table, insert, checkpoint, ADD COLUMN, insert, checkpoint") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("phase 2: restart — schema change and new rows must survive") {
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

TEST_CASE("integration::cpp::test_persistence::disk_index_mixed_ops_checkpoint_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/disk_index_mixed_ops_checkpoint_restart");
    test_clear_directory(config);

    INFO("phase 1: create disk table + index, apply mixed DML, checkpoint") {
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
                                               "WITH (storage = 'disk');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
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
            auto cur =
                dispatcher->execute_sql(session, "UPDATE TestDatabase.TestCollection SET count = count + 1000 WHERE count > 150;");
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

    INFO("phase 2: restart and verify index-backed predicates remain correct") {
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

    INFO("phase 1: insert long keys and checkpoint") {
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
                                               "WITH (storage = 'disk');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE INDEX idx_name ON TestDatabase.TestCollection (name);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ('" + long_a +
                                                   "', 1), ('" + long_b + "', 2);");
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

    INFO("phase 2: restart and verify long-key lookup") {
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

    INFO("phase 1: many batches with periodic checkpoint") {
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
                                               "WITH (storage = 'disk');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
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

    INFO("phase 2: restart and verify all data present") {
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
    auto config =
        test_create_config("/tmp/otterbrix/integration/test_persistence/index_recovery_phase4_catalog_driven_bootstrap");
    test_clear_directory(config);

    INFO("phase 1: create users(id, email) + email index, insert 10 rows, dtor checkpoint") {
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
                                               "WITH (storage = 'disk');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE INDEX users_email_idx ON TestDatabase.users (email);");
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

    INFO("phase 2: restart — bootstrap rewires the email index from pg_index") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        // Structural witness: the disk agent's bitcask dir at
        // ${disk.path}/${users_oid}/users_email_idx exists, proving bootstrap
        // respawned it. Walk the oid-keyed dirs (oid >= 16384 = user tables).
        bool found = false;
        if (std::filesystem::exists(config.disk.path)) {
            for (const auto& d : std::filesystem::directory_iterator(config.disk.path)) {
                if (!d.is_directory())
                    continue;
                try {
                    auto oid = std::stoull(d.path().filename().string());
                    if (oid < 16384)
                        continue;
                } catch (...) {
                    continue;
                }
                auto candidate = d.path() / "users_email_idx";
                if (std::filesystem::exists(candidate) && std::filesystem::is_directory(candidate)) {
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
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.users (id, email) VALUES (10, 'user_10@x');");
            REQUIRE(cur->is_success());
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users;", 11);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_10@x';", 1);
    }
}
