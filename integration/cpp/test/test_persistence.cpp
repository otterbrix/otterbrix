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
        REQUIRE(cur->size() == COUNT);                                                                                 \
    } while (false)

TEST_CASE("integration::cpp::test_persistence::wal_recovery_mixed_batch") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence/wal_mixed_batch");
    test_clear_directory(config);

    INFO("phase 1: insert two batches (no checkpoint)") {
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
            dispatcher->create_database(session, database_name);
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
            dispatcher->create_database(session, database_name);
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
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
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
            dispatcher->create_database(session, database_name);
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
            dispatcher->create_database(session, database_name);
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
            dispatcher->create_database(session, database_name);
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
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
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
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
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
            dispatcher->create_database(session, database_name);
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
            dispatcher->create_database(session, database_name);
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
            dispatcher->create_database(session, database_name);
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
            dispatcher->create_database(session, database_name);
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
            dispatcher->create_database(session, database_name);
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
            dispatcher->create_database(session, database_name);
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
            auto cur = dispatcher->execute_sql(
                session,
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
            auto cur = dispatcher->execute_sql(
                session,
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
            dispatcher->create_database(session, database_name);
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
            auto cur = dispatcher->execute_sql(
                session,
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
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }

        // Partial INSERT: only (name) — status gets DEFAULT 'pending'
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name) VALUES ('charlie');");
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
            dispatcher->create_database(session, database_name);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
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
            dispatcher->create_database(session, database_name);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
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
            dispatcher->create_database(session, database_name);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
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
            auto cur =
                dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

        // UPDATE SET count=999 WHERE count=50
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session, "UPDATE TestDatabase.TestCollection SET count = 999 WHERE count = 50;");
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
