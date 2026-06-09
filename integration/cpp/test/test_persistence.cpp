#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <chrono>
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
                                           "WITH (storage = 'disk');");
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
                                           "WITH (storage = 'disk');");
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
    INFO("setup: DISK table with rows, checkpointed so its payload file exists") {
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
                                               "WITH (storage = 'disk');");
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

    INFO("BEGIN; DROP TABLE; same-session SELECT fails to resolve; ROLLBACK — one shared session") {
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

    INFO("after ROLLBACK: a fresh session sees the table alive with all rows") {
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.DropVictim;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.DropVictim WHERE count = 20;", 1);
    }

    INFO("after ROLLBACK: the storage payload file was never dropped") {
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

    INFO("BEGIN; DROP TABLE; COMMIT — the deferred drop runs at commit time") {
        auto session = otterbrix::session_id_t();
        auto begin_cur = dispatcher->execute_sql(session, "BEGIN;");
        REQUIRE(begin_cur->is_success());

        auto drop_cur = dispatcher->execute_sql(session, "DROP TABLE TestDatabase.DropVictim;");
        REQUIRE(drop_cur->is_success());

        auto commit_cur = dispatcher->execute_sql(session, "COMMIT;");
        REQUIRE(commit_cur->is_success());
    }

    INFO("after COMMIT: the table is gone and its storage payload file is removed") {
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
    auto config =
        test_create_config("/tmp/otterbrix/integration/test_persistence/disk_index_mixed_ops_checkpoint_restart");
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
    auto config = test_create_config(
        "/tmp/otterbrix/integration/test_persistence/index_recovery_phase4_catalog_driven_bootstrap");
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

    INFO("phase 1: SET TIMEZONE, then create + populate a table") {
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

    INFO("phase 2: restart — persistence recovered cleanly, SET TIMEZONE still works") {
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

    INFO("phase 1: disk table + index, insert, delete >30%, commit, checkpoint") {
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

    INFO("phase 2: restart — bootstrap repopulate keeps index-path queries exact") {
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
