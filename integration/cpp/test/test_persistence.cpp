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
    auto config = test_create_config(integration_fixture_path("test_persistence/wal_mixed_batch"));
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
    auto config = test_create_config(integration_fixture_path("test_persistence/wal_multi_type"));
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
    auto config = test_create_config(integration_fixture_path("test_persistence/wal_not_null"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, tag) "
                                               "VALUES ('ghost', NULL);");
            REQUIRE(cur->is_error());
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);

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
    auto config = test_create_config(integration_fixture_path("test_persistence/wal_dml_cycle"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

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
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 95;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count > 90;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 90;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::default_application_in_session") {
    auto config = test_create_config(integration_fixture_path("test_persistence/default_application"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name) VALUES "
                                               "('alice'), ('bob'), ('charlie');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 3);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, count) VALUES "
                                               "('dave', 10), ('eve', 20);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 10;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 20;", 1);

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
    auto config = test_create_config(integration_fixture_path("test_persistence/partial_insert_wal"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name) VALUES "
                                               "('alice'), ('bob'), ('charlie'), ('dave'), ('eve');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
        }

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

        // PHYSICAL_INSERT carries the chunk after default-expansion, so defaults are baked into the
        // WAL record; restart synthesises storage from the WAL chunk's types.
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'bob';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'eve';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE status = 'active';", 5);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 5);
    }
}

TEST_CASE("integration::cpp::test_persistence::wal_recovery_not_null_with_default") {
    auto config = test_create_config(integration_fixture_path("test_persistence/wal_not_null_default"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name, status) VALUES (NULL, 'test');");
            REQUIRE(cur->is_error());
        }

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
    auto config = test_create_config(integration_fixture_path("test_persistence/partial_two_cols_wal"));
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 100;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 200;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE score = 300;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE tag = 'untagged';", 3);
    }
}

// PHYSICAL_ADD_COLUMN is written before the dependent PHYSICAL_INSERT (agent_disk::storage_append_inner)
// and replayed via direct_add_column_sync, so restart reconstructs the grown schema first.
TEST_CASE("integration::cpp::test_persistence::computed_schema_growth_wal_recovery") {
    auto config = test_create_config(integration_fixture_path("test_persistence/computed_schema_growth_wal"));
    test_clear_directory(config);

    INFO("phase 1: computing table, two INSERTs growing the schema, WAL ON");
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
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (id, name) VALUES "
                                               "(1, 'alice'), (2, 'bob');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (id, name, value) VALUES "
                                               "(3, 'charlie', 100);");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            REQUIRE(cur->column_count() == 3);
        }
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE value = 100;", 1);

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'alice';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE name = 'charlie';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE id = 3;", 1);
    }
}

// A table that lost is_computed on reload would glue the post-restart {a:bool} chunk into the
// bigint 'a' column by name, instead of opening a new variant column.
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
            REQUIRE(cur->column_count() == 3);
        }
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT id, a::?bigint FROM TestDatabase.TestCollection ORDER BY id;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 2);
            REQUIRE(c2->value(1, 0).value<int64_t>() == 10);
            REQUIRE(c2->value(1, 1).is_null());
        }
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE id = 1;", 1);
    }

    INFO("phase 2: restart, add the THIRD type variant (bool), all three merge");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);

        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE id = 1;", 1);
        {
            auto s2 = otterbrix::session_id_t();
            auto c2 = dispatcher->execute_sql(s2, "SELECT a::?bigint FROM TestDatabase.TestCollection WHERE id = 1;");
            REQUIRE(c2->is_success());
            REQUIRE(c2->size() == 1);
            REQUIRE(c2->value(0, 0).value<int64_t>() == 10);
        }

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
        REQUIRE(cur->column_count() == 4);

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
        // Pins the reopened-oid-generator defect: a re-minted (a, bool) attoid reused one already
        // taken by a persisted computed column, zeroing every pushed filter.
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

// Computed flag must survive WAL replay synthesis, not just a clean .otbx reload: a crash can
// keep pg_class+WAL while losing the file, so the synthesised entry must stay computed (from pg_class.relkind).
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);

        // Only a synthesised entry that kept is_computed grows a new column here instead of gluing by name.
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
        REQUIRE(cur->column_count() == 4);

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

// A table's file lives at ${disk_root}/${relnamespace}/${table_oid}/table.otbx, never under
// well_known_oid::main_database. relkind='g' is deliberate: rehydrate_missing_user_storages_sync
// skips 'g', so only replay synthesis rebuilds it.
TEST_CASE("integration::cpp::test_persistence::replay_synthesis_places_otbx_under_its_namespace") {
    auto config = test_create_config(integration_fixture_path("test_persistence/replay_ns_src"));
    test_clear_directory(config);

    const std::filesystem::path crash_dir = integration_fixture_path("test_persistence/replay_ns_copy");

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

        const auto live = user_table_dirs(config.disk.path);
        INFO("the live directory must hold exactly the one user table");
        REQUIRE(live.size() == 1);
        live_ns = live.front().first;
        live_tbl = live.front().second;

        std::filesystem::remove_all(crash_dir);
        std::filesystem::create_directories(crash_dir.parent_path());
        std::filesystem::copy(config.main_path, crash_dir, std::filesystem::copy_options::recursive);
    }

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
            CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 2);
        }

        REQUIRE(std::filesystem::exists(crash_config.disk.path / std::to_string(live_ns) /
                                        std::to_string(live_tbl) / "table.otbx"));
        REQUIRE_FALSE(std::filesystem::exists(
            crash_config.disk.path /
            std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::main_database)) /
            std::to_string(live_tbl) / "table.otbx"));
        const auto after = user_table_dirs(crash_config.disk.path);
        REQUIRE(after.size() == 1);
        REQUIRE(after.front().first == live_ns);
        REQUIRE(after.front().second == live_tbl);
    }

    std::filesystem::remove_all(crash_dir);
}

// A zero-column REGULAR table (relkind='r') must not come back computed — its empty pg_attribute
// schema is the same shape the computed-table heuristic looks for.
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
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.OneCol (id, a) VALUES (1, 10);");
            REQUIRE_FALSE(cur->is_success());
        }
    }
}

TEST_CASE("integration::cpp::test_persistence::double_restart") {
    auto config = test_create_config(integration_fixture_path("test_persistence/double_restart"));
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);

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
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}


TEST_CASE("integration::cpp::test_persistence::disk_checkpoint_basic") {
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_basic"));
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);

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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_update"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection SET count = 999 WHERE count = 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_plus_wal"));
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
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}


TEST_CASE("integration::cpp::test_persistence::disk_partial_insert") {
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_partial_insert"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "INSERT INTO TestDatabase.TestCollection (name) VALUES ('dave'), ('eve');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 5);

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

        // Only a new partial INSERT proves the DEFAULT survived restart.
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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_not_null_default"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.TestCollection (name, status) VALUES "
                                               "('alice', 'active'), ('bob', 'pending');");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(
                session,
                "INSERT INTO TestDatabase.TestCollection (name, status) VALUES (NULL, 'test');");
            REQUIRE(cur->is_error());
        }

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

        // A new partial INSERT proves the NOT NULL DEFAULT still fills after restart.
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

// CHECK (c IS NOT NULL) compiles against the plan's copy of the DEFAULT, but the value actually
// written comes from the storage-layer column list, which has no defaults after a restart — so
// NULL gets stored despite the CHECK admitting it.
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
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE c IS NULL;", 0);
    }
}

// Uniqueness diverges the same way: an omitted key is compared against the catalog DEFAULT,
// while NULL is what actually lands on disk after a restart.
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
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE code = 5;", 1);
        CHECK_FIND_SQL("SELECT id FROM TestDatabase.TestCollection WHERE code IS NULL;", 0);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_wal_only_recovery") {
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_wal_only"));
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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_double_restart"));
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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_dml_cycle"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection SET count = 999 WHERE count = 50;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 91);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 999;", 1);

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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_drop_table"));
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

// Used to diff the file set before/after a GC sweep.
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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_drop_gc"));
    test_clear_directory(config);

    // Two nets: PRIMARY drop_storage removes .otbx + sidecars during the DROP statement itself;
    // SECONDARY mark_storage_dropped_many tombstones it for the next commit's horizon broadcast to sweep.
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
    REQUIRE_FALSE(std::filesystem::exists(victim_otbx));
    REQUIRE_FALSE(std::filesystem::exists(victim_otbx.parent_path()));

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO TestDatabase.GcSurvivor (val) VALUES (43);");
        REQUIRE(cur->is_success());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto after_gc_files = scan_otbx_files(config.disk.path);
    for (const auto& f : baseline_files) {
        REQUIRE(after_gc_files.find(f) != after_gc_files.end());
    }
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.GcSurvivor;", 2);
}

// A DROP TABLE inside an explicit transaction is revertible until COMMIT: the catalog delete is
// MVCC-visible to the dropping session (self-write) but the storage drop is deferred to the
// post-publish commit tail, so ROLLBACK leaves the file untouched and only COMMIT reclaims it.
TEST_CASE("integration::cpp::test_persistence::drop_rollback") {
    auto config = test_create_config(integration_fixture_path("test_persistence/drop_rollback"));
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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_add_column"));
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

// MVCC commit-clock restore on reopen: without it a fresh reader snapshots published_horizon_=0
// and every committed DELETE reads as "after my snapshot", so deleted rows reappear. Restore
// raises published_horizon_ to the durable frontier (max persisted commit-id).
TEST_CASE("integration::cpp::test_persistence::reopen_keeps_committed_deletes_invisible") {
    auto config = test_create_config(integration_fixture_path("test_persistence/reopen_keeps_committed_deletes"));
    test_clear_directory(config);

    INFO("phase 1: WAL-backed table, INSERT 100, DELETE 50, CHECKPOINT");
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 50);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 50;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
    }
}

TEST_CASE("integration::cpp::test_persistence::disk_index_mixed_ops_checkpoint_restart") {
    auto config =
        test_create_config(integration_fixture_path("test_persistence/disk_index_mixed_ops_checkpoint_restart"));
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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_index_long_keys"));
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
    auto config = test_create_config(integration_fixture_path("test_persistence/disk_index_massive_checkpoint_cycle"));
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

// bootstrap_indexes_sync must re-mint the engine and respawn the disk agent from pg_index alone.
TEST_CASE("integration::cpp::test_persistence::index_recovery_phase4_catalog_driven_bootstrap") {
    auto config = test_create_config(
        integration_fixture_path("test_persistence/index_recovery_phase4_catalog_driven_bootstrap"));
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

        // Layout is oid-keyed with no index name, so the dir is found by content: the
        // ordered b+tree backend owns a `metadata` file (bitcask would own CURRENT instead).
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users;", 10);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_0@x';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_5@x';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'user_9@x';", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.users WHERE email = 'missing@x';", 0);

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

// SET TIMEZONE persists via pg_settings, refreshed into default_tz_cat_ on restart; not
// queryable via SELECT, so this only characterizes that the write survives restart and stays usable.
TEST_CASE("integration::cpp::test_persistence::set_timezone_survives_restart") {
    auto config = test_create_config(integration_fixture_path("test_persistence/set_timezone_survives_restart"));
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 3);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 3;", 1);

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

// Commit-path compaction is gated for indexed tables, so a commit doesn't shift ids, but the
// live index must already hide deleted rows; CHECKPOINT then repopulates the on-disk index.
TEST_CASE("integration::cpp::test_persistence::indexed_table_compact_survives_restart") {
    auto config =
        test_create_config(integration_fixture_path("test_persistence/indexed_table_compact_survives_restart"));
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

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count < 40;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 40);
        }

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 60);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 39;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 40;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);

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
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 39;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 40;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 70;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count >= 40;", 60);
    }
}

// Regression guard for the SSB "4ms / 0 rows" bug: with disk+wal OFF, pg_class still persists
// unconditionally, so phase-1 rows must survive reopen (storage_append must not silently no-op).
TEST_CASE("integration::cpp::test_persistence::reopen_reinsert_visible") {
    auto config = test_create_config(integration_fixture_path("test_persistence/reopen_in_memory_reinsert"));
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

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 200);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 2);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 99;", 2);
    }
}

// Disk is the only storage mode: a plain CREATE TABLE (no opt-in) produces a .otbx with
// relstoragemode == 'd', round-tripping create -> insert -> restart -> read back.
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

        // relstoragemode=='d' is asserted separately since `SELECT oid FROM pg_class` fails --
        // MEASURED two causes: no seeded pg_class rows to resolve by name, and unqualified names losing schema.
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

// A checkpoint truncates the WAL at/below the floor checkpoint_all reports; too far deletes
// un-checkpointed rows, too little replays a folded segment again. Truncation starts on the
// second checkpoint; max_segment_size is deliberately small so segments actually retire.
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
    constexpr int kBeforeCheckpoint = 200;
    constexpr int kAfterCheckpoint = 200;
    constexpr int kAfterTruncate = 50;

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

        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CHECKPOINT;")->is_success());
        }

        insert_range(kBeforeCheckpoint, kBeforeCheckpoint + kAfterCheckpoint);

        segments_before_truncate = count_wal_segments();
        REQUIRE(segments_before_truncate > 1);

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
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 0;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 199;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 200;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TruncCollection WHERE count = 399;", 1);

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
