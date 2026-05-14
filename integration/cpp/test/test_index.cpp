#include "test_config.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/tests/generaty.hpp>

#include <catch2/catch.hpp>
#include <unistd.h>

using components::expressions::compare_type;
using components::expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;
using namespace components::types;

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

constexpr int kDocuments = 100;

#define INIT_COLLECTION()                                                                                              \
    do {                                                                                                               \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->create_database(session, database_name);                                                       \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->execute_sql(session,                                                                            \
                fmt::format("CREATE TABLE {}.{}(count bigint, count_str string, "                                       \
                            "count_double double, count_bool bool, count_array ubigint[5], "                            \
                            "count_decimal decimal(15,7));",                                                            \
                            database_name, collection_name));                                                           \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION()                                                                                              \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        dispatcher->execute_sql(session,                                                                                \
            gen_data_chunk_insert_sql(database_name, collection_name, kDocuments));                                     \
    } while (false)

#define CREATE_INDEX(INDEX_NAME, KEY)                                                                                  \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     database_name,                                    \
                                                                     collection_name,                                  \
                                                                     INDEX_NAME,                                       \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        dispatcher->create_index(session, node);                                                                       \
    } while (false)

#define CREATE_EXISTED_INDEX(INDEX_NAME, KEY)                                                                          \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     database_name,                                    \
                                                                     collection_name,                                  \
                                                                     INDEX_NAME,                                       \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        auto res = dispatcher->create_index(session, node);                                                            \
        REQUIRE(res->is_error() == true);                                                                              \
        /* Phase 5: DML operators self-contain their I/O; the executor wraps any */                                    \
        /* operator-level set_error into create_physical_plan_error with the */                                        \
        /* original message. operator_create_index_backfill::set_error("index already exists") */                      \
        /* surfaces here as that wrapped code, not the older catalog-level index_create_fail. */                       \
        REQUIRE((res->get_error().type == components::cursor::error_code_t::index_create_fail ||                       \
                 res->get_error().type == components::cursor::error_code_t::create_physical_plan_error));              \
    } while (false)

#define DROP_INDEX(INDEX_NAME)                                                                                         \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        dispatcher->execute_sql(session,                                                                                \
            fmt::format("DROP INDEX {}.{}.{};", database_name, collection_name, INDEX_NAME));                          \
    } while (false)

#define CHECK_FIND_ALL()                                                                                               \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto c = dispatcher->execute_sql(session,                                                                       \
            fmt::format("SELECT * FROM {}.{};", database_name, collection_name));                                       \
        REQUIRE(c->size() == kDocuments);                                                                              \
    } while (false)

#define CHECK_FIND_COUNT_SQL(OP_STR, VALUE_SQL, COUNT)                                                                 \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto c = dispatcher->execute_sql(session,                                                                       \
            fmt::format("SELECT * FROM {}.{} WHERE count {} {};",                                                       \
                        database_name, collection_name, OP_STR, VALUE_SQL));                                            \
        REQUIRE(c->size() == COUNT);                                                                                   \
    } while (false)

// Phase 8.A migrated index disk layout from name-keyed
// (${path}/${db_name}/${coll_name}/${index_name}) to oid-keyed
// (${path}/${table_oid}/${index_name}). The test fixture creates exactly one
// user table, so we resolve the table_oid by scanning for the numeric directory
// that contains the named index dir.
#define CHECK_EXISTS_INDEX(NAME, EXISTS)                                                                               \
    do {                                                                                                               \
        bool found = false;                                                                                            \
        if (std::filesystem::exists(config.disk.path)) {                                                               \
            for (const auto& d : std::filesystem::directory_iterator(config.disk.path)) {                              \
                if (!d.is_directory()) continue;                                                                       \
                try {                                                                                                  \
                    auto oid = std::stoull(d.path().filename().string());                                              \
                    if (oid < 16384) continue;                                                                         \
                } catch (...) { continue; }                                                                            \
                auto candidate = d.path() / NAME;                                                                      \
                if (std::filesystem::exists(candidate) && std::filesystem::is_directory(candidate)) {                  \
                    found = true;                                                                                      \
                    break;                                                                                             \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
        REQUIRE(found == EXISTS);                                                                                      \
    } while (false)

TEST_CASE("integration::cpp::test_index::base") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/base");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        FILL_COLLECTION();
    }

    INFO("find") {
        CHECK_FIND_ALL();
        do {
            auto session = otterbrix::session_id_t();
            auto c = dispatcher->execute_sql(session,
                fmt::format("SELECT * FROM {}.{} WHERE count = 10;",
                            database_name, collection_name));
            REQUIRE(c->size() == 1);
        } while (false);
        CHECK_FIND_COUNT_SQL("=",  "10",  1);
        CHECK_FIND_COUNT_SQL(">",  "10",  90);
        CHECK_FIND_COUNT_SQL("<",  "10",  9);
        CHECK_FIND_COUNT_SQL("!=", "10",  99);
        CHECK_FIND_COUNT_SQL(">=", "10",  91);
        CHECK_FIND_COUNT_SQL("<=", "10",  10);
    }
}

TEST_CASE("integration::cpp::test_index::drop") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/drop");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("dcount", "count_double");
        FILL_COLLECTION();
        usleep(1000000); //todo: wait
    }

    INFO("drop indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("ncount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("scount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("dcount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", false);

        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", false);
    }
}

TEST_CASE("integration::cpp::test_index::index already exist") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/index_already_exist");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("dcount", "count_double");
        FILL_COLLECTION();
    }

    INFO("add existed ncount index") {
        CREATE_EXISTED_INDEX("ncount", "count");
        CREATE_EXISTED_INDEX("ncount", "count");
    }

    INFO("add existed scount index") {
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("scount", "count_str");
    }

    INFO("add existed dcount index") {
        CREATE_INDEX("dcount", "count_double");
        CREATE_INDEX("dcount", "count_double");
    }

    INFO("find") {
        CHECK_FIND_ALL();
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);
    }
}

TEST_CASE("integration::cpp::test_index::no_type base check") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/no_type_base_check");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("dcount", "count_double");
        CREATE_INDEX("scount", "count_str");
        FILL_COLLECTION();
    }

    INFO("check indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("dcount", true);
        CHECK_EXISTS_INDEX("scount", true);
    }

    INFO("find") {
        CHECK_FIND_COUNT_SQL("=",  "10",  1);
        CHECK_FIND_COUNT_SQL(">",  "10",  90);
        CHECK_FIND_COUNT_SQL("<",  "10",  9);
        CHECK_FIND_COUNT_SQL("!=", "10",  99);
        CHECK_FIND_COUNT_SQL(">=", "10",  91);
        CHECK_FIND_COUNT_SQL("<=", "10",  10);
    }
}

TEST_CASE("integration::cpp::test_index::delete_and_update") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/delete_and_update");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        FILL_COLLECTION();
    }

    INFO("verify initial state via index") {
        // count > 50 should match rows 51..100 → 50 rows
        CHECK_FIND_COUNT_SQL(">", "50", 50);
    }

    INFO("delete rows where count > 90") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("DELETE FROM {}.{} WHERE count > 90;", database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("verify index after delete") {
        // count > 50 should now match rows 51..90 → 40 rows
        CHECK_FIND_COUNT_SQL(">", "50", 40);
    }

    INFO("update row where count == 50 to count = 999") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("UPDATE {}.{} SET count = 999 WHERE count = 50;",
                            database_name, collection_name));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("verify index after update") {
        // count == 50 should now return 0 rows (was updated to 999)
        CHECK_FIND_COUNT_SQL("=", "50", 0);
        // count == 999 should return 1 row
        CHECK_FIND_COUNT_SQL("=", "999", 1);
    }
}
