#include "test_config.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>

#include <catch2/catch.hpp>
#include <sstream>
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
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");                                \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            auto types = gen_data_chunk(0, dispatcher->resource()).types();                                            \
            std::vector<components::table::column_definition_t> columns;                                               \
            columns.reserve(types.size());                                                                             \
            for (const auto& type : types) {                                                                           \
                columns.emplace_back(type.alias(), type);                                                              \
            }                                                                                                          \
            dispatcher->create_collection(session, database_name, collection_name, columns);                           \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION()                                                                                              \
    do {                                                                                                               \
        auto chunk = gen_data_chunk(kDocuments, dispatcher->resource());                                               \
        auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(                                  \
            dispatcher->resource(),                                                                                    \
            database_name,                                                                                             \
            collection_name,                                                                                           \
            components::logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));                     \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->execute_plan(session, ins);                                                                    \
        }                                                                                                              \
    } while (false)

#define CREATE_INDEX(INDEX_NAME, KEY)                                                                                  \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     core::indexname_t{INDEX_NAME},                    \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        dispatcher->create_index(session, database_name, collection_name, node);                                       \
    } while (false)

#define CREATE_EXISTED_INDEX(INDEX_NAME, KEY)                                                                          \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     core::indexname_t{INDEX_NAME},                    \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        auto res = dispatcher->create_index(session, database_name, collection_name, node);                            \
        REQUIRE(res->is_error() == true);                                                                              \
        /* DML operators self-contain their I/O; the executor wraps any */                                             \
        /* operator-level set_error into create_physical_plan_error with the */                                        \
        /* original message. operator_create_index_backfill::set_error("index already exists") */                      \
        /* surfaces here as that wrapped code. */                                                                      \
        REQUIRE((res->get_error().type == core::error_code_t::index_create_fail ||                                     \
                 res->get_error().type == core::error_code_t::create_physical_plan_error));                            \
    } while (false)

#define DROP_INDEX(INDEX_NAME)                                                                                         \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        /* drop_index carries no names; wrap with resolve_table siblings so resolve stamps OIDs. */                    \
        auto node = components::logical_plan::make_node_drop_index(dispatcher->resource());                            \
        std::vector<std::pair<std::string, std::string>> targets;                                                      \
        targets.emplace_back(database_name, collection_name);                                                          \
        targets.emplace_back(database_name, std::string{INDEX_NAME});                                                  \
        auto plan = components::sql::transform::maybe_wrap_with_catalog_resolve_tables(dispatcher->resource(),         \
                                                                                       std::move(targets),             \
                                                                                       node);                          \
        dispatcher->execute_plan(session, std::move(plan));                                                            \
    } while (false)

#define CHECK_FIND_ALL()                                                                                               \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),                              \
                                                                  core::dbname_t{database_name},                       \
                                                                  core::relname_t{collection_name});                   \
        auto c =                                                                                                       \
            dispatcher->find(session, plan, components::logical_plan::make_parameter_node(dispatcher->resource()));    \
        REQUIRE(c->size() == kDocuments);                                                                              \
    } while (false)

#define CHECK_FIND(KEY, COMPARE, SIDE, VALUE, COUNT)                                                                   \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),                              \
                                                                  core::dbname_t{database_name},                       \
                                                                  core::relname_t{collection_name});                   \
        auto expr = components::expressions::make_compare_expression(dispatcher->resource(),                           \
                                                                     COMPARE,                                          \
                                                                     key{dispatcher->resource(), KEY, SIDE},           \
                                                                     id_par{1});                                       \
        plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),                           \
                                                                     core::dbname_t{database_name},                    \
                                                                     core::relname_t{collection_name},                 \
                                                                     std::move(expr)));                                \
        auto params = components::logical_plan::make_parameter_node(dispatcher->resource());                           \
        params->add_parameter(id_par{1}, VALUE);                                                                       \
        auto c = dispatcher->find(session, plan, params);                                                              \
        REQUIRE(c->size() == COUNT);                                                                                   \
    } while (false)

#define CHECK_FIND_COUNT(COMPARE, SIDE, VALUE, COUNT) CHECK_FIND("count", COMPARE, SIDE, VALUE, COUNT)

// SQL-driven assertion helper: run QUERY in a fresh per-statement session and
// require it succeeds and returns COUNT rows. Used by the disk-index coherence
// cases below that need CHECKPOINT / VACUUM statements (SQL-only verbs).
#define CHECK_FIND_SQL(QUERY, COUNT)                                                                                    \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto cur = dispatcher->execute_sql(session, QUERY);                                                            \
        REQUIRE(cur->is_success());                                                                                    \
        REQUIRE(cur->size() == static_cast<std::size_t>(COUNT));                                                       \
    } while (false)

// Index disk layout is oid-keyed (${path}/${table_oid}/${index_name}).
// The test fixture creates exactly one user table, so we resolve the
// table_oid by scanning for the numeric directory that contains the named
// index dir.
#define CHECK_EXISTS_INDEX(NAME, EXISTS)                                                                               \
    do {                                                                                                               \
        bool found = false;                                                                                            \
        if (std::filesystem::exists(config.disk.path)) {                                                               \
            for (const auto& d : std::filesystem::directory_iterator(config.disk.path)) {                              \
                if (!d.is_directory())                                                                                 \
                    continue;                                                                                          \
                try {                                                                                                  \
                    auto oid = std::stoull(d.path().filename().string());                                              \
                    if (oid < 16384)                                                                                   \
                        continue;                                                                                      \
                } catch (...) {                                                                                        \
                    continue;                                                                                          \
                }                                                                                                      \
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

            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         core::dbname_t{database_name},
                                                                         core::relname_t{collection_name},
                                                                         std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, logical_value_t(dispatcher->resource(), 10));
            auto c = dispatcher->find(session, plan, params);
            REQUIRE(c->size() == 1);
        } while (false);
        CHECK_FIND_COUNT(compare_type::eq, side_t::left, logical_value_t(dispatcher->resource(), 10), 1);
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, logical_value_t(dispatcher->resource(), 10), 90);
        CHECK_FIND_COUNT(compare_type::lt, side_t::left, logical_value_t(dispatcher->resource(), 10), 9);
        CHECK_FIND_COUNT(compare_type::ne, side_t::left, logical_value_t(dispatcher->resource(), 10), 99);
        CHECK_FIND_COUNT(compare_type::gte, side_t::left, logical_value_t(dispatcher->resource(), 10), 91);
        CHECK_FIND_COUNT(compare_type::lte, side_t::left, logical_value_t(dispatcher->resource(), 10), 10);
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
        CHECK_FIND_COUNT(compare_type::eq, side_t::left, 10, 1);
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, 10, 90);
        CHECK_FIND_COUNT(compare_type::lt, side_t::left, 10, 9);
        CHECK_FIND_COUNT(compare_type::ne, side_t::left, 10, 99);
        CHECK_FIND_COUNT(compare_type::gte, side_t::left, 10, 91);
        CHECK_FIND_COUNT(compare_type::lte, side_t::left, 10, 10);
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
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, logical_value_t(dispatcher->resource(), 50), 50);
    }

    INFO("delete rows where count > 90") {
        {
            auto session = otterbrix::session_id_t();
            auto del = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                database_name,
                collection_name,
                components::logical_plan::make_node_delete_many(
                    dispatcher->resource(),
                    components::logical_plan::make_node_match(dispatcher->resource(),
                                                              core::dbname_t{database_name},
                                                              core::relname_t{collection_name},
                                                              components::expressions::make_compare_expression(
                                                                  dispatcher->resource(),
                                                                  compare_type::gt,
                                                                  key{dispatcher->resource(), "count", side_t::left},
                                                                  id_par{1}))));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, logical_value_t(dispatcher->resource(), 90));
            auto cur = dispatcher->execute_plan(session, del, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("verify index after delete") {
        // count > 50 should now match rows 51..90 → 40 rows
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, logical_value_t(dispatcher->resource(), 50), 40);
    }

    INFO("update row where count == 50 to count = 999") {
        {
            auto session = otterbrix::session_id_t();
            auto match = components::logical_plan::make_node_match(
                dispatcher->resource(),
                core::dbname_t{database_name},
                core::relname_t{collection_name},
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1}));
            components::expressions::update_expr_ptr update_expr = new components::expressions::update_expr_set_t(
                components::expressions::key_t{dispatcher->resource(), "count"});
            update_expr->left() = new components::expressions::update_expr_get_const_value_t(id_par{2});
            auto upd = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                database_name,
                collection_name,
                components::logical_plan::make_node_update_many(dispatcher->resource(), match, {update_expr}));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, logical_value_t(dispatcher->resource(), 50));
            params->add_parameter(id_par{2}, logical_value_t(dispatcher->resource(), 999));
            auto cur = dispatcher->execute_plan(session, upd, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("verify index after update") {
        // count == 50 should now return 0 rows (was updated to 999)
        CHECK_FIND_COUNT(compare_type::eq, side_t::left, logical_value_t(dispatcher->resource(), 50), 0);
        // count == 999 should return 1 row
        CHECK_FIND_COUNT(compare_type::eq, side_t::left, logical_value_t(dispatcher->resource(), 999), 1);
    }
}

// The CHECKPOINT compact path shifts storage_row ids of an indexed disk table
// within a SINGLE running session (no restart). Without a
// repopulate-on-compact, the on-disk index still holds the pre-compact ids
// (btree duplicate-growth / disk_hash wrong-row), so a same-session index
// lookup after the checkpoint returns stale or wrong rows. The clear-then-
// repopulate (txn_id=0) handler must rebuild the index against the compacted
// ids so equality lookups stay exact with no restart in between.
TEST_CASE("integration::cpp::test_index::checkpoint_then_index_scan_same_session") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/checkpoint_then_index_scan_same_session");
    test_clear_directory(config);
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

    // INSERT 50 rows, count = 0..49.
    {
        auto session = otterbrix::session_id_t();
        std::stringstream q;
        q << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
        for (int i = 0; i < 50; ++i) {
            q << "('row_" << i << "', " << i << ")" << (i == 49 ? ";" : ", ");
        }
        auto cur = dispatcher->execute_sql(session, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 50);
    }

    // DELETE the lower half (count < 25): 25 rows go, so compact actually has to
    // shift the surviving ids down (storage_row reuse is what corrupts the index).
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count < 25;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 25);
    }

    // CHECKPOINT compacts the heap (ids shift) and must repopulate the index.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
        REQUIRE(cur->is_success());
    }

    // SAME SESSION-scope (no restart): index-path lookups must be exact.
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 25);
    // A surviving value resolves to exactly its one row (not a wrong/stale row).
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 25;", 1);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
    // A deleted value must resolve to zero rows (no stale pre-compact id hit).
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 24;", 0);
}

// VACUUM rebuilds the index. Entries inserted under a real txn id stay
// PENDING-invisible unless that txn index-commits, and VACUUM never
// index-commits, so a rebuild under ctx->txn would be invisible to every reader
// (index-path SELECTs returning 0). VACUUM's rebuild must go through the
// repopulate path (txn_id=0, committed-for-everyone) so post-VACUUM lookups
// return the correct surviving rows.
TEST_CASE("integration::cpp::test_index::vacuum_rebuild_visible") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/vacuum_rebuild_visible");
    test_clear_directory(config);
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

    // INSERT 50 rows, count = 0..49.
    {
        auto session = otterbrix::session_id_t();
        std::stringstream q;
        q << "INSERT INTO TestDatabase.TestCollection (name, count) VALUES ";
        for (int i = 0; i < 50; ++i) {
            q << "('row_" << i << "', " << i << ")" << (i == 49 ? ";" : ", ");
        }
        auto cur = dispatcher->execute_sql(session, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 50);
    }

    // DELETE > 30% (every count divisible by 3 in 0..49 → 17 rows) so VACUUM has
    // real dead tuples to compact and the index must be rebuilt.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count % 3 = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 17);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "VACUUM;");
        REQUIRE(cur->is_success());
    }

    // After VACUUM the rebuilt index must be VISIBLE: surviving values return
    // their rows.
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", 33);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1;", 1);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 49;", 1);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count > 40;", 6);
    // Deleted multiples of 3 stay gone via the rebuilt index.
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 48;", 0);
}
