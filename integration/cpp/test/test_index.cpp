#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <algorithm>
#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>
#include <services/collection/context_storage.hpp>

#include <array>
#include <catch2/catch_test_macros.hpp>
#include <charconv>
#include <map>
#include <set>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <sstream>
#include <unistd.h>
#include <utility>

using components::expressions::compare_type;
using components::expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;
using namespace components::types;

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

constexpr int kDocuments = 100;

// The on-disk index layout is oid-keyed (<disk>/<table_oid>/<indexrelid>/) and carries
// no index name, so a test binds name -> directory by observing which directory a
// CREATE INDEX brought into being (see CREATE_INDEX). CHECK_EXISTS_INDEX then asserts
// on the recorded directory's existence.
static std::map<std::string, std::filesystem::path> g_created_index_dirs;

static std::set<std::filesystem::path> list_index_dirs(const std::filesystem::path& disk_path) {
    std::set<std::filesystem::path> dirs;
    if (!std::filesystem::exists(disk_path)) {
        return dirs;
    }
    for (const auto& tbl : std::filesystem::directory_iterator(disk_path)) {
        if (!tbl.is_directory()) {
            continue;
        }
        // user-table dirs are oid-named (>= FIRST_USER_OID)
        const auto fn = tbl.path().filename().string();
        uint64_t table_oid = 0;
        const auto [ptr, ec] = std::from_chars(fn.data(), fn.data() + fn.size(), table_oid);
        if (ec != std::errc{} || ptr != fn.data() + fn.size() || table_oid < 16384) {
            continue;
        }
        for (const auto& sub : std::filesystem::directory_iterator(tbl.path())) {
            if (sub.is_directory()) {
                dirs.insert(sub.path());
            }
        }
    }
    return dirs;
}

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
            test_create_collection(dispatcher, session, database_name, collection_name, columns);                      \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION()                                                                                              \
    do {                                                                                                               \
        auto chunk = gen_data_chunk(kDocuments, dispatcher->resource());                                               \
        auto ins = components::sql::transform::name_catalog_target(                                                    \
            database_name,                                                                                             \
            collection_name,                                                                                           \
            components::logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));                     \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->execute_plan(                                                                                  \
                session,                                                                                               \
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});                     \
        }                                                                                                              \
    } while (false)

#define CREATE_INDEX(INDEX_NAME, KEY)                                                                                  \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     core::indexname_t{INDEX_NAME},                    \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        auto plan = components::sql::transform::name_catalog_target(database_name, collection_name, node);             \
        const auto dirs_before = list_index_dirs(config.disk.path);                                                    \
        dispatcher->execute_plan(session,                                                                              \
                                 components::logical_plan::execution_plan_t{dispatcher->resource(), plan, nullptr});   \
        /* bind name -> the directory this CREATE brought into being (oid-keyed layout carries no name) */             \
        for (const auto& d : list_index_dirs(config.disk.path)) {                                                      \
            if (dirs_before.count(d) == 0) {                                                                           \
                g_created_index_dirs[INDEX_NAME] = d;                                                                  \
                break;                                                                                                 \
            }                                                                                                          \
        }                                                                                                              \
    } while (false)

#define CREATE_EXISTED_INDEX(INDEX_NAME, KEY)                                                                          \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto node = components::logical_plan::make_node_create_index(dispatcher->resource(),                           \
                                                                     core::indexname_t{INDEX_NAME},                    \
                                                                     components::logical_plan::index_type::single);    \
        node->keys().emplace_back(dispatcher->resource(), KEY);                                                        \
        auto plan = components::sql::transform::name_catalog_target(database_name, collection_name, node);             \
        auto res = dispatcher->execute_plan(                                                                           \
            session,                                                                                                   \
            components::logical_plan::execution_plan_t{dispatcher->resource(), plan, nullptr});                        \
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
        /* DROP INDEX names two pg_class rows: the parent table and the index itself. */                               \
        auto node = components::logical_plan::make_node_drop(dispatcher->resource(),                                   \
                                                             components::logical_plan::drop_target_kind::index);       \
        node->set_dbname(database_name);                                                                               \
        node->set_relname(collection_name);                                                                            \
        node->set_index_name(std::string{INDEX_NAME});                                                                 \
        dispatcher->execute_plan(session,                                                                              \
                                 components::logical_plan::execution_plan_t{dispatcher->resource(), node, nullptr});   \
    } while (false)

#define CHECK_FIND_ALL()                                                                                               \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),                              \
                                                                  core::dbname_t{database_name},                       \
                                                                  core::relname_t{collection_name});                   \
        auto c = dispatcher->execute_plan(session,                                                                     \
                                          components::logical_plan::execution_plan_t{                                  \
                                              dispatcher->resource(),                                                  \
                                              plan,                                                                    \
                                              components::logical_plan::make_parameter_node(dispatcher->resource())}); \
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
        auto c = dispatcher->execute_plan(                                                                             \
            session,                                                                                                   \
            components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});                         \
        REQUIRE(c->size() == COUNT);                                                                                   \
    } while (false)

#define CHECK_FIND_COUNT(COMPARE, SIDE, VALUE, COUNT) CHECK_FIND("count", COMPARE, SIDE, VALUE, COUNT)

// SQL-driven assertion helper: run QUERY in a fresh per-statement session and
// require it succeeds and returns COUNT rows. Used by the disk-index coherence
// cases below that need CHECKPOINT / VACUUM statements (SQL-only verbs).
#define CHECK_FIND_SQL(QUERY, COUNT)                                                                                   \
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
/* The on-disk index layout is oid-keyed (<disk>/<table_oid>/<indexrelid>/) and carries no
 * name, so resolve NAME -> indexrelid through pg_class (the only place the name lives),
 * then look for the oid-named directory. A dropped index loses both its pg_class row and
 * its directory, so "not found" covers either signal disappearing. */
#define CHECK_EXISTS_INDEX(NAME, EXISTS)                                                                               \
    do {                                                                                                               \
        /* the oid-keyed layout carries no name; the binding was recorded at CREATE time */                            \
        bool found = false;                                                                                            \
        auto rec = g_created_index_dirs.find(NAME);                                                                    \
        if (rec != g_created_index_dirs.end()) {                                                                       \
            found = std::filesystem::exists(rec->second) && std::filesystem::is_directory(rec->second);                \
        }                                                                                                              \
        REQUIRE(found == EXISTS);                                                                                      \
    } while (false)

TEST_CASE("integration::cpp::test_index::base") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/base");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        FILL_COLLECTION();
    }

    INFO("find");
    {
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
            auto c = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});
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

    INFO("initialization");
    {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("dcount", "count_double");
        FILL_COLLECTION();
        usleep(1000000); //todo: wait
    }

    INFO("drop indexes");
    {
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

    INFO("initialization");
    {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("dcount", "count_double");
        FILL_COLLECTION();
    }

    INFO("add existed ncount index");
    {
        CREATE_EXISTED_INDEX("ncount", "count");
        CREATE_EXISTED_INDEX("ncount", "count");
    }

    INFO("add existed scount index");
    {
        CREATE_INDEX("scount", "count_str");
        CREATE_INDEX("scount", "count_str");
    }

    INFO("add existed dcount index");
    {
        CREATE_INDEX("dcount", "count_double");
        CREATE_INDEX("dcount", "count_double");
    }

    INFO("find");
    {
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

    INFO("initialization");
    {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        CREATE_INDEX("dcount", "count_double");
        CREATE_INDEX("scount", "count_str");
        FILL_COLLECTION();
    }

    INFO("check indexes");
    {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("dcount", true);
        CHECK_EXISTS_INDEX("scount", true);
    }

    INFO("find");
    {
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
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", "count");
        FILL_COLLECTION();
    }

    INFO("verify initial state via index");
    {
        // count > 50 should match rows 51..100 → 50 rows
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, logical_value_t(dispatcher->resource(), 50), 50);
    }

    INFO("delete rows where count > 90");
    {
        {
            auto session = otterbrix::session_id_t();
            auto del = components::sql::transform::name_catalog_target(
                database_name,
                collection_name,
                components::logical_plan::make_node_delete(
                    dispatcher->resource(),
                    components::logical_plan::make_node_match(dispatcher->resource(),
                                                              core::dbname_t{database_name},
                                                              core::relname_t{collection_name},
                                                              components::expressions::make_compare_expression(
                                                                  dispatcher->resource(),
                                                                  compare_type::gt,
                                                                  key{dispatcher->resource(), "count", side_t::left},
                                                                  id_par{1})),
                    components::logical_plan::make_node_limit(dispatcher->resource(),
                                                              {},
                                                              {},
                                                              components::logical_plan::limit_t::unlimit())));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, logical_value_t(dispatcher->resource(), 90));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), del, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("verify index after delete");
    {
        // count > 50 should now match rows 51..90 → 40 rows
        CHECK_FIND_COUNT(compare_type::gt, side_t::left, logical_value_t(dispatcher->resource(), 50), 40);
    }

    INFO("update row where count == 50 to count = 999");
    {
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
            // SET count = $2 — the value expression's own key names the target column.
            auto update_expr = components::expressions::make_scalar_expression(
                dispatcher->resource(),
                components::expressions::scalar_type::constant,
                components::expressions::key_t{dispatcher->resource(), "count"});
            update_expr->append_param(id_par{2});
            auto upd = components::sql::transform::name_catalog_target(
                database_name,
                collection_name,
                components::logical_plan::make_node_update(
                    dispatcher->resource(),
                    match,
                    components::logical_plan::make_node_limit(dispatcher->resource(),
                                                              {},
                                                              {},
                                                              components::logical_plan::limit_t::unlimit()),
                    {update_expr}));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, logical_value_t(dispatcher->resource(), 50));
            params->add_parameter(id_par{2}, logical_value_t(dispatcher->resource(), 999));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), upd, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("verify index after update");
    {
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
                                           ";");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
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

namespace {

    constexpr uint64_t kMinBitcaskBytesAfterCheckpoint = 10'000;

    // The on-disk layout is oid-keyed (<disk>/<table_oid>/<indexrelid>/) and carries no
    // index name, so the single hash index of these tests is found by content: only the
    // bitcask backend leaves a CURRENT marker in its directory.
    std::filesystem::path find_hash_index_dir(const std::filesystem::path& disk_path) {
        if (!std::filesystem::exists(disk_path)) {
            return {};
        }
        for (const auto& d : std::filesystem::recursive_directory_iterator(disk_path)) {
            if (d.is_directory() && std::filesystem::exists(d.path() / "CURRENT")) {
                return d.path();
            }
        }
        return {};
    }

    uint64_t bitcask_data_bytes(const std::filesystem::path& index_dir) {
        uint64_t total = 0;
        if (index_dir.empty()) {
            return 0;
        }
        for (const auto& entry : std::filesystem::directory_iterator(index_dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto name = entry.path().filename().string();
            if (name.rfind("bitcask.", 0) == 0 && name.size() >= 5 && name.compare(name.size() - 5, 5, ".data") == 0) {
                total += static_cast<uint64_t>(entry.file_size());
            }
        }
        return total;
    }

} // namespace

// CHECKPOINT repopulate_table clears the bitcask backing and rebuilds the hash
// index in memory. The handler must also mirror txn_id=0 pending rows back to
// disk agents; otherwise bitcask.*.data segments stay empty while hash_index.bin
// only holds bucket pages — index-path lookups work only until the next restart
// rescans the table via bootstrap_repopulate_sync.
TEST_CASE("integration::cpp::test_index::checkpoint_repopulate_persists_bitcask_keylog") {
    constexpr int kRows = 500;
    static const std::string kHashIndexName = "idx_count_hash";

    auto config =
        test_create_config("/tmp/otterbrix/integration/test_index/checkpoint_repopulate_persists_bitcask_keylog");
    test_clear_directory(config);

    INFO("phase 1: disk hash index, bulk load, CHECKPOINT, bitcask keylog on disk");
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
                                               "CREATE TABLE TestDatabase.TestCollection (count bigint) "
                                               ";");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE INDEX idx_count_hash ON TestDatabase.TestCollection "
                                               "USING hash (count);");
            REQUIRE(cur->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            std::stringstream q;
            q << "INSERT INTO TestDatabase.TestCollection (count) VALUES ";
            for (int i = 0; i < kRows; ++i) {
                q << "(" << (i + 1) << ")" << (i + 1 == kRows ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == static_cast<std::size_t>(kRows));
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CHECKPOINT;");
            REQUIRE(cur->is_success());
        }

        const auto index_dir = find_hash_index_dir(config.disk.path);
        REQUIRE_FALSE(index_dir.empty());
        REQUIRE(bitcask_data_bytes(index_dir) >= kMinBitcaskBytesAfterCheckpoint);

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", kRows);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 250;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 500;", 1);
    }

    INFO("phase 2: restart — persisted bitcask keylog, index-path lookups exact");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        const auto index_dir = find_hash_index_dir(config.disk.path);
        REQUIRE_FALSE(index_dir.empty());
        REQUIRE(bitcask_data_bytes(index_dir) >= kMinBitcaskBytesAfterCheckpoint);

        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", kRows);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 500;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 501;", 0);
    }
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
                                           ";");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
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

// VACUUM must not resurrect committed deletes, and the seq-scan path and the index
// path must not disagree about which rows exist.
//
// The version-info GC (row_version_manager_t::cleanup_append -> chunk_info::cleanup)
// only ever looks at a FULL vector — vcount == DEFAULT_VECTOR_CAPACITY — and the default
// row_group_size is DEFAULT_VECTOR_CAPACITY too, so it is exactly the full row groups of a
// table that reach it. vacuum_rebuild_visible above runs on 50 rows and never gets there;
// this case fills a whole row group so it does.
//
// The shape is plain SQL with no crash and no restart in it: insert 1024 rows, commit a
// DELETE of the first 500, then VACUUM with no other transaction active (so the GC floor
// is already past the delete's commit). Both the unqualified scan and the indexed
// predicates must still report the 524 survivors. Two VACUUMs, because the two legs of the
// defect ripen at different depths: a partially deleted vector loses its stamps on the
// first pass, a fully deleted one only after the second.
//
// MEASURED, about the index leg specifically: the two paths did NOT disagree while the
// GC was dropping the stamps — they lied together. `SELECT WHERE count < 500` returned
// the same 500 resurrected rows the bare scan did. operator_vacuum_t runs vacuum_all
// (the heap-side GC) BEFORE it rebuilds each table's index through
// manager_index_t::repopulate_table, and that rebuild feeds off a fresh scan of the heap
// — so it faithfully re-indexed the resurrected rows. manager_index_t::cleanup_all_versions
// deletes nothing on its own (it is a documented no-op since the in-memory index went
// away). The index predicates stay in this test anyway: they pin the agreement, so a
// future change that reclaims index entries without the heap agreeing shows up here.
TEST_CASE("integration::cpp::test_index::vacuum_keeps_committed_deletes_full_row_group") {
    constexpr int kRows = 1024;  // exactly one full row group / one full vector
    constexpr int kDeleted = 500;

    auto config =
        test_create_config(integration_fixture_path("test_index/vacuum_keeps_committed_deletes_full_row_group"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.TestCollection (count bigint);");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
        REQUIRE(cur->is_success());
    }

    // count = 0..1023, loaded in batches so each INSERT stays within one data_chunk.
    {
        constexpr int kBatch = 512;
        static_assert(kBatch <= kRows);
        int inserted = 0;
        while (inserted < kRows) {
            const int batch = std::min(kBatch, kRows - inserted);
            auto session = otterbrix::session_id_t();
            std::stringstream q;
            q << "INSERT INTO TestDatabase.TestCollection (count) VALUES ";
            for (int i = 0; i < batch; ++i) {
                q << "(" << (inserted + i) << ")" << (i + 1 == batch ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == static_cast<std::size_t>(batch));
            inserted += batch;
        }
    }
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", kRows);

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE count < 500;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<std::size_t>(kDeleted));
    }
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", kRows - kDeleted);

    // Both paths must agree, before and after every VACUUM: the seq scan on the left,
    // the index predicates on the right.
    auto check_both_paths = [&] {
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", kRows - kDeleted);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count < 500;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count >= 500;", kRows - kDeleted);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 0;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 499;", 0);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 500;", 1);
        CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 1023;", 1);
    };

    check_both_paths();

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "VACUUM;");
        REQUIRE(cur->is_success());
    }
    check_both_paths();

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "VACUUM;");
        REQUIRE(cur->is_success());
    }
    check_both_paths();
}

// CREATE INDEX backfill scans the table via the streaming fetch-next leg, which emits
// chunks of at most DEFAULT_VECTOR_CAPACITY (1024) rows. A regression that
// overwrites the scan buffer on each chunk leaves only the last chunk indexed
// in the index, so equality lookups in the first (rows - 1024) keys
// return 0 rows even though the heap row exists.
TEST_CASE("integration::cpp::test_index::create_index_backfill_over_vector_capacity") {
    constexpr int kRows = 2000;
    constexpr int kVectorCapacity = 1024;
    static_assert(kRows > kVectorCapacity);

    auto config =
        test_create_config("/tmp/otterbrix/integration/test_index/create_index_backfill_over_vector_capacity");
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
                                           "CREATE TABLE TestDatabase.TestCollection (count bigint) "
                                           ";");
        REQUIRE(cur->is_success());
    }

    // Load more rows than a single scan chunk so backfill must stitch batches.
    // Each INSERT ... VALUES list materializes one data_chunk, which must stay within
    // the ≤DEFAULT_VECTOR_CAPACITY bound, so the rows are loaded in capped batches.
    {
        constexpr int kBatch = 500;
        static_assert(kBatch <= kVectorCapacity);
        int inserted = 0;
        while (inserted < kRows) {
            const int batch = std::min(kBatch, kRows - inserted);
            auto session = otterbrix::session_id_t();
            std::stringstream q;
            q << "INSERT INTO TestDatabase.TestCollection (count) VALUES ";
            for (int i = 0; i < batch; ++i) {
                q << "(" << (inserted + i + 1) << ")" << (i + 1 == batch ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, q.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == static_cast<std::size_t>(batch));
            inserted += batch;
        }
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE INDEX idx_count ON TestDatabase.TestCollection (count);");
        REQUIRE(cur->is_success());
    }

    // The index backfill must cover all chunks, not only the trailing one.
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection;", kRows);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 504;", 1);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 976;", 1);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 977;", 1);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 2000;", 1);

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "CREATE INDEX idx_count_hash ON TestDatabase.TestCollection "
                                           "USING hash (count);");
        REQUIRE(cur->is_success());
    }

    // hash index gets the same backfill scan; spot-check after both indexes exist.
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 504;", 1);
    CHECK_FIND_SQL("SELECT * FROM TestDatabase.TestCollection WHERE count = 2000;", 1);
}

// ----------------------------------------------------------------------------
// DROP INDEX catalog-delete folding (physical-plan-generator unit test).
//
// rewrite_drop_index emits sequence_t(catalog-delete node_delete_t × N, drop_index_t)
// — N=4: pg_index ×1, pg_depend ×2, pg_class ×1. create_plan_sequence's
// drop_index_t branch must FOLD all N catalog-delete leaves into the single
// operator_drop_index_t's catalog_deletes_ vector, so the operator can issue ONE
// batched delete_pg_catalog_rows_many at runtime instead of N singular
// delete_pg_catalog_rows sends.
//
// Observable invariant of a correct fold (no production accessor needed):
//   - the lowered plan is a SINGLE leaf operator (no children) tagged
//     operator_type::create_collection — the tag operator_drop_index_t reuses;
//   - NO operator_type::remove operator survives anywhere in the tree.
// A regression that drops the fold makes the N leaves fall through to the generic
// left-child chain, where each catalog-delete node_delete_t lowers to a standalone
// operator_delete (type remove) in its catalog branch. The control sub-case below
// builds the SAME leaves WITHOUT the trailing drop_index_t and asserts they DO
// produce N standalone operator_delete operators, so the fold is exactly what
// collapses them.
// ----------------------------------------------------------------------------
namespace {

    // Counts operators of a given type across the whole left_/right_ tree.
    std::size_t count_ops_of_type(const components::operators::operator_ptr& op,
                                  components::operators::operator_type type) {
        if (!op) {
            return 0;
        }
        std::size_t n = (op->type() == type) ? 1u : 0u;
        n += count_ops_of_type(op->left(), type);
        n += count_ops_of_type(op->right(), type);
        return n;
    }

} // namespace

TEST_CASE("integration::cpp::test_index::drop_index_folds_catalog_deletes") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;

    services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(res);

    namespace lp = components::logical_plan;
    namespace ops = components::operators;
    using components::catalog::oid_t;
    constexpr oid_t index_oid = 9001; // any non-INVALID oid

    constexpr oid_t pg_index = components::catalog::well_known_oid::pg_index_table;
    constexpr oid_t pg_depend = components::catalog::well_known_oid::pg_depend_table;
    constexpr oid_t pg_class = components::catalog::well_known_oid::pg_class_table;

    // The exact N=4 primitive_delete spec set rewrite_drop_index emits:
    // pg_index(objid col 0), pg_depend(objid col 1), pg_depend(refobjid col 3),
    // pg_class(oid col 0). Same order so the test fails loudly if that contract drifts.
    const std::array<std::pair<oid_t, std::int64_t>, 4> delete_specs = {{
        {pg_index, std::int64_t{0}},
        {pg_depend, std::int64_t{1}},
        {pg_depend, std::int64_t{3}},
        {pg_class, std::int64_t{0}},
    }};

    auto append_delete_leaves = [&](const lp::node_sequence_ptr& seq) {
        for (const auto& [catalog_oid, col] : delete_specs) {
            seq->append_child(lp::make_node_catalog_delete(res, catalog_oid, col, index_oid));
        }
    };

    INFO("trailing drop_index_t folds all N delete leaves into one operator_drop_index_t");
    {
        auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
        append_delete_leaves(seq);
        auto di = lp::make_node_drop(res, lp::drop_target_kind::index);
        di->set_index_oid(index_oid);
        seq->append_child(di); // trailing drop_index_t marker

        auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);
        REQUIRE(plan);

        // Folded → a single leaf operator, NOT a chain. operator_drop_index_t
        // reuses operator_type::create_collection as its tag and absorbs the
        // delete leaves into catalog_deletes_ (one batched send at runtime).
        CHECK(plan->type() == ops::operator_type::create_collection);
        CHECK(plan->left() == nullptr);
        CHECK(plan->right() == nullptr);

        // None of the N delete leaves leaked out as a standalone operator —
        // they were folded into the single drop_index operator's vector.
        CHECK(count_ops_of_type(plan, ops::operator_type::remove) == 0u);
    }

    INFO("control: same delete leaves with NO trailing drop_index_t stay N standalone operators");
    {
        // Without the drop_index_t marker the leaves fall through to the generic
        // left-child chain and each catalog-delete node_delete_t lowers to its own
        // operator_delete (catalog branch). This is the un-folded baseline the
        // drop_index branch collapses.
        auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
        append_delete_leaves(seq);

        auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);
        REQUIRE(plan);
        CHECK(count_ops_of_type(plan, ops::operator_type::remove) == delete_specs.size());
    }
}

// Expression index elements — CREATE INDEX ... ((expr)) — parse with a null
// IndexElem.name; the transformer read it unconditionally and threw an
// uncaught std::logic_error out of execute_sql. They must be rejected with a
// proper error cursor; plain column indexes keep working.
TEST_CASE("integration::cpp::test_index::expression_elements_rejected") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/expression_elements_rejected");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE t;")->is_success());
    REQUIRE(exec("CREATE TABLE t.ix (x BIGINT, y TEXT);")->is_success());
    REQUIRE(exec("INSERT INTO t.ix (x, y) VALUES (1, 'a');")->is_success());

    CHECK_FALSE(exec("CREATE INDEX arith_idx ON t.ix ((x + 1));")->is_success());
    CHECK_FALSE(exec("CREATE INDEX func_idx ON t.ix ((lower(y)));")->is_success());
    REQUIRE(exec("CREATE INDEX plain_idx ON t.ix (x);")->is_success());
}
