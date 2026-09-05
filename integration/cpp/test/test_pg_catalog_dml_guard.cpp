#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <string>

// SQL DML must never reach a pg_catalog table. pg_class IS the list of relations, so a
// `DELETE FROM pg_class` that lands takes every user table with it: the storage file stays on
// disk while the row that names it is gone, and the next `SELECT * FROM db.tbl` answers
// "collection does not exist".
//
// MEASURED, not argued. With the catalog made resolvable from SQL (a resolver that stamps
// system-table metadata, or seeded pg_class self-rows -- either route), on this fixture:
//     DELETE FROM pg_catalog.pg_class WHERE relname = 'alpha';  -> success, 1 row
//     SELECT * FROM probedb.alpha;                              -> collection does not exist
// One statement, one lost table. Today the DML is refused only as a SIDE EFFECT of pg_class
// not being addressable at all -- the refusal reads "could not find table in update/delete
// validation", which is not a decision anybody made. These cases pin the OUTCOME rather than
// that message, so they stay honest through whichever way the catalog is opened for reading:
// the day a read fix lands without a write guard, they go red.
//
// The refusal must stay loud AND non-fatal -- an abort on this path would make the database
// unopenable, which is strictly worse than the DML it refuses.

using namespace components;

namespace {

    constexpr std::string_view kUserDb = "guarddb";
    constexpr std::string_view kUserTable = "guarddb.alpha";

    // The fixture every case starts from: one user database, one user table, two rows.
    void seed_user_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE guarddb;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE guarddb.alpha (id BIGINT, name STRING);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO guarddb.alpha (id, name) VALUES (1, 'one'), (2, 'two');")
                    ->is_success());
    }

    // The invariant every case ends on: the table is still THERE and still holds its rows.
    // Reading it back through SQL (not through the filesystem) is the point -- a pg_class row
    // scrubbed out leaves the .otbx untouched and only the NAME lookup broken.
    void require_user_table_intact(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto cursor = test_helpers::exec(dispatcher, "SELECT * FROM guarddb.alpha;");
        REQUIRE(cursor);
        INFO("the user table must survive every refused catalog DML: "
             << (cursor->is_error() ? cursor->get_error().what.c_str() : "<no error>"));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 2);
    }

    // Loud: an error cursor, not a success and not a crash.
    void require_refused(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cursor = test_helpers::exec(dispatcher, sql);
        REQUIRE(cursor);
        INFO("[" << sql << "] must be refused, it reached a pg_catalog table");
        REQUIRE(cursor->is_error());
    }

} // namespace

// Both spellings: unqualified `pg_class` and namespace-qualified `pg_catalog.pg_class`. The
// qualified one is the one that lands first once the catalog becomes readable -- an
// unqualified name loses its schema at validation for every table, user tables included.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::delete_from_pg_class_cannot_erase_user_tables") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/delete_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "DELETE FROM pg_catalog.pg_class WHERE relname = 'alpha';");
    require_user_table_intact(dispatcher);

    require_refused(dispatcher, "DELETE FROM pg_class;");
    require_user_table_intact(dispatcher);
}

// pg_attribute is the column list; scrubbing it loses the SCHEMA rather than the relation, so
// it needs its own case -- a guard that only covered pg_class would pass the one above.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::delete_from_pg_attribute_cannot_erase_columns") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/delete_pg_attribute"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "DELETE FROM pg_catalog.pg_attribute;");
    require_user_table_intact(dispatcher);
}

// UPDATE, not DELETE: renaming a pg_class row leaves the count intact and still detaches the
// table from its name, so a guard written against row COUNTS would miss it.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::update_of_pg_class_cannot_rename_a_user_table") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/update_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "UPDATE pg_catalog.pg_class SET relname = 'renamed_by_dml';");
    require_user_table_intact(dispatcher);

    require_refused(dispatcher, "UPDATE pg_class SET relname = 'renamed_by_dml';");
    require_user_table_intact(dispatcher);
}

// INSERT mints a relation the engine never created: a pg_class row with no storage behind it,
// and an oid the allocator will hand out again after the next restart reseeds from max+1.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::insert_into_pg_class_cannot_mint_a_relation") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/insert_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher,
                    "INSERT INTO pg_catalog.pg_class (oid, relname, relnamespace, relkind, relstoragemode) "
                    "VALUES (999999, 'phantom', 1, 'r', 'd');");
    require_user_table_intact(dispatcher);
}

// DDL, not DML: once pg_class became resolvable through the ordinary resolver, DROP TABLE
// and ALTER TABLE reach it through the DDL leg, which never crosses the DML validation
// arms. Dropping pg_class takes the whole relation list with it — strictly worse than any
// single DML above.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::ddl_cannot_drop_or_alter_the_catalog") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/ddl_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "DROP TABLE pg_catalog.pg_class;");
    require_user_table_intact(dispatcher);

    require_refused(dispatcher, "ALTER TABLE pg_catalog.pg_class ADD COLUMN smuggled BIGINT;");
    require_user_table_intact(dispatcher);

    // The namespace itself: PostgreSQL refuses to drop pg_catalog because the
    // database system requires it. So does otterbrix.
    require_refused(dispatcher, "DROP DATABASE pg_catalog;");
    require_user_table_intact(dispatcher);
}

// CREATE INDEX is the remaining DDL door: it neither drops nor alters, so both guards above
// let it through, yet it writes pg_class + pg_index rows and then BACKFILLS by scanning the
// target -- an index over pg_class turns every later catalog write into a divergence between
// the heap and an index nobody can drop safely. PostgreSQL refuses this outright unless
// allow_system_table_mods is set; otterbrix has no such escape hatch.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::create_index_cannot_target_the_catalog") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/index_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "CREATE INDEX smuggled_idx ON pg_catalog.pg_class (relname);");
    require_user_table_intact(dispatcher);

    // pg_attribute too: a guard hard-wired to pg_class's oid would pass the case above.
    require_refused(dispatcher, "CREATE INDEX smuggled_attr_idx ON pg_catalog.pg_attribute (attname);");
    require_user_table_intact(dispatcher);

    // The refusal must not over-reach: the same statement against the user table still works,
    // and the refused attempts left no half-created index under the smuggled name.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE INDEX smuggled_idx ON guarddb.alpha (name);")->is_success());
    require_user_table_intact(dispatcher);
}
