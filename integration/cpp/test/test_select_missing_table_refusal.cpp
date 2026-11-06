#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

// SELECT from a table that does not exist must come back as an ERROR, not as a
// success. Today the unresolved name keeps INVALID_OID on the plan node, the
// scan operators read INVALID_OID as the no-FROM sentinel and synthesize one
// 1-row BOOLEAN placeholder batch, so the caller gets is_success() plus one
// fabricated cell instead of a refusal. The refusal must be loud but NOT fatal:
// the engine has to stay usable afterwards.

static const core::schema_t database_name{"testdatabase"};
static const core::relname_t collection_name{"existing_collection"};

using namespace components;

TEST_CASE("integration::cpp::select_missing_table_refusal") {
    auto config = test_create_config(integration_fixture_path("test_select_missing_table_refusal/base"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE SCHEMA " + database_name.t + ";");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "CREATE TABLE " + database_name.t + "." + collection_name.t +
                                               " (id INT, name TEXT);");
        REQUIRE(cur->is_success());
    }

    INFO("qualified missing table must be refused");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM testdatabase.no_such_table_at_all;");
        CAPTURE(cur->is_success(), cur->size(), cur->column_count());
        REQUIRE(cur->is_error());
    }

    INFO("unqualified missing table must be refused");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM no_such_table_at_all;");
        CAPTURE(cur->is_success(), cur->size(), cur->column_count());
        REQUIRE(cur->is_error());
    }

    INFO("refusal is not fatal: the engine still answers a valid query");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + database_name.t + "." + collection_name.t + ";");
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : ""));
        REQUIRE(cur->is_success());
    }
}

// The same root defect, other half: an unqualified name of a table that DOES exist
// loses its schema. A bare name looks in pg_catalog and then in the session's place only, so
// schemadb.alpha is reached by a bare name only from the place schemadb; there resolution and
// schema come from the same decision: the resolved catalog metadata.
TEST_CASE("integration::cpp::select_unqualified_existing_table") {
    auto config = test_create_config(integration_fixture_path("test_select_missing_table_refusal/unqualified"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE SCHEMA schemadb;");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE TABLE schemadb.alpha (id INT, name TEXT);");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "INSERT INTO schemadb.alpha (id, name) VALUES (1, 'one'), (2, 'two');");
        REQUIRE(cur->is_success());
    }

    INFO("from main.public a bare name does not reach another schema");
    for (const std::string sql :
         {"SELECT id FROM alpha;", "SELECT * FROM alpha;", "SELECT name FROM alpha WHERE id = 2;"}) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO(sql);
        REQUIRE(cur->is_error());
        CHECK(cur->get_error().type == core::error_code_t::table_not_exists);
        CHECK(std::string(cur->get_error().what.c_str()) == "relation \"alpha\" does not exist");
    }

    auto in_place = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(in_place, "SET search_path = 'schemadb';")->is_success());

    INFO("unqualified column projection resolves against the table's schema");
    {
        auto cur = dispatcher->execute_sql(in_place, "SELECT id FROM alpha;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "<none>"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 1);
    }

    INFO("unqualified star keeps the real column set, not a placeholder");
    {
        auto cur = dispatcher->execute_sql(in_place, "SELECT * FROM alpha;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "<none>"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 2);
    }

    INFO("unqualified name works under a WHERE over its own columns");
    {
        auto cur = dispatcher->execute_sql(in_place, "SELECT name FROM alpha WHERE id = 2;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "<none>"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 1);
    }
}
