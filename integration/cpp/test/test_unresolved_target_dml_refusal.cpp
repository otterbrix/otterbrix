// DML aimed at a table that does not exist must come back as an ERROR with the data
// untouched. The SELECT half of this family is pinned in
// test_select_missing_table_refusal.cpp; this file pins the WRITE half and the
// compound readers, because their failure mode is worse than extra rows: an
// unresolved DELETE lowered anyway becomes a sink with no scan behind it, which the
// streaming executor runs as a sourceless sink -- it deletes NOTHING and reports
// SUCCESS. Every section shows the row counts before and after.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

using namespace components;

namespace {

    int64_t count_rows(otterbrix::wrapper_dispatcher_t* d, const std::string& table) {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "SELECT count(*) FROM " + table + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        return cur->value(0, 0).value<int64_t>();
    }

} // namespace

TEST_CASE("integration::cpp::unresolved_target_dml_refusal") {
    auto config = test_create_config(integration_fixture_path("test_unresolved_target_dml_refusal/base"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    INFO("initialization");
    {
        REQUIRE(exec("CREATE DATABASE edb;")->is_success());
        REQUIRE(exec("CREATE TABLE edb.t (id bigint);")->is_success());
        REQUIRE(exec("INSERT INTO edb.t (id) VALUES (1), (2), (3);")->is_success());
        REQUIRE(count_rows(dispatcher, "edb.t") == 3);
    }

    INFO("DELETE from a missing table is an error, not a successful no-op");
    {
        auto cur = exec("DELETE FROM edb.ghost;");
        CAPTURE(cur->is_success(), cur->size());
        REQUIRE(cur->is_error());
    }
    {
        auto cur = exec("DELETE FROM ghost_unqualified;");
        CAPTURE(cur->is_success(), cur->size());
        REQUIRE(cur->is_error());
    }

    INFO("UPDATE of a missing table is an error, not a successful no-op");
    {
        auto cur = exec("UPDATE edb.ghost SET id = 5;");
        CAPTURE(cur->is_success(), cur->size());
        REQUIRE(cur->is_error());
    }

    INFO("a missing table inside a compound reader is an error, not partial data");
    {
        auto cur = exec("SELECT id FROM edb.t UNION ALL SELECT id FROM edb.ghost;");
        CAPTURE(cur->is_success(), cur->is_success() ? cur->size() : 0);
        REQUIRE(cur->is_error());
    }
    {
        auto cur = exec("SELECT * FROM edb.t WHERE id IN (SELECT id FROM edb.ghost);");
        CAPTURE(cur->is_success(), cur->is_success() ? cur->size() : 0);
        REQUIRE(cur->is_error());
    }
    {
        auto cur = exec("WITH c AS (SELECT id FROM edb.ghost) SELECT * FROM c;");
        CAPTURE(cur->is_success(), cur->is_success() ? cur->size() : 0);
        REQUIRE(cur->is_error());
    }
    {
        auto cur = exec("INSERT INTO edb.t SELECT id FROM edb.ghost;");
        CAPTURE(cur->is_success(), cur->is_success() ? cur->size() : 0);
        REQUIRE(cur->is_error());
    }

    INFO("EXPLAIN builds the plan, so it must refuse the missing table too");
    {
        auto cur = exec("EXPLAIN SELECT * FROM edb.ghost;");
        CAPTURE(cur->is_success());
        REQUIRE(cur->is_error());
    }
    {
        auto cur = exec("EXPLAIN DELETE FROM edb.ghost;");
        CAPTURE(cur->is_success());
        REQUIRE(cur->is_error());
    }

    INFO("nothing was deleted, updated, or inserted along the way; the engine still answers");
    {
        REQUIRE(count_rows(dispatcher, "edb.t") == 3);
        auto cur = exec("SELECT id FROM edb.t WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}
