// CREATE TABLE answers the pg_class oid of the table it created; every other reply carries INVALID_OID.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <cstdint>
#include <string>

using namespace test_helpers;
using components::catalog::INVALID_OID;
using components::catalog::oid_t;

namespace {

    configuration::config config_for(const std::string& leaf) {
        auto config = make_test_config(integration_fixture_path("test_created_oid/" + leaf));
        config.log.level = log_t::level::off;
        return config;
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    oid_t pg_class_oid(otterbrix::wrapper_dispatcher_t* d, const std::string& relname) {
        auto cur = run_ok(d, "SELECT oid FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(cur->size() == 1);
        auto oid = cur->get<std::uint32_t>(0, 0);
        REQUIRE_FALSE(oid.has_error());
        return oid.value();
    }

} // namespace

TEST_CASE("integration::cpp::created_oid::create_table_answers_its_oid") {
    auto config = config_for("sql");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA co;");

    auto created = run_ok(d, "CREATE TABLE co.t (id bigint);");
    REQUIRE(created->created_oid() != INVALID_OID);
    CHECK(created->created_oid() == pg_class_oid(d, "t"));

    auto second = run_ok(d, "CREATE TABLE co.u (id bigint);");
    CHECK(second->created_oid() == pg_class_oid(d, "u"));
    CHECK(second->created_oid() != created->created_oid());
}

TEST_CASE("integration::cpp::created_oid::create_table_with_inline_constraints_answers_the_table") {
    auto config = config_for("inline_constraints");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA co;");

    auto created = run_ok(d, "CREATE TABLE co.t (id bigint PRIMARY KEY, code bigint UNIQUE CHECK (code > 0));");
    CHECK(created->created_oid() == pg_class_oid(d, "t"));
}

TEST_CASE("integration::cpp::created_oid::create_table_by_plan_answers_the_planner_oid") {
    auto config = config_for("plan");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA co;");

    auto* resource = d->resource();
    std::pmr::vector<components::types::complex_logical_type> types{resource};
    types.emplace_back(components::types::logical_type::BIGINT, "id");
    std::vector<components::table::column_definition_t> columns;
    columns.emplace_back(types.front().alias(), types.front());
    auto create =
        components::logical_plan::make_node_create_collection(resource, core::relname_t{"t"}, std::move(columns), {});
    components::logical_plan::node_ptr node =
        components::sql::transform::name_catalog_target(core::schema_t{"co"}, core::relname_t{"t"}, create);
    auto cur = d->execute_plan(
        otterbrix::session_id_t(),
        components::logical_plan::execution_plan_t{resource,
                                                   node,
                                                   components::logical_plan::make_parameter_node(resource)});
    REQUIRE(cur->is_success());
    REQUIRE(cur->created_oid() != INVALID_OID);
    CHECK(cur->created_oid() == create->table_oid());
    CHECK(cur->created_oid() == pg_class_oid(d, "t"));
}

TEST_CASE("integration::cpp::created_oid::a_rolled_back_create_leaves_no_table_with_its_oid") {
    auto config = config_for("rollback");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA co;");

    const auto txn = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(txn, "BEGIN;")->is_success());
    auto created = d->execute_sql(txn, "CREATE TABLE co.t (id bigint);");
    REQUIRE(created->is_success());
    const oid_t oid = created->created_oid();
    REQUIRE(oid != INVALID_OID);
    const std::string by_oid = "SELECT relname FROM pg_catalog.pg_class WHERE oid = " + std::to_string(oid) + ";";
    auto inside = d->execute_sql(txn, by_oid);
    REQUIRE(inside->is_success());
    REQUIRE(inside->size() == 1);
    REQUIRE(d->execute_sql(txn, "ROLLBACK;")->is_success());

    CHECK(run_ok(d, by_oid)->size() == 0);
}

TEST_CASE("integration::cpp::created_oid::if_not_exists_on_an_existing_table_answers_no_oid") {
    auto config = config_for("if_not_exists");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA co;");
    auto first = run_ok(d, "CREATE TABLE IF NOT EXISTS co.t (id bigint);");
    CHECK(first->created_oid() == pg_class_oid(d, "t"));

    auto again = run_ok(d, "CREATE TABLE IF NOT EXISTS co.t (id bigint);");
    CHECK(again->created_oid() == INVALID_OID);
}

TEST_CASE("integration::cpp::created_oid::a_refused_create_answers_no_oid") {
    auto config = config_for("refused");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA co;");
    run_ok(d, "CREATE TABLE co.t (id bigint);");

    auto again = exec(d, "CREATE TABLE co.t (id bigint);");
    REQUIRE(again->is_error());
    CHECK(again->created_oid() == INVALID_OID);
}

TEST_CASE("integration::cpp::created_oid::other_statements_answer_no_oid") {
    auto config = config_for("others");
    test_spaces space(config);
    auto* d = space.dispatcher();
    CHECK(run_ok(d, "CREATE SCHEMA co;")->created_oid() == INVALID_OID);
    run_ok(d, "CREATE TABLE co.t (id bigint);");
    CHECK(run_ok(d, "CREATE INDEX t_id ON co.t (id);")->created_oid() == INVALID_OID);
    CHECK(run_ok(d, "CREATE VIEW co.v AS SELECT id FROM co.t;")->created_oid() == INVALID_OID);
    CHECK(run_ok(d, "INSERT INTO co.t (id) VALUES (1);")->created_oid() == INVALID_OID);
    CHECK(run_ok(d, "SELECT id FROM co.t;")->created_oid() == INVALID_OID);
    CHECK(run_ok(d, "SELECT 1;")->created_oid() == INVALID_OID);
}
