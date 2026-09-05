// ============================================================================
// A PLAN THAT DECLARES UPSERT MUST NOT BE EXECUTED AS A PLAIN UPDATE.
//
// node_update_t carries an `upsert` flag and PRINTS it ($upsert: 1), so a plan
// built with it asserts insert-or-update semantics. operator_update accepted the
// flag into upsert_ and then never read it: an upsert plan whose match found
// nothing reported SUCCESS with 0 affected rows — no insert, no error, and
// nothing anywhere to say the promised semantics were not delivered. No SQL
// reaches this flag (the grammar has neither `upsert` nor ON CONFLICT); the
// logical-plan API does, which is how this test drives it.
//
// Rule 6: an engine that does not implement what the plan declares refuses the
// plan, loudly, instead of executing a quieter statement in its place.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <string>
#include <unistd.h>

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

TEST_CASE("integration::cpp::update_upsert::an_upsert_plan_is_refused_not_quietly_updated", "[upsert]") {
    auto config =
        test_create_config("/tmp/test_update_upsert_refusal_" + std::to_string(::getpid()) + "/refused");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE upsdb;")->is_success());
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE upsdb.t (id bigint, count bigint);")->is_success());
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO upsdb.t (id, count) VALUES (1, 10);")->is_success());
    }

    // UPDATE ... SET count = $2 WHERE id = $1, upsert=true, and $1 matches NOTHING —
    // the exact shape whose declared outcome is an INSERT.
    auto session = otterbrix::session_id_t();
    auto match = logical_plan::make_node_match(
        dispatcher->resource(),
        core::dbname_t{"upsdb"},
        core::relname_t{"t"},
        expressions::make_compare_expression(dispatcher->resource(),
                                             compare_type::eq,
                                             key{dispatcher->resource(), "id", side_t::left},
                                             id_par{1}));
    auto update_expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                           expressions::scalar_type::constant,
                                                           key{dispatcher->resource(), "count"});
    update_expr->append_param(id_par{2});
    auto upd = logical_plan::make_node_update(
        dispatcher->resource(),
        match,
        logical_plan::make_node_limit(dispatcher->resource(), {}, {}, logical_plan::limit_t::unlimit()),
        {update_expr},
        /*upsert=*/true);
    upd->set_dbname("upsdb");
    upd->set_relname("t");

    auto params = logical_plan::make_parameter_node(dispatcher->resource());
    params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), int64_t(999)));
    params->add_parameter(id_par{2}, types::logical_value_t(dispatcher->resource(), int64_t(1000)));

    auto cur = dispatcher->execute_plan(
        session,
        logical_plan::execution_plan_t{dispatcher->resource(), upd, params});

    INFO("an upsert plan must be refused: the engine implements no upsert, and executing "
         "the update half in silence delivers different semantics than the plan declares");
    REQUIRE(cur->is_error());
    const std::string what{cur->get_error().what};
    INFO("error: " << what);
    CHECK(what.find("upsert") != std::string::npos);

    // And nothing may have moved.
    {
        auto s2 = otterbrix::session_id_t();
        auto rows = dispatcher->execute_sql(s2, "SELECT id, count FROM upsdb.t;");
        REQUIRE(rows->is_success());
        REQUIRE(rows->size() == 1);
        CHECK(rows->value(0, 0).value<int64_t>() == 1);
        CHECK(rows->value(1, 0).value<int64_t>() == 10);
    }
}
