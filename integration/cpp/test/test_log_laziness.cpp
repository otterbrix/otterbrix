#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node.hpp>
#include <string>

// Work done ONLY to feed a log line must not happen when that line is not logged.
//
// A counter test rather than a "wrap it in an if" because gating inside log.hpp would NOT have
// helped: a trace argument is evaluated at the CALL SITE before the logging function is entered,
// so `trace(log_, "... {}", plan.sub_queries.back()->to_string())` rendered the whole logical-plan
// tree into a string on every statement, at every log level including off, and threw it away.
// Only gating the call SITE avoids it.
//
// The counter sits in node_t::to_string, so it also catches anyone else who starts rendering
// plans on the hot path for a message nobody reads.
TEST_CASE("integration::cpp::test_log_laziness::plan_is_not_stringified_when_logging_is_off") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_log_laziness/off");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE lz;")->is_success());
    REQUIRE(exec("CREATE TABLE lz.t (id bigint, v bigint);")->is_success());

    components::logical_plan::reset_node_to_string_calls();
    for (int i = 0; i < 20; ++i) {
        REQUIRE(exec("INSERT INTO lz.t (id, v) VALUES (" + std::to_string(i) + ", 1);")->is_success());
    }
    const auto renders = components::logical_plan::node_to_string_calls();

    INFO("plan-tree stringifications over 20 statements with logging off: " << renders);
    CHECK(renders == 0);
}
