#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node.hpp>
#include <string>

// Gating inside log.hpp wouldn't help: trace()'s arguments are evaluated at the call site
// regardless of log level, so only gating the call site avoids stringifying the plan on every
// statement. The counter lives in node_t::to_string, catching any other hot-path renderer.
TEST_CASE("integration::cpp::test_log_laziness::plan_is_not_stringified_when_logging_is_off") {
    auto config = test_create_config(integration_fixture_path("test_log_laziness/off"));
    test_clear_directory(config);
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
