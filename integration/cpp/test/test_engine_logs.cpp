#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/log/test/test_log.hpp>

#include <spdlog/spdlog.h>

#include <string>

using namespace test_helpers;

TEST_CASE("integration::cpp::engine_logs::an_engine_leaves_the_process_default_logger_alone") {
    auto config = make_test_config(integration_fixture_path("test_engine_logs/default_logger"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    REQUIRE(spdlog::get("__default__") == nullptr);
}

TEST_CASE("integration::cpp::engine_logs::two_engines_write_to_their_own_logs") {
    auto config_a = make_test_config(integration_fixture_path("test_engine_logs/a"));
    auto config_b = make_test_config(integration_fixture_path("test_engine_logs/b"));
    config_a.log.level = log_t::level::trace;
    config_b.log.level = log_t::level::trace;
    {
        test_spaces a(config_a);
        test_spaces b(config_b);
        REQUIRE(exec(a.dispatcher(), "CREATE DATABASE only_in_a;")->is_success());
        REQUIRE(exec(b.dispatcher(), "CREATE DATABASE only_in_b;")->is_success());
    }
    const auto log_a = log_text(config_a.log.path);
    const auto log_b = log_text(config_b.log.path);
    CHECK(log_a.find("only_in_a") != std::string::npos);
    CHECK(log_a.find("only_in_b") == std::string::npos);
    CHECK(log_b.find("only_in_b") != std::string::npos);
    CHECK(log_b.find("only_in_a") == std::string::npos);
}
