#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <string>

// A statement's floor is hops * the pump's in-flight tick. This pins the first factor.
//
// While work is in flight a completed future notifies nobody, so a pump discovers readiness by its
// wait timing out and resumes the continuation — one resume per hop. The count is the same on a
// loaded machine as on an idle one, which is what makes it testable; the latency it multiplies out
// to is not. A wall-clock assertion was tried twice and failed on CI both times: a runner resolves a
// 5 us wait as 63-77 us and has 3-4 cores, so the floor there is set by the environment.
//
// Adding a round trip to the statement path is the regression this catches.

TEST_CASE("integration::cpp::test_pump_hops::a_single_statement_crosses_a_bounded_number_of_hops") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_pump_hops/single");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE h;")->is_success());
    REQUIRE(exec("CREATE TABLE h.t (id bigint, payload bigint);")->is_success());
    for (int i = 0; i < 20; ++i) {
        REQUIRE(exec("INSERT INTO h.t (id, payload) VALUES (" + std::to_string(i) + ", 0);")->is_success());
    }

    constexpr int kStatements = 50;
    services::dispatcher::reset_pump_hops();
    for (int i = 0; i < kStatements; ++i) {
        REQUIRE(exec("INSERT INTO h.t (id, payload) VALUES (" + std::to_string(1000 + i) + ", 0);")->is_success());
    }
    const auto hops = services::dispatcher::pump_hops();
    const double per_statement = static_cast<double>(hops) / kStatements;

    WARN("hops per single-row INSERT: " << per_statement);

    // Positive control: a counter reading zero would satisfy any upper bound.
    REQUIRE(hops > 0);
    // Measured at exactly 8 here, stable across repeats and under 40 threads of load. The bound
    // allows drift but not a new round trip; the WARN above reports the real number, so a CI log
    // says whether it is 8 there too.
    CHECK(per_statement <= 12.0);
}
