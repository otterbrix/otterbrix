#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>

// The dispatcher-loop watchdog pokes executors whenever an in-flight await
// stays busy past ~2ms — routine for any multi-row DML — and used to log every
// firing at WARN, flooding logs on perfectly healthy runs (a 14-statement
// insert workload produced 24 warnings). Routine firings must be trace-level;
// only a slot stale across hundreds of consecutive poke rounds (a genuine
// stall) escalates to warn, with a distinct message.
TEST_CASE("integration::cpp::dispatcher::watchdog_quiet_on_healthy_runs") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_dispatcher_watchdog/healthy");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    config.log.level = log_t::level::warn;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE db;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE db.t (a bigint, b bigint);")->is_success());
        auto cur = test_helpers::seed_rows(dispatcher, "db.t", "a, b", 5000, [](unsigned i) {
            return "(" + std::to_string(i) + ", " + std::to_string(i * 2) + ")";
        });
        REQUIRE(cur->is_success());
        auto count = test_helpers::exec(dispatcher, "SELECT count(*) FROM db.t;");
        REQUIRE(count->is_success());
        REQUIRE(count->value(0, 0).value<int64_t>() == 5000);
    }

    // The engine is destructed, so the file sink is flushed. A healthy run must
    // not surface the routine watchdog firing at warn level.
    bool found_routine_warn = false;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(std::string(config.log.path.string()))) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (line.find("stale await detected") != std::string::npos) {
                found_routine_warn = true;
                break;
            }
        }
        if (found_routine_warn) {
            break;
        }
    }
    REQUIRE_FALSE(found_routine_warn);
}
