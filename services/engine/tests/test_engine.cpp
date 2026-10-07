#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <core/pmr.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/dev_pump.hpp>
#include <services/engine/engine.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <components/log/test/test_log.hpp>
#include <core/tests/skip_under_root.hpp>
#include <core/tests/wait_ready.hpp>

using namespace services::engine;

namespace {

    std::filesystem::path test_root(const std::string& leaf) {
        auto root = std::filesystem::temp_directory_path() /
                    ("otterbrix_engine_test_" + std::to_string(static_cast<long>(::getpid()))) / leaf;
        std::filesystem::remove_all(root);
        std::filesystem::create_directories(root);
        return root;
    }

    // The host side of the contract: it owns the resource, the logger and the pools, and keeps
    // the pools alive until after the engine is gone.
    struct host_t {
        explicit host_t(const std::filesystem::path& root)
            : config(configuration::config::create_config(root)) {
            config.log.level = log_t::level::off;
            log = make_test_log("engine_test", config.log.path.string());
            log.set_level(config.log.level);
        }

        schedulers_t schedulers() { return {general.get(), exec.get(), disk.get()}; }

        core::error_t open() {
            auto opened = open_engine(&resource, schedulers(), config, log, {});
            if (opened.has_error()) {
                return opened.error();
            }
            engine.emplace(std::move(opened.value()));
            return core::error_t::no_error();
        }

        components::cursor::cursor_t_ptr execute(const std::string& sql) {
            std::pmr::monotonic_buffer_resource arena(&resource);
            auto* tree = raw_parser(&arena, sql.c_str());
            components::sql::transform::transformer transformer(&resource, sql.c_str());
            auto plan = transformer.transform(components::sql::transform::pg_cell_to_node_cast(linitial(tree)))
                            .finalize();
            REQUIRE_FALSE(plan.has_error());
            auto [_, future] = actor_zeta::otterbrix::send(engine->dispatcher_address(),
                                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                                           components::session::session_id_t(),
                                                           std::move(plan.value()));
            REQUIRE(test_helpers::wait_ready(future));
            return std::move(future).take_ready();
        }

        configuration::config config;
        core::pmr::otterbrix_resource resource;
        log_t log;
        actor_zeta::scheduler_ptr general{new actor_zeta::shared_work(2, 1000)};
        actor_zeta::scheduler_ptr exec{new actor_zeta::shared_work(2, 1000)};
        actor_zeta::scheduler_ptr disk{new actor_zeta::shared_work(2, 1000)};
        std::optional<engine_t> engine;
    };

} // namespace

TEST_CASE("services::engine::directory_lock::a_second_owner_is_refused_across_threads") {
    const auto root = test_root("lock");
    std::optional<host_t> first;
    first.emplace(root);
    REQUIRE_FALSE(first->open().contains_error());

    bool refused = false;
    std::thread second([&] {
        host_t host(root);
        refused = host.open().contains_error() && !host.engine.has_value();
    });
    second.join();
    REQUIRE(refused);

    first.reset();
    host_t again(root);
    REQUIRE_FALSE(again.open().contains_error());
}

TEST_CASE("services::engine::factory::rows_survive_a_reopen_without_a_wrapper") {
    const auto root = test_root("reopen");
    {
        host_t host(root);
        REQUIRE_FALSE(host.open().contains_error());
        REQUIRE(host.execute("CREATE DATABASE e;")->is_success());
        REQUIRE(host.execute("CREATE TABLE e.t (id BIGINT);")->is_success());
        REQUIRE(host.execute("INSERT INTO e.t (id) VALUES (1), (2), (3);")->is_success());
    }
    host_t host(root);
    REQUIRE_FALSE(host.open().contains_error());
    auto cur = host.execute("SELECT id FROM e.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
}

TEST_CASE("services::engine::factory::a_failed_bootstrap_hands_out_no_engine") {
    const auto root = test_root("bootstrap_refusal");
    host_t host(root);
    // The pg_catalog directory is a file, so bootstrap cannot lay out a single system table.
    std::filesystem::create_directories(host.config.disk.path);
    {
        std::ofstream blocker(
            (host.config.disk.path /
             std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::main_database)))
                .string());
        blocker << "x";
    }
    auto refusal = host.open();
    REQUIRE(refusal.contains_error());
    REQUIRE_FALSE(host.engine.has_value());
}

namespace {

    // The dispatcher, disk, index and WAL loops.
    constexpr std::uint64_t kPumpLoops = 4;

    // An idle interval no test outlives: a loop that sleeps it out instead of being woken never answers.
    constexpr auto kNeverWakesByItself = std::chrono::hours(1);

    bool every_loop_waits_idle() {
        return test_helpers::wait_until([] { return services::dev_pump_idle_waiters() == kPumpLoops; });
    }

} // namespace

// A spinning loop comes out of its wait over and over; an idle one waits for work or its interval.
TEST_CASE("services::engine::pump::an_idle_engine_burns_no_cpu") {
    const auto root = test_root("idle_cpu");
    host_t host(root);
    host.config.execution.pump.idle = kNeverWakesByItself;
    REQUIRE_FALSE(host.open().contains_error());
    REQUIRE(host.execute("CREATE DATABASE idle;")->is_success());
    REQUIRE(every_loop_waits_idle());

    const auto wakeups_before = services::dev_pump_wakeups();
    for (int i = 0; i < 100000; ++i) {
        std::this_thread::yield();
    }
    INFO("loop wake-ups while every loop was idle: " << services::dev_pump_wakeups() - wakeups_before);
    CHECK(services::dev_pump_wakeups() == wakeups_before);
    CHECK(services::dev_pump_idle_waiters() == kPumpLoops);
}

// A request wakes each idle loop it reaches: with an idle interval of an hour, a missed wake-up never answers.
TEST_CASE("services::engine::pump::a_query_to_an_idle_engine_does_not_wait_for_the_idle_interval") {
    const auto root = test_root("idle_latency");
    host_t host(root);
    host.config.execution.pump.idle = kNeverWakesByItself;
    REQUIRE_FALSE(host.open().contains_error());
    REQUIRE(host.execute("CREATE DATABASE lat;")->is_success());
    REQUIRE(host.execute("CREATE TABLE lat.t (id BIGINT);")->is_success());
    REQUIRE(host.execute("INSERT INTO lat.t (id) VALUES (1), (2);")->is_success());
    REQUIRE(every_loop_waits_idle());

    auto cur = host.execute("SELECT id FROM lat.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
}

TEST_CASE("services::engine::pump::intervals_out_of_order_are_refused_at_startup") {
    const auto root = test_root("pump_refusal");
    SECTION("in_flight is zero") {
        host_t host(root);
        host.config.execution.pump.in_flight = std::chrono::microseconds(0);
        REQUIRE(host.open().contains_error());
    }
    SECTION("idle does not exceed in_flight") {
        host_t host(root);
        host.config.execution.pump.idle = host.config.execution.pump.in_flight;
        REQUIRE(host.open().contains_error());
    }
}

TEST_CASE("services::engine::log::a_log_directory_that_cannot_be_written_answers_an_error") {
    test_helpers::skip_under_root();
    const auto root = test_root("log_refusal");
    std::filesystem::permissions(root, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
    auto refused = make_log("refused", root / "log");
    std::filesystem::permissions(root, std::filesystem::perms::owner_all);
    REQUIRE(refused.has_error());
    INFO("refusal: " << refused.error().what.c_str());
    CHECK(std::string(refused.error().what.c_str()).find((root / "log").string()) != std::string::npos);
}
