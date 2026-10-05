#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <core/pmr.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/engine/engine.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <thread>
#include <sys/resource.h>
#include <unistd.h>
#include <components/log/test_log.hpp>

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
            while (!future.is_ready()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
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

    double process_cpu_seconds() {
        rusage usage{};
        REQUIRE(::getrusage(RUSAGE_SELF, &usage) == 0);
        const auto seconds = [](const timeval& t) {
            return static_cast<double>(t.tv_sec) + static_cast<double>(t.tv_usec) / 1e6;
        };
        return seconds(usage.ru_utime) + seconds(usage.ru_stime);
    }

} // namespace

// Process CPU time, not per-thread: it is the one measure both macOS and Linux give without
// naming the loop threads, and a spinning loop burns a whole core per wall second.
TEST_CASE("services::engine::pump::an_idle_engine_burns_no_cpu") {
    const auto root = test_root("idle_cpu");
    host_t host(root);
    REQUIRE_FALSE(host.open().contains_error());
    REQUIRE(host.execute("CREATE DATABASE idle;")->is_success());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const auto cpu_before = process_cpu_seconds();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const auto cpu_spent = process_cpu_seconds() - cpu_before;
    INFO("CPU seconds spent by an idle engine over one wall second: " << cpu_spent);
    CHECK(cpu_spent < 0.2);
}

// A request wakes each idle loop it reaches, so it never waits out the idle interval.
// The bound is half the idle interval, not an absolute latency: under gcc ASAN with
// fast_unwind_on_malloc=0 the SELECT alone takes ~160 ms, while a missed wake-up costs the
// remaining ~1.5 s of an idle wait.
TEST_CASE("services::engine::pump::a_query_to_an_idle_engine_does_not_wait_for_the_idle_interval") {
    constexpr auto idle = std::chrono::milliseconds(2000);
    const auto root = test_root("idle_latency");
    host_t host(root);
    host.config.execution.pump.idle = idle;
    REQUIRE_FALSE(host.open().contains_error());
    REQUIRE(host.execute("CREATE DATABASE lat;")->is_success());
    REQUIRE(host.execute("CREATE TABLE lat.t (id BIGINT);")->is_success());
    REQUIRE(host.execute("INSERT INTO lat.t (id) VALUES (1), (2);")->is_success());
    std::this_thread::sleep_for(idle + idle / 4);

    const auto started = std::chrono::steady_clock::now();
    auto cur = host.execute("SELECT id FROM lat.t;");
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    INFO("a SELECT on an engine idle for 2.5 s took " << elapsed.count() << " ms, idle interval "
                                                       << idle.count() << " ms");
    CHECK(elapsed < idle / 2);
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
    if (::geteuid() == 0) {
        SKIP("root writes into a read-only directory anyway");
    }
    const auto root = test_root("log_refusal");
    std::filesystem::permissions(root, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
    auto refused = make_log("refused", root / "log");
    std::filesystem::permissions(root, std::filesystem::perms::owner_all);
    REQUIRE(refused.has_error());
    INFO("refusal: " << refused.error().what.c_str());
    CHECK(std::string(refused.error().what.c_str()).find((root / "log").string()) != std::string::npos);
}
