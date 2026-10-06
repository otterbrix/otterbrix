#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/physical_plan/operators/operator_checkpoint.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

// Both scenarios run the engine in a child process: the unfixed shutdown kills the process, and
// the parent has to be the one left to report it.

using namespace test_helpers;
using services::wal::auto_checkpoint_park_t;
using services::wal::auto_checkpoint_point_t;

namespace {

    constexpr std::uintmax_t kThresholdBytes = 64 * 1024;
    constexpr int kMaxStatements = 400;
    constexpr auto kParkLimit = std::chrono::seconds(5);

    using clock_type = std::chrono::steady_clock;

    std::atomic<bool> g_round_parked{false};
    std::atomic<bool> g_final_checkpoint_compacted{false};
    std::atomic<std::int64_t> g_round_parked_at{0};
    std::atomic<std::int64_t> g_flush_parked_at{0};
    std::filesystem::path g_marker;

    std::int64_t now_ticks() { return clock_type::now().time_since_epoch().count(); }

    bool expired(const std::atomic<std::int64_t>& since) {
        return clock_type::now() - clock_type::time_point(clock_type::duration(since.load())) > kParkLimit;
    }

    void note_first_park(std::atomic<std::int64_t>& since) {
        std::int64_t unset = 0;
        since.compare_exchange_strong(unset, now_ticks());
    }

    bool rebuild_marker_names_an_index() {
        std::ifstream in(g_marker);
        unsigned long long table_oid = 0;
        unsigned long long index_oid = 0;
        return static_cast<bool>(in >> table_oid >> index_oid);
    }

    struct final_checkpoint_probe_t final : components::operators::checkpoint_repopulate_gate_t {
        bool hold() override {
            g_final_checkpoint_compacted = true;
            return false;
        }
    };

    // Bug B: a round parked on the index manager across the whole shutdown.
    auto_checkpoint_park_t park_on_index_at_start(auto_checkpoint_point_t point) {
        if (point != auto_checkpoint_point_t::round_start) {
            return auto_checkpoint_park_t::go;
        }
        note_first_park(g_round_parked_at);
        g_round_parked = true;
        return expired(g_round_parked_at) ? auto_checkpoint_park_t::go : auto_checkpoint_park_t::on_index;
    }

    // Bug A: the round flushes the indexes only after the final CHECKPOINT has rebuilt them and
    // cleared the rebuild marker, then stays parked past the end of the shutdown.
    auto_checkpoint_park_t flush_after_the_final_checkpoint(auto_checkpoint_point_t point) {
        if (point == auto_checkpoint_point_t::round_start) {
            note_first_park(g_round_parked_at);
            g_round_parked = true;
            const bool final_rebuilt = g_final_checkpoint_compacted && !rebuild_marker_names_an_index();
            return final_rebuilt || expired(g_round_parked_at) ? auto_checkpoint_park_t::go
                                                               : auto_checkpoint_park_t::on_dispatcher;
        }
        note_first_park(g_flush_parked_at);
        return expired(g_flush_parked_at) ? auto_checkpoint_park_t::go : auto_checkpoint_park_t::on_dispatcher;
    }

    configuration::config shutdown_config(const std::string& leaf) {
        auto config = make_test_config(integration_fixture_path("test_engine_shutdown/" + leaf));
        config.log.level = log_t::level::off;
        config.wal.auto_checkpoint_threshold_bytes = kThresholdBytes;
        return config;
    }

    // Exit codes: 0 = the engine shut down; 2 = it did not start; 3 = no round ever started.
    int run_until_a_round_parks_then_shut_down(const configuration::config& config,
                                               services::wal::auto_checkpoint_gate_fn gate) {
        ::signal(SIGABRT, SIG_DFL);
        g_marker = config.disk.path / "index_rebuild_pending";
        final_checkpoint_probe_t probe;
        components::operators::dev_set_checkpoint_repopulate_gate(&probe);
        services::wal::dev_set_auto_checkpoint_gate(gate);
        {
            auto opened = otterbrix::base_otterbrix_t::open(config);
            if (opened.has_error()) {
                return 2;
            }
            otterbrix::otterbrix_t space(std::move(opened.value()));
            auto* d = space.dispatcher();
            if (!exec(d, "CREATE DATABASE sdb;")->is_success() ||
                !exec(d, "CREATE TABLE sdb.t (id bigint, k bigint, payload text);")->is_success() ||
                !exec(d, "CREATE INDEX t_k ON sdb.t USING hash (k);")->is_success()) {
                return 2;
            }
            const std::string payload(512, 'x');
            std::int64_t next_id = 1;
            for (int statement = 0; statement < kMaxStatements && !g_round_parked; ++statement) {
                const auto inserted = seed_rows(d, "sdb.t", "id, k, payload", 64, [&](unsigned row) {
                    const auto id = next_id + static_cast<std::int64_t>(row);
                    return "(" + std::to_string(id) + ", " + std::to_string(10 * id) + ", '" + payload + "')";
                });
                next_id += 64;
                if (!inserted->is_success()) {
                    return 2;
                }
            }
            if (!g_round_parked) {
                return 3;
            }
        }
        services::wal::dev_set_auto_checkpoint_gate(nullptr);
        components::operators::dev_set_checkpoint_repopulate_gate(nullptr);
        return 0;
    }

    int exit_status_of_child(const configuration::config& config, services::wal::auto_checkpoint_gate_fn gate) {
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            ::_exit(run_until_a_round_parks_then_shut_down(config, gate));
        }
        int status = 0;
        REQUIRE(::waitpid(child, &status, 0) == child);
        INFO("child " << (WIFSIGNALED(status) ? "killed by signal " : "exited with ")
                      << (WIFSIGNALED(status) ? WTERMSIG(status) : WEXITSTATUS(status)));
        REQUIRE(WIFEXITED(status));
        return WEXITSTATUS(status);
    }

} // namespace

TEST_CASE("integration::cpp::engine_shutdown::a_round_in_flight_does_not_outlive_its_neighbours") {
    const auto config = shutdown_config("round_on_index");
    REQUIRE(exit_status_of_child(config, &park_on_index_at_start) == 0);
}

TEST_CASE("integration::cpp::engine_shutdown::an_index_is_wired_after_a_shutdown_during_a_round") {
    const auto config = shutdown_config("round_after_final");
    REQUIRE(exit_status_of_child(config, &flush_after_the_final_checkpoint) == 0);

    test_spaces space(config);
    auto plan = exec(space.dispatcher(), "EXPLAIN SELECT id FROM sdb.t WHERE k = 10;");
    REQUIRE(plan->is_success());
    std::string text;
    for (std::size_t r = 0; r < plan->size(); ++r) {
        text += std::string(plan->value(0, r).value<std::string_view>());
        text += '\n';
    }
    INFO("plan after the reopen:\n" << text);
    CHECK(text.find("Index Scan") != std::string::npos);
}
