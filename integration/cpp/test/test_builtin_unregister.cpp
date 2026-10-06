#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <services/disk/agent_disk.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

// A builtin belongs to every registry copy, the disk agents' included, and a scan filter pushed to disk
// runs it between batches; so a builtin is never unregistered.

using namespace test_helpers;

namespace {

    constexpr std::size_t kRows = 3072; // three 1024-row batches: one flows, the unregister lands, more fetch

    struct pause_gate_t final : services::disk::scan_advance_gate_t {
        std::atomic<bool> armed{false};
        std::atomic<bool> reached{false};
        std::atomic<bool> released{false};

        bool hold(components::catalog::oid_t table_oid, uint64_t /*cursor_id*/) override {
            if (!armed.load(std::memory_order_acquire) ||
                static_cast<uint32_t>(table_oid) < static_cast<uint32_t>(components::catalog::FIRST_USER_OID)) {
                return false;
            }
            reached.store(true, std::memory_order_release);
            return !released.load(std::memory_order_acquire);
        }
    };

    struct gate_guard_t {
        pause_gate_t gate;
        gate_guard_t() { services::disk::dev_set_scan_advance_gate(&gate); }
        ~gate_guard_t() { services::disk::dev_set_scan_advance_gate(nullptr); }
        gate_guard_t(const gate_guard_t&) = delete;
        gate_guard_t& operator=(const gate_guard_t&) = delete;
    };

    bool wait_flag(const std::atomic<bool>& flag, std::chrono::seconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!flag.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE bdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE bdb.t (id bigint);")->is_success());
        for (std::size_t done = 0; done < kRows; done += 512) {
            const auto batch = static_cast<unsigned>(std::min<std::size_t>(512, kRows - done));
            REQUIRE(seed_rows(d, "bdb.t", "id", batch, [done](unsigned i) {
                        return "(" + std::to_string(done + i) + ")";
                    })->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cpp::builtin_unregister::an_unregister_of_a_builtin_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_builtin_unregister/refused"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto refused =
        d->unregister_udf(otterbrix::session_id_t(), "abs", {components::types::logical_type::BIGINT});
    INFO("unregister of abs: " << refused.what.c_str());
    REQUIRE(refused.contains_error());
    CHECK(refused.type == core::error_code_t::invalid_parameter);

    for (std::size_t i = 0; i < 2 * config.execution.executor_pool_size; ++i) {
        auto cur = exec(d, "SELECT abs(id) FROM bdb.t WHERE id < 3;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 3);
    }
}

TEST_CASE("integration::cpp::builtin_unregister::a_pushed_filter_outlives_an_unregister_attempt_between_batches") {
    auto config = make_test_config(integration_fixture_path("test_builtin_unregister/between_batches"));
    config.log.level = log_t::level::off;

    // Engine first: a seam outliving the engine spins on teardown.
    test_spaces space(config);
    gate_guard_t guard;
    auto* d = space.dispatcher();
    seed(d);

    guard.gate.armed.store(true, std::memory_order_release);
    components::cursor::cursor_t_ptr reader_cursor;
    std::thread reader(
        [&] { reader_cursor = d->execute_sql(otterbrix::session_id_t(), "SELECT id FROM bdb.t WHERE abs(id) >= 0;"); });

    INFO("the reader must reach the between-batches seam");
    REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));

    std::atomic<bool> unregister_answered{false};
    core::error_t unregistered = core::error_t::no_error();
    std::thread unregisterer([&] {
        unregistered = d->unregister_udf(otterbrix::session_id_t(), "abs", {components::types::logical_type::BIGINT});
        unregister_answered.store(true, std::memory_order_release);
    });
    // The reader's executor is parked in the scan, not blocked: the unregister reaches it between batches.
    const bool answered_while_parked = wait_flag(unregister_answered, std::chrono::seconds(5));

    guard.gate.released.store(true, std::memory_order_release);
    reader.join();
    unregisterer.join();

    INFO("the unregister answered while the reader was parked between batches: " << answered_while_parked);
    CHECK(unregistered.contains_error());
    REQUIRE(reader_cursor != nullptr);
    INFO("reader: " << (reader_cursor->is_error() ? reader_cursor->get_error().what.c_str() : "<ok>"));
    REQUIRE(reader_cursor->is_success());
    CHECK(reader_cursor->size() == kRows);
}
