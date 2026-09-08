#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <services/disk/agent_disk.hpp>

#include <atomic>
#include <chrono>
#include <sstream>
#include <string>
#include <thread>

// A cursor re-applies its stored positional projection to storage's current schema on every
// fetch, so a concurrent DROP COLUMN between two fetches makes stored positions name different
// columns. Measured on a 50k-row table under plain timing (no gate): SELECT b returned success
// but silently served the neighbour column from the DROP's batch on, in 5 of 8 timing-based runs.

namespace {

    constexpr std::size_t ROWS = 3072; // three 1024-row batches: one flows, two cross the DDL
    constexpr std::size_t INSERT_BATCH = 512;
    constexpr int64_t B_BASE = 1'000'000;
    constexpr int64_t C_BASE = 2'000'000;

    // Holds a user-table scan while armed-and-not-released; catalog scans (oid < FIRST_USER_OID)
    // pass free, so the interleaved DDL's own internal reads never park on the gate.
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

    // Arms the process-wide gate for the block's lifetime; disarms even on a failed REQUIRE.
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

    void run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO("SQL: " << sql);
        REQUIRE(cur->is_success());
    }

    void seed_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        run_sql(dispatcher, "CREATE DATABASE TestDatabase;");
        run_sql(dispatcher, "CREATE TABLE TestDatabase.t (id INT, b BIGINT, c BIGINT);");
        std::size_t done = 0;
        while (done < ROWS) {
            const std::size_t batch = std::min(INSERT_BATCH, ROWS - done);
            std::stringstream q;
            q << "INSERT INTO TestDatabase.t (id, b, c) VALUES ";
            for (std::size_t i = 0; i < batch; ++i) {
                const std::size_t row = done + i;
                q << "(" << row << ", " << (B_BASE + static_cast<int64_t>(row)) << ", "
                  << (C_BASE + static_cast<int64_t>(row)) << ")" << (i + 1 == batch ? ";" : ", ");
            }
            run_sql(dispatcher, q.str());
            done += batch;
        }
    }

    struct interleave_result_t {
        components::cursor::cursor_t_ptr reader_cursor;
        bool gate_reached{false};
        bool ddl_ok{false};
    };

    // Every REQUIRE runs after reader.join(), so a failed assertion can never leave the reader
    // thread running.
    interleave_result_t run_interleaved(otterbrix::wrapper_dispatcher_t* dispatcher,
                                        const std::string& reader_sql,
                                        const std::string& ddl_sql) {
        interleave_result_t out;
        gate_guard_t guard;
        guard.gate.armed.store(true, std::memory_order_release);

        std::thread reader([&] {
            auto session = otterbrix::session_id_t();
            out.reader_cursor = dispatcher->execute_sql(session, reader_sql);
        });

        out.gate_reached = wait_flag(guard.gate.reached, std::chrono::seconds(30));
        if (out.gate_reached) {
            auto session = otterbrix::session_id_t();
            auto ddl_cur = dispatcher->execute_sql(session, ddl_sql);
            out.ddl_ok = ddl_cur->is_success();
        }
        guard.gate.released.store(true, std::memory_order_release);
        reader.join();
        return out;
    }

} // namespace

TEST_CASE("integration::cursor_under_concurrent_ddl::surviving_projected_column_stays_itself") {
    auto config = test_create_config(integration_fixture_path("cursor_under_ddl/surviving_column"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    auto r = run_interleaved(dispatcher, "SELECT b FROM TestDatabase.t;", "ALTER TABLE TestDatabase.t DROP COLUMN id;");
    REQUIRE(r.gate_reached);
    REQUIRE(r.ddl_ok);

    REQUIRE(r.reader_cursor != nullptr);
    REQUIRE(r.reader_cursor->is_success());
    REQUIRE(r.reader_cursor->size() == ROWS);
    std::size_t wrong = 0;
    std::size_t first_wrong = ROWS;
    for (std::size_t row = 0; row < ROWS; ++row) {
        const auto cell = r.reader_cursor->value(0, row);
        if (cell.value<int64_t>() != B_BASE + static_cast<int64_t>(row)) {
            if (wrong == 0) {
                first_wrong = row;
            }
            ++wrong;
        }
    }
    INFO("first wrong row: " << first_wrong << " of " << ROWS);
    REQUIRE(wrong == 0);

    // A fresh cursor must still see the post-DDL schema normally, unbent by the identity
    // resolution above.
    auto session = otterbrix::session_id_t();
    auto fresh = dispatcher->execute_sql(session, "SELECT b FROM TestDatabase.t;");
    REQUIRE(fresh->is_success());
    REQUIRE(fresh->size() == ROWS);
    const auto fresh_first = fresh->value(0, 0);
    REQUIRE(fresh_first.value<int64_t>() == B_BASE);
}

TEST_CASE("integration::cursor_under_concurrent_ddl::projected_column_dropped_refuses_loudly") {
    auto config = test_create_config(integration_fixture_path("cursor_under_ddl/dropped_column"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // b and c are both BIGINT, so a type mismatch can't accidentally catch the DROP-b case.
    auto r = run_interleaved(dispatcher, "SELECT b FROM TestDatabase.t;", "ALTER TABLE TestDatabase.t DROP COLUMN b;");
    REQUIRE(r.gate_reached);
    REQUIRE(r.ddl_ok);

    REQUIRE(r.reader_cursor != nullptr);
    REQUIRE_FALSE(r.reader_cursor->is_success());
}

TEST_CASE("integration::cursor_under_concurrent_ddl::shifted_filter_ordinals_refuse_loudly") {
    auto config = test_create_config(integration_fixture_path("cursor_under_ddl/shifted_filter"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    auto r = run_interleaved(dispatcher,
                             "SELECT b FROM TestDatabase.t WHERE b >= 0;",
                             "ALTER TABLE TestDatabase.t DROP COLUMN id;");
    REQUIRE(r.gate_reached);
    REQUIRE(r.ddl_ok);

    // The filter was bound against pre-DDL ordinals; evaluating it after the shift would
    // silently test the wrong columns.
    REQUIRE(r.reader_cursor != nullptr);
    REQUIRE_FALSE(r.reader_cursor->is_success());
}
