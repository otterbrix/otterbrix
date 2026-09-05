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

// An open streaming cursor meets a committed ALTER TABLE DROP COLUMN from another session.
//
// The cursor stores the POSITIONAL projection it was opened with and re-applies it to the
// storage's CURRENT schema on every fetch; the commit of a concurrent DROP COLUMN rebuilds the
// table between two fetches of the same cursor, so from that batch on the stored positions name
// DIFFERENT columns. Observed on a 50k-row table without any seam: SELECT b returned SUCCESS and
// the full row count, but from a batch boundary on every value was byte-for-byte the NEIGHBOUR
// column of the same row — a silent wrong answer, hit in 5 of 8 timing-based runs.
//
// These tests close the window deterministically with the between-batches pause gate
// (services::disk::scan_advance_gate_t): batch one flows, the scan is held between batches, the
// ALTER runs to full commit from another session, the scan is released. No timing, no flake.
//
// The pinned semantics:
//   * a projected column that SURVIVES the DROP keeps answering with its own data
//     (the cursor resolves columns by identity, not by stored position);
//   * a projected column whose data is physically GONE refuses loudly — the fetch errors
//     instead of silently serving whichever column now sits at the stored position;
//   * a cursor whose pushed-down filter was bound against ordinals the DROP shifted refuses
//     loudly — the filter would otherwise be evaluated against the wrong columns.

namespace {

    constexpr std::size_t ROWS = 3072; // three 1024-row batches: one flows, two cross the DDL
    constexpr std::size_t INSERT_BATCH = 512;
    constexpr int64_t B_BASE = 1'000'000;
    constexpr int64_t C_BASE = 2'000'000;

    // Holds every ADVANCE of a USER-table scan while armed-and-not-released. Catalog scans
    // (oids below FIRST_USER_OID) pass freely, so the interleaved DDL's own internal reads can
    // never park themselves on the gate. The only user table in these fixtures is the one the
    // reader scans.
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

    // The deterministic interleave: `reader_sql` streams from one session, the gate parks it
    // between batch one and batch two, `ddl_sql` runs TO FULL COMMIT from another session,
    // then the reader is released and joined. Every REQUIRE happens after the join so a failed
    // assertion can never leave the reader thread running.
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
    config.wal.on = true;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    auto r = run_interleaved(dispatcher, "SELECT b FROM TestDatabase.t;", "ALTER TABLE TestDatabase.t DROP COLUMN id;");
    REQUIRE(r.gate_reached);
    REQUIRE(r.ddl_ok);

    // The reader projected `b` and `b` survived the DROP: the answer must be `b` — the whole
    // column, its own data. The broken cursor answered SUCCESS with the full row count but
    // served the NEIGHBOUR column `c` from the first post-DDL batch on.
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

    // A FRESH cursor over the post-DDL table sees the post-DDL schema — the identity
    // resolution above must not have bent the normal path.
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
    config.wal.on = true;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    // Project the column the DROP takes away, with a SAME-TYPED neighbour right behind it:
    // after the rebuild the stored position names `c` (BIGINT, like `b`), so no downstream
    // type check can error by accident — the broken cursor answers SUCCESS and serves `c`.
    auto r = run_interleaved(dispatcher, "SELECT b FROM TestDatabase.t;", "ALTER TABLE TestDatabase.t DROP COLUMN b;");
    REQUIRE(r.gate_reached);
    REQUIRE(r.ddl_ok);

    // The reader projected the column the DROP took away: its data is physically gone, so the
    // only honest answer is a loud error.
    REQUIRE(r.reader_cursor != nullptr);
    REQUIRE_FALSE(r.reader_cursor->is_success());
}

TEST_CASE("integration::cursor_under_concurrent_ddl::shifted_filter_ordinals_refuse_loudly") {
    auto config = test_create_config(integration_fixture_path("cursor_under_ddl/shifted_filter"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_table(dispatcher);

    auto r = run_interleaved(dispatcher,
                             "SELECT b FROM TestDatabase.t WHERE b >= 0;",
                             "ALTER TABLE TestDatabase.t DROP COLUMN id;");
    REQUIRE(r.gate_reached);
    REQUIRE(r.ddl_ok);

    // The pushed-down filter was bound against the pre-DDL ordinals and the DROP shifted every
    // column it can touch; evaluating it against the rebuilt table would silently test the
    // wrong columns. The only honest answer is a loud error.
    REQUIRE(r.reader_cursor != nullptr);
    REQUIRE_FALSE(r.reader_cursor->is_success());
}
