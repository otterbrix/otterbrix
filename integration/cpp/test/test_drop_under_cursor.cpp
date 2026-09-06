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

// A streaming reader whose table is DROP'd out from under it, between two fetches of its cursor,
// used to get a TRUNCATED result reported as SUCCESS: the re-resolve leg in
// storage_fetch_next_batch_inner found the storage gone and replied the drained sentinel, which
// the executor reads as "end of table". The reader saw a prefix of the rows and a success verdict.
//
// This is NOT PostgreSQL parity. PG's DROP takes ACCESS EXCLUSIVE, conflicting with the reader's
// ACCESS SHARE, so the reader either finishes fully (the DROP waits) or the NEXT statement fails
// with "relation does not exist" — it never gets an error mid-read. We have nothing to wait on
// without a lock (a mailbox held across a fetch deadlocks), so a loud error on a fetch over a
// vanished entry is the best achievable without locks: strictly better than the silent short
// answer, still short of PG.
//
// The between-batches pause gate (services::disk::scan_advance_gate_t) holds the reader after its
// first batch so the DROP commits deterministically in the window.

using namespace test_helpers;

namespace {

    constexpr std::size_t ROWS = 3072; // three 1024-row batches: one flows, the DROP lands, more fetch

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
        REQUIRE(exec(d, "CREATE DATABASE adb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE adb.t (id bigint);")->is_success());
        std::size_t done = 0;
        while (done < ROWS) {
            const std::size_t batch = std::min<std::size_t>(512, ROWS - done);
            std::stringstream q;
            q << "INSERT INTO adb.t (id) VALUES ";
            for (std::size_t i = 0; i < batch; ++i) {
                q << "(" << (done + i) << ")" << (i + 1 == batch ? ";" : ", ");
            }
            REQUIRE(exec(d, q.str())->is_success());
            done += batch;
        }
    }

} // namespace

TEST_CASE("integration::cpp::drop_under_cursor::vanished_entry_fails_loudly_not_a_short_success") {
    auto config = test_create_config(integration_fixture_path("test_drop_under_cursor/db"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    gate_guard_t guard;

    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    guard.gate.armed.store(true, std::memory_order_release);

    components::cursor::cursor_t_ptr reader_cursor;
    std::thread reader([&] {
        auto session = otterbrix::session_id_t();
        reader_cursor = d->execute_sql(session, "SELECT id FROM adb.t;");
    });

    INFO("the reader must reach the between-batches seam");
    REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));

    // Drop the table to full commit from another session, while the reader is held.
    {
        auto session = otterbrix::session_id_t();
        auto drop_cur = d->execute_sql(session, "DROP TABLE adb.t;");
        REQUIRE(drop_cur->is_success());
    }

    guard.gate.released.store(true, std::memory_order_release);
    reader.join();

    // The reader's storage vanished mid-scan: the only honest answer is a loud error. The bug
    // reported SUCCESS with a truncated prefix of the rows.
    REQUIRE(reader_cursor != nullptr);
    INFO("a fetch over a dropped table must be a loud error, not a short success");
    REQUIRE_FALSE(reader_cursor->is_success());
}
