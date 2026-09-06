#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

// A committed DELETE must stay deleted across a restart.
//
// DELETE journals its physical_delete on one message (executor -> WAL) and applies the storage
// mark on another (executor -> disk), with the executor free to yield between them. In the OLD
// WAL-first order the record existed — its id counted into the checkpoint boundary — while the
// table still looked UNCHANGED, so a checkpoint racing that window took the unchanged-table gate
// and advanced this table's DURABLE WAL floor (its .otbx.wal_id sidecar) past a delete that was
// never folded into the .otbx. A restart then read that floor and SKIPPED the delete's replay,
// and the rows the client was told were gone came back.
//
// The window is microseconds wide under natural timing; the delete_wal_apply_gate_t seam holds
// the delete right after its WAL record is durable so a checkpoint from another session lands in
// the window deterministically. No timing, no flake.
//
// Storage-first (apply then journal, matching operator_update) closes it: while the delete is
// unapplied no WAL id for it exists to advance the floor to, and once its id exists the table
// already carries the pending delete stamp, so the checkpoint DEFERS the table (MVCC gate)
// instead of advancing it. The seam then holds AFTER the apply, so the racing checkpoint sees the
// stamp and the floor never moves past the delete.

using namespace test_helpers;

namespace {

    constexpr int kRows = 200;
    constexpr int kDeleteUpTo = 50; // DELETE ... WHERE id <= 50

    struct delete_hold_gate_t final : components::operators::delete_wal_apply_gate_t {
        std::atomic<bool> armed{false};
        std::atomic<bool> reached{false};
        std::atomic<bool> released{false};

        // Only user tables (the fixture's adb.t); a catalog delete never reaches this seam
        // anyway (it takes the sourceless delete_pg_catalog_rows path), but the guard keeps the
        // gate honest if that ever changes.
        bool hold(components::catalog::oid_t table_oid) override {
            if (!armed.load(std::memory_order_acquire) ||
                static_cast<uint32_t>(table_oid) < static_cast<uint32_t>(components::catalog::FIRST_USER_OID)) {
                return false;
            }
            reached.store(true, std::memory_order_release);
            return !released.load(std::memory_order_acquire);
        }
    };

    struct gate_guard_t {
        delete_hold_gate_t gate;
        gate_guard_t() { components::operators::dev_set_delete_wal_apply_gate(&gate); }
        ~gate_guard_t() { components::operators::dev_set_delete_wal_apply_gate(nullptr); }
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
        REQUIRE(exec(d, "CREATE TABLE adb.t (id bigint, k bigint);")->is_success());
        std::string sql = "INSERT INTO adb.t (id, k) VALUES ";
        for (int i = 1; i <= kRows; ++i) {
            sql += (i == 1 ? "" : ", ") + std::string("(") + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
        }
        sql += ";";
        REQUIRE(exec(d, sql)->is_success());
    }

    std::size_t rows_at_or_below(otterbrix::wrapper_dispatcher_t* d, int id_cap) {
        auto cur = exec(d, "SELECT id FROM adb.t WHERE id <= " + std::to_string(id_cap) + ";");
        REQUIRE(cur->is_success());
        return cur->size();
    }

} // namespace

TEST_CASE("integration::cpp::delete_floor_resurrection::committed_delete_survives_restart") {
    auto config = test_create_config(integration_fixture_path("test_delete_floor_resurrection/src"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    // No automatic checkpoint: the ONLY checkpoint in this case is the explicit one we fire into
    // the held window, so a resurrection can only be that checkpoint's floor advance.
    config.wal.auto_checkpoint_threshold_bytes = 0;

    const std::filesystem::path crash_dir = integration_fixture_path("test_delete_floor_resurrection/crash");

    gate_guard_t guard;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        seed(d);

        // Durable root BEFORE the delete: the .otbx now holds all 200 rows and the table is clean,
        // so the racing checkpoint below takes the unchanged-table gate.
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        guard.gate.armed.store(true, std::memory_order_release);

        // The delete streams on its own session; it parks at the seam right after its WAL record
        // is durable.
        std::thread deleter([&] {
            auto session = otterbrix::session_id_t();
            auto cur = d->execute_sql(session, "DELETE FROM adb.t WHERE id <= " + std::to_string(kDeleteUpTo) + ";");
            REQUIRE(cur->is_success());
        });

        INFO("the delete must reach the post-WAL seam");
        REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));

        // Another session checkpoints while the delete is held. In the buggy order the table looks
        // unchanged here and its sidecar is advanced past the delete's WAL id.
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        guard.gate.released.store(true, std::memory_order_release);
        deleter.join();

        // The delete is applied in memory: the live engine agrees the rows are gone.
        INFO("the delete must be visible in the live engine (not a resurrection of a never-applied delete)");
        REQUIRE(rows_at_or_below(d, kDeleteUpTo) == 0);

        // Crash image: copy the LIVE directory before the destructor's shutdown checkpoint can fold
        // the delete into the .otbx (that would mask the bug).
        std::filesystem::remove_all(crash_dir);
        std::filesystem::create_directories(crash_dir.parent_path());
        std::filesystem::copy(config.main_path, crash_dir, std::filesystem::copy_options::recursive);
    }

    // Reopen the crash image. If the floor was advanced past the delete, replay skips it and the
    // rows resurrect.
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.wal.on = true;
        crash_config.log.level = log_t::level::off;
        crash_config.wal.auto_checkpoint_threshold_bytes = 0;

        test_spaces space(crash_config);
        auto* d = space.dispatcher();

        const auto survivors = rows_at_or_below(d, kDeleteUpTo);
        INFO("rows with id <= " << kDeleteUpTo << " after restart (0 = delete survived; >0 = resurrected): "
                                << survivors);
        REQUIRE(survivors == 0);
        // The rows the delete DID NOT touch must all still be there.
        auto cur = exec(d, "SELECT id FROM adb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<std::size_t>(kRows - kDeleteUpTo));
    }
}
