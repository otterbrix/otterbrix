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

// WAL-first let a delete's WAL id count into a checkpoint boundary before its storage mark was
// applied, so a racing checkpoint could advance the table's durable WAL floor (.otbx.wal_id) past
// an unfolded delete and a restart would skip its replay; delete_wal_apply_gate_t pins the race to
// right after the WAL write so it happens deterministically.

using namespace test_helpers;

namespace {

    constexpr int kRows = 200;
    constexpr int kDeleteUpTo = 50;

    struct delete_hold_gate_t final : components::operators::delete_wal_apply_gate_t {
        std::atomic<bool> armed{false};
        std::atomic<bool> reached{false};
        std::atomic<bool> released{false};

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
    // auto_checkpoint disabled: the only checkpoint here is the explicit one fired into the held window.
    config.wal.auto_checkpoint_threshold_bytes = 0;

    const std::filesystem::path crash_dir = integration_fixture_path("test_delete_floor_resurrection/crash");

    gate_guard_t guard;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        seed(d);

        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        guard.gate.armed.store(true, std::memory_order_release);

        std::thread deleter([&] {
            auto session = otterbrix::session_id_t();
            auto cur = d->execute_sql(session, "DELETE FROM adb.t WHERE id <= " + std::to_string(kDeleteUpTo) + ";");
            REQUIRE(cur->is_success());
        });

        INFO("the delete must reach the post-WAL seam");
        REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));

        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        guard.gate.released.store(true, std::memory_order_release);
        deleter.join();

        INFO("the delete must be visible in the live engine (not a resurrection of a never-applied delete)");
        REQUIRE(rows_at_or_below(d, kDeleteUpTo) == 0);

        // Copied while still open: the destructor's shutdown checkpoint would fold the delete into
        // the .otbx and mask the bug.
        std::filesystem::remove_all(crash_dir);
        std::filesystem::create_directories(crash_dir.parent_path());
        std::filesystem::copy(config.main_path, crash_dir, std::filesystem::copy_options::recursive);
    }

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
        auto cur = exec(d, "SELECT id FROM adb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == static_cast<std::size_t>(kRows - kDeleteUpTo));
    }
}
