#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>

#include <services/disk/agent_disk.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>

// An index scan's matched absolute row ids must stay valid until the storage_fetch that applies
// them.
//
// index_scan crosses actors twice with those ids in hand: the one-shot index search answers a set
// of ABSOLUTE physical row ids, and a later storage_fetch reads the table at exactly those
// positions. Between the two awaits the executor yields; a compacting checkpoint from another
// session renumbers every surviving row (compact() rebuilds at id 0). Neither existing gate
// protects this window: the cursor gate sees no cursor (index_scan holds none), and the MVCC gate
// looks at version stamps IN the table, which a reading snapshot leaves none of. The stale
// positions then pass the visibility filter — every row is "just committed" — so the SELECT
// silently answers a DIFFERENT row (or none, when the stale position fell off the end).
//
// The window is microseconds wide under natural timing; the index_fetch_gate_t seam holds the
// scan between its two awaits so the checkpoint lands inside deterministically. No timing, no
// flake. Same methodology as test_delete_floor_resurrection.

using namespace test_helpers;

namespace {

    // 3 row groups; deleting the FRONT third shifts every survivor down by a full 1000 ids, and
    // a mid-table survivor's STALE position still lands inside the compacted table — so the
    // corruption shape is a silently WRONG row, not a short read.
    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteUpTo = 1000; // DELETE ... WHERE id <= 1000
    // The probe: id 1500 sits at physical row 1499 before the compact and 499 after it. The row
    // the STALE position 1499 names after the compact is id 2500 — 1000 ids away, unmissable.
    constexpr int64_t kProbeId = 1500;
    constexpr int64_t kProbeKey = 10 * kProbeId;

    struct index_fetch_hold_gate_t final : components::operators::index_fetch_gate_t {
        std::atomic<bool> armed{false};
        std::atomic<bool> reached{false};
        std::atomic<bool> released{false};

        // Only user tables: catalog reads never plan an index_scan, but the guard keeps the gate
        // honest if that ever changes.
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
        index_fetch_hold_gate_t gate;
        gate_guard_t() { components::operators::dev_set_index_fetch_gate(&gate); }
        ~gate_guard_t() { components::operators::dev_set_index_fetch_gate(nullptr); }
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

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    std::string indexed_query() { return "SELECT id FROM rdb.t WHERE k = " + std::to_string(kProbeKey) + ";"; }

    // UNINDEXED control leg: same table, same row, a column no index covers — a full scan, so it
    // reads the post-compact truth regardless of what the indexed leg did.
    std::string control_query() { return "SELECT k FROM rdb.t WHERE id = " + std::to_string(kProbeId) + ";"; }

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE rdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE rdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX k_idx ON rdb.t (k);")->is_success());
        for (int64_t start = 1; start <= kRows; start += 500) {
            std::string sql = "INSERT INTO rdb.t (id, k) VALUES ";
            for (int64_t i = start; i < start + 500 && i <= kRows; ++i) {
                if (i != start) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
            }
            sql += ";";
            REQUIRE(exec(d, sql)->is_success());
        }
        // The front third goes: after the racing compact every survivor's physical id drops by
        // 1000 while the index scan already holds the OLD ids.
        REQUIRE(exec(d, "DELETE FROM rdb.t WHERE id <= " + std::to_string(kDeleteUpTo) + ";")->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::index_scan_compact_race::matched_row_ids_survive_a_compacting_checkpoint") {
    auto config = test_create_config(integration_fixture_path("test_index_scan_compact_race/src"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    // No automatic checkpoint: the ONLY compact in this case is the explicit CHECKPOINT fired
    // into the held window, so a wrong answer can only be that compact's renumbering.
    config.wal.auto_checkpoint_threshold_bytes = 0;

    gate_guard_t guard;

    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // Load-bearing: without this the case can pass (or fail) via a full scan instead of the
    // index (methodology of test_index_stale_after_compact).
    {
        auto plan = exec(d, "EXPLAIN " + indexed_query());
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed probe:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    // Baseline, gate unarmed: the indexed probe answers the right row.
    {
        auto cur = exec(d, indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == kProbeId);
    }

    // CONTROL: the held window ALONE (no checkpoint inside it) does not corrupt the answer —
    // whatever goes wrong below is the checkpoint's doing, not the seam's.
    {
        guard.gate.armed.store(true, std::memory_order_release);
        components::cursor::cursor_t_ptr held_cur;
        std::thread reader([&] {
            auto session = otterbrix::session_id_t();
            held_cur = d->execute_sql(session, indexed_query());
        });
        REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));
        guard.gate.released.store(true, std::memory_order_release);
        reader.join();
        guard.gate.armed.store(false, std::memory_order_release);
        guard.gate.reached.store(false, std::memory_order_release);
        guard.gate.released.store(false, std::memory_order_release);
        REQUIRE(held_cur->is_success());
        REQUIRE(held_cur->size() == 1);
        REQUIRE(held_cur->value(0, 0).value<int64_t>() == kProbeId);
    }

    // THE RACE: the same probe parks between its index search (old ids in hand) and its
    // storage_fetch; a checkpoint from another session compacts the table inside the window.
    services::disk::reset_checkpoint_entry_tallies();
    guard.gate.armed.store(true, std::memory_order_release);
    components::cursor::cursor_t_ptr raced_cur;
    std::thread reader([&] {
        auto session = otterbrix::session_id_t();
        raced_cur = d->execute_sql(session, indexed_query());
    });

    INFO("the scan must reach the between-awaits seam");
    REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));

    REQUIRE(exec(d, "CHECKPOINT;")->is_success());

    guard.gate.released.store(true, std::memory_order_release);
    reader.join();
    guard.gate.armed.store(false, std::memory_order_release);

    INFO("checkpoint round inside the window: rewritten=" << services::disk::checkpoint_entries_rewritten()
                                                          << " deferred=" << services::disk::checkpoint_entries_deferred());

    // The TABLE is intact either way — the full-scan control leg reads the post-compact truth.
    {
        auto cur = exec(d, control_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == kProbeKey);
    }

    // The raced indexed probe must still answer THE ROW IT MATCHED — not a renumbered stranger,
    // not nothing.
    REQUIRE(raced_cur->is_success());
    {
        INFO("raced indexed probe returned " << raced_cur->size() << " row(s)");
        REQUIRE(raced_cur->size() == 1);
        const auto got = raced_cur->value(0, 0).value<int64_t>();
        INFO("raced indexed probe answered id=" << got << ", expected id=" << kProbeId);
        REQUIRE(got == kProbeId);
    }

    // Once the reader is gone a later round must still compact and rebuild, and the indexed
    // probe must keep answering through the REBUILT index — the race protection must defer, not
    // permanently pin.
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());
    {
        auto cur = exec(d, indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == kProbeId);
    }
}

