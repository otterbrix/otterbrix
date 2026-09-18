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

// An index scan's matched row ids cross two actor hops (search, then storage_fetch); a compact
// landing between them renumbers every survivor (rebuilt at id 0), so index_scan holds
// compaction off its table from before the search until the fetch has the rows. The
// index_fetch_gate_t seam below parks the scan between the two awaits so a checkpoint lands
// inside the window deterministically instead of racing for it.

using namespace test_helpers;

namespace {

    // Deleting the front 1000 of 3000 rows shifts every survivor's physical id down by 1000, so
    // probe id 1500's stale position (1499) lands on id 2500 after compaction -- a wrong row,
    // not a short read or an out-of-range miss.
    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteUpTo = 1000;
    constexpr int64_t kProbeId = 1500;
    constexpr int64_t kProbeKey = 10 * kProbeId;

    struct index_fetch_hold_gate_t final : components::operators::index_fetch_gate_t {
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

    // Full scan on an unindexed column, so it reads the post-compact truth regardless of the
    // indexed leg's outcome.
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
        REQUIRE(exec(d, "DELETE FROM rdb.t WHERE id <= " + std::to_string(kDeleteUpTo) + ";")->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::index_scan_compact_race::matched_row_ids_survive_a_compacting_checkpoint") {
    auto config = test_create_config(integration_fixture_path("test_index_scan_compact_race/src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    // No automatic checkpoint, so the only compact is the explicit CHECKPOINT fired into the
    // held window.
    config.wal.auto_checkpoint_threshold_bytes = 0;

    // The engine FIRST: a guard declared before it is destroyed after it, so the seam stays armed
    // while the world tears down and the hold loop spins inside a dying engine.
    test_spaces space(config);
    gate_guard_t guard;
    auto* d = space.dispatcher();
    seed(d);

    // Load-bearing: without it the case could pass via a full scan instead of the index.
    {
        auto plan = exec(d, "EXPLAIN " + indexed_query());
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed probe:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    {
        auto cur = exec(d, indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == kProbeId);
    }

    // Control: the held window alone, with no checkpoint inside it, must not corrupt the answer.
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

    services::disk::reset_checkpoint_entry_tallies();
    guard.gate.armed.store(true, std::memory_order_release);
    components::cursor::cursor_t_ptr raced_cur;
    const auto scan_session = otterbrix::session_id_t();
    std::thread reader([&] { raced_cur = d->execute_sql(scan_session, indexed_query()); });

    INFO("the scan must reach the between-awaits seam");
    REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));

    // Away from the scan's executor: it is parked on the seam and holds its mailbox.
    const auto cp_session = session_avoiding_executor(executor_of(scan_session));
    REQUIRE(d->execute_sql(cp_session, "CHECKPOINT;")->is_success());

    guard.gate.released.store(true, std::memory_order_release);
    reader.join();
    guard.gate.armed.store(false, std::memory_order_release);

    INFO("checkpoint round inside the window: rewritten=" << services::disk::checkpoint_entries_rewritten()
                                                          << " deferred="
                                                          << services::disk::checkpoint_entries_deferred());

    {
        auto cur = exec(d, control_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == kProbeKey);
    }

    REQUIRE(raced_cur->is_success());
    {
        INFO("raced indexed probe returned " << raced_cur->size() << " row(s)");
        REQUIRE(raced_cur->size() == 1);
        const auto got = raced_cur->value(0, 0).value<int64_t>();
        INFO("raced indexed probe answered id=" << got << ", expected id=" << kProbeId);
        REQUIRE(got == kProbeId);
    }

    // The protection must defer compaction, not disable it: a later round must still compact
    // and rebuild, and the probe must keep answering through the rebuilt index.
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());
    {
        auto cur = exec(d, indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == kProbeId);
    }
}
