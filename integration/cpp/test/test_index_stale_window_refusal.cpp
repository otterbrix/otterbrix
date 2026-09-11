#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/physical_plan/operators/operator_checkpoint.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>

// PIN (loud-refusal policy): a reader that ARRIVES between the two phases of a checkpoint round
// — after compact() renumbered the physical row ids, before repopulate_indexes_after_compaction
// rebuilt the index — must be REFUSED with core::error_t, never answered with a stranger row or
// a missing row. Its storage_open_scan_hold lands after checkpoint_inner already consulted
// has_active_scan_for_oid for this entry, so the hold defers nothing this round; the epoch check
// at storage_fetch is the only thing standing between such a reader and a silent wrong answer.
//
// Measured on the unfixed tree (test_index_stale_window_measure, seam leg): 6/6 attempts
// corrupted at every delay >= 20 ms. This case uses the same seam and requires 6/6 REFUSALS.

using namespace test_helpers;

namespace {

    constexpr int64_t kRows = 3000;
    constexpr int64_t kSlide = 1000;
    constexpr int kAttempts = 6;
    constexpr int kDelayMs = 100;

    struct repopulate_hold_gate_t final : components::operators::checkpoint_repopulate_gate_t {
        std::atomic<bool> armed{false};
        std::atomic<bool> reached{false};
        std::atomic<bool> released{false};

        bool hold() override {
            if (!armed.load(std::memory_order_acquire)) {
                return false;
            }
            reached.store(true, std::memory_order_release);
            return !released.load(std::memory_order_acquire);
        }

        void reset() {
            armed.store(false, std::memory_order_release);
            reached.store(false, std::memory_order_release);
            released.store(false, std::memory_order_release);
        }
    };

    struct gate_guard_t {
        repopulate_hold_gate_t gate;
        gate_guard_t() { components::operators::dev_set_checkpoint_repopulate_gate(&gate); }
        ~gate_guard_t() { components::operators::dev_set_checkpoint_repopulate_gate(nullptr); }
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

    std::string probe_sql(int64_t probe_id) {
        return "SELECT id FROM rdb.t WHERE k = " + std::to_string(10 * probe_id) + ";";
    }

    std::string control_sql(int64_t probe_id) {
        return "SELECT k FROM rdb.t WHERE id = " + std::to_string(probe_id) + ";";
    }

    void insert_range(otterbrix::wrapper_dispatcher_t* d, int64_t from, int64_t to) {
        for (int64_t start = from; start <= to; start += 500) {
            std::string sql = "INSERT INTO rdb.t (id, k) VALUES ";
            const int64_t stop = std::min(start + 500 - 1, to);
            for (int64_t i = start; i <= stop; ++i) {
                if (i != start) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
            }
            sql += ";";
            REQUIRE(exec(d, sql)->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cpp::index_stale_window::reader_in_window_is_refused_not_lied_to") {
    auto config = test_create_config(integration_fixture_path("test_index_stale_window_refusal/src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    // No automatic checkpoint: the only compacts are the explicit CHECKPOINTs below, so every
    // outcome is attributable to a known round.
    config.wal.auto_checkpoint_threshold_bytes = 0;

    // Engine first: a seam outliving the engine keeps the hold loop spinning during teardown.
    test_spaces space(config);
    gate_guard_t guard;
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE rdb;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE rdb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec(d, "CREATE INDEX k_idx ON rdb.t (k);")->is_success());
    insert_range(d, 1, kRows);
    int64_t lo = 1;
    int64_t hi = kRows;

    // Load-bearing: the probe must be answered by the index, not a full scan.
    {
        auto plan = exec(d, "EXPLAIN " + probe_sql(lo + 500));
        REQUIRE(plan->is_success());
        std::string text;
        for (std::size_t r = 0; r < plan->size(); ++r) {
            text += std::string(plan->value(0, r).value<std::string_view>());
            text += '\n';
        }
        INFO("plan for the indexed probe:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    int refused = 0, clean = 0, wrong = 0, missing = 0, extra = 0;
    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        // Give the compact something to move: kill the front kSlide rows, append kSlide fresh.
        REQUIRE(exec(d,
                     "DELETE FROM rdb.t WHERE id >= " + std::to_string(lo) + " AND id < " +
                         std::to_string(lo + kSlide) + ";")
                    ->is_success());
        insert_range(d, hi + 1, hi + kSlide);
        lo += kSlide;
        hi += kSlide;
        const int64_t p = lo + 500;
        const std::string probe = probe_sql(p);

        auto cp_session = otterbrix::session_id_t();
        const std::size_t cp_idx = executor_of(cp_session);

        guard.gate.armed.store(true, std::memory_order_release);
        components::cursor::cursor_t_ptr cp_cur;
        std::thread checkpointer([&] { cp_cur = d->execute_sql(cp_session, "CHECKPOINT;"); });

        INFO("the round must reach the between-phases seam");
        REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));

        // The round is parked: compaction BEHIND, index rebuild AHEAD. This reader arrives
        // inside the window.
        auto rd_session = session_avoiding_executor(cp_idx);
        components::cursor::cursor_t_ptr rd_cur;
        std::thread reader([&] { rd_cur = d->execute_sql(rd_session, probe); });

        std::this_thread::sleep_for(std::chrono::milliseconds(kDelayMs));
        guard.gate.released.store(true, std::memory_order_release);
        reader.join();
        checkpointer.join();
        guard.gate.reset();

        REQUIRE(cp_cur->is_success());

        const char* shape = "clean";
        std::string detail;
        if (rd_cur->is_error()) {
            shape = "REFUSED";
            ++refused;
            detail = std::string(rd_cur->get_error().what.c_str());
        } else if (rd_cur->size() == 0) {
            shape = "MISSING";
            ++missing;
        } else if (rd_cur->size() > 1) {
            shape = "EXTRA";
            ++extra;
        } else if (rd_cur->value(0, 0).value<int64_t>() != p) {
            shape = "WRONG";
            ++wrong;
            detail = "got id=" + std::to_string(rd_cur->value(0, 0).value<int64_t>());
        } else {
            ++clean;
        }
        std::fprintf(stderr,
                     "[refusal-pin] attempt=%d shape=%s expected=%lld %s\n",
                     attempt,
                     shape,
                     static_cast<long long>(p),
                     detail.c_str());

        // The TABLE is intact either way: full-scan control of the same row.
        {
            auto ctl = exec(d, control_sql(p));
            REQUIRE(ctl->is_success());
            REQUIRE(ctl->size() == 1);
            REQUIRE(ctl->value(0, 0).value<int64_t>() == 10 * p);
        }
        // The refusal is NOT sticky: once the round finished, the rebuilt index answers again.
        {
            auto post = exec(d, probe);
            REQUIRE(post->is_success());
            REQUIRE(post->size() == 1);
            REQUIRE(post->value(0, 0).value<int64_t>() == p);
        }
    }

    std::fprintf(stderr,
                 "[refusal-pin] SUMMARY refused=%d clean=%d wrong=%d missing=%d extra=%d of %d\n",
                 refused,
                 clean,
                 wrong,
                 missing,
                 extra,
                 kAttempts);

    // THE PIN. No shape of silent corruption is ever acceptable...
    REQUIRE(wrong == 0);
    REQUIRE(missing == 0);
    REQUIRE(extra == 0);
    // ...and the reader in the window is REFUSED, not accidentally clean: the unfixed tree
    // measured 6/6 corrupt at this delay, so 6/6 refusals is the deterministic expectation.
    REQUIRE(refused == kAttempts);
}
