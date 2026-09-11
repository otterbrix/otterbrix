#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator_checkpoint.hpp>

#include <services/disk/agent_disk.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

// MEASUREMENT, not a regression test. Question under measurement: between the two phases of a
// checkpoint round — checkpoint_all (compact() renumbers physical row ids) and
// repopulate_indexes_after_compaction (the index still holds pre-compact ids) — can a reader
// that ARRIVES inside the window read through the stale index? Such a reader's
// storage_open_scan_hold lands after checkpoint_inner already consulted has_active_scan_for_oid
// for its entry, so the hold defers nothing; the covered case (reader overlapping the round from
// before compact) is NOT what is probed here.
//
// Three legs:
//   1. natural timing — N rounds, a reader thread hammers the indexed probe concurrently;
//   2. seam — checkpoint_repopulate_gate_t parks the round exactly between the phases for
//      0/20/50/100 ms while one reader fires;
//   3. controls — seam armed with NO checkpoint (expect 0 corruptions), and a full-scan probe of
//      the same row after every round (the TABLE must always be right; only the index path may lie).
//
// Corruption shapes: rows==0 = MISSING row (fatal under superset semantics); rows==1 with a
// different id = WRONG row (the equality predicate was answered by the index, not re-checked).

using namespace test_helpers;

namespace {

    constexpr int64_t kSlide = 1000; // per-iteration front DELETE + tail INSERT

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

    struct probe_outcome_t {
        bool success{false};
        std::size_t rows{0};
        int64_t got{0}; // id of the first returned row, when rows >= 1
    };

    probe_outcome_t classify(const components::cursor::cursor_t_ptr& cur) {
        probe_outcome_t out;
        out.success = cur->is_success();
        if (!out.success) {
            return out;
        }
        out.rows = cur->size();
        if (out.rows >= 1) {
            out.got = cur->value(0, 0).value<int64_t>();
        }
        return out;
    }

    std::string probe_sql(int64_t probe_id) {
        return "SELECT id FROM rdb.t WHERE k = " + std::to_string(10 * probe_id) + ";";
    }

    // UNINDEXED control leg: same row through a full scan (no index on id) — reads the
    // post-compact truth regardless of what the indexed leg answered.
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

    struct table_state_t {
        int64_t lo{1}; // lowest alive id
        int64_t hi{0}; // highest alive id
    };

    void seed(otterbrix::wrapper_dispatcher_t* d, table_state_t& st, int64_t live_rows) {
        REQUIRE(exec(d, "CREATE DATABASE rdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE rdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX k_idx ON rdb.t (k);")->is_success());
        insert_range(d, 1, live_rows);
        st.lo = 1;
        st.hi = live_rows;
    }

    // Give the next compact something to move: kill the front kSlide alive rows (they sit
    // physically first, so every survivor's id drops on compact) and append kSlide fresh ones.
    void slide(otterbrix::wrapper_dispatcher_t* d, table_state_t& st) {
        REQUIRE(exec(d,
                     "DELETE FROM rdb.t WHERE id >= " + std::to_string(st.lo) + " AND id < " +
                         std::to_string(st.lo + kSlide) + ";")
                    ->is_success());
        insert_range(d, st.hi + 1, st.hi + kSlide);
        st.lo += kSlide;
        st.hi += kSlide;
    }

    configuration::config make_config(const std::string& sub) {
        auto config = test_create_config(integration_fixture_path("test_index_stale_window_measure/" + sub));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        // No automatic checkpoint: the only compacts are the explicit CHECKPOINTs this
        // measurement fires, so every corruption is attributable to a known round.
        config.wal.auto_checkpoint_threshold_bytes = 0;
        return config;
    }

    void require_index_plan(otterbrix::wrapper_dispatcher_t* d, int64_t probe_id) {
        auto plan = exec(d, "EXPLAIN " + probe_sql(probe_id));
        REQUIRE(plan->is_success());
        std::string text;
        for (std::size_t r = 0; r < plan->size(); ++r) {
            auto v = plan->value(0, r);
            text += std::string(v.value<std::string_view>());
            text += '\n';
        }
        INFO("plan for the indexed probe:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    struct leg_tally_t {
        int rounds{0};
        int hit_rounds{0};
        long probes{0};
        long wrong{0};
        long missing{0};
        long extra{0};
        long failed{0};
    };

    void run_natural_leg(otterbrix::wrapper_dispatcher_t* d, table_state_t& st, int iterations, const char* label) {
        leg_tally_t tally;
        for (int it = 0; it < iterations; ++it) {
            slide(d, st);
            const int64_t p = st.lo + 500;
            const std::string probe = probe_sql(p);

            auto cp_session = otterbrix::session_id_t();
            const std::size_t cp_idx = executor_of(cp_session);

            std::atomic<bool> stop{false};
            std::vector<probe_outcome_t> outcomes;
            std::thread reader([&] {
                while (!stop.load(std::memory_order_acquire)) {
                    auto s = session_avoiding_executor(cp_idx);
                    outcomes.push_back(classify(d->execute_sql(s, probe)));
                }
            });

            services::disk::reset_checkpoint_entry_tallies();
            auto cp_cur = d->execute_sql(cp_session, "CHECKPOINT;");
            stop.store(true, std::memory_order_release);
            reader.join();
            REQUIRE(cp_cur->is_success());

            long wrong = 0, missing = 0, extra = 0, failed = 0;
            for (const auto& o : outcomes) {
                if (!o.success) {
                    ++failed;
                } else if (o.rows == 0) {
                    ++missing;
                } else if (o.rows > 1) {
                    ++extra;
                } else if (o.got != p) {
                    ++wrong;
                }
            }
            tally.rounds++;
            tally.probes += static_cast<long>(outcomes.size());
            tally.wrong += wrong;
            tally.missing += missing;
            tally.extra += extra;
            tally.failed += failed;
            if (wrong + missing + extra > 0) {
                tally.hit_rounds++;
            }

            std::fprintf(stderr,
                         "[%s] iter=%02d probes=%zu wrong=%ld missing=%ld extra=%ld failed=%ld "
                         "rewritten=%llu deferred=%llu\n",
                         label,
                         it,
                         outcomes.size(),
                         wrong,
                         missing,
                         extra,
                         failed,
                         static_cast<unsigned long long>(services::disk::checkpoint_entries_rewritten()),
                         static_cast<unsigned long long>(services::disk::checkpoint_entries_deferred()));

            // CONTROL: the table itself must always be right — a full scan of the same row.
            {
                auto ctl = classify(exec(d, control_sql(p)));
                REQUIRE(ctl.success);
                REQUIRE(ctl.rows == 1);
                REQUIRE(ctl.got == 10 * p);
            }
            // After the round the rebuilt index must answer correctly again.
            {
                auto post = classify(exec(d, probe));
                CHECK(post.success);
                CHECK(post.rows == 1);
                CHECK(post.got == p);
            }
        }
        std::fprintf(stderr,
                     "[%s] SUMMARY hit_rounds=%d/%d probes=%ld wrong=%ld missing=%ld extra=%ld failed=%ld\n",
                     label,
                     tally.hit_rounds,
                     tally.rounds,
                     tally.probes,
                     tally.wrong,
                     tally.missing,
                     tally.extra,
                     tally.failed);

        // The counters are the point, so they are ASSERTED and not merely printed: a leg that only
        // reported numbers would stay green if the corruption came back. Measured 0 of every form
        // after the epoch check landed, against 10/12 and 21/21 corrupted rounds before it. This
        // cannot flake green-to-red: a round where the compact never ran simply scores zero.
        INFO("stale row ids must never reach a reader: refusing is allowed, answering wrongly is not");
        REQUIRE(tally.wrong == 0);
        REQUIRE(tally.missing == 0);
        REQUIRE(tally.extra == 0);
    }

} // namespace

TEST_CASE("integration::cpp::index_stale_window::natural_timing_3k") {
    auto config = make_config("natural3k");
    test_spaces space(config);
    auto* d = space.dispatcher();
    table_state_t st;
    seed(d, st, 3000);
    require_index_plan(d, st.lo + 500);
    run_natural_leg(d, st, 36, "natural-3k");
}

TEST_CASE("integration::cpp::index_stale_window::natural_timing_30k", "[.][stalewindow]") {
    auto config = make_config("natural30k");
    test_spaces space(config);
    auto* d = space.dispatcher();
    table_state_t st;
    seed(d, st, 30000);
    require_index_plan(d, st.lo + 500);
    run_natural_leg(d, st, 36, "natural-30k");
}

TEST_CASE("integration::cpp::index_stale_window::seam_between_compact_and_rebuild") {
    auto config = make_config("seam");
    gate_guard_t guard;
    test_spaces space(config);
    auto* d = space.dispatcher();
    table_state_t st;
    seed(d, st, 3000);
    require_index_plan(d, st.lo + 500);

    // CONTROL: seam armed, NO checkpoint — the gate holds nothing (only the checkpoint operator
    // polls it), so every probe must be clean.
    {
        guard.gate.armed.store(true, std::memory_order_release);
        int clean = 0;
        const int64_t p = st.lo + 500;
        for (int i = 0; i < 6; ++i) {
            auto o = classify(exec(d, probe_sql(p)));
            if (o.success && o.rows == 1 && o.got == p) {
                ++clean;
            }
        }
        guard.gate.reset();
        std::fprintf(stderr, "[seam-control] armed-no-checkpoint clean=%d/6\n", clean);
        REQUIRE(clean == 6);
    }

    const int delays_ms[] = {0, 20, 50, 100};
    for (const int delay : delays_ms) {
        int hits = 0, wrong = 0, missing = 0, extra = 0, clean = 0;
        constexpr int kAttempts = 6;
        for (int attempt = 0; attempt < kAttempts; ++attempt) {
            slide(d, st);
            const int64_t p = st.lo + 500;
            const std::string probe = probe_sql(p);

            auto cp_session = otterbrix::session_id_t();
            const std::size_t cp_idx = executor_of(cp_session);

            services::disk::reset_checkpoint_entry_tallies();
            guard.gate.armed.store(true, std::memory_order_release);
            components::cursor::cursor_t_ptr cp_cur;
            std::thread checkpointer([&] { cp_cur = d->execute_sql(cp_session, "CHECKPOINT;"); });

            INFO("the round must reach the between-phases seam");
            REQUIRE(wait_flag(guard.gate.reached, std::chrono::seconds(30)));

            // The round is parked: compaction is BEHIND, the index rebuild is AHEAD. This reader
            // arrives inside the window.
            auto rd_session = session_avoiding_executor(cp_idx);
            components::cursor::cursor_t_ptr rd_cur;
            std::thread reader([&] { rd_cur = d->execute_sql(rd_session, probe); });

            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            guard.gate.released.store(true, std::memory_order_release);
            reader.join();
            checkpointer.join();
            guard.gate.reset();

            REQUIRE(cp_cur->is_success());
            auto o = classify(rd_cur);
            const char* shape = "clean";
            if (!o.success) {
                shape = "failed";
            } else if (o.rows == 0) {
                shape = "MISSING";
                ++missing;
                ++hits;
            } else if (o.rows > 1) {
                shape = "EXTRA";
                ++extra;
                ++hits;
            } else if (o.got != p) {
                shape = "WRONG";
                ++wrong;
                ++hits;
            } else {
                ++clean;
            }
            std::fprintf(stderr,
                         "[seam delay=%dms] attempt=%d shape=%s rows=%zu got=%lld expected=%lld "
                         "rewritten=%llu deferred=%llu\n",
                         delay,
                         attempt,
                         shape,
                         o.rows,
                         static_cast<long long>(o.got),
                         static_cast<long long>(p),
                         static_cast<unsigned long long>(services::disk::checkpoint_entries_rewritten()),
                         static_cast<unsigned long long>(services::disk::checkpoint_entries_deferred()));

            // CONTROL: the table itself is right either way.
            {
                auto ctl = classify(exec(d, control_sql(p)));
                REQUIRE(ctl.success);
                REQUIRE(ctl.rows == 1);
                REQUIRE(ctl.got == 10 * p);
            }
            // The rebuilt index must answer correctly once the round finished.
            {
                auto post = classify(exec(d, probe));
                CHECK(post.success);
                CHECK(post.rows == 1);
                CHECK(post.got == p);
            }
        }
        std::fprintf(stderr,
                     "[seam delay=%dms] SUMMARY hits=%d/%d wrong=%d missing=%d extra=%d clean=%d\n",
                     delay,
                     hits,
                     kAttempts,
                     wrong,
                     missing,
                     extra,
                     clean);
    }
}
