// ===========================================================================
// Focused DML + restart micro-benchmark.
//
// The restart-consistency fixes touch exactly two hot paths:
//   * UPDATE  -- the disk agent now writes the WAL record inside its mailbox
//               handler, and the update carries an extra deep-copy of the chunk.
//   * REPLAY  -- an UPDATE record now replays as an MVCC delete+append instead
//               of an in-place rewrite.
// The stock analytical benchmarks (SSB/TPC-H) are read-only and exercise
// neither. This measures the two paths directly, in wall-clock milliseconds, so
// the branch can be compared against its own baseline commit.
//
// Deterministic and self-contained: no external data, a fixed workload, and a
// data directory taken from RC_DATA_ROOT (kept off tmpfs). Prints one
// "BENCH|<name>|<ms>" line per phase so an A/B script can diff them.
// ===========================================================================

#include "test_config.hpp"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

using clock_type = std::chrono::steady_clock;

namespace {

    double ms_since(clock_type::time_point start) {
        return std::chrono::duration<double, std::milli>(clock_type::now() - start).count();
    }

    void emit(const std::string& name, double ms) {
        std::cout << "BENCH|" << name << "|" << ms << '\n' << std::flush;
    }

    configuration::config bench_config(const std::string& sub) {
        std::string root = "/var/tmp/otterbrix/bench";
        if (const char* env = std::getenv("RC_DATA_ROOT")) {
            root = env;
        }
        auto config = test_create_config(std::filesystem::path(root) / sub);
        test_clear_directory(config);
        return config;
    }

    otterbrix::wrapper_dispatcher_t* run(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = d->execute_sql(otterbrix::session_id_t(), sql);
        if (!cur || cur->is_error()) {
            std::cerr << "bench statement failed: " << sql.substr(0, 80) << " -> "
                      << (cur ? std::string(cur->get_error().what.c_str()) : "null") << '\n';
            std::abort();
        }
        return d;
    }

    // Number of rows / iterations. Small enough to finish in seconds under
    // contention, large enough that the timed loop dominates fixed startup cost.
    constexpr int kRows = 2000;
    constexpr int kUpdatePasses = 20;

    // Time kUpdatePasses full-table UPDATEs on a disk+WAL table, then the reopen
    // that must replay all of them.
    void bench_update_and_replay(const char* label, bool disk) {
        const std::string storage = disk ? " WITH (storage = 'disk')" : "";
        auto config = bench_config(std::string("update_") + label);
        config.disk.on = disk;
        config.wal.on = true;

        // --- phase 1: build, then time the UPDATE storm ---
        {
            test_spaces space(config);
            auto* d = space.dispatcher();
            run(d, "CREATE DATABASE b;");
            run(d, "CREATE TABLE b.t (k bigint, v bigint)" + storage + ";");
            {
                std::stringstream q;
                q << "INSERT INTO b.t (k, v) VALUES ";
                for (int i = 0; i < kRows; ++i) {
                    q << '(' << i << ", " << i << ')' << (i + 1 == kRows ? ";" : ", ");
                }
                run(d, q.str());
            }

            const auto start = clock_type::now();
            for (int pass = 0; pass < kUpdatePasses; ++pass) {
                run(d, "UPDATE b.t SET v = v + 1 WHERE k >= 0;");
            }
            emit(std::string("update.") + label, ms_since(start));
            // Leave the scope WITHOUT an explicit checkpoint on the in-memory run,
            // so the whole UPDATE storm sits in the WAL and the reopen must replay
            // it. On the disk run the destructor checkpoints (that is the realistic
            // clean-shutdown path).
        }

        // --- phase 2: time the reopen (replay of the update storm) ---
        {
            const auto start = clock_type::now();
            test_spaces space(config);
            auto* d = space.dispatcher();
            const double reopen_ms = ms_since(start);
            // Touch the table so a broken reopen would fault here rather than pass
            // silently.
            run(d, "SELECT COUNT(*) FROM b.t;");
            emit(std::string("reopen.") + label, reopen_ms);
        }
    }

} // namespace

int main() {
    components::compute::function_registry_t::reset_default();
    bench_update_and_replay("mem", /*disk=*/false);
    bench_update_and_replay("disk", /*disk=*/true);
    return 0;
}
