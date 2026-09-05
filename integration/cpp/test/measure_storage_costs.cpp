// Storage-cost measurement harness. NOT a test: it asserts nothing and is not
// registered with ctest. It prints numbers; judgement is the reader's.
//
// It takes four numbers:
//
//   1. checkpoint  — a round over 100 tables x 100 rows, dirty, then the EMPTY
//                    round straight after it;
//   2. footprint   — resident set and open file descriptors at 100 tables,
//                    against the same engine before any table exists (measured
//                    inside the same run as (1));
//   3. insert      — per-row cost of one 100-row INSERT into a table carrying a
//                    column named `_id`, at 1k rows and at 100k rows, plus the
//                    preload of 100k with and without that column name;
//   4. index       — 1000 point SELECTs through an index over 100k rows, plus
//                    the preload, the pre-checkpoint and the CREATE INDEX.
//
// Every number is printed per run and summarized as min/median/max across runs.
// No thresholds: a harness that fails a build on a wall-clock number measures
// the machine it happens to be on.
//
// Config: the untouched `test_create_config` default (log level `trace`, what
// every integration test runs at; WAL on). Data goes under
// /tmp/otterbrix/measure_storage_costs/, never the repository.
//
// Build: target `measure_storage_costs` (Debug, for the numbers above to mean
// anything next to their recorded baseline).
// Run:   ./measure_storage_costs [all|checkpoint|insert|index] [runs]

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <mach/mach.h>
#else
#include <sys/resource.h>
#endif

using namespace test_helpers;

namespace {

    // ---- shapes, fixed so the numbers stay comparable across re-takes --------
    constexpr unsigned kTables = 100;         // (1)(2) tables in the checkpoint round
    constexpr unsigned kRowsPerTable = 100;   // (1)(2) rows in each of them
    constexpr unsigned kSmallRows = 1'000;    // (3) small table
    constexpr unsigned kLargeRows = 100'000;  // (3)(4) large table
    constexpr unsigned kPreloadBatch = 1'000; // rows per INSERT while filling
    constexpr unsigned kProbeRows = 100;      // (3) rows in the timed probe INSERT
    constexpr unsigned kQueries = 1'000;      // (4) point SELECTs

    using clock_t_ = std::chrono::steady_clock;

    double ms_since(clock_t_::time_point t0) {
        return std::chrono::duration<double, std::milli>(clock_t_::now() - t0).count();
    }

    // Resident set of THIS process, MB. mach reports the live resident size;
    // getrusage's ru_maxrss is a high-water mark, which is why it is only the
    // fallback — the "at start" sample would otherwise be contaminated by any
    // peak the process had already been through.
    double rss_mb() {
#if defined(__APPLE__)
        mach_task_basic_info info;
        mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
        if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) ==
            KERN_SUCCESS) {
            return static_cast<double>(info.resident_size) / (1024.0 * 1024.0);
        }
        return -1.0;
#else
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) == 0) {
            return static_cast<double>(ru.ru_maxrss) / 1024.0; // Linux reports KB
        }
        return -1.0;
#endif
    }

    // Open descriptors, counted by probing the whole table. The soft limit is
    // the thing this number is read against, so print that too.
    int open_fds() {
        int n = 0;
        const int limit = getdtablesize();
        for (int fd = 0; fd < limit; ++fd) {
            if (fcntl(fd, F_GETFD) != -1) {
                ++n;
            }
        }
        return n;
    }

    // Rule 2/6: a measurement path must not swallow an error. A failed statement
    // makes every number after it meaningless, so it stops the harness loudly.
    void must(const components::cursor::cursor_t_ptr& cur, const std::string& what) {
        if (!cur || cur->is_error()) {
            std::fprintf(stderr,
                         "FATAL: %s failed: %s\n",
                         what.c_str(),
                         cur ? cur->get_error().what.c_str() : "null cursor");
            std::exit(1);
        }
    }

    void must_sql(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        must(exec(d, sql), sql.size() > 120 ? sql.substr(0, 117) + "..." : sql);
    }

    // ---- summary across runs -------------------------------------------------
    struct series_t {
        std::string name;
        std::string unit;
        std::vector<double> v;

        void add(double x) { v.push_back(x); }
    };

    double pick(std::vector<double> s, double q) {
        if (s.empty()) {
            return 0.0;
        }
        std::sort(s.begin(), s.end());
        auto idx = static_cast<size_t>(q * static_cast<double>(s.size()));
        if (idx >= s.size()) {
            idx = s.size() - 1;
        }
        return s[idx];
    }

    double median(std::vector<double> s) {
        if (s.empty()) {
            return 0.0;
        }
        std::sort(s.begin(), s.end());
        const size_t n = s.size();
        return (n % 2 == 1) ? s[n / 2] : 0.5 * (s[n / 2 - 1] + s[n / 2]);
    }

    void report(const std::vector<series_t>& all) {
        std::printf("\n================ SPREAD ACROSS RUNS ================\n");
        for (const auto& s : all) {
            if (s.v.empty()) {
                continue;
            }
            std::printf("%-42s ", (s.name + " [" + s.unit + "]").c_str());
            for (size_t i = 0; i < s.v.size(); ++i) {
                std::printf("%s%.4g", i ? " / " : "", s.v[i]);
            }
            auto sorted = s.v;
            std::sort(sorted.begin(), sorted.end());
            std::printf("   (min %.4g, median %.4g, max %.4g)\n", sorted.front(), median(s.v), sorted.back());
        }
        std::printf("====================================================\n");
    }

    series_t* find_series(std::vector<series_t>& all, const std::string& name, const std::string& unit) {
        for (auto& s : all) {
            if (s.name == name) {
                return &s;
            }
        }
        all.push_back(series_t{name, unit, {}});
        return &all.back();
    }

    void record(std::vector<series_t>& all, const std::string& name, const std::string& unit, double x) {
        find_series(all, name, unit)->add(x);
    }

    // ---- fills ---------------------------------------------------------------
    // One INSERT per kPreloadBatch rows. Returns wall-clock for the whole fill.
    double fill(otterbrix::wrapper_dispatcher_t* d,
                const std::string& table,
                const std::string& key_col,
                const std::string& key_prefix,
                unsigned rows) {
        const auto t0 = clock_t_::now();
        for (unsigned base = 0; base < rows; base += kPreloadBatch) {
            const unsigned n = std::min<unsigned>(kPreloadBatch, rows - base);
            std::string q = "INSERT INTO " + table + " (" + key_col + ", v) VALUES ";
            for (unsigned i = 0; i < n; ++i) {
                const auto id = base + i;
                q += "('" + key_prefix + std::to_string(id) + "', " + std::to_string(id) + ")";
                q += (i + 1 == n) ? ";" : ", ";
            }
            must(exec(d, q), "preload batch into " + table);
        }
        return ms_since(t0);
    }

    // One timed INSERT of kProbeRows rows. Returns ms per row.
    double probe_insert(otterbrix::wrapper_dispatcher_t* d,
                        const std::string& table,
                        const std::string& key_col,
                        const std::string& key_prefix) {
        std::string q = "INSERT INTO " + table + " (" + key_col + ", v) VALUES ";
        for (unsigned i = 0; i < kProbeRows; ++i) {
            q += "('" + key_prefix + std::to_string(i) + "', " + std::to_string(i) + ")";
            q += (i + 1 == kProbeRows) ? ";" : ", ";
        }
        const auto t0 = clock_t_::now();
        auto cur = exec(d, q);
        const double elapsed = ms_since(t0);
        must(cur, "probe INSERT into " + table);
        if (cur->size() != kProbeRows) {
            std::fprintf(stderr,
                         "FATAL: probe INSERT into %s reported %zu rows, expected %u\n",
                         table.c_str(),
                         static_cast<size_t>(cur->size()),
                         kProbeRows);
            std::exit(1);
        }
        return elapsed / static_cast<double>(kProbeRows);
    }

    // ---- (1) checkpoint round + (2) footprint --------------------------------
    void measure_checkpoint_and_footprint(unsigned run, std::vector<series_t>& all) {
        const std::string dir =
            integration_fixture_path("measure_storage_costs/checkpoint_" + std::to_string(run)).string();
        auto config = make_test_config(dir, /*wal_on=*/true);

        test_spaces space(config);
        auto* d = space.dispatcher();

        must_sql(d, "CREATE DATABASE bdb;");
        const double rss_start = rss_mb();
        const int fds_start = open_fds();

        for (unsigned t = 0; t < kTables; ++t) {
            const std::string tbl = "bdb.t" + std::to_string(t);
            must_sql(d, "CREATE TABLE " + tbl + " (id bigint, v bigint);");
            std::string q = "INSERT INTO " + tbl + " (id, v) VALUES ";
            for (unsigned i = 0; i < kRowsPerTable; ++i) {
                q += "(" + std::to_string(i) + ", " + std::to_string(i * 7) + ")";
                q += (i + 1 == kRowsPerTable) ? ";" : ", ";
            }
            must(exec(d, q), "seed " + tbl);
        }

        const double rss_tables = rss_mb();
        const int fds_tables = open_fds();

        const auto t_dirty = clock_t_::now();
        must_sql(d, "CHECKPOINT;");
        const double dirty_ms = ms_since(t_dirty);

        const auto t_empty = clock_t_::now();
        must_sql(d, "CHECKPOINT;");
        const double empty_ms = ms_since(t_empty);

        std::printf("[run %u] checkpoint  dirty %.1f ms   empty %.1f ms\n", run, dirty_ms, empty_ms);
        std::printf("[run %u] footprint   RSS %.1f -> %.1f MB   fds %d -> %d (+%d over %u tables, soft limit %d)\n",
                    run,
                    rss_start,
                    rss_tables,
                    fds_start,
                    fds_tables,
                    fds_tables - fds_start,
                    kTables,
                    getdtablesize());

        record(all, "1. checkpoint round, dirty", "ms", dirty_ms);
        record(all, "1. checkpoint round, EMPTY", "ms", empty_ms);
        record(all, "2. RSS at start", "MB", rss_start);
        record(all, "2. RSS at 100 tables", "MB", rss_tables);
        record(all, "2. fds at start", "count", fds_start);
        record(all, "2. fds at 100 tables", "count", fds_tables);
        record(all, "2. fds added per 100 tables", "count", fds_tables - fds_start);
    }

    // ---- (3) insert cost vs table size ---------------------------------------
    void measure_insert(unsigned run, std::vector<series_t>& all) {
        {
            const std::string dir =
                integration_fixture_path("measure_storage_costs/insert_1k_" + std::to_string(run)).string();
            auto config = make_test_config(dir, /*wal_on=*/true);
            test_spaces space(config);
            auto* d = space.dispatcher();
            must_sql(d, "CREATE DATABASE bdb;");
            must_sql(d, "CREATE TABLE bdb.items (_id text, v bigint);");
            fill(d, "bdb.items", "_id", "k", kSmallRows);
            const double per_row_1k = probe_insert(d, "bdb.items", "_id", "probe_a_");
            std::printf("[run %u] insert      %.4f ms/row into a %u-row table with _id\n", run, per_row_1k, kSmallRows);
            record(all, "3. ms/row, 100-row batch @1k", "ms", per_row_1k);
        }
        {
            const std::string dir =
                integration_fixture_path("measure_storage_costs/insert_100k_" + std::to_string(run)).string();
            auto config = make_test_config(dir, /*wal_on=*/true);
            test_spaces space(config);
            auto* d = space.dispatcher();
            must_sql(d, "CREATE DATABASE bdb;");
            must_sql(d, "CREATE TABLE bdb.items (_id text, v bigint);");
            must_sql(d, "CREATE TABLE bdb.plain (k text, v bigint);");

            const double preload_id = fill(d, "bdb.items", "_id", "k", kLargeRows);
            const double preload_plain = fill(d, "bdb.plain", "k", "k", kLargeRows);
            const double per_row_100k = probe_insert(d, "bdb.items", "_id", "probe_b_");

            std::printf("[run %u] insert      %.4f ms/row into a %u-row table with _id\n",
                        run,
                        per_row_100k,
                        kLargeRows);
            std::printf("[run %u] preload     %u rows with _id %.0f ms   without _id %.0f ms\n",
                        run,
                        kLargeRows,
                        preload_id,
                        preload_plain);

            record(all, "3. ms/row, 100-row batch @100k", "ms", per_row_100k);
            record(all, "3. preload 100k WITH _id", "ms", preload_id);
            record(all, "3. preload 100k WITHOUT _id", "ms", preload_plain);
        }
    }

    // ---- (4) point SELECT through an index -----------------------------------
    void measure_index(unsigned run, std::vector<series_t>& all) {
        const std::string dir = integration_fixture_path("measure_storage_costs/index_" + std::to_string(run)).string();
        auto config = make_test_config(dir, /*wal_on=*/true);
        test_spaces space(config);
        auto* d = space.dispatcher();

        must_sql(d, "CREATE DATABASE bdb;");
        must_sql(d, "CREATE TABLE bdb.items (id bigint, val bigint);");

        const auto t_preload = clock_t_::now();
        for (unsigned base = 0; base < kLargeRows; base += kPreloadBatch) {
            const unsigned n = std::min<unsigned>(kPreloadBatch, kLargeRows - base);
            std::string q = "INSERT INTO bdb.items (id, val) VALUES ";
            for (unsigned i = 0; i < n; ++i) {
                const auto v = base + i;
                q += "(" + std::to_string(v) + ", " + std::to_string(v) + ")";
                q += (i + 1 == n) ? ";" : ", ";
            }
            must(exec(d, q), "index preload batch");
        }
        const double preload_ms = ms_since(t_preload);

        const auto t_cp = clock_t_::now();
        must_sql(d, "CHECKPOINT;");
        const double precheckpoint_ms = ms_since(t_cp);

        const auto t_idx = clock_t_::now();
        must_sql(d, "CREATE INDEX idx_val ON bdb.items (val);");
        const double create_index_ms = ms_since(t_idx);

        // Print the plan once so the reader can see whether the index is used at
        // all. A latency number for a "point SELECT by index" that silently ran a
        // full scan would be a different measurement wearing the same label.
        {
            auto cur = exec(d, "EXPLAIN SELECT * FROM bdb.items WHERE val = 42;");
            must(cur, "EXPLAIN of the point SELECT");
            std::printf("[run %u] plan        ", run);
            if (cur->size() > 0) {
                for (std::size_t r = 0; r < cur->size(); ++r) {
                    // Bind the cell before reading the view: cursor_t::value returns a
                    // temporary, and a string_view into it would dangle at the semicolon.
                    auto cell = cur->value(0, r);
                    const auto line = cell.value<std::string_view>();
                    std::printf("%s%.*s", r ? " | " : "", static_cast<int>(line.size()), line.data());
                }
                std::printf("\n");
            } else {
                std::printf("(EXPLAIN returned no rows)\n");
            }
        }

        std::vector<double> lat;
        lat.reserve(kQueries);
        uint64_t rows_seen = 0;
        uint64_t x = 88172645463325252ull; // xorshift, so the probe order is not a scan
        for (unsigned q = 0; q < kQueries; ++q) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            const auto key = x % kLargeRows;
            const std::string sql = "SELECT * FROM bdb.items WHERE val = " + std::to_string(key) + ";";
            const auto t0 = clock_t_::now();
            auto cur = exec(d, sql);
            lat.push_back(ms_since(t0));
            must(cur, sql);
            rows_seen += cur->size();
        }

        double sum = 0.0;
        for (double v : lat) {
            sum += v;
        }
        const double mean = sum / static_cast<double>(lat.size());

        std::printf("[run %u] preload     %u rows without _id %.0f ms\n", run, kLargeRows, preload_ms);
        std::printf("[run %u] index       pre-checkpoint %.0f ms   CREATE INDEX %.0f ms\n",
                    run,
                    precheckpoint_ms,
                    create_index_ms);
        std::printf("[run %u] select      mean %.3f  p50 %.3f  p95 %.3f  p99 %.3f ms  (%llu rows over %u queries)\n",
                    run,
                    mean,
                    pick(lat, 0.50),
                    pick(lat, 0.95),
                    pick(lat, 0.99),
                    static_cast<unsigned long long>(rows_seen),
                    kQueries);

        record(all, "4. point SELECT mean", "ms", mean);
        record(all, "4. point SELECT p50", "ms", pick(lat, 0.50));
        record(all, "4. point SELECT p95", "ms", pick(lat, 0.95));
        record(all, "4. point SELECT p99", "ms", pick(lat, 0.99));
        record(all, "4. CREATE INDEX over 100k", "ms", create_index_ms);
        record(all, "4. pre-checkpoint of 100k", "ms", precheckpoint_ms);
        record(all, "4. preload 100k (bigint cols)", "ms", preload_ms);
        record(all, "4. rows returned by 1000 point SELECTs", "count", static_cast<double>(rows_seen));
    }

} // namespace

int main(int argc, char** argv) {
    const std::string which = argc > 1 ? argv[1] : "all";
    const unsigned runs = argc > 2 ? static_cast<unsigned>(std::atoi(argv[2])) : 3u;

    std::printf("measure_storage_costs: which=%s runs=%u  (fd soft limit %d)\n", which.c_str(), runs, getdtablesize());

    std::vector<series_t> all;
    for (unsigned run = 1; run <= runs; ++run) {
        if (which == "all" || which == "checkpoint") {
            measure_checkpoint_and_footprint(run, all);
        }
        if (which == "all" || which == "insert") {
            measure_insert(run, all);
        }
        if (which == "all" || which == "index") {
            measure_index(run, all);
        }
        std::fflush(stdout);
    }
    report(all);
    return 0;
}
