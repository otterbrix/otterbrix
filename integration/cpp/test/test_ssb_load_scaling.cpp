#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <services/disk/agent_disk.hpp>
#include <sstream>
#include <string>
#include <vector>

// Loading SSB's lineorder must cost the same per row at the end as at the start.
//
// It once took 8h38m without loading the table and without a single error: disk-growth throughput
// fell from 190 MB/min to 6.5, and 55 MB of source CSV turned into 12 GB on disk. It was not the
// insert path — there are no checkpoints during a file's load (csv_checkpoint_interval_bytes
// defaults to 0), and the table has no primary key and no index, so neither the per-batch
// unique-constraint scan nor index maintenance could be the cause. It was one checkpoint round per
// COMMIT (see the round count checked at the end of measure_lineorder_load).
//
// This loads the real lineorder.tbl the same way the benchmark runner does — batched multi-row
// INSERT through execute_sql — and reports wall time and on-disk bytes per slice, so a cost that
// grows with the rows already loaded shows up as a curve rather than as a slow run.
//
// Hidden by default ([.]) and needs benchmark/data/ssb/lineorder.tbl. Run it with [ssbload].

namespace {
    constexpr int kSlice = 50000;
    constexpr int kSlices = 13; // 650k > the 600 598 rows of lineorder

    uint64_t directory_bytes(const std::filesystem::path& root) {
        std::error_code ec;
        if (!std::filesystem::exists(root, ec)) {
            return 0;
        }
        uint64_t total = 0;
        for (std::filesystem::recursive_directory_iterator it(root, ec), end; it != end; it.increment(ec)) {
            if (ec) {
                break;
            }
            if (it->is_regular_file(ec)) {
                total += it->file_size(ec);
            }
        }
        return total;
    }

    // lineorder.tbl is pipe-delimited with a trailing separator; columns 6, 7 and 16 are text.
    std::string tuple_from_line(const std::string& line) {
        std::stringstream ss(line);
        std::string field;
        std::vector<std::string> fields;
        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }
        if (fields.size() < 17) {
            return {};
        }
        static const bool is_text[17] = {false,
                                         false,
                                         false,
                                         false,
                                         false,
                                         false,
                                         true,
                                         true,
                                         false,
                                         false,
                                         false,
                                         false,
                                         false,
                                         false,
                                         false,
                                         false,
                                         true};
        std::string out = "(";
        for (int i = 0; i < 17; ++i) {
            if (i != 0) {
                out += ", ";
            }
            if (is_text[i]) {
                out += "'" + fields[static_cast<size_t>(i)] + "'";
            } else {
                out += fields[static_cast<size_t>(i)];
            }
        }
        return out + ")";
    }
} // namespace

// Capacity probe: does the engine hold an SF=1-sized lineorder at all?
//
// The repo ships ~600k rows, roughly SF 0.1. The standard SSB scale factor 1 is ten times that, and
// the buffer pool's ceiling is a hardwired 4 GiB, so "SSB passes" on the shipped data says nothing
// about the real one. This reloads the shipped file pass after pass and demands at least SF 1's worth
// of rows — the keys repeat, which does not matter here because what is being tested is volume, not
// answers.
//
// Hidden ([.]) and slow. Run it with [ssbcapacity].
namespace {
    void probe_capacity(const std::filesystem::path& root, bool wal_on);
} // namespace

TEST_CASE("integration::cpp::test_ssb_load_scaling::lineorder_at_scale_factor_one", "[.][ssbcapacity]") {
    probe_capacity(integration_fixture_path("test_ssb/capacity"), true);
}

// The same volume with the WAL OFF, where there are no auto-checkpoints at all: nothing reaches the
// .otbx during the load, so the only thing that can reclaim buffer memory is spilling the transient
// column segments (can_unload refuses a block with no disk copy). If that path regresses, this case
// runs out of memory where the WAL-on one does not.
TEST_CASE("integration::cpp::test_ssb_load_scaling::capacity_without_wal", "[.][ssbcapacity]") {
    probe_capacity(integration_fixture_path("test_ssb/capacity_nowal"), false);
}

namespace {
    void probe_capacity(const std::filesystem::path& root, bool wal_on) {
        const std::filesystem::path source =
            std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path() / "benchmark" /
            "data" / "ssb" / "lineorder.tbl";
        REQUIRE(std::filesystem::exists(source));

        auto config = test_create_config(root);
        test_clear_directory(config);
        config.wal.on = wal_on;
        config.log.level = log_t::level::off;
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE ssb;")->is_success());
        REQUIRE(exec("CREATE TABLE ssb.lineorder ("
                     "lo_orderkey bigint, lo_linenumber bigint, lo_custkey bigint, lo_partkey bigint, "
                     "lo_suppkey bigint, lo_orderdate bigint, lo_orderpriority text, lo_shippriority text, "
                     "lo_quantity bigint, lo_extendedprice bigint, lo_ordtotalprice bigint, lo_discount bigint, "
                     "lo_revenue bigint, lo_supplycost bigint, lo_tax bigint, lo_commitdate bigint, "
                     "lo_shipmode text);")
                    ->is_success());

        static const std::string kColumns =
            "(lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, "
            "lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, "
            "lo_supplycost, lo_tax, lo_commitdate, lo_shipmode)";

        uint64_t rows_total = 0;
        std::string failure;
        const auto start = std::chrono::steady_clock::now();
        for (int pass = 0; pass < 70 && failure.empty(); ++pass) {
            std::ifstream file(source);
            REQUIRE(file.is_open());
            std::string line;
            std::getline(file, line); // header
            while (failure.empty()) {
                std::string values;
                int in_batch = 0;
                while (in_batch < 1000 && std::getline(file, line)) {
                    auto tuple = tuple_from_line(line);
                    if (tuple.empty()) {
                        continue;
                    }
                    if (in_batch != 0) {
                        values += ", ";
                    }
                    values += tuple;
                    ++in_batch;
                }
                if (in_batch == 0) {
                    break;
                }
                auto cur = exec("INSERT INTO ssb.lineorder " + kColumns + " VALUES " + values + ";");
                if (cur->is_error()) {
                    failure = cur->get_error().what;
                    break;
                }
                rows_total += static_cast<uint64_t>(in_batch);
            }
            WARN("after pass " << (pass + 1) << ": " << rows_total << " rows, on disk "
                               << (directory_bytes(root) / (1024 * 1024)) << " MiB");
        }
        const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        WARN("loaded " << rows_total << " rows in " << elapsed
                       << " s; failure: " << (failure.empty() ? std::string{"none"} : failure));
        CHECK(failure.empty());
        CHECK(rows_total >= 6000000);
        WARN("ceiling probe: stopped at " << rows_total << " rows");
    }
} // namespace

namespace {
    // The measurement body, shared by the two configurations below. The benchmark runner uses
    // batches of 100 rows and turns the WAL on; the first case here deliberately differs on both so
    // the two can be compared.
    void measure_lineorder_load(const std::filesystem::path& root, int batch, bool wal_on, const char* label);
} // namespace

TEST_CASE("integration::cpp::test_ssb_load_scaling::lineorder_cost_per_slice", "[.][ssbload]") {
    measure_lineorder_load(integration_fixture_path("test_ssb/load_batch1000_nowal"),
                           1000,
                           false,
                           "batch 1000, WAL off");
}

// The benchmark runner's exact shape: 100-row batches and the WAL enabled. If this one degrades
// while the case above stays flat, the cost is not in the insert path at all.
TEST_CASE("integration::cpp::test_ssb_load_scaling::lineorder_as_the_runner_loads_it", "[.][ssbload]") {
    measure_lineorder_load(integration_fixture_path("test_ssb/load_batch100_wal"), 100, true, "batch 100, WAL on");
}

namespace {
    void measure_lineorder_load(const std::filesystem::path& root, int batch, bool wal_on, const char* label) {
        const std::filesystem::path source =
            std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path() / "benchmark" /
            "data" / "ssb" / "lineorder.tbl";
        std::ifstream file(source);
        REQUIRE(file.is_open());
        std::string header;
        std::getline(file, header); // column names

        auto config = test_create_config(root);
        test_clear_directory(config);
        config.wal.on = wal_on;
        // The auto-checkpoint threshold is left at its production default on purpose: the defect this
        // guards against — the counter holding the WHOLE WAL directory size instead of the bytes written
        // since the last checkpoint, so the threshold stayed tripped forever once crossed — only shows
        // up against the real threshold. The round count is checked at the end of this function.
        config.log.level = log_t::level::off;
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE ssb;")->is_success());
        REQUIRE(exec("CREATE TABLE ssb.lineorder ("
                     "lo_orderkey bigint, lo_linenumber bigint, lo_custkey bigint, lo_partkey bigint, "
                     "lo_suppkey bigint, lo_orderdate bigint, lo_orderpriority text, lo_shippriority text, "
                     "lo_quantity bigint, lo_extendedprice bigint, lo_ordtotalprice bigint, lo_discount bigint, "
                     "lo_revenue bigint, lo_supplycost bigint, lo_tax bigint, lo_commitdate bigint, "
                     "lo_shipmode text);")
                    ->is_success());

        static const std::string kColumns =
            "(lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, "
            "lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, "
            "lo_supplycost, lo_tax, lo_commitdate, lo_shipmode)";

        std::vector<double> slice_ms;
        std::vector<uint64_t> slice_bytes;
        std::vector<uint64_t> slice_checkpoints;

        std::string line;
        bool exhausted = false;
        uint64_t rows_total = 0;
        uint64_t failed_at_row = 0;
        for (int slice = 0; slice < kSlices && !exhausted; ++slice) {
            services::disk::reset_table_checkpoints();
            const auto slice_start = std::chrono::steady_clock::now();
            int rows_in_slice = 0;
            while (rows_in_slice < kSlice) {
                std::string values;
                int in_batch = 0;
                while (in_batch < batch && std::getline(file, line)) {
                    auto tuple = tuple_from_line(line);
                    if (tuple.empty()) {
                        continue;
                    }
                    if (in_batch != 0) {
                        values += ", ";
                    }
                    values += tuple;
                    ++in_batch;
                }
                if (in_batch == 0) {
                    exhausted = true;
                    break;
                }
                auto cur = exec("INSERT INTO ssb.lineorder " + kColumns + " VALUES " + values + ";");
                if (!cur->is_error()) {
                    rows_in_slice += in_batch;
                    rows_total += static_cast<uint64_t>(in_batch);
                    continue;
                }
                WARN("INSERT failed after " << rows_total << " rows (slice " << (slice + 1)
                                            << "): " << cur->get_error().what);
                failed_at_row = rows_total;
                exhausted = true;
                break;
            }
            const auto elapsed =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - slice_start).count();
            slice_ms.push_back(elapsed);
            slice_bytes.push_back(directory_bytes(root));
            slice_checkpoints.push_back(services::disk::table_checkpoints());
        }

        WARN("rows loaded in total: " << rows_total);
        CHECK(failed_at_row == 0);
        REQUIRE(slice_ms.size() >= 2);
        WARN(label);
        for (size_t i = 0; i < slice_ms.size(); ++i) {
            WARN("  slice " << (i + 1) << ": " << slice_ms[i] << " ms, on disk " << (slice_bytes[i] / (1024 * 1024))
                            << " MiB, checkpoint rounds " << slice_checkpoints[i]);
        }
        INFO("first slice " << slice_ms.front() << " ms, last slice " << slice_ms.back() << " ms, ratio "
                            << (slice_ms.back() / slice_ms.front()));

        // Loading the Nth slice must not cost meaningfully more than loading the first: an append does
        // not get harder because rows already exist. The bound is deliberately loose — this is about
        // catching a growth curve, not about defending a constant.
        CHECK(slice_ms.back() < slice_ms.front() * 3.0);

        // The number that explains the curve. A checkpoint round copies every disk table's .otbx file
        // whole, so it may only happen once per auto_checkpoint_threshold_bytes of WAL — this load
        // writes a few times that, so a handful of rounds is expected. One per COMMIT is the defect,
        // and with 100-row batches that is six thousand of them for this many rows.
        uint64_t total_rounds = 0;
        for (auto n : slice_checkpoints) {
            total_rounds += n;
        }
        WARN("total checkpoint rounds during the load: " << total_rounds);
        CHECK(total_rounds <= 24);
    }
} // namespace

// An explicit CHECKPOINT must not switch the automatic one off.
//
// The auto-checkpoint window is measured as (current WAL directory size - size at the last
// checkpoint). An explicit CHECKPOINT truncates the WAL through truncate_before without going
// anywhere near that baseline, so the directory shrinks below it. Until the WAL grows back past the
// stale, larger baseline the window reads zero and no automatic checkpoint fires — the table can
// then run arbitrarily far behind the log. Only the since-last-checkpoint accounting can fail this
// way, so the guard belongs with it.
TEST_CASE("integration::cpp::test_ssb_load_scaling::explicit_checkpoint_does_not_suppress_the_automatic_one",
          "[.][ssbload]") {
    const std::filesystem::path source =
        std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path() / "benchmark" / "data" /
        "ssb" / "lineorder.tbl";
    REQUIRE(std::filesystem::exists(source));

    auto config = test_create_config(integration_fixture_path("test_ssb/explicit_checkpoint"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    // Small enough that a slice of rows crosses it several times over.
    config.wal.auto_checkpoint_threshold_bytes = 2 * 1024 * 1024;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE ssb;")->is_success());
    REQUIRE(exec("CREATE TABLE ssb.lineorder ("
                 "lo_orderkey bigint, lo_linenumber bigint, lo_custkey bigint, lo_partkey bigint, "
                 "lo_suppkey bigint, lo_orderdate bigint, lo_orderpriority text, lo_shippriority text, "
                 "lo_quantity bigint, lo_extendedprice bigint, lo_ordtotalprice bigint, lo_discount bigint, "
                 "lo_revenue bigint, lo_supplycost bigint, lo_tax bigint, lo_commitdate bigint, "
                 "lo_shipmode text);")
                ->is_success());
    static const std::string kColumns =
        "(lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, "
        "lo_shippriority, lo_quantity, lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, "
        "lo_supplycost, lo_tax, lo_commitdate, lo_shipmode)";

    std::ifstream file(source);
    REQUIRE(file.is_open());
    std::string line;
    std::getline(file, line);
    auto load_rows = [&](int rows) {
        int done = 0;
        while (done < rows) {
            std::string values;
            int in_batch = 0;
            while (in_batch < 100 && std::getline(file, line)) {
                auto tuple = tuple_from_line(line);
                if (tuple.empty()) {
                    continue;
                }
                if (in_batch != 0) {
                    values += ", ";
                }
                values += tuple;
                ++in_batch;
            }
            if (in_batch == 0) {
                break;
            }
            REQUIRE(exec("INSERT INTO ssb.lineorder " + kColumns + " VALUES " + values + ";")->is_success());
            done += in_batch;
        }
    };

    // Warm the WAL past the threshold so the automatic checkpoint is demonstrably working.
    services::disk::reset_table_checkpoints();
    load_rows(30000);
    const auto rounds_before = services::disk::table_checkpoints();
    WARN("automatic checkpoint rounds before the explicit one: " << rounds_before);
    REQUIRE(rounds_before > 0); // otherwise the rest of this test proves nothing

    REQUIRE(exec("CHECKPOINT;")->is_success());

    services::disk::reset_table_checkpoints();
    load_rows(30000);
    const auto rounds_after = services::disk::table_checkpoints();
    WARN("automatic checkpoint rounds after the explicit one: " << rounds_after);
    CHECK(rounds_after > 0);
}
