#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <services/disk/agent_disk.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

// Why does loading SSB's lineorder never finish?
//
// A full run took 8h38m without loading the table and without a single error: throughput fell from
// 190 MB/min of disk growth to 6.5, and 55 MB of source CSV had turned into 12 GB on disk. The
// out-of-memory failure that used to stop the load at 3255 rows is gone, so what is left is
// something superlinear in the insert path itself. There are no checkpoints during a file's load
// (csv_checkpoint_interval_bytes defaults to 0), no primary key and no index on the table, so
// neither the per-batch unique-constraint scan nor index maintenance can be the cause.
//
// This loads the real lineorder.tbl the same way the benchmark runner does — batched multi-row
// INSERT through execute_sql — and reports wall time and on-disk bytes per slice. A flat cost per
// slice means the load is linear and something else explains the 8 hours; a rising one names the
// defect and gives the shape to fix.
//
// Hidden by default ([.]) and needs benchmark/data/ssb/lineorder.tbl. Run it with [ssbload].

namespace {
    constexpr int kBatch = 1000;
    constexpr int kSlice = 25000;
    constexpr int kSlices = 8;

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
        static const bool is_text[17] =
            {false, false, false, false, false, false, true, true, false, false, false, false, false, false, false,
             false, true};
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

namespace {
    // The measurement body, shared by the two configurations below. The benchmark runner uses
    // batches of 100 rows and turns the WAL on; the first case here deliberately differs on both so
    // the two can be compared.
    void measure_lineorder_load(const std::filesystem::path& root, int batch, bool wal_on, const char* label);
} // namespace

TEST_CASE("integration::cpp::test_ssb_load_scaling::lineorder_cost_per_slice", "[.][ssbload]") {
    measure_lineorder_load("/tmp/otterbrix/integration/test_ssb/load_batch1000_nowal", 1000, false,
                           "batch 1000, WAL off");
}

// The benchmark runner's exact shape: 100-row batches and the WAL enabled. If this one degrades
// while the case above stays flat, the cost is not in the insert path at all.
TEST_CASE("integration::cpp::test_ssb_load_scaling::lineorder_as_the_runner_loads_it", "[.][ssbload]") {
    measure_lineorder_load("/tmp/otterbrix/integration/test_ssb/load_batch100_wal", 100, true,
                           "batch 100, WAL on");
}

namespace {
    void measure_lineorder_load(const std::filesystem::path& root, int batch, bool wal_on, const char* label) {
    const std::filesystem::path source = std::filesystem::path(__FILE__)
                                             .parent_path()
                                             .parent_path()
                                             .parent_path()
                                             .parent_path() /
                                         "benchmark" / "data" / "ssb" / "lineorder.tbl";
    std::ifstream file(source);
    REQUIRE(file.is_open());
    std::string header;
    std::getline(file, header); // column names

    auto config = test_create_config(root);
    test_clear_directory(config);
    config.disk.on = true;
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

    std::vector<double> slice_ms;
    std::vector<uint64_t> slice_bytes;
    std::vector<uint64_t> slice_checkpoints;

    std::string line;
    bool exhausted = false;
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
            REQUIRE(cur->is_success());
            rows_in_slice += in_batch;
        }
        const auto elapsed =
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - slice_start).count();
        slice_ms.push_back(elapsed);
        slice_bytes.push_back(directory_bytes(root));
        slice_checkpoints.push_back(services::disk::table_checkpoints());
    }

    REQUIRE(slice_ms.size() >= 4);
    WARN(label);
    for (size_t i = 0; i < slice_ms.size(); ++i) {
        WARN("  slice " << (i + 1) << ": " << slice_ms[i] << " ms, on disk " << (slice_bytes[i] / (1024 * 1024))
                        << " MiB, table checkpoints " << slice_checkpoints[i]);
    }
    INFO("first slice " << slice_ms.front() << " ms, last slice " << slice_ms.back() << " ms, ratio "
                        << (slice_ms.back() / slice_ms.front()));

    // Loading the Nth slice must not cost meaningfully more than loading the first: an append does
    // not get harder because rows already exist. The bound is deliberately loose — this is about
    // catching a growth curve, not about defending a constant.
    CHECK(slice_ms.back() < slice_ms.front() * 3.0);
    }
} // namespace
