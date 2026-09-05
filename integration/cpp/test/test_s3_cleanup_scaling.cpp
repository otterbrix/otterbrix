#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/table/collection.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/pmr.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

// Disk tables (agent_disk_t::maybe_cleanup_inner) skip the per-commit deleted-row scan: under
// the split free pool, a compact whose header never commits cannot RETURN space (measured
// +2.9 MB/call), so compaction is now one unit with the checkpoint.
// Hidden by default ([.]): repeated 200k-row update passes. Run with [s3cleanup].

namespace {
    constexpr int kRows = 200000;
    constexpr int kBatch = 1000;
    constexpr int kUpdatedPerPass = 50000;

    void fill(otterbrix::wrapper_dispatcher_t* d, const std::string& table, int rows) {
        for (int base = 0; base < rows; base += kBatch) {
            std::string sql = "INSERT INTO tomb." + table + " (id, v) VALUES ";
            for (int i = 0; i < kBatch; ++i) {
                if (i != 0) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(base + i) + ", 1)";
            }
            sql += ";";
            auto session = otterbrix::session_id_t();
            REQUIRE(d->execute_sql(session, sql)->is_success());
        }
    }

    // oid > FIRST_USER_OID picks the one user table; every system catalog sits below it.
    std::filesystem::path find_user_table_otbx(const std::filesystem::path& root) {
        std::filesystem::path found;
        if (!std::filesystem::exists(root)) {
            return found;
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
            if (!entry.is_regular_file() || entry.path().filename() != "table.otbx") {
                continue;
            }
            const std::string oid_dir = entry.path().parent_path().filename().string();
            char* end = nullptr;
            const unsigned long oid = std::strtoul(oid_dir.c_str(), &end, 10);
            if (end == nullptr || *end != '\0' || oid < components::catalog::FIRST_USER_OID) {
                continue;
            }
            found = entry.path();
        }
        return found;
    }

    // A checkpoint whose compact is refused writes nothing, leaving the previous root's count
    // standing. Collection handle is scoped to this read only -- kept longer, it holds block
    // handles alive past a reclaim.
    uint64_t durable_row_count(const std::filesystem::path& otbx, std::pmr::memory_resource* resource) {
        services::disk::table_storage_t ts(resource, otbx, std::vector<components::table::column_definition_t>{});
        REQUIRE_FALSE(ts.construction_failed());
        auto collection = ts.table().row_group();
        return collection->total_rows();
    }
} // namespace

TEST_CASE("integration::cpp::test_s3_cleanup_scaling::contiguous_tombstones_reclaimed_at_checkpoint",
          "[.][s3cleanup]") {
    auto config = test_create_config(integration_fixture_path("test_s3/cleanup"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;

    core::pmr::otterbrix_resource resource;
    int64_t live_rows = 0;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE tomb;")->is_success());
        REQUIRE(exec("CREATE TABLE tomb.t (id bigint, v bigint);")->is_success());
        fill(d, "t", kRows);

        // Must be a DELETE: an INSERT probe wouldn't trigger the cleanup fan-out at all
        // (operator_commit_transaction.cpp skips it for append-only commits). Deletes from the
        // untouched tail so it never overlaps rows the update passes tombstone.
        int probe_id = kRows - 1;
        auto cleanup_slots_for_one_commit = [&]() {
            components::table::reset_cleanup_slots_visited();
            REQUIRE(exec("DELETE FROM tomb.t WHERE id = " + std::to_string(probe_id--) + ";")->is_success());
            return components::table::cleanup_slots_visited();
        };

        const auto before_any_update = cleanup_slots_for_one_commit();
        INFO("cleanup slots for one commit, before any update pass: " << before_any_update);
        CHECK(before_any_update == 0);

        std::vector<uint64_t> series;
        for (int pass = 1; pass <= 10; ++pass) {
            // UPDATE is tombstone+append: this leaves kUpdatedPerPass committed tombstones
            // behind, and ten passes leave half a million.
            REQUIRE(
                exec("UPDATE tomb.t SET v = v + 1 WHERE id < " + std::to_string(kUpdatedPerPass) + ";")->is_success());
            series.push_back(cleanup_slots_for_one_commit());
        }

        INFO("cleanup slots walked by ONE trivial commit, after each update pass:");
        for (size_t i = 0; i < series.size(); ++i) {
            INFO("  pass " << (i + 1) << ": " << series[i]);
        }

        for (size_t i = 0; i < series.size(); ++i) {
            INFO("pass " << (i + 1));
            CHECK(series[i] == 0);
        }

        // A fully deleted vector collapses into a chunk_constant_info (committed_deleted_count
        // is O(1)) -- cheapest way to put a big block of dead rows on the table.
        REQUIRE(exec("DELETE FROM tomb.t WHERE id >= 100000 AND id < 150000;")->is_success());
        components::table::reset_cleanup_slots_visited();
        REQUIRE(exec("DELETE FROM tomb.t WHERE id = 60000;")->is_success());
        const auto after_bulk = components::table::cleanup_slots_visited();
        INFO("cleanup slots for one commit after 50k rows were deleted: " << after_bulk);
        CHECK(after_bulk == 0);

        {
            auto cur = exec("SELECT count(*) FROM tomb.t;");
            REQUIRE(cur->is_success());
            live_rows = cur->value(0, 0).value<int64_t>();
        }
        INFO("live rows before the checkpoint: " << live_rows);
        REQUIRE(live_rows > 0);
        REQUIRE(live_rows < kRows);

        REQUIRE(exec("CHECKPOINT;")->is_success());
    }

    const auto otbx = find_user_table_otbx(config.main_path);
    INFO("user .otbx: " << otbx.string());
    REQUIRE_FALSE(otbx.empty());

    const auto physical = durable_row_count(otbx, &resource);
    INFO("physical rows in the durable root: " << physical << ", live rows: " << live_rows
                                               << ", rows ever inserted: " << kRows);
    CHECK(physical == static_cast<uint64_t>(live_rows));
}

// SCATTERED complement: every vector keeps some live and some tombstoned rows (any_deleted
// set), so none collapses into the O(1) chunk_constant_info form and none can be dropped
// whole -- the worst case for this reclaim, and the shape an OLTP update-by-key workload makes.
TEST_CASE("integration::cpp::test_s3_cleanup_scaling::scattered_tombstones_reclaimed_at_checkpoint",
          "[.][s3cleanup]") {
    auto config = test_create_config(integration_fixture_path("test_s3/cleanup_scattered"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;

    core::pmr::otterbrix_resource resource;
    int64_t live_rows = 0;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE tomb;")->is_success());
        REQUIRE(exec("CREATE TABLE tomb.t (id bigint, v bigint);")->is_success());
        fill(d, "t", kRows);

        int probe_id = kRows - 1;
        auto cleanup_slots_for_one_commit = [&]() {
            components::table::reset_cleanup_slots_visited();
            REQUIRE(exec("DELETE FROM tomb.t WHERE id = " + std::to_string(probe_id--) + ";")->is_success());
            return components::table::cleanup_slots_visited();
        };

        const auto clean = cleanup_slots_for_one_commit();
        INFO("scattered: cleanup slots before any update pass: " << clean);
        CHECK(clean == 0);

        std::vector<uint64_t> series;
        for (int pass = 1; pass <= 5; ++pass) {
            // Every 4th row: each 1024-row vector keeps ~256 live and ~256 tombstoned rows, so
            // no vector can collapse into the O(1) constant form.
            REQUIRE(exec("UPDATE tomb.t SET v = v + 1 WHERE id % 4 = 0;")->is_success());
            series.push_back(cleanup_slots_for_one_commit());
        }

        INFO("scattered: cleanup slots walked by ONE trivial commit, per pass:");
        for (size_t i = 0; i < series.size(); ++i) {
            INFO("  pass " << (i + 1) << ": " << series[i]);
        }

        // Same claim, worst-case shape.
        for (size_t i = 0; i < series.size(); ++i) {
            INFO("pass " << (i + 1));
            CHECK(series[i] == 0);
        }

        {
            auto cur = exec("SELECT count(*) FROM tomb.t;");
            REQUIRE(cur->is_success());
            live_rows = cur->value(0, 0).value<int64_t>();
        }
        INFO("scattered: live rows before the checkpoint: " << live_rows);
        REQUIRE(live_rows > 0);

        REQUIRE(exec("CHECKPOINT;")->is_success());
    }

    const auto otbx = find_user_table_otbx(config.main_path);
    INFO("user .otbx: " << otbx.string());
    REQUIRE_FALSE(otbx.empty());

    // Five passes leave a million dead rows, none in a droppable-whole vector.
    const auto physical = durable_row_count(otbx, &resource);
    INFO("scattered: physical rows in the durable root: " << physical << ", live rows: " << live_rows);
    CHECK(physical == static_cast<uint64_t>(live_rows));
}

// Indexed table: compact() shifts row positions, so operator_commit_transaction filters the
// compact set through manager_index_t::tables_without_indexes before maybe_cleanup_many --
// dropped from safe_oids for two independent reasons (index filter + disk gate), hence the
// bound below must hold under either.
TEST_CASE("integration::cpp::test_s3_cleanup_scaling::indexed_table_cleanup_cost", "[.][s3cleanup]") {
    auto config = test_create_config(integration_fixture_path("test_s3/cleanup_indexed"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    constexpr int kIndexedRows = 100000;

    REQUIRE(exec("CREATE DATABASE tomb;")->is_success());
    REQUIRE(exec("CREATE TABLE tomb.t (id bigint, v bigint);")->is_success());
    fill(d, "t", kIndexedRows);
    REQUIRE(exec("CREATE INDEX t_id ON tomb.t (id);")->is_success());

    int probe_id = kIndexedRows - 1;
    auto cleanup_slots_for_one_commit = [&]() {
        components::table::reset_cleanup_slots_visited();
        REQUIRE(exec("DELETE FROM tomb.t WHERE id = " + std::to_string(probe_id--) + ";")->is_success());
        return components::table::cleanup_slots_visited();
    };

    std::vector<uint64_t> series;
    series.push_back(cleanup_slots_for_one_commit());
    for (int pass = 1; pass <= 3; ++pass) {
        REQUIRE(exec("UPDATE tomb.t SET v = v + 1 WHERE id % 4 = 0;")->is_success());
        series.push_back(cleanup_slots_for_one_commit());
    }

    INFO("indexed: cleanup slots walked by ONE trivial commit, clean then after each pass:");
    for (size_t i = 0; i < series.size(); ++i) {
        INFO("  step " << i << ": " << series[i]);
    }
    INFO("indexed: first = " << series.front() << ", last = " << series.back());

    CHECK(series.back() <= 4 * components::vector::DEFAULT_VECTOR_CAPACITY);
}
