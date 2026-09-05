#include "test_config.hpp"

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

// Where does a disk-backed table's dead-row reclaim happen, and what does a small commit pay
// for it?
//
// These two cases used to answer a narrower question. agent_disk_t::maybe_cleanup_inner rode the
// COMMIT fan-out and asked collection_t::committed_row_count() on every commit to decide whether
// to compact; that call reaches chunk_vector_info::committed_deleted_count, which re-scans all
// 1024 slots of EVERY vector still carrying a committed tombstone. UPDATE here is
// tombstone+append, so a table under a long UPDATE workload accumulates them and every later
// commit, however small, paid for all of them.
//
// That walk is no longer on the write path for a DISK-backed table, which is every user table
// (operator_create_collection_t). Under the split free pool a compact whose header never commits
// cannot RETURN space, only spend it — measured at +2.9 MB per call — so compaction of a disk
// table was made one indivisible unit with the checkpoint that commits it, and
// maybe_cleanup_inner now turns disk entries away. The old probes therefore read a flat zero and
// their positive controls (`REQUIRE(control > 0)`, `REQUIRE(series.back() > 0)`) failed for the
// one reason a positive control exists to catch: the instrument was wired to a path deliberately
// not taken.
//
// Retargeted, with the owner's per-test consent, to measure the reclaim where it now happens.
// Each case asserts BOTH halves, because either alone is satisfiable by a broken engine:
//
//   * the commit pays NOTHING — cleanup_slots_visited() stays 0 across the whole workload no
//     matter how many tombstones earlier statements piled up; it goes red the moment the walk
//     returns to the write path;
//   * the reclaim still HAPPENS — after CHECKPOINT the durable root holds exactly the live rows.
//     Measured with the engine down against a freshly loaded .otbx, the only reading that cannot
//     be produced by in-memory state, and because a checkpoint whose compact was refused defers
//     the whole entry, leaving the OLD row count on disk. This is the new positive control: a
//     number that must move from 200000 to the live count, and that reads wrong rather than zero
//     if the apparatus comes unwired.
//
// Hidden by default ([.]): repeated 200k-row update passes. Run them with [s3cleanup].

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

    // The only user table in these cases: `<main_path>/.../<oid>/table.otbx` with oid past
    // FIRST_USER_OID. Every system catalog sits below it, so the filter finds the user table
    // without the test having to learn its oid.
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

    // Physical rows the DURABLE root stores, read with the engine shut down against a freshly
    // loaded .otbx. A compacted checkpoint writes only live rows; a checkpoint whose compact
    // was refused writes nothing at all and leaves the previous root's count standing. The
    // counted collection copy is scoped to the read — a holder kept past a reclaim keeps block
    // handles alive with it.
    uint64_t durable_row_count(const std::filesystem::path& otbx, std::pmr::memory_resource* resource) {
        services::disk::table_storage_t ts(resource, otbx, std::vector<components::table::column_definition_t>{});
        REQUIRE_FALSE(ts.construction_failed());
        auto collection = ts.table().row_group();
        return collection->total_rows();
    }
} // namespace

TEST_CASE("integration::cpp::test_s3_cleanup_scaling::contiguous_tombstones_reclaimed_at_checkpoint",
          "[.][s3cleanup]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_s3/cleanup");
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

        // The probe statement must be a DELETE, not an INSERT: the cleanup fan-out is gated on
        // the commit carrying base deletes (operator_commit_transaction.cpp skips it for an
        // append-only commit, which produces zero dead rows), so an append-only probe would
        // read zero for the wrong reason — the fan-out never leaves the operator.
        //
        // It deletes one row from the UNTOUCHED tail of the table, so the probe never overlaps
        // the rows the update passes tombstone: what it would pay for is other statements'
        // leftovers.
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

        // The claim under test, in its strict form: a small commit pays for NO tombstone, not
        // even its own. Half a million accumulated tombstones later the write path still walks
        // zero slots, because the decision walk left it entirely.
        for (size_t i = 0; i < series.size(); ++i) {
            INFO("pass " << (i + 1));
            CHECK(series[i] == 0);
        }

        // Contiguous bulk delete: a fully deleted vector collapses into a chunk_constant_info
        // whose committed_deleted_count is O(1), so this shape could never make the old walk
        // grow past the one partially-hit boundary vector. It is still the cheapest way to put
        // a big block of dead rows on the table for the reclaim half below.
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

    // Engine down. The durable root must hold the live rows and nothing else — which is the
    // reclaim the commit-side gate no longer performs, done by the round that can commit it.
    const auto otbx = find_user_table_otbx(config.main_path);
    INFO("user .otbx: " << otbx.string());
    REQUIRE_FALSE(otbx.empty());

    const auto physical = durable_row_count(otbx, &resource);
    INFO("physical rows in the durable root: " << physical << ", live rows: " << live_rows
                                               << ", rows ever inserted: " << kRows);
    CHECK(physical == static_cast<uint64_t>(live_rows));
}

// The case the contiguous one above cannot reach.
//
// Deleting a contiguous range takes whole vectors out at once, and a fully deleted vector is
// recorded as a chunk_constant_info whose committed_deleted_count is O(1) — so the contiguous
// case leaves just the one partially-hit boundary vector behind.
//
// A SCATTERED update leaves every vector partially tombstoned, so every vector keeps a
// chunk_vector_info with any_deleted set, and the old cleanup re-walked all 1024 slots of each
// on every later commit — the shape an OLTP workload updating rows by primary key produces,
// and the worst case the pair was written for. It is also the harder reclaim: no vector can be
// dropped whole, so the checkpoint's compact has to rebuild every one of them.
TEST_CASE("integration::cpp::test_s3_cleanup_scaling::scattered_tombstones_reclaimed_at_checkpoint",
          "[.][s3cleanup]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_s3/cleanup_scattered");
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

        // Same claim, on the shape that used to make it fail: one small commit pays for no
        // tombstone any other statement left behind.
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

    // Five scattered update passes over 200k rows leave a million dead rows behind, none of
    // them in a vector that can be dropped whole. The durable root must carry none of them.
    const auto physical = durable_row_count(otbx, &resource);
    INFO("scattered: physical rows in the durable root: " << physical << ", live rows: " << live_rows);
    CHECK(physical == static_cast<uint64_t>(live_rows));
}

// The corner that matters most in practice: a table WITH an index.
//
// compact() shifts row positions and the index engines hold positional row refs, so
// operator_commit_transaction filters the compact set through manager_index_t::tables_without_indexes
// before sending maybe_cleanup_many. An indexed table is therefore dropped from safe_oids twice
// over — once by that index filter, once by the DISK gate the two cases above describe — so its
// per-commit cleanup cost is zero for two independent reasons, and this case pins the bound that
// holds under either. Its reclaim rides the checkpoint round like every other disk table's, and
// operator_checkpoint_t repopulates the index afterwards precisely because that compact renumbers
// the rows underneath it.
TEST_CASE("integration::cpp::test_s3_cleanup_scaling::indexed_table_cleanup_cost", "[.][s3cleanup]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_s3/cleanup_indexed");
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
