#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <string>

// B0: insert cost must not depend on table size.
//
// agent_disk_t::storage_append_inner used to carry a "dedup" stage inherited from the
// pre-#460 document store: when the incoming chunk had a column aliased `_id` and the
// table was non-empty, it materialized the ENTIRE table into one chunk (storage_t::scan
// with limit -1), built a std::unordered_set<std::string> with one heap string per
// existing row, and silently DROPPED incoming rows whose `_id` already existed — while
// the statement still reported success. That made every insert batch O(table rows)
// (measured 24.2x per-row cost between a 1k-row and a 100k-row table), lost rows
// without an error (rule 6), and MASKED a declared UNIQUE/PRIMARY KEY on a column
// named `_id`: the duplicate row was filtered before the append, so
// operator_unique_constraint_t's existing-row scan found only one match and let the
// statement succeed where the same statement on any other column name fails loudly.
//
// The fix removes the stage. Uniqueness has exactly one implementation —
// operator_unique_constraint_t over DECLARED constraints — and `_id` is an ordinary
// column name. These tests pin all four faces of that:
//   1. work: one insert batch streams a table-size-independent number of rows
//      (counted via table_scan_rows_streamed, not wall-clock — Debug builds time badly);
//   2. no constraint: duplicate `_id` values are ordinary duplicates — kept, counted;
//   3. declared PK on `_id`: the duplicate statement FAILS and the row is not kept;
//   4. restart: the PK enforcement holds after close+reopen with no in-process state
//      to rebuild (the operator's existing-row check scans storage, which reloads).

using namespace test_helpers;

namespace {
    constexpr unsigned kSmallRows = 1'000;
    constexpr unsigned kLargeRows = 100'000;
    constexpr unsigned kFillBatch = 1'000;
    constexpr unsigned kProbeRows = 100;

    void fill_rows(otterbrix::wrapper_dispatcher_t* d,
                   const std::string& table,
                   const std::string& prefix,
                   unsigned from,
                   unsigned to) {
        for (unsigned base = from; base < to; base += kFillBatch) {
            const unsigned n = std::min(kFillBatch, to - base);
            auto cur = seed_rows(d, table, "_id, v", n, [&](unsigned i) {
                return "('" + prefix + std::to_string(base + i) + "', " + std::to_string(base + i) + ")";
            });
            REQUIRE(cur->is_success());
        }
    }

    // Rows streamed out of data_table_t::scan by ONE probe INSERT of kProbeRows rows.
    uint64_t probe_scan_rows(otterbrix::wrapper_dispatcher_t* d, const std::string& table, const std::string& prefix) {
        components::table::reset_table_scan_rows_streamed();
        auto cur = seed_rows(d, table, "_id, v", kProbeRows, [&](unsigned i) {
            return "('" + prefix + std::to_string(i) + "', " + std::to_string(i) + ")";
        });
        REQUIRE(cur->is_success());
        return components::table::table_scan_rows_streamed();
    }
} // namespace

// ---------------------------------------------------------------------------
// (1) Structural criterion: the rows an insert batch READS do not grow with the
//     rows already stored. Counted work, not wall-clock.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_insert_scaling::insert_scan_cost_does_not_grow_with_table_size") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_insert_scaling/scan_cost", /*disk_on=*/true);
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE B0;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE B0.items (_id text, v bigint);")->is_success());

    fill_rows(d, "B0.items", "small_", 0, kSmallRows);
    const auto small_probe = probe_scan_rows(d, "B0.items", "probe_a_");

    fill_rows(d, "B0.items", "big_", 0, kLargeRows - kSmallRows - kProbeRows);
    const auto large_probe = probe_scan_rows(d, "B0.items", "probe_b_");

    INFO("rows streamed by one " << kProbeRows << "-row INSERT into a ~" << kSmallRows << "-row table: "
                                 << small_probe);
    INFO("rows streamed by one " << kProbeRows << "-row INSERT into a ~" << kLargeRows << "-row table: "
                                 << large_probe);
    // Equal work within one probe batch of slack. The old dedup streamed the whole
    // table here (small_probe ~ 1k, large_probe ~ 100k); an insert that reads
    // O(table) rows cannot satisfy this at any constant.
    REQUIRE(large_probe <= small_probe + kProbeRows);
}

// ---------------------------------------------------------------------------
// (2) `_id` without a declared constraint is an ordinary column: a duplicate
//     value is kept, and the caller is told exactly what was inserted.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_insert_scaling::duplicate_id_without_constraint_is_kept") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_insert_scaling/no_constraint", /*disk_on=*/true);
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE B0;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE B0.docs (_id text, v bigint);")->is_success());
    {
        auto cur = exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('k', 1);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("the duplicate _id row is inserted and reported, not silently dropped");
    {
        auto cur = exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('k', 2);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1); // the old dedup reported 0 affected rows here
    }
    {
        auto cur = exec(d, "SELECT * FROM B0.docs WHERE _id = 'k';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2); // the old dedup left 1: the second row vanished
    }
}

// ---------------------------------------------------------------------------
// (3) A DECLARED PRIMARY KEY on `_id` is enforced the one canonical way: the
//     duplicate statement fails loudly and the row is not kept. (The old dedup
//     dropped the row BEFORE the append, so the constraint operator's
//     existing-row scan saw no duplicate and the statement succeeded.)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_insert_scaling::duplicate_id_with_primary_key_fails_loud") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_insert_scaling/pk", /*disk_on=*/true);
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE B0;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE B0.docs (_id text, v bigint);")->is_success());
    REQUIRE(exec(d, "ALTER TABLE B0.docs ADD CONSTRAINT pk_docs_id PRIMARY KEY (_id);")->is_success());
    REQUIRE(exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('k', 1);")->is_success());

    INFO("duplicate key against the existing row is REJECTED, not silently skipped");
    {
        auto cur = exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('k', 2);");
        REQUIRE(cur->is_error());
    }
    {
        auto cur = exec(d, "SELECT * FROM B0.docs;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("distinct key still accepted");
    REQUIRE(exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('m', 3);")->is_success());
}

// ---------------------------------------------------------------------------
// (4) The PK enforcement survives a restart. There is no in-process id set to
//     rebuild: the constraint's existing-row check reads the reloaded storage,
//     so close + reopen must reject the same duplicate it rejected before.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_insert_scaling::duplicate_id_rejection_survives_restart") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_insert_scaling/restart",
                                   /*disk_on=*/true,
                                   /*wal_on=*/true);

    INFO("phase 1: table with PRIMARY KEY(_id), two rows, duplicate rejected");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE B0;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE B0.docs (_id text, v bigint);")->is_success());
        REQUIRE(exec(d, "ALTER TABLE B0.docs ADD CONSTRAINT pk_docs_id PRIMARY KEY (_id);")->is_success());
        REQUIRE(exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('a', 1), ('b', 2);")->is_success());
        REQUIRE(exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('a', 9);")->is_error());
    }

    INFO("phase 2: reopen — the SAME duplicate is rejected the SAME way");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        {
            auto cur = exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('a', 9);");
            REQUIRE(cur->is_error());
        }
        {
            auto cur = exec(d, "SELECT * FROM B0.docs;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        REQUIRE(exec(d, "INSERT INTO B0.docs (_id, v) VALUES ('c', 3);")->is_success());
        {
            auto cur = exec(d, "SELECT * FROM B0.docs;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
    }
}
