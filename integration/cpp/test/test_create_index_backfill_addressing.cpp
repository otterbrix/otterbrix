#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <unistd.h>

// insert_rows(table_oid) fans a batch to every index on the table, so a backfill re-stages rows already indexed.
// No row assertion catches it: both stores dedup repeated (key, row id) pairs, so the case counts staging messages.

using namespace test_helpers;

namespace {

    // Small: the defect is per-row, so one row group is plenty and the case stays fast.
    constexpr int64_t kRows = 200;

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    // Qualified by pid so two binaries running at once can't open, truncate, or unlink each other's files.
    std::string fixture_root() { return integration_fixture_path("test_create_index_backfill_addressing").string(); }

} // namespace

TEST_CASE("integration::cpp::create_index_backfill_addressing::a_second_build_may_not_restage_the_first_index") {
    auto config = make_test_config(fixture_root() + "/db");
    config.log.level = log_t::level::off;
    // The meter is process-wide and an automatic checkpoint's repopulate_table bumps it too, so the threshold
    // below takes that off the board rather than hoping it won't fire mid-window.
    // Measured: at the 16 MB default this table never trips auto-checkpoint (0 rounds in both windows, six
    // runs); at 1 KB the same two windows see 2 and 4 rounds instead.
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE bdb;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE bdb.t (id bigint, a bigint, b bigint);")->is_success());
    {
        auto cur = seed_rows(d, "bdb.t", "id, a, b", static_cast<unsigned>(kRows), [](unsigned i) {
            const auto id = static_cast<int64_t>(i) + 1;
            return "(" + std::to_string(id) + ", " + std::to_string(id) + ", " + std::to_string(1000 + id) + ")";
        });
        REQUIRE(cur->is_success());
    }

    {
        auto cur = exec(d, "SELECT id FROM bdb.t WHERE id = 7;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // Reset happens before the assert-success check, so a no-op reset can't pass for a build that sent nothing;
    // index_stage_insert_foreign_batches() proves the count below is this manager's alone, not the process's.
    services::disk::reset_table_checkpoints();
    services::index::reset_index_stage_insert_batches();
    REQUIRE(services::index::index_stage_insert_batches() == 0);
    REQUIRE(services::index::index_stage_insert_foreign_batches() == 0);
    REQUIRE(services::disk::table_checkpoints() == 0);
    REQUIRE(exec(d, "CREATE INDEX a_idx ON bdb.t (a);")->is_success());
    const auto staged_by_the_first_build = services::index::index_stage_insert_batches();
    INFO("stage_inserts messages the FIRST build sent: " << staged_by_the_first_build);
    INFO("checkpoint rounds inside the FIRST window (must be 0, or the meter is not the build's): "
         << services::disk::table_checkpoints());
    REQUIRE(services::disk::table_checkpoints() == 0);
    INFO("batches staged by a SECOND manager inside the FIRST window (must be 0, or the number is nobody's): "
         << services::index::index_stage_insert_foreign_batches());
    REQUIRE(services::index::index_stage_insert_foreign_batches() == 0);
    REQUIRE(staged_by_the_first_build > 0);

    {
        auto plan = exec(d, "EXPLAIN SELECT id FROM bdb.t WHERE a = 7;");
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed predicate:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    INFO("one index, one entry per row: the baseline the second build must not disturb");
    {
        auto cur = exec(d, "SELECT id FROM bdb.t WHERE a = 7;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    services::disk::reset_table_checkpoints();
    services::index::reset_index_stage_insert_batches();
    REQUIRE(services::index::index_stage_insert_batches() == 0);
    REQUIRE(services::index::index_stage_insert_foreign_batches() == 0);
    REQUIRE(services::disk::table_checkpoints() == 0);
    REQUIRE(exec(d, "CREATE INDEX b_idx ON bdb.t (b);")->is_success());
    const auto staged_by_the_second_build = services::index::index_stage_insert_batches();
    INFO("checkpoint rounds inside the SECOND window (must be 0, or the meter is not the build's): "
         << services::disk::table_checkpoints());
    REQUIRE(services::disk::table_checkpoints() == 0);
    INFO("batches staged by a SECOND manager inside the SECOND window (must be 0, or the comparison is not "
         "between two builds): "
         << services::index::index_stage_insert_foreign_batches());
    REQUIRE(services::index::index_stage_insert_foreign_batches() == 0);

    // Same table, rows, and scan decomposition, so a build that feeds only the index it is building sends as many
    // staging messages as the first build did.
    INFO("stage_inserts messages the SECOND build sent: " << staged_by_the_second_build << " , the first build sent "
                                                          << staged_by_the_first_build);
    CHECK(staged_by_the_second_build == staged_by_the_first_build);

    {
        auto plan = exec(d, "EXPLAIN SELECT id FROM bdb.t WHERE a = 7;");
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the first index's predicate AFTER the second build:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    INFO("the first index must still name row 7 exactly once");
    {
        auto cur = exec(d, "SELECT id FROM bdb.t WHERE a = 7;");
        REQUIRE(cur->is_success());
        INFO("rows answered through a_idx: " << cur->size() << " , expected 1");
        REQUIRE(cur->size() == 1);
    }

    INFO("and so must every other row -- a per-row defect shows up on all of them");
    for (int64_t probe : {int64_t{1}, int64_t{2}, kRows / 2, kRows - 1, kRows}) {
        auto cur = exec(d, "SELECT id FROM bdb.t WHERE a = " + std::to_string(probe) + ";");
        REQUIRE(cur->is_success());
        INFO("a = " << probe << " answered " << cur->size() << " rows, expected 1");
        CHECK(cur->size() == 1);
    }

    INFO("the new index is correct too -- the fix must not cost the build its own rows");
    {
        auto plan = exec(d, "EXPLAIN SELECT id FROM bdb.t WHERE b = 1007;");
        REQUIRE(plan->is_success());
        REQUIRE(plan_text(plan).find("Index Scan") != std::string::npos);
    }
    {
        auto cur = exec(d, "SELECT id FROM bdb.t WHERE b = 1007;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == 7);
    }

    INFO("the unindexed control: the TABLE never held more than one row for this id");
    {
        auto cur = exec(d, "SELECT id FROM bdb.t WHERE id = 7;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("and the whole table is still exactly as long as it was");
    {
        auto cur = exec(d, "SELECT id FROM bdb.t;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == static_cast<std::size_t>(kRows));
    }
}
