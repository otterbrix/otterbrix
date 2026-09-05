#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <unistd.h>

// A CREATE INDEX backfill must feed the index it is building, and nothing else.
//
// operator_create_index_backfill_t streams the base table and hands each batch to the index
// manager through a TABLE-scoped door, manager_index_t::insert_rows(table_oid), whose handler
// fans the batch out over EVERY index registered for the oid -- correct for DML (an INSERT
// must reach every index) but wrong for a backfill, since the rows it reads are already in
// every pre-existing index. A second CREATE INDEX on a non-empty table therefore re-stages
// every row into the first index too, under the build's own transaction and commit -- a
// second full staging, publication and flush per pre-existing index, every time one is added.
//
// No row assertion can see this, which is why the case counts messages instead: both index
// stores dedup a repeated (key, row id) pair on the way in (btree_index_disk_t's bulk append
// writes the pair as the tree's own key; bitcask_index_disk_t::insert_bulk_unchecked forwards
// to insert(), whose first act is that check), so the fan-out costs the extra work and
// answers exactly the same rows. The meter, index_stage_insert_batches(), bumps once per
// index_agent_contract::stage_inserts message the manager sends -- one per index reached.
//
// The comparison is self-calibrating: the same table and rows are backfilled twice, once with
// one index registered and once with two, so the scan's batch/run decomposition is identical
// and the only variable is how many indexes each run was fed to -- an absolute expected
// number would encode the scan's own batching instead.
//
// The EXPLAIN assertion is load-bearing for the answer half: every row assertion below would
// also pass over a full scan, so the case first proves the predicate routes to the index it's
// talking about, and the unindexed control (`WHERE id = ...`) separates "the table holds one
// row" from "the index can find exactly one".

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

    // Fixture roots are qualified by pid: two binaries running at once (two build
    // directories, or ctest -j against a second checkout) must not open, truncate and unlink
    // each other's files. integration_fixture_path.hpp holds this directory's root, the way
    // services/index/tests/index_fixture_path.hpp holds that one's.
    std::string fixture_root() {
        return integration_fixture_path("test_create_index_backfill_addressing").string();
    }

} // namespace

// The failure this meters: over a 200-row table the first build sends 1 staging message and
// the second 2 -- one per index registered on the table -- while every row assertion below
// stays green, which is exactly why the meter is here.
TEST_CASE("integration::cpp::create_index_backfill_addressing::a_second_build_may_not_restage_the_first_index") {
    auto config = make_test_config(fixture_root() + "/db", /*wal_on=*/true);
    config.log.level = log_t::level::off;
    // The meter below is process-wide, so the window it's read over has to be exclusive.
    // g_index_stage_insert_batches (services/index/manager_index.cpp) is bumped from the DML
    // insert/update legs, the CREATE INDEX backfill this case measures, and repopulate_table
    // (the index rebuild an automatic checkpoint drives). The DML legs can't fire since the
    // window is exactly one CREATE INDEX statement; repopulate_table is asynchronous and
    // would count just as legally, so the threshold below takes it off the board rather than
    // hoping against it -- same knob, same reason, as test_index_stale_marker_crash.cpp.
    //
    // Measured, not superstition: at the config default (16 MB) this table never trips the
    // auto-checkpoint (0 rounds in both windows, six runs). Lowered to 1 KB, the same two
    // windows see 2 and 4 rounds -- the meter then counts a checkpoint's repopulate_table
    // beside the build it's supposed to measure. The witness below turns "it didn't happen to
    // fire" into "it provably didn't fire in this window".
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

    // The table's own answer, established with no index in play.
    {
        auto cur = exec(d, "SELECT id FROM bdb.t WHERE id = 7;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // BUILD ONE, with nothing else registered on the table: this is the calibration.
    //
    // Order matters: reset first (so a reset that didn't take can't pass for a build that
    // sent nothing), assert the action succeeded, only then read the meter -- a counter
    // cleared before a failed attempt would be measuring emptiness. The checkpoint-round
    // witness rules out the one other thing that could bump the batch counter and leaves a
    // trace of its own; index_stage_insert_foreign_batches() covers what a second live
    // manager_index_t would not leave a trace of, by counting batches staged by any manager
    // other than the first to stage after the reset -- 0 means the number below is one
    // manager's sends, not the process's.
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

    // THE SECOND BUILD. It reads the same rows and must feed b_idx alone. Same order and
    // the same two witnesses as the first window, so the two numbers are comparable.
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

    // THE POINT. Same table, same rows, same scan decomposition -- so a build that feeds
    // only the index it is building sends the same number of staging messages as the first
    // one did. A build that fans out over the table's index list sends one set per index.
    INFO("stage_inserts messages the SECOND build sent: " << staged_by_the_second_build
                                                          << " , the first build sent "
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
