#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <unistd.h>

// A CREATE INDEX BACKFILL MUST FEED THE INDEX IT IS BUILDING, AND NOTHING ELSE.
//
// operator_create_index_backfill_t streams the base table and hands each batch to the index
// manager. The door it used was TABLE-scoped -- manager_index_t::insert_rows(table_oid) --
// and that handler fans the batch out over EVERY index registered for the oid
// (`for (const auto& record : it->second)`), which is the right shape for DML: an INSERT
// must reach every index of the table. A BACKFILL is the opposite case, since the rows it
// reads are already in every PRE-EXISTING index. So a second CREATE INDEX on a non-empty
// table re-stages every row into the FIRST index as well, under the build's transaction,
// and the build's commit publishes them there too -- a second full staging, publication and
// flush of a table the index already held, per pre-existing index, every time one is added.
//
// NO ROW ASSERTION CAN SEE THIS, AND THAT IS WHY THE CASE COUNTS MESSAGES. Both stores dedup
// a repeated (key, row id) PAIR on the way in -- btree_index_disk_t's bulk append writes the
// pair as the tree's own key, and bitcask_index_disk_t::insert_bulk_unchecked forwards to
// insert(), whose first act is that check -- so the fan-out costs the extra work and then
// answers exactly the same rows. The meter is index_stage_insert_batches(): one bump per
// index_agent_contract::stage_inserts message the manager sends, i.e. one per INDEX a batch
// reached.
//
// THE COMPARISON IS SELF-CALIBRATING: the same table and the same rows are backfilled twice,
// once with one index registered and once with two, so the batch/run decomposition of the
// scan is identical and the only variable is how many indexes each run was fed to. An
// absolute expected number would encode the scan's batching instead.
//
// THE EXPLAIN ASSERTION IS LOAD-BEARING for the answer half. Every row assertion below would
// also pass over a full scan, so the case first proves the predicate is routed to the index
// it is talking about; the unindexed control (`WHERE id = ...`, id carries no index)
// separates "the table holds one row" from "the index can find exactly one".

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
    // THE METER BELOW IS PROCESS-WIDE, SO THE WINDOW IT IS READ OVER HAS TO BE EXCLUSIVE.
    // g_index_stage_insert_batches (services/index/manager_index.cpp) is bumped from FOUR
    // places: the DML insert and update legs, the CREATE INDEX backfill this case measures,
    // and repopulate_table -- the index rebuild an automatic checkpoint drives. The first
    // two cannot fire inside the window because the window contains exactly one statement
    // and it is a CREATE INDEX. The fourth is asynchronous and would count as legally as
    // the backfill does, so it is taken off the board here rather than hoped against: a
    // threshold this large is never reached by a 200-row table's log. Same knob, same
    // reason, as test_index_stale_marker_crash.cpp.
    //
    // MEASURED, so the knob is not superstition. At the config default (16 MB) this table
    // never trips the auto-checkpoint and both windows saw 0 rounds in six runs -- the leak
    // is latent, not active. Lower the threshold to 1 KB and the same two windows see 2 and
    // 4 rounds respectively: the meter then counts a checkpoint's repopulate_table beside
    // the build it is supposed to be measuring. The witness below is what turns "it did not
    // happen to fire" into "it provably did not fire in this window".
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
    // THE ORDER IS THE POINT. The reset comes first, the BASE IS PINNED AT ZERO so a reset
    // that did not take cannot pass for a build that sent nothing, the action is asserted
    // SUCCESSFUL, and only then is the meter read: a counter cleared before an attempt that
    // then failed would be measuring emptiness and calling it a measurement. The second
    // meter is the exclusivity witness -- an automatic checkpoint round is the one other
    // thing that could bump the batch counter, and this proves none ran in the window.
    //
    // THE THIRD METER IS THE AUTHORSHIP WITNESS, and it is the one that finally makes the
    // process-wide counter attributable. The checkpoint witness above rules out the one
    // pollutant that leaves a trace of its own; a SECOND LIVE manager_index_t leaves none,
    // and its rounds land in the same total and read exactly like the fan-out this case is
    // hunting. index_stage_insert_foreign_batches() counts the batches staged by any
    // manager other than the first to stage after the reset, so 0 says the number below is
    // one manager's sends rather than the process's.
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
