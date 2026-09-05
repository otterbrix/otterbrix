#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/index/manager_index.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <unistd.h>

// A CREATE INDEX BACKFILL MUST FEED THE INDEX IT IS BUILDING, AND NOTHING ELSE.
//
// operator_create_index_backfill_t streams the base table and hands each batch to the index
// manager. The door it used was TABLE-scoped -- manager_index_t::insert_rows(table_oid) --
// and that handler fans the batch out over EVERY index record registered for the oid
// (`for (const auto& record : it->second)`), because that is the right shape for DML: an
// INSERT must reach every index of the table. A BACKFILL is the opposite case. The rows it
// reads are already in every PRE-EXISTING index; only the index being built is missing them.
//
// So a second CREATE INDEX on a non-empty table re-stages every row of the table into the
// FIRST index as well, under the build's transaction, and the build's commit publishes them
// there too -- a second full staging, a second full publication and a second full flush of a
// table the index already held, per pre-existing index, every time an index is added.
//
// NO ROW ASSERTION CAN SEE THIS, AND THAT IS WHY THE CASE COUNTS MESSAGES. Both stores
// dedup a repeated (key, row id) PAIR on the way in -- btree_index_disk_t's bulk append
// writes the pair as the tree's own key, and bitcask_index_disk_t::insert_bulk_unchecked
// forwards to insert(), whose first act is that check -- so the fan-out costs a full extra
// staging and publication of the whole table per pre-existing index and then answers
// exactly the same rows. The meter is index_stage_insert_batches(): one bump per
// index_agent_contract::stage_inserts message the manager sends, i.e. one per INDEX a batch
// reached.
//
// THE COMPARISON IS SELF-CALIBRATING: the same table and the same rows are backfilled
// twice, once with one index registered and once with two, so the batch/run decomposition
// of the scan is identical and the only variable is how many indexes each run was fed to.
// An absolute expected number would encode the scan's batching instead.
//
// THE EXPLAIN ASSERTION IS LOAD-BEARING for the answer half. Every row assertion below
// would also pass over a full scan, so the case first proves the predicate is routed to the
// index it is talking about; the unindexed control (`WHERE id = ...`, id carries no index)
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
    // each other's files. Mirrors services/index/tests/index_fixture_path.hpp.
    std::string fixture_root() {
        return "/tmp/otterbrix/integration/test_create_index_backfill_addressing_" +
               std::to_string(static_cast<long>(::getpid()));
    }

} // namespace

// RED before the fix: over a 200-row table the first build sent 1 staging message and the
// second sent 2 -- one per index registered on the table -- while every row assertion below
// stayed green, which is exactly why the meter is here.
TEST_CASE("integration::cpp::create_index_backfill_addressing::a_second_build_may_not_restage_the_first_index") {
    auto config = make_test_config(fixture_root() + "/db", /*wal_on=*/true);
    config.log.level = log_t::level::off;

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
    services::index::reset_index_stage_insert_batches();
    REQUIRE(exec(d, "CREATE INDEX a_idx ON bdb.t (a);")->is_success());
    const auto staged_by_the_first_build = services::index::index_stage_insert_batches();
    INFO("stage_inserts messages the FIRST build sent: " << staged_by_the_first_build);
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

    // THE SECOND BUILD. It reads the same rows and must feed b_idx alone.
    services::index::reset_index_stage_insert_batches();
    REQUIRE(exec(d, "CREATE INDEX b_idx ON bdb.t (b);")->is_success());
    const auto staged_by_the_second_build = services::index::index_stage_insert_batches();

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
