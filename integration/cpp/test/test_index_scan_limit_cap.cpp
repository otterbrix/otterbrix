// ============================================================================
// THE READ-CAP ON AN INDEX SCAN IS A LIMIT ON ROWS, NOT ON CANDIDATE IDS.
//
// The index answer is a SUPERSET of row ids — manager_index says so — and since
// C4b the point fetch DROPS the rows the reader's snapshot may not see. So the
// LIMIT cap has exactly one correct place: BELOW that filter, over rows that
// survived it. Cutting the id list to `limit` before the fetch can cut away the
// very ids whose rows survive, and answer a LIMIT 7 with three rows.
//
// The cap has now moved from the operator's own emit loop onto the fetch message
// itself (storage_fetch's `limit`), so the agent stops gathering once it has
// handed out `limit` VISIBLE rows — the same post-filter, post-visibility count
// cap full_scan already pushes onto storage_fetch_next_batch. That move is a
// performance change and must not be an answer change, so THIS FILE ASSERTS THE
// ANSWER, not the clock: the capped query must return exactly the uncapped
// query's first `limit` rows, cell for cell.
//
// THE SNAPSHOT IS WHAT MAKES THE CASE BITE, AND THE SIZES ARE CHOSEN, NOT ROUND.
// A reader opens a transaction; a writer then commits rows whose KEYS sort BELOW
// every row the reader can see, so the head of the reader's index answer is
// MORE THAN ONE FETCH WINDOW (1024 ids) of rows its snapshot hides. The first
// window therefore produces ZERO visible rows, and the cap has to survive into
// the second one. That is what separates "budget spent on rows produced" from
// "budget spent on ids requested": the second reads window one, declares the
// budget met, and answers a LIMIT 7 with nothing at all. A head shorter than a
// window would let both spellings pass.
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <sstream>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {

    // Seeded keys start high so every late key sorts BELOW all of them.
    constexpr unsigned kSeedRows = 3000; // ids 10000 .. 12999, val == id
    constexpr int64_t kSeedIdBase = 10000;
    // > DEFAULT_VECTOR_CAPACITY (1024): the invisible head fills a whole fetch window.
    constexpr unsigned kLateRows = 1200; // ids 1000 .. 2199, committed after the snapshot
    constexpr int64_t kLateIdBase = 1000;
    constexpr int64_t kLateValBase = 1000000;
    constexpr int64_t kPredicate = 5; // WHERE id > 5 — matches every row of both sets
    constexpr size_t kLimit = 7;

    cursor_t_ptr
    exec(otterbrix::wrapper_dispatcher_t* dispatcher, otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    size_t col_of(const cursor_t_ptr& cur, const std::string& alias) {
        const auto& chunk = cur->chunks().front();
        for (size_t c = 0; c < chunk.column_count(); ++c) {
            if (std::string(chunk.data[c].type().alias()) == alias) {
                return c;
            }
        }
        FAIL("no column aliased '" << alias << "'");
        return 0;
    }

    // Every returned (id, val) pair IN REPLY ORDER. Order matters here: the claim
    // under test is that one answer is the other's PREFIX, which a set comparison
    // could not distinguish from "the same rows in a different order".
    std::vector<std::pair<int64_t, int64_t>> rows_of(const cursor_t_ptr& cur) {
        std::vector<std::pair<int64_t, int64_t>> out;
        REQUIRE(cur->is_success());
        // A source that produced nothing answers with the 0-column drain chunk (or with no
        // chunk at all). Report that as ZERO ROWS so a short answer fails on the row count
        // the case is about, not on a column lookup inside it.
        if (cur->chunks().empty() || cur->chunks().front().column_count() == 0) {
            return out;
        }
        const auto id_col = col_of(cur, "id");
        const auto val_col = col_of(cur, "val");
        for (const auto& chunk : cur->chunks()) {
            for (size_t r = 0; r < chunk.size(); ++r) {
                out.emplace_back(chunk.get_value<int64_t>(id_col, r), chunk.get_value<int64_t>(val_col, r));
            }
        }
        return out;
    }

} // namespace

TEST_CASE("integration::cpp::index_scan_limit_cap::capped_answer_is_the_uncapped_answer_truncated") {
    auto config = test_create_config("/tmp/test_index_scan_limit_cap/prefix");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE DATABASE LimDb;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE TABLE LimDb.t (id bigint, val bigint);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE INDEX idx_id ON LimDb.t (id);")->is_success());
    }
    {
        std::stringstream q;
        q << "INSERT INTO LimDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < kSeedRows; ++i) {
            const int64_t id = kSeedIdBase + static_cast<int64_t>(i);
            q << "(" << id << ", " << id << ")" << (i + 1 == kSeedRows ? ";" : ", ");
        }
        auto s = otterbrix::session_id_t();
        auto cur = exec(dispatcher, s, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kSeedRows);
    }

    // READER: takes its snapshot BEFORE the head-of-range rows exist.
    auto reader = otterbrix::session_id_t();
    REQUIRE(exec(dispatcher, reader, "BEGIN;")->is_success());

    // WRITER: commits a whole window's worth of keys BELOW the reader's range. The
    // reader's index search still answers with their ids — it answers a superset — and
    // storage_fetch must drop their rows.
    {
        std::stringstream q;
        q << "INSERT INTO LimDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < kLateRows; ++i) {
            q << "(" << (kLateIdBase + i) << ", " << (kLateValBase + i) << ")" << (i + 1 == kLateRows ? ";" : ", ");
        }
        auto writer = otterbrix::session_id_t();
        auto cur = exec(dispatcher, writer, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kLateRows);
    }

    std::stringstream where;
    where << "SELECT id, val FROM LimDb.t WHERE id > " << kPredicate;

    const auto uncapped = rows_of(exec(dispatcher, reader, where.str() + ";"));
    std::stringstream capped_q;
    capped_q << where.str() << " LIMIT " << kLimit << ";";
    const auto capped = rows_of(exec(dispatcher, reader, capped_q.str()));

    INFO("the snapshot hides every late row: none of the reader's rows carries a late val");
    for (const auto& row : uncapped) {
        REQUIRE(row.second < kLateValBase);
    }

    INFO("the uncapped answer is the whole predicate range, seeded rows only");
    REQUIRE(uncapped.size() == kSeedRows);

    INFO("LIMIT means rows, so the cap is met exactly — not shortened by rows the reader cannot see");
    REQUIRE(capped.size() == kLimit);

    INFO("and the capped rows ARE the uncapped rows, truncated: same rows, same order");
    for (size_t i = 0; i < kLimit; ++i) {
        REQUIRE(capped[i].first == uncapped[i].first);
        REQUIRE(capped[i].second == uncapped[i].second);
    }

    REQUIRE(exec(dispatcher, reader, "COMMIT;")->is_success());
}

// A cap LARGER than the matched set must not turn into a short answer either: the
// budget is an upper bound, and running out of rows before running out of budget is
// the ordinary case, not a boundary the cap may mishandle.
TEST_CASE("integration::cpp::index_scan_limit_cap::a_cap_wider_than_the_match_returns_every_row") {
    auto config = test_create_config("/tmp/test_index_scan_limit_cap/wide");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE DATABASE LimDb;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE TABLE LimDb.t (id bigint, val bigint);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, "CREATE INDEX idx_id ON LimDb.t (id);")->is_success());
    }
    {
        std::stringstream q;
        q << "INSERT INTO LimDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < 50; ++i) {
            q << "(" << i << ", " << i << ")" << (i + 1 == 50 ? ";" : ", ");
        }
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(dispatcher, s, q.str())->is_success());
    }

    auto s = otterbrix::session_id_t();
    const auto all = rows_of(exec(dispatcher, s, "SELECT id, val FROM LimDb.t WHERE id > 40;"));
    const auto wide = rows_of(exec(dispatcher, s, "SELECT id, val FROM LimDb.t WHERE id > 40 LIMIT 1000;"));
    REQUIRE(all.size() == 9);
    REQUIRE(wide.size() == all.size());
    for (size_t i = 0; i < all.size(); ++i) {
        REQUIRE(wide[i].first == all[i].first);
        REQUIRE(wide[i].second == all[i].second);
    }
}
