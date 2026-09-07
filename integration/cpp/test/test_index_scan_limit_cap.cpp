// Capping the id list before the visibility filter can discard survivors: the index answers a
// SUPERSET, so storage_fetch enforces the cap on VISIBLE rows post-fetch; this file checks the answer, not timing.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>

#include <sstream>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {

    constexpr unsigned kSeedRows = 3000;
    constexpr int64_t kSeedIdBase = 10000;
    // > DEFAULT_VECTOR_CAPACITY (1024): the invisible head fills a whole fetch window.
    constexpr unsigned kLateRows = 1200;
    constexpr int64_t kLateIdBase = 1000;
    constexpr int64_t kLateValBase = 1000000;
    constexpr int64_t kPredicate = 5; // matches every row of both sets
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

    // Order matters here (a PREFIX check); a 0-column drain chunk means an empty source, read as zero rows.
    std::vector<std::pair<int64_t, int64_t>> rows_of(const cursor_t_ptr& cur) {
        std::vector<std::pair<int64_t, int64_t>> out;
        REQUIRE(cur->is_success());
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
    auto config = test_create_config(integration_fixture_path("test_index_scan_limit_cap/prefix"));
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

    auto reader = otterbrix::session_id_t();
    REQUIRE(exec(dispatcher, reader, "BEGIN;")->is_success());

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

// The budget is an upper bound; running out of rows before budget is the ordinary case, not a boundary to mishandle.
TEST_CASE("integration::cpp::index_scan_limit_cap::a_cap_wider_than_the_match_returns_every_row") {
    auto config = test_create_config(integration_fixture_path("test_index_scan_limit_cap/wide"));
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
