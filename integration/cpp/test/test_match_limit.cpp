// ============================================================================
// operator_match_t + LIMIT — the count-cap on the filter operator.
//
// WHY THIS FILE EXISTS. Nothing else in the suite drives operator_match_t with a
// LIMIT. The comments that claim otherwise are stale: filter pushdown
// (create_plan_match_::is_pure_compare) has since claimed
//   * column-vs-column  (`a < b`)      -> column_column_filter_t on the scan,
//   * LIKE / regexp vs a literal        -> constant_filter_t on the scan,
//   * a bare LIMIT with no WHERE        -> transfer_scan (the match node has no
//                                          expressions at all),
// so every one of those now caps inside the SCAN, never in operator_match.
//
// That is why EVERY case below first PROVES its own routing with an assertion on
// "Filter" (the EXPLAIN label of operator_type::match, renderer_postgres.cpp:52)
// appearing in the plan. Without that proof a test silently exercises the scan's
// cap and stays green no matter what operator_match does with its counter.
//
// THE PREDICATE. `a + 1 <= b` is an ARITHMETIC EXPRESSION compared against a
// COLUMN. is_pure_compare accepts only `col OP const` (A), `col OP col` (B) and
// `expr OP const` (C); expr-vs-col is none of them, so the predicate cannot be
// pushed and lowers to
//      match(streaming) -> full_scan(source, unlimit)
// with the LIMIT carried by operator_match. Seeded so that b is huge on even ids
// and 0 on odd ones, the predicate selects EXACTLY the even-id rows.
//
// WHAT IS PINNED. filter_batch_ counts survivors into the caller-owned running
// total and `break`s the row loop the moment limit_t::check() turns false, so
// out_count is min(survivors in this chunk, remaining budget) and the cap holds
// ACROSS batches. The cases below cover the boundaries that formula can get
// wrong: a cap landing mid-chunk, a cap exactly on a chunk boundary, a cap wider
// than the table, LIMIT 0, a DICTIONARY-vector input (a scan whose MVCC
// selection sliced the columns), and a sparse (projected) input chunk.
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>

using namespace components;
using namespace components::cursor;
using namespace test_helpers;

namespace {

    // >> DEFAULT_VECTOR_CAPACITY (1024): the scan source emits 3 batches, so a cap
    // has somewhere to land mid-chunk AND on a chunk boundary.
    constexpr unsigned kRowCount = 3000;
    // Even ids match `a + 1 <= b`; 1500 of 3000.
    constexpr unsigned kMatches = kRowCount / 2;
    // Matches inside ONE full 1024-row batch (ids 0..1023 -> 512 even).
    constexpr int64_t kMatchesPerChunk = 512;

    const std::string kWhere = "a + 1 <= b";

    // EXPLAIN rows come back as one text column; join them into one blob.
    std::string plan_text(const cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            out += std::string(cur->value(0, r).value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    bool contains(const std::string& hay, const char* needle) { return hay.find(needle) != std::string::npos; }

    // ROUTING PROOF. Assert the plan for `sql` really lowers to operator_match.
    // A test that skips this is testing the scan's cap, not the filter's.
    void require_routes_through_match(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto ex = exec(dispatcher, "EXPLAIN " + sql);
        INFO("EXPLAIN " << sql << " :: " << (ex->is_error() ? std::string(ex->get_error().what) : std::string("ok")));
        REQUIRE(ex->is_success());
        const std::string plan = plan_text(ex);
        INFO("plan:\n" << plan);
        // "Filter" is the label of operator_type::match and of NOTHING else
        // (renderer_postgres.cpp::pg_label is an exhaustive switch, and a scan-side
        // predicate is never rendered as a "Filter:" detail line), so this is an
        // exact proof of routing rather than a substring coincidence.
        REQUIRE(contains(plan, "Filter"));
        REQUIRE(contains(plan, "Seq Scan")); // ... sitting on top of a scan source
    }

    // The negative of the same proof: `sql` does NOT reach operator_match.
    void require_no_match(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto ex = exec(dispatcher, "EXPLAIN " + sql);
        INFO("EXPLAIN " << sql << " :: " << (ex->is_error() ? std::string(ex->get_error().what) : std::string("ok")));
        REQUIRE(ex->is_success());
        const std::string plan = plan_text(ex);
        INFO("plan:\n" << plan);
        REQUIRE_FALSE(contains(plan, "Filter"));
    }

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& table, unsigned rows = kRowCount) {
        REQUIRE(exec(dispatcher, "CREATE TABLE " + table + " (id bigint, a bigint, b bigint, pad bigint);")
                    ->is_success());
        // a = i, b = 1000000 on even i (predicate true) and 0 on odd i (false).
        // pad exists only so a projected SELECT can leave a placeholder column
        // behind and drive the `sparse` path.
        auto cur = seed_rows(dispatcher, table, "id, a, b, pad", rows, [](unsigned i) {
            std::stringstream row;
            row << "(" << i << ", " << i << ", " << (i % 2 == 0 ? 1000000 : 0) << ", " << i << ")";
            return row.str();
        });
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == rows);
    }

    int64_t count_rows(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& table) {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM " + table + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        return static_cast<int64_t>(cur->value(0, 0).value<uint64_t>());
    }

} // namespace

// ---------------------------------------------------------------------------
// (0) THE STALE-COMMENT CONTROL.
//
// Five tests in the suite still describe column-vs-column, LIKE and a bare LIMIT
// as "operator_match" shapes. They are not, and this pins why: each of the three
// is accepted by is_pure_compare (or has no expression at all) and therefore caps
// inside the scan. Anything asserting operator_match's LIMIT through one of these
// queries is measuring the scan. If a future pushdown change hands one of them
// back to operator_match, THIS test goes red and the ones below stop being the
// only coverage.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::match_limit::pushed_shapes_never_reach_the_filter") {
    auto config = make_test_config(test_temp_path("test_match_limit/control"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchLimitDb;")->is_success());
    seed(dispatcher, "MatchLimitDb.t_ctl", 64);
    REQUIRE(exec(dispatcher, "CREATE TABLE MatchLimitDb.t_txt (id bigint, name text);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO MatchLimitDb.t_txt (id, name) VALUES (1, 'match_a'), (2, 'other');")
                ->is_success());

    // (a) column-vs-column -> column_column_filter_t on the scan.
    require_no_match(dispatcher, "SELECT id FROM MatchLimitDb.t_ctl WHERE a < b LIMIT 3;");
    // (b) LIKE against a literal -> constant_filter_t (regex) on the scan.
    require_no_match(dispatcher, "SELECT id FROM MatchLimitDb.t_txt WHERE name LIKE 'match%' LIMIT 1;");
    // (c) a bare LIMIT with no WHERE -> transfer_scan; the match node has no
    //     expressions, so create_plan_match never builds an operator_match at all.
    require_no_match(dispatcher, "SELECT id FROM MatchLimitDb.t_ctl LIMIT 3;");

    // ... whereas the expr-vs-column predicate every case below uses DOES.
    require_routes_through_match(dispatcher, "SELECT id FROM MatchLimitDb.t_ctl WHERE " + kWhere + " LIMIT 3;");
}

// ---------------------------------------------------------------------------
// (1) A cap landing MID-CHUNK on a DML root, over more than 1024 rows.
//
// A DML root has NO operator_limit above it, so operator_match's counter is the
// AUTHORITATIVE affected-row bound — not an advisory read-cap. 700 > the 512
// matches the first 1024-row batch yields, so the cap fires 188 rows into the
// SECOND batch: the `break` must stop that batch mid-way AND the running total
// must have survived the batch boundary to know it should.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::match_limit::dml_cap_lands_mid_chunk") {
    auto config = make_test_config(test_temp_path("test_match_limit/mid_chunk"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchLimitDb;")->is_success());
    seed(dispatcher, "MatchLimitDb.t_mid");

    const std::string sql = "DELETE FROM MatchLimitDb.t_mid WHERE " + kWhere + " LIMIT 700 RETURNING id;";
    require_routes_through_match(dispatcher, sql);

    auto cur = exec(dispatcher, sql);
    INFO("delete: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 700);
    REQUIRE(count_rows(dispatcher, "MatchLimitDb.t_mid") == static_cast<int64_t>(kRowCount) - 700);

    // The cap must not have leaked across the batch boundary as "700 per batch".
    REQUIRE(700 > kMatchesPerChunk);
}

// ---------------------------------------------------------------------------
// (2) A cap EXACTLY on a chunk boundary.
//
// 512 is precisely the number of matches in the first 1024-row batch. The `break`
// fires on the very last surviving row of that batch, so out_count == 512 and the
// SECOND batch must emit nothing at all (the early `!limit_.check()` return).
// An off-by-one in either place shows up here as 511 or 513.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::match_limit::cap_on_chunk_boundary") {
    auto config = make_test_config(test_temp_path("test_match_limit/boundary"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchLimitDb;")->is_success());
    seed(dispatcher, "MatchLimitDb.t_bnd");

    const std::string sql =
        "DELETE FROM MatchLimitDb.t_bnd WHERE " + kWhere + " LIMIT 512 RETURNING id;";
    require_routes_through_match(dispatcher, sql);

    auto cur = exec(dispatcher, sql);
    INFO("delete: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == static_cast<std::size_t>(kMatchesPerChunk));
    REQUIRE(count_rows(dispatcher, "MatchLimitDb.t_bnd") == static_cast<int64_t>(kRowCount) - kMatchesPerChunk);
}

// ---------------------------------------------------------------------------
// (3) A cap LARGER than the table: the LIMIT never fires and every match lands.
// The interesting half is that the running total keeps climbing across all three
// batches without ever tripping check(), i.e. the cap is inert rather than
// truncating at the last batch.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::match_limit::cap_larger_than_table") {
    auto config = make_test_config(test_temp_path("test_match_limit/oversized"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchLimitDb;")->is_success());
    seed(dispatcher, "MatchLimitDb.t_big");

    const std::string sql =
        "DELETE FROM MatchLimitDb.t_big WHERE " + kWhere + " LIMIT 1000000 RETURNING id;";
    require_routes_through_match(dispatcher, sql);

    auto cur = exec(dispatcher, sql);
    INFO("delete: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == kMatches);
    REQUIRE(count_rows(dispatcher, "MatchLimitDb.t_big") == static_cast<int64_t>(kRowCount - kMatches));
}

// ---------------------------------------------------------------------------
// (4) LIMIT 0 — the degenerate cap. limit_t::check(0) is `0 < 0`, false, so
// filter_batch_ returns on its FIRST guard and emits no chunk at all. Nothing may
// be deleted, and the operator must not treat 0 as "unlimited" (the unlimit_
// sentinel is -1, and this is exactly the confusion that rewriting the loop as
// "remaining = limit - total" would introduce outside limit_t).
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::match_limit::limit_zero_emits_nothing") {
    auto config = make_test_config(test_temp_path("test_match_limit/zero"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchLimitDb;")->is_success());
    seed(dispatcher, "MatchLimitDb.t_zero");

    const std::string sql = "DELETE FROM MatchLimitDb.t_zero WHERE " + kWhere + " LIMIT 0 RETURNING id;";
    require_routes_through_match(dispatcher, sql);

    auto cur = exec(dispatcher, sql);
    INFO("delete: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 0);
    REQUIRE(count_rows(dispatcher, "MatchLimitDb.t_zero") == static_cast<int64_t>(kRowCount));
}

// ---------------------------------------------------------------------------
// (5) A DICTIONARY-vector source.
//
// When a scanned vector has invisible (deleted) rows the row_group takes the
// select_committed path, which ends in vector_t::slice() — that STAMPS the result
// column DICTIONARY (vector.cpp:257). operator_match then filters a chunk whose
// columns are not FLAT: the per-cell read has to go through the dictionary's
// indexing, and so does anything that replaces it. Deleting the ids in [0,600)
// leaves 1200 matches; the cap of 300 must count DICTIONARY rows, not slots.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::match_limit::dictionary_source_with_cap") {
    auto config = make_test_config(test_temp_path("test_match_limit/dictionary"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchLimitDb;")->is_success());
    seed(dispatcher, "MatchLimitDb.t_dict");

    // Punch holes so the scan's MVCC selection is not the identity: ids 0..599 go
    // away, which takes 300 matching (even) rows with them.
    {
        auto cur = exec(dispatcher, "DELETE FROM MatchLimitDb.t_dict WHERE id < 600;");
        REQUIRE(cur->is_success());
    }
    REQUIRE(count_rows(dispatcher, "MatchLimitDb.t_dict") == static_cast<int64_t>(kRowCount) - 600);

    const std::string sql = "SELECT id FROM MatchLimitDb.t_dict WHERE " + kWhere + " LIMIT 300;";
    require_routes_through_match(dispatcher, sql);

    auto cur = exec(dispatcher, sql);
    INFO("select: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 300);
    // Every id handed back must be an even id >= 600: a dictionary read that lost
    // its indexing would surface a deleted or odd row here, not merely a wrong count.
    for (std::size_t r = 0; r < cur->size(); ++r) {
        const auto id = cur->value(0, r).value<int64_t>();
        INFO("row " << r << " id " << id);
        REQUIRE(id >= 600);
        REQUIRE(id % 2 == 0);
    }

    // Without the cap the same shape yields all 1200 surviving matches.
    auto all = exec(dispatcher, "SELECT id FROM MatchLimitDb.t_dict WHERE " + kWhere + ";");
    REQUIRE(all->is_success());
    REQUIRE(all->size() == 1200);
}

// ---------------------------------------------------------------------------
// (6) A SPARSE (projected) input chunk together with a cap.
//
// Column pruning hands full_scan a projected_cols list; the un-projected slots
// stay in the chunk as PLACEHOLDERS with no buffer, so operator_match copies only
// populated_cols and builds its output chunk through data_chunk_t's projected
// constructor. `pad` is referenced by nothing here, so it is exactly such a
// placeholder — reading it would crash, which is what makes the sparse flag
// load-bearing rather than cosmetic.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::match_limit::sparse_projection_with_cap") {
    auto config = make_test_config(test_temp_path("test_match_limit/sparse"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE MatchLimitDb;")->is_success());
    seed(dispatcher, "MatchLimitDb.t_sparse");

    // id, a and b are read (a and b by the predicate); pad never is.
    const std::string sql = "SELECT id FROM MatchLimitDb.t_sparse WHERE " + kWhere + " LIMIT 700;";
    require_routes_through_match(dispatcher, sql);

    auto cur = exec(dispatcher, sql);
    INFO("select: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 700);
    // 700 straddles the first batch's 512 matches, so this is the sparse path AND
    // the cross-batch counter at once.
    for (std::size_t r = 0; r < cur->size(); ++r) {
        const auto id = cur->value(0, r).value<int64_t>();
        INFO("row " << r << " id " << id);
        REQUIRE(id % 2 == 0);
    }
}
