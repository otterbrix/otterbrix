#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>

// SQL three-valued logic over NULL operands.
//
// A NULL operand makes a value comparison UNKNOWN, not FALSE, and UNKNOWN rows are excluded
// from WHERE / DELETE / UPDATE. The distinction matters under NOT: `NOT UNKNOWN` is UNKNOWN
// (the row stays excluded), whereas `NOT FALSE` is TRUE (the row would be wrongly included).
//
// Before the fix, a pure comparison was pushed down into the storage scan, whose fast path
// compared the raw data buffer without consulting the validity mask. Validity lives in a
// separate sibling column, so a NULL row's payload bytes are a meaningless 0 — every NULL row
// therefore matched `= 0` / `>= 0` / `< 1` ..., and DELETE/UPDATE mutated those rows.
//
// Both surfaces are covered here because they are the same bug: a jsonb nav (`t ->> 'x'`) is
// resolved at transform time into a plain column reference, and an absent key is stored as a
// NULL. A plain NULL and an absent jsonb key reach the identical scan path.

namespace {

    struct probe_t {
        bool ok{false};
        size_t rows{0};
    };

    template<typename Dispatcher>
    probe_t run(Dispatcher* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        if (!cur || !cur->is_success()) {
            return {false, 0};
        }
        return {true, cur->size()};
    }

    // id=1 -> x=5 ; id=2 -> x NULL ; id=3 -> x=0
    template<typename Dispatcher>
    void seed_plain(Dispatcher* d, const std::string& table) {
        REQUIRE(run(d, "CREATE TABLE " + table + " (id INT, x BIGINT);").ok);
        REQUIRE(run(d, "INSERT INTO " + table + " (id, x) VALUES (1, 5);").ok);
        REQUIRE(run(d, "INSERT INTO " + table + " (id, x) VALUES (2, NULL);").ok);
        REQUIRE(run(d, "INSERT INTO " + table + " (id, x) VALUES (3, 0);").ok);
    }

    // The same three rows on a computing table, where id=2 simply has no 'x' key at all.
    template<typename Dispatcher>
    void seed_computed(Dispatcher* d, const std::string& table) {
        REQUIRE(run(d, "CREATE TABLE " + table + " ();").ok);
        REQUIRE(run(d, "INSERT INTO " + table + " (id, x) VALUES (1, 5);").ok);
        REQUIRE(run(d, "INSERT INTO " + table + " (id) VALUES (2);").ok);
        REQUIRE(run(d, "INSERT INTO " + table + " (id, x) VALUES (3, 0);").ok);
    }

} // namespace

TEST_CASE("integration::cpp::null_3vl::comparisons_exclude_null") {
    auto config = test_create_config(integration_fixture_path("test_null_3vl/cmp"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);
    seed_plain(d, "n3.t");

    // The NULL row (id=2) must satisfy NO value comparison.
    CHECK(run(d, "SELECT id FROM n3.t WHERE x = 0;").rows == 1);  // id=3
    CHECK(run(d, "SELECT id FROM n3.t WHERE x <> 0;").rows == 1); // id=1
    CHECK(run(d, "SELECT id FROM n3.t WHERE x >= 0;").rows == 2); // id=1,3
    CHECK(run(d, "SELECT id FROM n3.t WHERE x > -1;").rows == 2); // id=1,3
    CHECK(run(d, "SELECT id FROM n3.t WHERE x < 1;").rows == 1);  // id=3
    CHECK(run(d, "SELECT id FROM n3.t WHERE x <= 5;").rows == 2); // id=1,3
    CHECK(run(d, "SELECT id FROM n3.t WHERE x = 5;").rows == 1);  // id=1

    // IS NULL / IS NOT NULL keep working — they are TRUE/FALSE, never UNKNOWN.
    CHECK(run(d, "SELECT id FROM n3.t WHERE x IS NULL;").rows == 1);     // id=2
    CHECK(run(d, "SELECT id FROM n3.t WHERE x IS NOT NULL;").rows == 2); // id=1,3
}

TEST_CASE("integration::cpp::null_3vl::not_does_not_resurrect_null") {
    // The guard against a naive fix. Merely excluding NULL from a comparison is not enough:
    // if the filter tree is two-valued, NOT flips that exclusion into an inclusion.
    // NOT UNKNOWN must stay UNKNOWN.
    auto config = test_create_config(integration_fixture_path("test_null_3vl/not"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);
    seed_plain(d, "n3.t");

    CHECK(run(d, "SELECT id FROM n3.t WHERE NOT (x = 0);").rows == 1);  // id=1 only
    CHECK(run(d, "SELECT id FROM n3.t WHERE NOT (x >= 0);").rows == 0); // nobody
    CHECK(run(d, "SELECT id FROM n3.t WHERE NOT (x = 5);").rows == 1);  // id=3 only
    CHECK(run(d, "SELECT id FROM n3.t WHERE NOT (x <> 0);").rows == 1); // id=3 only
}

TEST_CASE("integration::cpp::null_3vl::and_or_propagate_unknown") {
    auto config = test_create_config(integration_fixture_path("test_null_3vl/andor"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);
    seed_plain(d, "n3.t");

    // OR: UNKNOWN or TRUE = TRUE; UNKNOWN or FALSE = UNKNOWN (excluded).
    CHECK(run(d, "SELECT id FROM n3.t WHERE x = 0 OR x = 5;").rows == 2);     // id=1,3
    CHECK(run(d, "SELECT id FROM n3.t WHERE x = 999 OR x = 5;").rows == 1);   // id=1
    CHECK(run(d, "SELECT id FROM n3.t WHERE x = 0 OR id = 2;").rows == 2);    // id=2,3 — TRUE rescues it
    CHECK(run(d, "SELECT id FROM n3.t WHERE x IS NULL OR x = 0;").rows == 2); // id=2,3

    // AND: UNKNOWN and TRUE = UNKNOWN (excluded); UNKNOWN and FALSE = FALSE.
    CHECK(run(d, "SELECT id FROM n3.t WHERE x >= 0 AND x <= 9;").rows == 2);    // id=1,3
    CHECK(run(d, "SELECT id FROM n3.t WHERE x >= 0 AND id = 2;").rows == 0);    // nobody
    CHECK(run(d, "SELECT id FROM n3.t WHERE x IS NULL AND id = 2;").rows == 1); // id=2
}

TEST_CASE("integration::cpp::null_3vl::dml_does_not_touch_null_rows") {
    auto config = test_create_config(integration_fixture_path("test_null_3vl/dml"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);

    // DELETE must not remove the NULL row.
    seed_plain(d, "n3.del");
    CHECK(run(d, "DELETE FROM n3.del WHERE x = 0;").ok);
    CHECK(run(d, "SELECT id FROM n3.del;").rows == 2);                 // id=1,2 survive
    CHECK(run(d, "SELECT id FROM n3.del WHERE x IS NULL;").rows == 1); // id=2 still there

    // UPDATE must not clobber the NULL row.
    seed_plain(d, "n3.upd");
    CHECK(run(d, "UPDATE n3.upd SET id = 99 WHERE x > -1;").ok);
    CHECK(run(d, "SELECT id FROM n3.upd WHERE id = 99;").rows == 2); // only id=1,3 -> 99
    CHECK(run(d, "SELECT id FROM n3.upd WHERE id = 2;").rows == 1);  // the NULL row is untouched
}

TEST_CASE("integration::cpp::null_3vl::update_overlay_keeps_null_excluded") {
    // The update-overlay branch of column_data_t::check_predicate: once a vector carries
    // updates, matching is answered from the overlay. The NULL row must stay excluded there
    // too — the validity gate runs before the overlay is consulted.
    auto config = test_create_config(integration_fixture_path("test_null_3vl/overlay"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);
    seed_plain(d, "n3.t");

    CHECK(run(d, "UPDATE n3.t SET x = 7 WHERE id = 1;").ok); // put this vector into the overlay

    CHECK(run(d, "SELECT id FROM n3.t WHERE x = 7;").rows == 1);     // id=1, read from the overlay
    CHECK(run(d, "SELECT id FROM n3.t WHERE x = 5;").rows == 0);     // old value is gone
    CHECK(run(d, "SELECT id FROM n3.t WHERE x = 0;").rows == 1);     // id=3 only — NOT the NULL row
    CHECK(run(d, "SELECT id FROM n3.t WHERE x >= 0;").rows == 2);    // id=1(7),3(0) — NOT the NULL row
    CHECK(run(d, "SELECT id FROM n3.t WHERE x IS NULL;").rows == 1); // id=2 still NULL
}

TEST_CASE("integration::cpp::null_3vl::string_column_null") {
    // The string fast path (string_check_row) has the same raw-buffer shape as the fixed-size
    // one: an empty string must not be conflated with a NULL.
    auto config = test_create_config(integration_fixture_path("test_null_3vl/str"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);
    REQUIRE(run(d, "CREATE TABLE n3.s (id INT, name TEXT);").ok);
    REQUIRE(run(d, "INSERT INTO n3.s (id, name) VALUES (1, 'ann');").ok);
    REQUIRE(run(d, "INSERT INTO n3.s (id, name) VALUES (2, NULL);").ok);
    REQUIRE(run(d, "INSERT INTO n3.s (id, name) VALUES (3, '');").ok);

    CHECK(run(d, "SELECT id FROM n3.s WHERE name = '';").rows == 1);     // id=3, NOT the NULL
    CHECK(run(d, "SELECT id FROM n3.s WHERE name <> 'ann';").rows == 1); // id=3
    CHECK(run(d, "SELECT id FROM n3.s WHERE name IS NULL;").rows == 1);  // id=2
}

// NOTE — two adjacent gaps are deliberately NOT covered here. They are separate pre-existing
// bugs that this change neither causes nor fixes (verified: identical results with and without
// it), and pinning them would misattribute them to this fix:
//
//   * INDEX SCAN route. `CREATE INDEX idx_x ON t (x)` then `WHERE x = 0` returns 0 rows — it
//     loses id=3, whose value genuinely IS 0. A pure compare on an indexed column is planned
//     as an index_scan (create_plan_match.cpp), bypassing the scan filter entirely,
//     and the index mishandles a column containing NULLs.
//   * `UPDATE t SET x = NULL` does not mark the row NULL: afterwards, `x IS NULL` still does
//     not see it.
//
// Both are silent-wrong-result bugs and deserve their own fix and tests.

// ---------------------------------------------------------------------------
// JSONB surface: the same bug, reached through a nav operator over an absent key.
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::null_3vl::jsonb_absent_key_comparisons") {
    auto config = test_create_config(integration_fixture_path("test_null_3vl/jsonb_cmp"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);
    seed_computed(d, "n3.co");

    // An absent key projects as NULL (this was already correct before the fix).
    {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "SELECT co ->> 'x' AS v FROM n3.co WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).is_null());
    }

    // ...and it must now be excluded from every value comparison through the nav operand.
    CHECK(run(d, "SELECT id FROM n3.co WHERE co ->> 'x' = 0;").rows == 1);  // id=3
    CHECK(run(d, "SELECT id FROM n3.co WHERE co ->> 'x' > -1;").rows == 2); // id=1,3
    CHECK(run(d, "SELECT id FROM n3.co WHERE co ->> 'x' < 1;").rows == 1);  // id=3
    CHECK(run(d, "SELECT id FROM n3.co WHERE co ->> 'x' >= 0;").rows == 2); // id=1,3
    CHECK(run(d, "SELECT id FROM n3.co WHERE co ->> 'x' <> 0;").rows == 1); // id=1

    // The same rows, addressed as a plain column on the same computing table.
    CHECK(run(d, "SELECT id FROM n3.co WHERE x = 0;").rows == 1);
    CHECK(run(d, "SELECT id FROM n3.co WHERE x >= 0;").rows == 2);
}

TEST_CASE("integration::cpp::null_3vl::jsonb_not_and_dml") {
    auto config = test_create_config(integration_fixture_path("test_null_3vl/jsonb_dml"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);

    seed_computed(d, "n3.co");
    CHECK(run(d, "SELECT id FROM n3.co WHERE NOT (co ->> 'x' = 0);").rows == 1); // id=1 only

    // DELETE through a nav operand must not remove the keyless row.
    seed_computed(d, "n3.cod");
    CHECK(run(d, "DELETE FROM n3.cod WHERE cod ->> 'x' = 0;").ok);
    CHECK(run(d, "SELECT id FROM n3.cod;").rows == 2); // id=1,2 survive

    // UPDATE through a nav operand must not clobber it.
    seed_computed(d, "n3.cou");
    CHECK(run(d, "UPDATE n3.cou SET id = 99 WHERE cou ->> 'x' > -1;").ok);
    CHECK(run(d, "SELECT id FROM n3.cou WHERE id = 99;").rows == 2); // id=1,3 only
    CHECK(run(d, "SELECT id FROM n3.cou WHERE id = 2;").rows == 1);  // keyless row untouched
}

TEST_CASE("integration::cpp::null_3vl::jsonb_nested_absent_key") {
    // A dotted/nested key flattens to the column "a/b"; an absent nested key is a NULL there.
    auto config = test_create_config(integration_fixture_path("test_null_3vl/jsonb_nested"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(run(d, "CREATE DATABASE n3;").ok);
    REQUIRE(run(d, "CREATE TABLE n3.nd ();").ok);
    REQUIRE(run(d, "INSERT INTO n3.nd (id, a.b) VALUES (1, 5);").ok);
    REQUIRE(run(d, "INSERT INTO n3.nd (id) VALUES (2);").ok); // no a.b
    REQUIRE(run(d, "INSERT INTO n3.nd (id, a.b) VALUES (3, 0);").ok);

    CHECK(run(d, "SELECT id FROM n3.nd WHERE nd #>> '{a,b}' = 0;").rows == 1);  // id=3
    CHECK(run(d, "SELECT id FROM n3.nd WHERE nd #>> '{a,b}' >= 0;").rows == 2); // id=1,3
}
