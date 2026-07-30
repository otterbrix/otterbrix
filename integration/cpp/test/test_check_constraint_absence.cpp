// ============================================================================
// THE ABSENCE POLICY OF A COMPARISON CHECK.
//
// operator_check_constraint validates the SUBMITTED write-set, but a column omitted
// from an INSERT column list is filled with the table DEFAULT agent-side at
// storage_append -- so the STORED row is not what was submitted, and the check has to
// reason about a column that is not in front of it. There are exactly THREE outcomes,
// all resolved once when the predicate is built:
//
//   (1) column PRESENT              -> compare its value
//   (2) column ABSENT, non-NULL DEFAULT, name-addressed write-set
//                                   -> compare the DEFAULT (that is what gets stored)
//   (3) column ABSENT, no default   -> the stored value really is NULL, so the
//       (or a positional write-set)    comparison is UNKNOWN, which permits() lets pass
//
// (3) also covers the positional case: an INSERT with no column list aliases its
// write-set arbitrarily, so absence-by-name proves nothing and the requirement is not
// dropped on that evidence.
//
// WHAT IS ALREADY COVERED ELSEWHERE, and deliberately not repeated here:
//   * the same three outcomes for `CHECK (col IS NOT NULL)` --
//     test_stacked_constraints::check_is_not_null_with_default
//   * the fixed-ARRAY size requirement that must NOT be dropped by a name miss on an
//     unnamed write-set -- test_list_array::array_size_unnamed_write_set
// Those pin the IS-NULL family and the array requirement; this file pins the
// COMPARISON family, which is the part the bound-layer migration rewrites.
// ============================================================================

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>

using namespace components;
using namespace components::cursor;
using namespace test_helpers;

namespace {

    int64_t row_count(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& table) {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM AbsDb." + table + ";");
        REQUIRE(cur->is_success());
        return static_cast<int64_t>(cur->value(0, 0).value<uint64_t>());
    }

} // namespace

// (1) The column is PRESENT: the comparison simply reads it. The control for the two
//     absence outcomes below -- if this reddens, the fault is in the comparison itself
//     and not in the absence policy.
TEST_CASE("integration::cpp::check_absence::present_column_is_compared") {
    auto config = make_test_config(test_temp_path("check_absence/present"), /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE AbsDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AbsDb.t (id bigint, x bigint DEFAULT 10);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE AbsDb.t ADD CONSTRAINT chk_x CHECK (x > 5);")->is_success());

    auto ok = exec(dispatcher, "INSERT INTO AbsDb.t (id, x) VALUES (1, 10);");
    INFO("satisfying: " << (ok->is_error() ? std::string(ok->get_error().what) : std::string("ok")));
    REQUIRE(ok->is_success());

    auto bad = exec(dispatcher, "INSERT INTO AbsDb.t (id, x) VALUES (2, 1);");
    INFO("violating: " << (bad->is_error() ? std::string(bad->get_error().what) : std::string("accepted")));
    REQUIRE(bad->is_error());

    REQUIRE(row_count(dispatcher, "t") == 1);
}

// (2) The column is ABSENT and has a non-NULL DEFAULT: the comparison must be made
//     against THE DEFAULT, because that is the value the row will actually store.
//     Both directions matter -- a default that satisfies the check must let the row in,
//     and a default that violates it must keep the row out. Testing only the first
//     would pass on an implementation that ignores absent columns entirely.
TEST_CASE("integration::cpp::check_absence::absent_column_is_compared_as_its_default") {
    auto config = make_test_config(test_temp_path("check_absence/default"), /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE AbsDb;")->is_success());

    INFO("a DEFAULT that SATISFIES the check: the omitting INSERT is accepted");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE AbsDb.pass (id bigint, x bigint DEFAULT 10);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE AbsDb.pass ADD CONSTRAINT chk_p CHECK (x > 5);")->is_success());
        auto cur = exec(dispatcher, "INSERT INTO AbsDb.pass (id) VALUES (1);");
        INFO("omitting insert: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
        REQUIRE(cur->is_success());
        // ... and the stored row really does carry the default.
        auto sel = exec(dispatcher, "SELECT x FROM AbsDb.pass WHERE id = 1;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 1);
        REQUIRE(sel->value(0, 0).value<int64_t>() == 10);
    }

    INFO("a DEFAULT that VIOLATES the check: the omitting INSERT is rejected");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE AbsDb.fail (id bigint, x bigint DEFAULT 1);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE AbsDb.fail ADD CONSTRAINT chk_f CHECK (x > 5);")->is_success());
        auto cur = exec(dispatcher, "INSERT INTO AbsDb.fail (id) VALUES (1);");
        INFO("omitting insert: " << (cur->is_error() ? std::string(cur->get_error().what)
                                                     : std::string("ACCEPTED -- the default was not checked")));
        REQUIRE(cur->is_error());
        REQUIRE(row_count(dispatcher, "fail") == 0);
    }
}

// (3) The column is ABSENT with NO default: the stored value really is NULL, so the
//     comparison is UNKNOWN -- and a CHECK is violated only by a definite FALSE, so the
//     row passes. This is the outcome most easily broken by a migration: a typed layer
//     that treats a missing column as an error, or that folds UNKNOWN into FALSE, turns
//     this accepted row into a rejected one.
TEST_CASE("integration::cpp::check_absence::absent_column_with_no_default_permits") {
    auto config = make_test_config(test_temp_path("check_absence/nodefault"), /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE AbsDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AbsDb.t (id bigint, x bigint);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE AbsDb.t ADD CONSTRAINT chk_x CHECK (x > 5);")->is_success());

    auto cur = exec(dispatcher, "INSERT INTO AbsDb.t (id) VALUES (1);");
    INFO("omitting insert: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
    REQUIRE(cur->is_success());
    REQUIRE(row_count(dispatcher, "t") == 1);

    // An EXPLICIT NULL takes the same route: NULL > 5 is UNKNOWN, and UNKNOWN permits.
    auto explicit_null = exec(dispatcher, "INSERT INTO AbsDb.t (id, x) VALUES (2, NULL);");
    INFO("explicit NULL: " << (explicit_null->is_error() ? std::string(explicit_null->get_error().what)
                                                         : std::string("ok")));
    REQUIRE(explicit_null->is_success());
    REQUIRE(row_count(dispatcher, "t") == 2);
}

// A CHECK combined with AND/OR over a defaulted and a present column at once. The
// absence policy is resolved per LEAF, so a tree mixing both has to get each leaf right
// independently -- a single global "absent = pass" would let the violating half through.
TEST_CASE("integration::cpp::check_absence::mixed_tree_resolves_each_leaf") {
    auto config = make_test_config(test_temp_path("check_absence/mixed"), /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE AbsDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AbsDb.t (id bigint, a bigint DEFAULT 10, b bigint);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE AbsDb.t ADD CONSTRAINT chk_ab CHECK ((a > 5) AND (b > 5));")
                ->is_success());

    INFO("a takes its satisfying default, b is present and satisfies: accepted");
    {
        auto cur = exec(dispatcher, "INSERT INTO AbsDb.t (id, b) VALUES (1, 10);");
        INFO("insert: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
        REQUIRE(cur->is_success());
    }

    INFO("a takes its satisfying default, b is present and VIOLATES: rejected");
    {
        auto cur = exec(dispatcher, "INSERT INTO AbsDb.t (id, b) VALUES (2, 1);");
        INFO("insert: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ACCEPTED")));
        REQUIRE(cur->is_error());
    }

    REQUIRE(row_count(dispatcher, "t") == 1);
}

// ---------------------------------------------------------------------------
// DELETE ... USING IS A SEMI-JOIN.
//
// A target row is deleted ONCE however many USING rows it matches, and RETURNING
// yields one row per deleted target, not one per matched pair. The boxed path got
// this from a `break` after the first matching right row; a restructure that
// evaluates the whole right chunk at once has to take only the FIRST match and not
// emit a row per match. Pinned here because the difference is invisible unless a
// target matches MORE THAN ONE using row -- which is the case this sets up.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::dml_semijoin::delete_using_deletes_a_target_once_per_row") {
    auto config = make_test_config(test_temp_path("dml_semijoin/delete_using"), /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE AbsDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AbsDb.target (id bigint, k bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE AbsDb.src (k bigint, tag bigint);")->is_success());

    // target rows 1 and 2 carry k=7; row 3 carries k=9 and matches nothing.
    REQUIRE(exec(dispatcher, "INSERT INTO AbsDb.target (id, k) VALUES (1, 7), (2, 7), (3, 9);")->is_success());
    // THREE source rows share k=7, so each matching target matches three of them.
    REQUIRE(exec(dispatcher, "INSERT INTO AbsDb.src (k, tag) VALUES (7, 100), (7, 200), (7, 300);")->is_success());

    auto cur = exec(dispatcher, "DELETE FROM AbsDb.target USING AbsDb.src WHERE target.k = src.k RETURNING id;");
    INFO("delete using: " << (cur->is_error() ? std::string(cur->get_error().what) : std::string("ok")));
    REQUIRE(cur->is_success());
    // TWO rows deleted, not six: the semi-join stops at the first matching source row.
    REQUIRE(cur->size() == 2);

    auto remaining = exec(dispatcher, "SELECT COUNT(id) AS c FROM AbsDb.target;");
    REQUIRE(remaining->is_success());
    REQUIRE(remaining->value(0, 0).value<uint64_t>() == 1u); // only id=3 survives
}
