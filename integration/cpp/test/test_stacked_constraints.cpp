#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// Regression tests for the "silent-skip" bug in STACKED constraint SINK operators.
//
// The planner stacks constraint SINK operators above ONE DML op as a left-linear
// chain: check_constraint OUTERMOST, one fk_check per outgoing FK, one fk_cascade
// per referencing FK — constraint_N( ... constraint_1( DML( scan ) ) ).
//
// Only the DML op snapshots the just-written rows into constraint_input() (via
// record_flush). Constraint ops do NOT propagate constraint_input() upward, so a
// NON-adjacent constraint op's immediate left_ is ANOTHER constraint op whose
// constraint_input() is empty. Each constraint op must therefore walk DOWN the
// left_ spine to the FIRST populated constraint_input() — the DML's write-set
// (constraint_util.hpp resolve_constraint_source); an op that reads only its
// IMMEDIATE left_->constraint_input() silently validates NOTHING.
//
// Each case below produces a stack with a non-adjacent constraint op.
//
// DDL note: all forms used here appear in existing passing tests
// (test_sql_features.cpp, test_large_aggregate_dml.cpp): inline NOT NULL, per-FK
// FOREIGN KEY via ALTER TABLE ADD CONSTRAINT (one per outgoing FK), CHECK via
// ALTER TABLE ADD CONSTRAINT, and ON DELETE CASCADE. There is no combined
// multi-column single-statement multi-FK syntax; multiple FKs are added as
// separate ALTER statements, which is exactly what stacks multiple fk_check ops.

using namespace test_helpers;

// ---------------------------------------------------------------------------
// (A) check_constraint( fk_check( insert ) )
//
// A table with an outgoing FK AND a CHECK on a DIFFERENT column. The CHECK op is
// OUTERMOST; its immediate left_ is the fk_check op (empty constraint_input).
// A row with a VALID FK reference but a VIOLATING CHECK must be rejected.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_stacked_constraints::fk_plus_check") {
    auto config = make_test_config("/tmp/test_stacked_constraints/fk_plus_check");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: parent + child(orders) with FK on customer_id and CHECK on amount");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.customers (id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.orders (id bigint, customer_id bigint, amount bigint);")
                    ->is_success());
        REQUIRE(exec(dispatcher,
                     "ALTER TABLE TestDatabase.orders ADD CONSTRAINT fk_customer "
                     "FOREIGN KEY (customer_id) REFERENCES TestDatabase.customers (id);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.orders ADD CONSTRAINT chk_amount CHECK (amount > 0);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.customers (id, name) VALUES (1, 'Alice');")->is_success());
    }

    INFO("valid FK reference but VIOLATING CHECK: must be rejected (CHECK was silently skipped pre-fix)");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.orders (id, customer_id, amount) VALUES (10, 1, -5);");
        REQUIRE(cur->is_error());
    }

    INFO("fully-valid INSERT: accepted");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.orders (id, customer_id, amount) VALUES (11, 1, 42);");
        INFO("valid insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

// ---------------------------------------------------------------------------
// (A') check_constraint( fk_check( insert ) ), NOT NULL variant.
//
// Same stack shape but the outermost constraint is a NOT NULL / IS NOT NULL
// CHECK on a non-FK column. A valid FK reference with a NULL required column
// must be rejected.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_stacked_constraints::fk_plus_notnull") {
    auto config = make_test_config("/tmp/test_stacked_constraints/fk_plus_notnull");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.customers (id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.orders (id bigint, customer_id bigint, label text);")
                    ->is_success());
        REQUIRE(exec(dispatcher,
                     "ALTER TABLE TestDatabase.orders ADD CONSTRAINT fk_customer "
                     "FOREIGN KEY (customer_id) REFERENCES TestDatabase.customers (id);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.orders ADD CONSTRAINT chk_label CHECK (label IS NOT NULL);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.customers (id, name) VALUES (1, 'Alice');")->is_success());
    }

    INFO("valid FK reference but NULL required column: must be rejected");
    {
        // label omitted (NULL) — CHECK (label IS NOT NULL) must fire despite valid FK.
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.orders (id, customer_id) VALUES (10, 1);");
        REQUIRE(cur->is_error());
    }

    INFO("fully-valid INSERT: accepted");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.orders (id, customer_id, label) VALUES (11, 1, 'ok');");
        INFO("valid insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

// ---------------------------------------------------------------------------
// (B) fk_check( fk_check( insert ) )
//
// A table with TWO outgoing FKs → two stacked fk_check ops. Whichever fk_check
// is NON-adjacent to the insert sees an empty immediate constraint_input and
// must still validate THAT FK. Each FK is violated in turn so both stack
// positions are covered regardless of the planner's stacking order.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_stacked_constraints::two_outgoing_fks") {
    auto config = make_test_config("/tmp/test_stacked_constraints/two_outgoing_fks");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: orders references BOTH customers and products");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.customers (id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.products (id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.orders (id bigint, customer_id bigint, product_id bigint);")
                    ->is_success());
        REQUIRE(exec(dispatcher,
                     "ALTER TABLE TestDatabase.orders ADD CONSTRAINT fk_customer "
                     "FOREIGN KEY (customer_id) REFERENCES TestDatabase.customers (id);")
                    ->is_success());
        REQUIRE(exec(dispatcher,
                     "ALTER TABLE TestDatabase.orders ADD CONSTRAINT fk_product "
                     "FOREIGN KEY (product_id) REFERENCES TestDatabase.products (id);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.customers (id, name) VALUES (1, 'Alice');")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.products (id, name) VALUES (100, 'Widget');")->is_success());
    }

    INFO("valid FK#1 (customer) but VIOLATING FK#2 (product): rejected");
    {
        auto cur =
            exec(dispatcher, "INSERT INTO TestDatabase.orders (id, customer_id, product_id) VALUES (10, 1, 999);");
        REQUIRE(cur->is_error());
    }

    INFO("valid FK#2 (product) but VIOLATING FK#1 (customer): rejected");
    {
        auto cur =
            exec(dispatcher, "INSERT INTO TestDatabase.orders (id, customer_id, product_id) VALUES (11, 999, 100);");
        REQUIRE(cur->is_error());
    }

    INFO("both FKs valid: accepted");
    {
        auto cur =
            exec(dispatcher, "INSERT INTO TestDatabase.orders (id, customer_id, product_id) VALUES (12, 1, 100);");
        INFO("valid insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

// ---------------------------------------------------------------------------
// (C) fk_cascade( fk_cascade( delete ) )
//
// A parent referenced by TWO child tables, each with ON DELETE CASCADE → two
// stacked fk_cascade ops above the parent DELETE. The non-adjacent fk_cascade
// sees an empty immediate constraint_input; BOTH children must still cascade —
// a skipped cascade leaves dangling child rows.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_stacked_constraints::two_cascade_children") {
    auto config = make_test_config("/tmp/test_stacked_constraints/two_cascade_children");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: parent referenced by child_a and child_b, both ON DELETE CASCADE");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.parent (id bigint, val text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.child_a (id bigint, parent_id bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.child_b (id bigint, parent_id bigint);")->is_success());
        REQUIRE(exec(dispatcher,
                     "ALTER TABLE TestDatabase.child_a ADD CONSTRAINT fk_a "
                     "FOREIGN KEY (parent_id) REFERENCES TestDatabase.parent (id) ON DELETE CASCADE;")
                    ->is_success());
        REQUIRE(exec(dispatcher,
                     "ALTER TABLE TestDatabase.child_b ADD CONSTRAINT fk_b "
                     "FOREIGN KEY (parent_id) REFERENCES TestDatabase.parent (id) ON DELETE CASCADE;")
                    ->is_success());
        REQUIRE(
            exec(dispatcher, "INSERT INTO TestDatabase.parent (id, val) VALUES (1, 'p1'), (2, 'p2');")->is_success());
        // child_a: two rows referencing parent 1, one referencing parent 2 (survives).
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.child_a (id, parent_id) VALUES (10, 1), (11, 1), (12, 2);")
                    ->is_success());
        // child_b: one row referencing parent 1, one referencing parent 2 (survives).
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.child_b (id, parent_id) VALUES (20, 1), (21, 2);")
                    ->is_success());
    }

    INFO("delete parent 1: cascade must remove parent-1 rows from BOTH children");
    {
        auto cur = exec(dispatcher, "DELETE FROM TestDatabase.parent WHERE id = 1;");
        INFO("cascade delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

    INFO("child_a parent-1 rows are gone (both cascaded)");
    {
        auto cur = exec(dispatcher, "SELECT id FROM TestDatabase.child_a WHERE parent_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("child_b parent-1 row is gone (non-adjacent cascade was silently skipped pre-fix)");
    {
        auto cur = exec(dispatcher, "SELECT id FROM TestDatabase.child_b WHERE parent_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("parent-2 rows survive in both children (correct deleted-row count, not over-deletion)");
    {
        auto cur_a = exec(dispatcher, "SELECT id FROM TestDatabase.child_a WHERE parent_id = 2;");
        REQUIRE(cur_a->is_success());
        REQUIRE(cur_a->size() == 1);
        auto cur_b = exec(dispatcher, "SELECT id FROM TestDatabase.child_b WHERE parent_id = 2;");
        REQUIRE(cur_b->is_success());
        REQUIRE(cur_b->size() == 1);
    }

    INFO("total surviving child rows == 2 (one per child); no stray rows");
    {
        auto cur_a = exec(dispatcher, "SELECT id FROM TestDatabase.child_a;");
        REQUIRE(cur_a->is_success());
        REQUIRE(cur_a->size() == 1);
        auto cur_b = exec(dispatcher, "SELECT id FROM TestDatabase.child_b;");
        REQUIRE(cur_b->is_success());
        REQUIRE(cur_b->size() == 1);
    }
}

// ---------------------------------------------------------------------------
// CHECK (col IS NOT NULL) vs a DEFAULT-backed column.
//
// The check validates the MATERIALISED row: a column omitted from the INSERT column
// list is expanded to its table DEFAULT by the insert operator, above the journal, so
// the row the check reads is the row that is stored. `CHECK (col IS NOT NULL)` must
// therefore PASS for an omitted column with a non-NULL DEFAULT and still FAIL for an
// explicit NULL or an omitted column with NO default (which really stores NULL).
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_stacked_constraints::check_is_not_null_with_default") {
    auto config = make_test_config("/tmp/test_stacked_constraints/check_is_not_null_default");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: walls(height DEFAULT 5) + CHECK (height IS NOT NULL)");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(
            exec(dispatcher, "CREATE TABLE TestDatabase.walls (id bigint, height bigint DEFAULT 5);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.walls ADD CONSTRAINT chk_height CHECK (height IS NOT NULL);")
                    ->is_success());
    }

    INFO("INSERT omitting the DEFAULT-backed column passes (stored row carries 5)");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.walls (id) VALUES (1);");
        INFO("insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
        auto sel = exec(dispatcher, "SELECT height FROM TestDatabase.walls WHERE id = 1;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 1);
        REQUIRE(sel->value(0, 0).value<int64_t>() == 5);
    }

    INFO("an explicit NULL still violates the check");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.walls (id, height) VALUES (2, NULL);");
        REQUIRE(cur->is_error());
    }

    INFO("omitting a column with NO default still violates the check (stores NULL)");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.floors (id bigint, depth bigint);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.floors ADD CONSTRAINT chk_depth CHECK (depth IS NOT NULL);")
                    ->is_success());
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.floors (id) VALUES (1);");
        REQUIRE(cur->is_error());
    }
}
