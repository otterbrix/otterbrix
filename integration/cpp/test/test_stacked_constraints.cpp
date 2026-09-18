#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// Constraint SINK ops stack above one DML as a left-linear chain; only the DML populates constraint_input(),
// so a non-adjacent op must walk down left_ to find it or it silently validates nothing.
// No combined multi-FK syntax exists; each FK is added via its own ALTER, stacking fk_check ops.

using namespace test_helpers;

// Stack: check_constraint( fk_check( insert ) ) -- CHECK is non-adjacent to the DML.
TEST_CASE("integration::cpp::test_stacked_constraints::fk_plus_check") {
    auto config = make_test_config(integration_fixture_path("test_stacked_constraints/fk_plus_check"));
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

TEST_CASE("integration::cpp::test_stacked_constraints::fk_plus_notnull") {
    auto config = make_test_config(integration_fixture_path("test_stacked_constraints/fk_plus_notnull"));
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

// Two outgoing FKs stack as fk_check( fk_check( insert ) ); each is violated in turn to cover both positions.
TEST_CASE("integration::cpp::test_stacked_constraints::two_outgoing_fks") {
    auto config = make_test_config(integration_fixture_path("test_stacked_constraints/two_outgoing_fks"));
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

// Two ON DELETE CASCADE children stack as fk_cascade( fk_cascade( delete ) ); a skipped cascade leaves dangling rows.
TEST_CASE("integration::cpp::test_stacked_constraints::two_cascade_children") {
    auto config = make_test_config(integration_fixture_path("test_stacked_constraints/two_cascade_children"));
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
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.child_a (id, parent_id) VALUES (10, 1), (11, 1), (12, 2);")
                    ->is_success());
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

// CHECK validates the materialised row: an omitted column expands to its DEFAULT before the check runs.
TEST_CASE("integration::cpp::test_stacked_constraints::check_is_not_null_with_default") {
    auto config = make_test_config(integration_fixture_path("test_stacked_constraints/check_is_not_null_default"));
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
