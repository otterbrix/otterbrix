#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <string>

// Tests for the RETURNING clause on INSERT / UPDATE / DELETE.

namespace {
    void setup(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            // qty carries a DEFAULT so RETURNING * / RETURNING qty exercises the
            // default-fill (read-back) path on INSERT.
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection (id bigint, name string, qty bigint DEFAULT 7);");
        }
    }
} // namespace

TEST_CASE("integration::cpp::test_returning::insert") {
    auto config = test_create_config("/tmp/test_returning/insert");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    INFO("INSERT ... RETURNING * fills DEFAULT columns") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name) VALUES "
                                           "(1, 'Alice') RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 3);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 0).value<std::string_view>() == "Alice");
        // qty was not supplied; RETURNING * must reflect the DEFAULT (7).
        REQUIRE(cur->value(2, 0).value<int64_t>() == 7);
    }

    INFO("INSERT ... RETURNING column list, multiple rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(2, 'Bob', 20), (3, 'Carol', 30) RETURNING id, qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 2);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 20);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 3);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 30);
    }

    INFO("INSERT ... RETURNING arithmetic with alias") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(4, 'Dan', 10) RETURNING qty * 2 AS double_qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 20);
    }

    INFO("INSERT without RETURNING still reports affected-row count") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(5, 'Eve', 50);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::test_returning::update") {
    auto config = test_create_config("/tmp/test_returning/update");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                "(1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30);");
    }

    INFO("UPDATE ... RETURNING returns the NEW value") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = qty + 5 "
                                           "WHERE id = 1 RETURNING id, qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 15);
    }

    INFO("UPDATE ... RETURNING * over multiple rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = 100 "
                                           "WHERE id >= 2 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 3);
        REQUIRE(cur->value(2, 0).value<int64_t>() == 100);
        REQUIRE(cur->value(2, 1).value<int64_t>() == 100);
    }

    INFO("UPDATE ... RETURNING with no matching rows yields no rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = 0 "
                                           "WHERE id = 999 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::test_returning::delete") {
    auto config = test_create_config("/tmp/test_returning/delete");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                "(1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30);");
    }

    INFO("DELETE ... RETURNING returns the deleted (old) rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "DELETE FROM TestDatabase.TestCollection WHERE id = 2 RETURNING id, name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 2);
        REQUIRE(cur->value(1, 0).value<std::string_view>() == "Bob");
    }

    INFO("DELETE ... RETURNING * over multiple rows") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id >= 1 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2); // ids 1 and 3 remain
        REQUIRE(cur->column_count() == 3);
    }

    INFO("DELETE ... RETURNING with no matching rows yields no rows") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id = 999 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::test_returning::delete_using") {
    // DELETE ... USING ... RETURNING that references columns of BOTH the target
    // (destination) table and the joined (USING) table.
    auto config = test_create_config("/tmp/test_returning/delete_using");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "CREATE TABLE TestDatabase.Orders (id bigint, customer_id bigint, total bigint);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Customers (id bigint, name string);");
    }

    INFO("RETURNING projects target and joined columns; cardinality == deleted rows") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "INSERT INTO TestDatabase.Customers (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
        }
        {
            // Order 12 references a non-existent customer (3), so it must NOT be
            // deleted by the join and must NOT appear in RETURNING.
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES "
                                    "(10, 1, 100), (11, 2, 200), (12, 3, 300);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id "
                                               "RETURNING Orders.id, Orders.total, Customers.name;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->column_count() == 3);
            // Row 0: order 10 joined to Alice; Row 1: order 11 joined to Bob.
            REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 100);
            REQUIRE(cur->value(2, 0).value<std::string_view>() == "Alice");
            REQUIRE(cur->value(0, 1).value<int64_t>() == 11);
            REQUIRE(cur->value(1, 1).value<int64_t>() == 200);
            REQUIRE(cur->value(2, 1).value<std::string_view>() == "Bob");
        }
        {
            // Only the unmatched order (12) survives.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 12);
        }
    }

    INFO("a target row matching multiple USING rows is deleted and returned once") {
        // Two customers share id 7, so order 70 joins both. DELETE ... USING is a
        // semi-join: the order must be deleted once and RETURNING must emit one row.
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "INSERT INTO TestDatabase.Customers (id, name) VALUES (7, 'Dup1'), (7, 'Dup2');");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES (70, 7, 700);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id "
                                               "RETURNING Orders.id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 70);
        }
    }

    INFO("RETURNING a joined table.* expands the USING table's columns") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES (13, 1, 130);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id AND Orders.id = 13 "
                                               "RETURNING Orders.id, Customers.*;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->column_count() == 3); // Orders.id + Customers(id, name)
            REQUIRE(cur->value(0, 0).value<int64_t>() == 13);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 1);
            REQUIRE(cur->value(2, 0).value<std::string_view>() == "Alice");
        }
    }
}

TEST_CASE("integration::cpp::test_returning::delete_using_absolute_row_ids") {
    // REGRESSION: the USING-join DELETE branch must collect the ABSOLUTE table
    // row id of each matched target row, NOT the left-chunk-relative loop index.
    // A prior delete leaves a gap so a matched row's absolute table position no
    // longer equals its scan-loop index — the only configuration that tells the
    // two apart. With the bug, the loop index addresses the WRONG absolute row,
    // so the wrong row is deleted (and the index mirror is misaligned too).
    //
    // Harness mirrors ::delete_using (Orders/Customers) so the join scan settles
    // identically; the gap is created by a FIRST USING-delete (as in that test's
    // section 2), then a SECOND USING-delete is verified against the table state.
    auto config = test_create_config("/tmp/test_returning/delete_using_absolute_row_ids");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Orders (id bigint, customer_id bigint);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Customers (id bigint, name string);");
    }
    // Index on the join column so the post-delete point lookup goes through the
    // index — an index-consistency check that the DELETE's index mirror deleted
    // the MATCHED row, not the first-N scan rows.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(session, "CREATE INDEX idx_cust ON TestDatabase.Orders (customer_id);")
                ->is_success());
    }
    {
        // Customer 7 matches order 13; customer 1 is used only for the gap-maker.
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Customers (id, name) VALUES (1, 'Alice'), (7, 'Grace');");
    }
    {
        // Orders 10..13. customer_id 99 is unmatched (no such customer).
        //   10 -> cust 1   (matched by the gap-making delete)
        //   11 -> cust 99  (never matched, survives)
        //   12 -> cust 99  (never matched, survives)
        //   13 -> cust 7   (matched by the SECOND delete; absolute row 3)
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Orders (id, customer_id) VALUES "
                                "(10, 1), (11, 99), (12, 99), (13, 7);");
    }

    INFO("a prior USING-delete creates a gap, then a second USING-delete removes the right absolute row") {
        // FIRST USING-delete: scope to order 10 only (customer 1). This removes the
        // HEAD row (absolute row 0), leaving a gap. Survivors {11,12,13} now sit at
        // absolute {1,2,3} while the surviving scan presents them at loop {0,1,2}.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id AND Orders.id = 10;");
            REQUIRE(cur->is_success());
        }
        {
            // Sanity: orders {11,12,13} survive after the gap-making delete.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        // SECOND USING-delete: order 13 joins customer 7. Order 13 is at ABSOLUTE
        // row 3 but at scan-loop index 2 (the gap shifted them) — exactly the
        // divergence the bug mishandles: it deletes loop-index 2 == absolute row 2
        // (order 12) and leaves order 13.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id;");
            REQUIRE(cur->is_success());
        }
        // Survivors must be exactly {11, 12}; the buggy code deletes absolute row 2
        // (order 12) and leaves order 13.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 11);
            REQUIRE(cur->value(0, 1).value<int64_t>() == 12);
        }
        // INDEX CONSISTENCY via the point lookup on the indexed join column: order
        // 13 (customer_id 7) is gone; orders 11/12 (customer_id 99) are still found.
        // The buggy index mirror deletes the first-N scan rows, leaving 13 reachable.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders WHERE customer_id = 7;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders WHERE customer_id = 99;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
    }
}

TEST_CASE("integration::cpp::test_returning::update_from") {
    // UPDATE ... FROM ... RETURNING that references columns of BOTH the target
    // table and the joined (FROM) table.
    auto config = test_create_config("/tmp/test_returning/update_from");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "CREATE TABLE TestDatabase.Orders (id bigint, customer_id bigint, total bigint);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Customers (id bigint, name string);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Customers (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
    }
    {
        // Order 12 references a non-existent customer (3): unmatched, not updated.
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES "
                                "(10, 1, 100), (11, 2, 200), (12, 3, 300);");
    }

    INFO("RETURNING projects the NEW target value alongside a joined column") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.Orders SET total = total + 1 "
                                           "FROM TestDatabase.Customers "
                                           "WHERE Orders.customer_id = Customers.id "
                                           "RETURNING Orders.id, Orders.total, Customers.name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 3);
        // Order 10 -> Alice (total now 101); order 11 -> Bob (total now 201).
        REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 101);
        REQUIRE(cur->value(2, 0).value<std::string_view>() == "Alice");
        REQUIRE(cur->value(0, 1).value<int64_t>() == 11);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 201);
        REQUIRE(cur->value(2, 1).value<std::string_view>() == "Bob");
    }

    INFO("a target row matching multiple FROM rows is updated and returned once") {
        // Two customers share id 5, so order 50 joins both. UPDATE ... FROM is a
        // semi-join: the row is updated once (total +1, not +2) and returned once.
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "INSERT INTO TestDatabase.Customers (id, name) VALUES (5, 'Dup1'), (5, 'Dup2');");
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES (50, 5, 500);");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.Orders SET total = total + 1 "
                                               "FROM TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id AND Orders.id = 50 "
                                               "RETURNING Orders.id, Orders.total;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 50);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 501);
        }
    }

    INFO("RETURNING a joined table.* expands the FROM table's columns") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.Orders SET total = total + 1 "
                                           "FROM TestDatabase.Customers "
                                           "WHERE Orders.customer_id = Customers.id AND Orders.id = 11 "
                                           "RETURNING Orders.id, Customers.*;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 3); // Orders.id + Customers(id, name)
        REQUIRE(cur->value(0, 0).value<int64_t>() == 11);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 2);
        REQUIRE(cur->value(2, 0).value<std::string_view>() == "Bob");
    }
}

TEST_CASE("integration::cpp::test_returning::roundtrip") {
    // Consume the RETURNING output of one statement to drive the next.
    auto config = test_create_config("/tmp/test_returning/roundtrip");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Source (id bigint, name string, qty bigint);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Archive (id bigint, name string, qty bigint);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Source (id, name, qty) VALUES "
                                "(1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30);");
    }

    INFO("archive rows moved out of a DELETE ... RETURNING") {
        // DELETE the rows and capture them via RETURNING, then re-insert the
        // captured values into the Archive table (an "insert from deleted").
        std::string ins = "INSERT INTO TestDatabase.Archive (id, name, qty) VALUES ";
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "DELETE FROM TestDatabase.Source WHERE id <= 2 RETURNING id, name, qty;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            for (std::size_t row = 0; row < cur->size(); ++row) {
                auto id = cur->value(0, row).value<int64_t>();
                auto name = std::string(cur->value(1, row).value<std::string_view>());
                auto qty = cur->value(2, row).value<int64_t>();
                ins += "(" + std::to_string(id) + ", '" + std::string(name) + "', " + std::to_string(qty) + ")";
                ins += (row + 1 < cur->size()) ? "," : ";";
            }
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            // Archive now holds the two moved rows; Source keeps only id=3.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Archive;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Source;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 3);
        }
    }

    INFO("UPDATE ... RETURNING matches a subsequent SELECT") {
        int64_t returned_qty = 0;
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.Source SET qty = qty + 100 "
                                               "WHERE id = 3 RETURNING id, qty;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 3);
            returned_qty = cur->value(1, 0).value<int64_t>();
            REQUIRE(returned_qty == 130);
        }
        {
            // The value RETURNING reported must be what actually persisted.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT qty FROM TestDatabase.Source WHERE id = 3;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == returned_qty);
        }
    }

    INFO("RETURNING value feeds a CTE-based SELECT") {
        int64_t new_id = 0;
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.Source (id, name, qty) VALUES "
                                               "(42, 'Zoe', 5) RETURNING id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            new_id = cur->value(0, 0).value<int64_t>();
            REQUIRE(new_id == 42);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "WITH recent AS (SELECT id, name FROM TestDatabase.Source WHERE id = " +
                                                   std::to_string(new_id) + ") SELECT name FROM recent;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<std::string_view>() == "Zoe");
        }
    }
}

TEST_CASE("integration::cpp::test_returning::batching") {
    // More rows than DEFAULT_VECTOR_CAPACITY (1024) so RETURNING crosses chunk
    // boundaries on all three operators (windowed read-back / split paths).
    constexpr int kRows = 2500;
    auto config = test_create_config("/tmp/test_returning/batching");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    INFO("INSERT ... RETURNING across chunk boundaries") {
        std::string sql = "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES ";
        for (int i = 0; i < kRows; ++i) {
            sql += "(" + std::to_string(i) + ", 'n', " + std::to_string(i) + ")";
            sql += (i + 1 < kRows) ? "," : " RETURNING id;";
        }
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);
    }

    INFO("UPDATE ... RETURNING across chunk boundaries") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = qty + 1 "
                                           "WHERE id >= 0 RETURNING id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);
    }

    INFO("DELETE ... RETURNING across chunk boundaries") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id >= 0 RETURNING id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);
    }
}

TEST_CASE("integration::cpp::test_returning::update_from_absolute_row_ids") {
    // REGRESSION (streaming UPDATE ... FROM): the FROM-join UPDATE branch must key
    // the storage/index update on the ABSOLUTE table row id of each matched target
    // row, NOT the left-chunk-relative loop index. A prior delete leaves a gap so a
    // matched row's absolute table position no longer equals its scan-loop index —
    // the configuration that tells the two apart. Mirrors delete_using_absolute_row_ids
    // but for UPDATE ... FROM, and verifies both the updated value and index
    // consistency after the streaming per-batch join apply.
    auto config = test_create_config("/tmp/test_returning/update_from_absolute_row_ids");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "CREATE TABLE TestDatabase.Orders (id bigint, customer_id bigint, total bigint);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Customers (id bigint, name string);");
    }
    // Index on the join column so the post-update point lookup goes through the
    // index — an index-consistency check that the UPDATE's index mirror updated the
    // MATCHED row, not the first-N scan rows.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE INDEX idx_cust ON TestDatabase.Orders (customer_id);")
                    ->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Customers (id, name) VALUES (1, 'Alice'), (7, 'Grace');");
    }
    {
        // Orders 10..13.  10 -> cust 1 (the gap-maker); 11,12 -> cust 99 (unmatched);
        // 13 -> cust 7 (matched by the FROM update; absolute row 3, loop index 2
        // after the gap).
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES "
                                "(10, 1, 100), (11, 99, 110), (12, 99, 120), (13, 7, 130);");
    }

    INFO("a prior delete creates a gap, then an UPDATE ... FROM updates the right absolute row") {
        // Gap-maker: delete order 10 (absolute row 0). Survivors {11,12,13} now sit
        // at absolute {1,2,3} while the scan presents them at loop {0,1,2}.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.Orders WHERE id = 10;");
            REQUIRE(cur->is_success());
        }
        // UPDATE ... FROM: order 13 joins customer 7. Order 13 is at ABSOLUTE row 3
        // but at scan-loop index 2 — the divergence the loop-index bug mishandles.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.Orders SET total = total + 1000 "
                                               "FROM TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id "
                                               "RETURNING Orders.id, Orders.total;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            // Order 13 (the matched absolute row) got +1000; nothing else changed.
            REQUIRE(cur->value(0, 0).value<int64_t>() == 13);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 1130);
        }
        // Only order 13's total moved; orders 11/12 are untouched.
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session, "SELECT id, total FROM TestDatabase.Orders ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 11);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 110);
            REQUIRE(cur->value(0, 1).value<int64_t>() == 12);
            REQUIRE(cur->value(1, 1).value<int64_t>() == 120);
            REQUIRE(cur->value(0, 2).value<int64_t>() == 13);
            REQUIRE(cur->value(1, 2).value<int64_t>() == 1130);
        }
        // INDEX CONSISTENCY via the point lookup on the indexed join column: the
        // matched row (customer_id 7) is still found exactly once after the update.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT total FROM TestDatabase.Orders WHERE customer_id = 7;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 1130);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders WHERE customer_id = 99;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
    }
}

TEST_CASE("integration::cpp::test_returning::join_dml_streaming_multibatch") {
    // STREAMING the LEFT (target) scan across MANY batches: with more target rows
    // than DEFAULT_VECTOR_CAPACITY (1024), the LEFT scan feeds the join sink in
    // multiple push() batches while the RIGHT (USING/FROM) build side stays fully
    // materialized. Exercises the per-batch match / modified_ / index-old / RETURNING
    // accumulation for both DELETE ... USING and UPDATE ... FROM.
    constexpr int kRows = 2500;
    auto config = test_create_config("/tmp/test_returning/join_dml_streaming_multibatch");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "CREATE TABLE TestDatabase.Orders (id bigint, customer_id bigint, total bigint);");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Customers (id bigint, name string);");
    }
    // Two customers: id 1 (matches even orders) and id 2 (matches odd orders), so
    // every order joins exactly one customer — all kRows rows match across batches.
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Customers (id, name) VALUES (1, 'Even'), (2, 'Odd');");
    }
    {
        std::string sql = "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES ";
        for (int i = 0; i < kRows; ++i) {
            // customer_id alternates 1/2 so each order matches exactly one customer.
            sql += "(" + std::to_string(i) + ", " + std::to_string((i % 2) + 1) + ", " + std::to_string(i) + ")";
            sql += (i + 1 < kRows) ? "," : ";";
        }
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    INFO("UPDATE ... FROM streams the target scan across batches; all rows updated once") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.Orders SET total = total + 1 "
                                           "FROM TestDatabase.Customers "
                                           "WHERE Orders.customer_id = Customers.id "
                                           "RETURNING Orders.id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);
    }
    {
        // Every total advanced by exactly 1 (semi-join: once, not per customer).
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT total FROM TestDatabase.Orders WHERE id = 1000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1001);
    }

    INFO("DELETE ... USING streams the target scan across batches; all rows deleted once") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id "
                                               "RETURNING Orders.id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kRows);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }
}
