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
        REQUIRE(cur->chunk_data().column_count() == 3);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 0).value<std::string_view>() == "Alice");
        // qty was not supplied; RETURNING * must reflect the DEFAULT (7).
        REQUIRE(cur->chunk_data().value(2, 0).value<int64_t>() == 7);
    }

    INFO("INSERT ... RETURNING column list, multiple rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(2, 'Bob', 20), (3, 'Carol', 30) RETURNING id, qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 20);
        REQUIRE(cur->chunk_data().value(0, 1).value<int64_t>() == 3);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 30);
    }

    INFO("INSERT ... RETURNING arithmetic with alias") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(4, 'Dan', 10) RETURNING qty * 2 AS double_qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 20);
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
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 15);
    }

    INFO("UPDATE ... RETURNING * over multiple rows") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = 100 "
                                           "WHERE id >= 2 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->chunk_data().column_count() == 3);
        REQUIRE(cur->chunk_data().value(2, 0).value<int64_t>() == 100);
        REQUIRE(cur->chunk_data().value(2, 1).value<int64_t>() == 100);
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
        REQUIRE(cur->chunk_data().column_count() == 2);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 2);
        REQUIRE(cur->chunk_data().value(1, 0).value<std::string_view>() == "Bob");
    }

    INFO("DELETE ... RETURNING * over multiple rows") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id >= 1 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2); // ids 1 and 3 remain
        REQUIRE(cur->chunk_data().column_count() == 3);
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
            REQUIRE(cur->chunk_data().column_count() == 3);
            // Row 0: order 10 joined to Alice; Row 1: order 11 joined to Bob.
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 10);
            REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 100);
            REQUIRE(cur->chunk_data().value(2, 0).value<std::string_view>() == "Alice");
            REQUIRE(cur->chunk_data().value(0, 1).value<int64_t>() == 11);
            REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 200);
            REQUIRE(cur->chunk_data().value(2, 1).value<std::string_view>() == "Bob");
        }
        {
            // Only the unmatched order (12) survives.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 12);
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
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 70);
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
            REQUIRE(cur->chunk_data().column_count() == 3); // Orders.id + Customers(id, name)
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 13);
            REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 1);
            REQUIRE(cur->chunk_data().value(2, 0).value<std::string_view>() == "Alice");
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
        REQUIRE(cur->chunk_data().column_count() == 3);
        // Order 10 -> Alice (total now 101); order 11 -> Bob (total now 201).
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 10);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 101);
        REQUIRE(cur->chunk_data().value(2, 0).value<std::string_view>() == "Alice");
        REQUIRE(cur->chunk_data().value(0, 1).value<int64_t>() == 11);
        REQUIRE(cur->chunk_data().value(1, 1).value<int64_t>() == 201);
        REQUIRE(cur->chunk_data().value(2, 1).value<std::string_view>() == "Bob");
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
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 50);
            REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 501);
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
        REQUIRE(cur->chunk_data().column_count() == 3); // Orders.id + Customers(id, name)
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 11);
        REQUIRE(cur->chunk_data().value(1, 0).value<int64_t>() == 2);
        REQUIRE(cur->chunk_data().value(2, 0).value<std::string_view>() == "Bob");
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
                auto id = cur->chunk_data().value(0, row).value<int64_t>();
                auto name = cur->chunk_data().value(1, row).value<std::string_view>();
                auto qty = cur->chunk_data().value(2, row).value<int64_t>();
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
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 3);
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
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 3);
            returned_qty = cur->chunk_data().value(1, 0).value<int64_t>();
            REQUIRE(returned_qty == 130);
        }
        {
            // The value RETURNING reported must be what actually persisted.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT qty FROM TestDatabase.Source WHERE id = 3;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == returned_qty);
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
            new_id = cur->chunk_data().value(0, 0).value<int64_t>();
            REQUIRE(new_id == 42);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "WITH recent AS (SELECT id, name FROM TestDatabase.Source WHERE id = " +
                                                   std::to_string(new_id) + ") SELECT name FROM recent;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<std::string_view>() == "Zoe");
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
