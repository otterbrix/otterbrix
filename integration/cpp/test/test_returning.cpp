#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
#include <services/collection/executor.hpp>
#include <string>

namespace {
    void setup(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;");
        }
        {
            auto session = otterbrix::session_id_t();
            // qty carries a DEFAULT so RETURNING */qty exercises the default-fill read-back path.
            dispatcher->execute_sql(
                session,
                "CREATE TABLE TestDatabase.TestCollection (id bigint, name string, qty bigint DEFAULT 7);");
        }
    }
} // namespace

TEST_CASE("integration::cpp::test_returning::insert") {
    auto config = test_create_config(integration_fixture_path("test_returning/insert"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    INFO("INSERT ... RETURNING * fills DEFAULT columns");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name) VALUES "
                                           "(1, 'Alice') RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 3);
        auto cell0 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell0.has_error());
        REQUIRE(cell0.value() == 1);
        auto cell1 = cur->get<std::string_view>(1, 0);
        REQUIRE_FALSE(cell1.has_error());
        REQUIRE(cell1.value() == "Alice");
        auto cell2 = cur->get<int64_t>(2, 0);
        REQUIRE_FALSE(cell2.has_error());
        REQUIRE(cell2.value() == 7);
    }

    INFO("INSERT ... RETURNING column list, multiple rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(2, 'Bob', 20), (3, 'Carol', 30) RETURNING id, qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 2);
        auto cell3 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell3.has_error());
        REQUIRE(cell3.value() == 2);
        auto cell4 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(cell4.has_error());
        REQUIRE(cell4.value() == 20);
        auto cell5 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell5.has_error());
        REQUIRE(cell5.value() == 3);
        auto cell6 = cur->get<int64_t>(1, 1);
        REQUIRE_FALSE(cell6.has_error());
        REQUIRE(cell6.value() == 30);
    }

    INFO("INSERT ... RETURNING arithmetic with alias");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(4, 'Dan', 10) RETURNING qty * 2 AS double_qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 1);
        auto cell7 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell7.has_error());
        REQUIRE(cell7.value() == 20);
    }

    INFO("INSERT without RETURNING still reports affected-row count");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(5, 'Eve', 50);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->affected_rows() == 1);
    }
}

TEST_CASE("integration::cpp::test_returning::update") {
    auto config = test_create_config(integration_fixture_path("test_returning/update"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                "(1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30);");
    }

    INFO("UPDATE ... RETURNING returns the NEW value");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = qty + 5 "
                                           "WHERE id = 1 RETURNING id, qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 2);
        auto cell8 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell8.has_error());
        REQUIRE(cell8.value() == 1);
        auto cell9 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(cell9.has_error());
        REQUIRE(cell9.value() == 15);
    }

    INFO("UPDATE ... RETURNING * over multiple rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = 100 "
                                           "WHERE id >= 2 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 3);
        auto cell10 = cur->get<int64_t>(2, 0);
        REQUIRE_FALSE(cell10.has_error());
        REQUIRE(cell10.value() == 100);
        auto cell11 = cur->get<int64_t>(2, 1);
        REQUIRE_FALSE(cell11.has_error());
        REQUIRE(cell11.value() == 100);
    }

    INFO("UPDATE ... RETURNING with no matching rows yields no rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = 0 "
                                           "WHERE id = 999 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::test_returning::delete") {
    auto config = test_create_config(integration_fixture_path("test_returning/delete"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                "(1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30);");
    }

    INFO("DELETE ... RETURNING returns the deleted (old) rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "DELETE FROM TestDatabase.TestCollection WHERE id = 2 RETURNING id, name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 2);
        auto cell12 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell12.has_error());
        REQUIRE(cell12.value() == 2);
        auto cell13 = cur->get<std::string_view>(1, 0);
        REQUIRE_FALSE(cell13.has_error());
        REQUIRE(cell13.value() == "Bob");
    }

    INFO("DELETE ... RETURNING * over multiple rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id >= 1 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 3);
    }

    INFO("DELETE ... RETURNING with no matching rows yields no rows");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id = 999 RETURNING *;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::test_returning::delete_using") {
    auto config = test_create_config(integration_fixture_path("test_returning/delete_using"));
    test_clear_directory(config);
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

    INFO("RETURNING projects target and joined columns; cardinality == deleted rows");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                                    "INSERT INTO TestDatabase.Customers (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
        }
        {
            // Order 12 references a non-existent customer (3), so it must not be deleted or returned.
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
            auto cell14 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell14.has_error());
            REQUIRE(cell14.value() == 10);
            auto cell15 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell15.has_error());
            REQUIRE(cell15.value() == 100);
            auto cell16 = cur->get<std::string_view>(2, 0);
            REQUIRE_FALSE(cell16.has_error());
            REQUIRE(cell16.value() == "Alice");
            auto cell17 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell17.has_error());
            REQUIRE(cell17.value() == 11);
            auto cell18 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell18.has_error());
            REQUIRE(cell18.value() == 200);
            auto cell19 = cur->get<std::string_view>(2, 1);
            REQUIRE_FALSE(cell19.has_error());
            REQUIRE(cell19.value() == "Bob");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell20 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell20.has_error());
            REQUIRE(cell20.value() == 12);
        }
    }

    INFO("a target row matching multiple USING rows is deleted and returned once");
    {
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
            auto cell21 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell21.has_error());
            REQUIRE(cell21.value() == 70);
        }
    }

    INFO("RETURNING a joined table.* expands the USING table's columns");
    {
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
            REQUIRE(cur->column_count() == 3);
            auto cell22 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell22.has_error());
            REQUIRE(cell22.value() == 13);
            auto cell23 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell23.has_error());
            REQUIRE(cell23.value() == 1);
            auto cell24 = cur->get<std::string_view>(2, 0);
            REQUIRE_FALSE(cell24.has_error());
            REQUIRE(cell24.value() == "Alice");
        }
    }
}

TEST_CASE("integration::cpp::test_returning::delete_using_absolute_row_ids") {
    // REGRESSION: the USING-join DELETE branch must key on the matched row's absolute table id,
    // not the left-chunk-relative loop index; a prior delete opens a gap between the two, and a
    // first USING-delete makes that gap for a second USING-delete to be checked against.
    auto config = test_create_config(integration_fixture_path("test_returning/delete_using_absolute_row_ids"));
    test_clear_directory(config);
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
    // Proves the DELETE's index mirror deleted the matched row, not the first-N scan rows.
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
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Orders (id, customer_id) VALUES "
                                "(10, 1), (11, 99), (12, 99), (13, 7);");
    }

    INFO("a prior USING-delete creates a gap, then a second USING-delete removes the right absolute row");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id AND Orders.id = 10;");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        // Order 13 now sits at absolute row 3 but scan-loop index 2 — the divergence the bug mishandles.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id;");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            auto cell25 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell25.has_error());
            REQUIRE(cell25.value() == 11);
            auto cell26 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell26.has_error());
            REQUIRE(cell26.value() == 12);
        }
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
    auto config = test_create_config(integration_fixture_path("test_returning/update_from"));
    test_clear_directory(config);
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
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES "
                                "(10, 1, 100), (11, 2, 200), (12, 3, 300);");
    }

    INFO("RETURNING projects the NEW target value alongside a joined column");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.Orders SET total = total + 1 "
                                           "FROM TestDatabase.Customers "
                                           "WHERE Orders.customer_id = Customers.id "
                                           "RETURNING Orders.id, Orders.total, Customers.name;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 3);
        auto cell27 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell27.has_error());
        REQUIRE(cell27.value() == 10);
        auto cell28 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(cell28.has_error());
        REQUIRE(cell28.value() == 101);
        auto cell29 = cur->get<std::string_view>(2, 0);
        REQUIRE_FALSE(cell29.has_error());
        REQUIRE(cell29.value() == "Alice");
        auto cell30 = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(cell30.has_error());
        REQUIRE(cell30.value() == 11);
        auto cell31 = cur->get<int64_t>(1, 1);
        REQUIRE_FALSE(cell31.has_error());
        REQUIRE(cell31.value() == 201);
        auto cell32 = cur->get<std::string_view>(2, 1);
        REQUIRE_FALSE(cell32.has_error());
        REQUIRE(cell32.value() == "Bob");
    }

    INFO("a target row matching multiple FROM rows is updated and returned once");
    {
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
            auto cell33 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell33.has_error());
            REQUIRE(cell33.value() == 50);
            auto cell34 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell34.has_error());
            REQUIRE(cell34.value() == 501);
        }
    }

    INFO("RETURNING a joined table.* expands the FROM table's columns");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.Orders SET total = total + 1 "
                                           "FROM TestDatabase.Customers "
                                           "WHERE Orders.customer_id = Customers.id AND Orders.id = 11 "
                                           "RETURNING Orders.id, Customers.*;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 3);
        auto cell35 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell35.has_error());
        REQUIRE(cell35.value() == 11);
        auto cell36 = cur->get<int64_t>(1, 0);
        REQUIRE_FALSE(cell36.has_error());
        REQUIRE(cell36.value() == 2);
        auto cell37 = cur->get<std::string_view>(2, 0);
        REQUIRE_FALSE(cell37.has_error());
        REQUIRE(cell37.value() == "Bob");
    }
}

TEST_CASE("integration::cpp::test_returning::roundtrip") {
    auto config = test_create_config(integration_fixture_path("test_returning/roundtrip"));
    test_clear_directory(config);
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

    INFO("archive rows moved out of a DELETE ... RETURNING");
    {
        std::string ins = "INSERT INTO TestDatabase.Archive (id, name, qty) VALUES ";
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "DELETE FROM TestDatabase.Source WHERE id <= 2 RETURNING id, name, qty;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            for (std::size_t row = 0; row < cur->size(); ++row) {
                auto cell38 = cur->get<int64_t>(0, row);
                REQUIRE_FALSE(cell38.has_error());
                auto id = cell38.value();
                auto cell39 = cur->get<std::string_view>(1, row);
                REQUIRE_FALSE(cell39.has_error());
                auto name = std::string(cell39.value());
                auto cell40 = cur->get<int64_t>(2, row);
                REQUIRE_FALSE(cell40.has_error());
                auto qty = cell40.value();
                ins += "(" + std::to_string(id) + ", '" + std::string(name) + "', " + std::to_string(qty) + ")";
                ins += (row + 1 < cur->size()) ? "," : ";";
            }
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 2);
        }
        {
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
            auto cell41 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell41.has_error());
            REQUIRE(cell41.value() == 3);
        }
    }

    INFO("UPDATE ... RETURNING matches a subsequent SELECT");
    {
        int64_t returned_qty = 0;
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.Source SET qty = qty + 100 "
                                               "WHERE id = 3 RETURNING id, qty;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell42 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell42.has_error());
            REQUIRE(cell42.value() == 3);
            auto cell43 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell43.has_error());
            returned_qty = cell43.value();
            REQUIRE(returned_qty == 130);
        }
        {
            // The value RETURNING reported must be what actually persisted.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT qty FROM TestDatabase.Source WHERE id = 3;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell44 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell44.has_error());
            REQUIRE(cell44.value() == returned_qty);
        }
    }

    INFO("RETURNING value feeds a CTE-based SELECT");
    {
        int64_t new_id = 0;
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO TestDatabase.Source (id, name, qty) VALUES "
                                               "(42, 'Zoe', 5) RETURNING id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell45 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell45.has_error());
            new_id = cell45.value();
            REQUIRE(new_id == 42);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "WITH recent AS (SELECT id, name FROM TestDatabase.Source WHERE id = " +
                                                   std::to_string(new_id) + ") SELECT name FROM recent;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell46 = cur->get<std::string_view>(0, 0);
            REQUIRE_FALSE(cell46.has_error());
            REQUIRE(cell46.value() == "Zoe");
        }
    }
}

TEST_CASE("integration::cpp::test_returning::batching") {
    // >> DEFAULT_VECTOR_CAPACITY (1024) so RETURNING crosses chunk boundaries on all three operators.
    constexpr int kRows = 2500;
    auto config = test_create_config(integration_fixture_path("test_returning/batching"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    INFO("INSERT ... RETURNING across chunk boundaries");
    {
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

    INFO("UPDATE ... RETURNING across chunk boundaries");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = qty + 1 "
                                           "WHERE id >= 0 RETURNING id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);
    }

    INFO("DELETE ... RETURNING across chunk boundaries");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection WHERE id >= 0 RETURNING id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);
    }
}

TEST_CASE("integration::cpp::test_returning::update_from_absolute_row_ids") {
    // REGRESSION: mirrors delete_using_absolute_row_ids's absolute-row-id-vs-loop-index bug, but
    // for the streaming UPDATE ... FROM branch; also verifies index consistency after the update.
    auto config = test_create_config(integration_fixture_path("test_returning/update_from_absolute_row_ids"));
    test_clear_directory(config);
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
    // Proves the UPDATE's index mirror updated the matched row, not the first-N scan rows.
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
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES "
                                "(10, 1, 100), (11, 99, 110), (12, 99, 120), (13, 7, 130);");
    }

    INFO("a prior delete creates a gap, then an UPDATE ... FROM updates the right absolute row");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.Orders WHERE id = 10;");
            REQUIRE(cur->is_success());
        }
        // Order 13 now sits at absolute row 3 but scan-loop index 2 — the divergence the bug mishandles.
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.Orders SET total = total + 1000 "
                                               "FROM TestDatabase.Customers "
                                               "WHERE Orders.customer_id = Customers.id "
                                               "RETURNING Orders.id, Orders.total;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell47 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell47.has_error());
            REQUIRE(cell47.value() == 13);
            auto cell48 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell48.has_error());
            REQUIRE(cell48.value() == 1130);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id, total FROM TestDatabase.Orders ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            auto cell49 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell49.has_error());
            REQUIRE(cell49.value() == 11);
            auto cell50 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell50.has_error());
            REQUIRE(cell50.value() == 110);
            auto cell51 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell51.has_error());
            REQUIRE(cell51.value() == 12);
            auto cell52 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell52.has_error());
            REQUIRE(cell52.value() == 120);
            auto cell53 = cur->get<int64_t>(0, 2);
            REQUIRE_FALSE(cell53.has_error());
            REQUIRE(cell53.value() == 13);
            auto cell54 = cur->get<int64_t>(1, 2);
            REQUIRE_FALSE(cell54.has_error());
            REQUIRE(cell54.value() == 1130);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT total FROM TestDatabase.Orders WHERE customer_id = 7;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell55 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell55.has_error());
            REQUIRE(cell55.value() == 1130);
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
    // >> DEFAULT_VECTOR_CAPACITY (1024) target rows so the LEFT scan feeds the join sink in
    // multiple push() batches while the RIGHT (USING/FROM) build side stays fully materialized.
    constexpr int kRows = 2500;
    auto config = test_create_config(integration_fixture_path("test_returning/join_dml_streaming_multibatch"));
    test_clear_directory(config);
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
    // Two customers, matching even/odd orders respectively, so every order joins exactly one.
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session,
                                "INSERT INTO TestDatabase.Customers (id, name) VALUES (1, 'Even'), (2, 'Odd');");
    }
    {
        std::string sql = "INSERT INTO TestDatabase.Orders (id, customer_id, total) VALUES ";
        for (int i = 0; i < kRows; ++i) {
            sql += "(" + std::to_string(i) + ", " + std::to_string((i % 2) + 1) + ", " + std::to_string(i) + ")";
            sql += (i + 1 < kRows) ? "," : ";";
        }
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    INFO("UPDATE ... FROM streams the target scan across batches; all rows updated once");
    {
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
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT total FROM TestDatabase.Orders WHERE id = 1000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell56 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell56.has_error());
        REQUIRE(cell56.value() == 1001);
    }

    INFO("DELETE ... USING streams the target scan across batches; all rows deleted once");
    {
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

// A RETURNING projection can fail at runtime (e.g. division by zero) after DML storage work is
// staged: INSERT has already WAL-appended the rows, so the abort tail must revert that range;
// UPDATE's projection runs before the storage mutation, so a failing RETURNING leaves zero writes.
TEST_CASE("integration::cpp::test_returning::insert_returning_error_reverts_append") {
    auto config = test_create_config(integration_fixture_path("test_returning/insert_error_revert"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    const auto reverts_before = services::collection::executor::dml_appends_reverted();
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name) VALUES "
                                           "(1, 'Alice') RETURNING id / 0;");
        REQUIRE(cur->is_error());
    }
    const auto reverts_after = services::collection::executor::dml_appends_reverted();
    REQUIRE(reverts_after == reverts_before + 1);

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM TestDatabase.TestCollection;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell57 = cur->get<uint64_t>(0, 0);
        REQUIRE_FALSE(cell57.has_error());
        REQUIRE(cell57.value() == 0);
    }

    INFO("the table stays fully usable after the failed statement");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name) VALUES "
                                           "(2, 'Bob') RETURNING id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::test_returning::update_returning_error_leaves_no_writes") {
    auto config = test_create_config(integration_fixture_path("test_returning/update_error_clean"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES (1, 'Alice', 7);")
                    ->is_success());
    }

    const auto sends_before = components::operators::update_storage_update_sends();
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = 8 WHERE id = 1 "
                                           "RETURNING qty / 0;");
        REQUIRE(cur->is_error());
    }
    const auto sends_after = components::operators::update_storage_update_sends();
    REQUIRE(sends_after == sends_before);

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT qty FROM TestDatabase.TestCollection WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell58 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell58.has_error());
        REQUIRE(cell58.value() == 7);
    }

    INFO("a subsequent valid UPDATE ... RETURNING still works");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = 9 WHERE id = 1 "
                                           "RETURNING qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto cell59 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell59.has_error());
        REQUIRE(cell59.value() == 9);
    }
}

// RETURNING over DELETE ... USING reads the matched rows through slices of the scan batch and of the USING chunk.
// A slice shares its source's validity, so a boolean condition must read the row it selected, not the row at the output
// position (#764).
TEST_CASE("integration::cpp::test_returning::delete_using_returning_boolean_reads_the_selected_rows") {
    auto config = test_create_config(integration_fixture_path("test_returning/delete_using_boolean"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.T (k bigint, flag boolean);")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.S (k bigint, flag boolean);")->is_success());
    // T row 0 is unmatched with a NULL flag, so the matched row 1 is output row 0 over a NULL outer position.
    REQUIRE(exec("INSERT INTO TestDatabase.T (k, flag) VALUES (0, NULL), (1, true), (2, false), (3, true);")
                ->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.S (k, flag) VALUES (99, NULL), (1, true), (2, true), (3, NULL);")
                ->is_success());

    // 1 when the flag is TRUE, 0 for FALSE or NULL.
    auto cur = exec("DELETE FROM TestDatabase.T USING TestDatabase.S WHERE T.k = S.k "
                    "RETURNING T.k, CASE WHEN T.flag = true THEN 1 ELSE 0 END, "
                    "CASE WHEN S.flag = true THEN 1 ELSE 0 END;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    for (uint64_t row = 0; row < 3; ++row) {
        auto k = cur->get<int64_t>(0, row);
        auto left_flag = cur->get<int64_t>(1, row);
        auto right_flag = cur->get<int64_t>(2, row);
        REQUIRE_FALSE(k.has_error());
        REQUIRE_FALSE(left_flag.has_error());
        REQUIRE_FALSE(right_flag.has_error());
        // T flags: k1 true, k2 false, k3 true. S flags: k1 true, k2 true, k3 NULL.
        REQUIRE(left_flag.value() == (k.value() == 2 ? 0 : 1));
        REQUIRE(right_flag.value() == (k.value() == 3 ? 0 : 1));
    }
}

// The RETURNING chunks outlive the statement while they reference the scan batches and the USING chunks, across
// mid-pump flushes, several right chunks, and the sparse branch that gathers instead of slicing.
TEST_CASE("integration::cpp::test_returning::delete_using_returning_slices_outlive_the_statement") {
    constexpr int kOrders = 2500;
    constexpr int kCustomers = 1500;
    auto config = test_create_config(integration_fixture_path("test_returning/delete_using_slices"));
    config.execution.dml_flush_row_threshold = 300;
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.Orders (id bigint, customer_id bigint, note string);")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.Customers (id bigint, name string);")->is_success());
    {
        std::string sql = "INSERT INTO TestDatabase.Customers (id, name) VALUES ";
        for (int i = 0; i < kCustomers; ++i) {
            sql += "(" + std::to_string(i) + ", 'customer-" + std::to_string(i) + "-" + std::string(40, 'c') + "')";
            sql += (i + 1 < kCustomers) ? "," : ";";
        }
        REQUIRE(exec(sql)->is_success());
    }
    {
        std::string sql = "INSERT INTO TestDatabase.Orders (id, customer_id, note) VALUES ";
        for (int i = 0; i < kOrders; ++i) {
            sql += "(" + std::to_string(i) + ", " + std::to_string(i % kCustomers) + ", 'order-" + std::to_string(i) +
                   "-" + std::string(40, 'o') + "')";
            sql += (i + 1 < kOrders) ? "," : ";";
        }
        REQUIRE(exec(sql)->is_success());
    }

    auto check = [](const components::cursor::cursor_t_ptr& cur, uint64_t expected_rows) {
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == expected_rows);
        for (uint64_t row = 0; row < expected_rows; ++row) {
            auto id = cur->get<int64_t>(0, row);
            auto customer = cur->get<int64_t>(1, row);
            auto note = cur->get<std::string_view>(2, row);
            auto name = cur->get<std::string_view>(3, row);
            REQUIRE_FALSE(id.has_error());
            REQUIRE_FALSE(customer.has_error());
            REQUIRE_FALSE(note.has_error());
            REQUIRE_FALSE(name.has_error());
            REQUIRE(customer.value() == id.value() % kCustomers);
            REQUIRE(note.value() == "order-" + std::to_string(id.value()) + "-" + std::string(40, 'o'));
            REQUIRE(name.value() == "customer-" + std::to_string(customer.value()) + "-" + std::string(40, 'c'));
        }
    };

    INFO("sparse: 7 of 2500 rows are gathered");
    check(exec("DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
               "WHERE Orders.customer_id = Customers.id AND Orders.id < 7 "
               "RETURNING Orders.id, Orders.customer_id, Orders.note, Customers.name;"),
          7);
    INFO("dense: the rest are sliced, partners come from two right chunks");
    check(exec("DELETE FROM TestDatabase.Orders USING TestDatabase.Customers "
               "WHERE Orders.customer_id = Customers.id "
               "RETURNING Orders.id, Orders.customer_id, Orders.note, Customers.name;"),
          kOrders - 7);
    REQUIRE(exec("SELECT id FROM TestDatabase.Orders;")->size() == 0);
}

TEST_CASE("integration::cpp::test_returning::delete_returning_over_filtered_scan_batches") {
    constexpr int kRows = 2500;
    auto config = test_create_config(integration_fixture_path("test_returning/delete_sparse_dense"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.T (id bigint, note string);")->is_success());
    std::string sql = "INSERT INTO TestDatabase.T (id, note) VALUES ";
    for (int i = 0; i < kRows; ++i) {
        sql += "(" + std::to_string(i) + ", 'note-" + std::to_string(i) + "-" + std::string(40, 'n') + "')";
        sql += (i + 1 < kRows) ? "," : ";";
    }
    REQUIRE(exec(sql)->is_success());

    auto check = [](const components::cursor::cursor_t_ptr& cur, uint64_t expected_rows) {
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == expected_rows);
        for (uint64_t row = 0; row < expected_rows; ++row) {
            auto id = cur->get<int64_t>(0, row);
            auto note = cur->get<std::string_view>(1, row);
            REQUIRE_FALSE(id.has_error());
            REQUIRE_FALSE(note.has_error());
            REQUIRE(note.value() == "note-" + std::to_string(id.value()) + "-" + std::string(40, 'n'));
        }
    };
    check(exec("DELETE FROM TestDatabase.T WHERE id < 5 RETURNING id, note;"), 5);
    check(exec("DELETE FROM TestDatabase.T WHERE id >= 5 RETURNING id, note;"), kRows - 5);
}

// A cast of a list or array column reads the rows the slice selected, not the rows at the output positions.
TEST_CASE("integration::cpp::test_returning::delete_using_returning_casts_composites_of_the_selected_rows") {
    auto config = test_create_config(integration_fixture_path("test_returning/delete_using_composite_cast"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.L (id bigint, v bigint[]);")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.S (id bigint);")->is_success());
    // L row 0 is unmatched, so every matched row sits at an output position other than its own.
    REQUIRE(exec("INSERT INTO TestDatabase.L (id, v) VALUES (0, ARRAY[1, 2, 3]), (1, ARRAY[4, 5]), (2, ARRAY[6]);")
                ->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.S (id) VALUES (1), (2);")->is_success());

    auto cur = exec("DELETE FROM TestDatabase.L USING TestDatabase.S WHERE L.id = S.id "
                    "RETURNING L.id, L.v::int[], L.v::text;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    for (uint64_t row = 0; row < 2; ++row) {
        auto id = cur->get<int64_t>(0, row);
        REQUIRE_FALSE(id.has_error());
        const int32_t first = id.value() == 1 ? 4 : 6;
        auto head = cur->get<int32_t>(1, row, {0});
        REQUIRE_FALSE(head.has_error());
        REQUIRE(head.value() == first);
        auto text = cur->get<std::string_view>(2, row);
        REQUIRE_FALSE(text.has_error());
        REQUIRE(text.value() == (id.value() == 1 ? "{4,5}" : "{6}"));
    }
}

TEST_CASE("integration::cpp::test_returning::delete_using_returning_expression_over_both_sides") {
    auto config = test_create_config(integration_fixture_path("test_returning/delete_using_both_sides"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.A (id bigint);")->is_success());
    REQUIRE(exec("CREATE TABLE TestDatabase.B (id bigint, w bigint);")->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.A (id) VALUES (0), (1), (2), (3);")->is_success());
    REQUIRE(exec("INSERT INTO TestDatabase.B (id, w) VALUES (9, 900), (3, 30), (1, 10);")->is_success());

    auto cur = exec("DELETE FROM TestDatabase.A USING TestDatabase.B WHERE A.id = B.id "
                    "RETURNING A.id, A.id + B.w;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    for (uint64_t row = 0; row < 2; ++row) {
        auto id = cur->get<int64_t>(0, row);
        auto sum = cur->get<int64_t>(1, row);
        REQUIRE_FALSE(id.has_error());
        REQUIRE_FALSE(sum.has_error());
        REQUIRE(sum.value() == (id.value() == 1 ? 11 : 33));
    }
}
