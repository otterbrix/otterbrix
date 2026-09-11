#include "test_config.hpp"
#include "integration_fixture_path.hpp"

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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 0).value<std::string_view>() == "Alice");
        REQUIRE(cur->value(2, 0).value<int64_t>() == 7);
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 2);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 20);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 3);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 30);
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 20);
    }

    INFO("INSERT without RETURNING still reports affected-row count");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO TestDatabase.TestCollection (id, name, qty) VALUES "
                                           "(5, 'Eve', 50);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 15);
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
        REQUIRE(cur->value(2, 0).value<int64_t>() == 100);
        REQUIRE(cur->value(2, 1).value<int64_t>() == 100);
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 2);
        REQUIRE(cur->value(1, 0).value<std::string_view>() == "Bob");
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
            REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 100);
            REQUIRE(cur->value(2, 0).value<std::string_view>() == "Alice");
            REQUIRE(cur->value(0, 1).value<int64_t>() == 11);
            REQUIRE(cur->value(1, 1).value<int64_t>() == 200);
            REQUIRE(cur->value(2, 1).value<std::string_view>() == "Bob");
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.Orders;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 12);
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
            REQUIRE(cur->value(0, 0).value<int64_t>() == 70);
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
            REQUIRE(cur->value(0, 0).value<int64_t>() == 13);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 1);
            REQUIRE(cur->value(2, 0).value<std::string_view>() == "Alice");
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
            REQUIRE(cur->value(0, 0).value<int64_t>() == 11);
            REQUIRE(cur->value(0, 1).value<int64_t>() == 12);
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 101);
        REQUIRE(cur->value(2, 0).value<std::string_view>() == "Alice");
        REQUIRE(cur->value(0, 1).value<int64_t>() == 11);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 201);
        REQUIRE(cur->value(2, 1).value<std::string_view>() == "Bob");
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
            REQUIRE(cur->value(0, 0).value<int64_t>() == 50);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 501);
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 11);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 2);
        REQUIRE(cur->value(2, 0).value<std::string_view>() == "Bob");
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
            REQUIRE(cur->value(0, 0).value<int64_t>() == 13);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 1130);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT id, total FROM TestDatabase.Orders ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 11);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 110);
            REQUIRE(cur->value(0, 1).value<int64_t>() == 12);
            REQUIRE(cur->value(1, 1).value<int64_t>() == 120);
            REQUIRE(cur->value(0, 2).value<int64_t>() == 13);
            REQUIRE(cur->value(1, 2).value<int64_t>() == 1130);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT total FROM TestDatabase.Orders WHERE customer_id = 7;");
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1001);
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
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 0);
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == 7);
    }

    INFO("a subsequent valid UPDATE ... RETURNING still works");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "UPDATE TestDatabase.TestCollection SET qty = 9 WHERE id = 1 "
                                           "RETURNING qty;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 9);
    }
}
