#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {

    enum class yields
    {
        column_ref,  // one value read straight from a column — a usable grouping key
        value,       // one ordinary computed value
        array_value, // one value that is a whole array
        integer,     // one value, but a bare integer literal — an ORDINAL in GROUP BY / ORDER BY
        boolean,     // one value of boolean type
        aggregate,   // reduces a group of rows; legal only where grouping is
        star,        // the whole row: expands to N columns, cannot be named or nested
        table_star,  // qualified `t.*` — likewise N columns
    };

    // Every clause that takes one value takes any of these.
    constexpr bool is_single_value(yields kind) {
        return kind == yields::column_ref || kind == yields::value || kind == yields::array_value ||
               kind == yields::integer || kind == yields::boolean;
    }

    constexpr bool is_row(yields kind) { return kind == yields::star || kind == yields::table_star; }

    struct shape_t {
        const char* name;
        const char* sql;
        yields kind;
        // Without a GROUP BY the whole table is one group, so only
        // aggregates and column-free expressions have a defined value in HAVING.
        bool reads_columns;
    };

    constexpr shape_t shapes[] = {
        {"column", "name", yields::column_ref, true},
        {"qualified_column", "orders.name", yields::column_ref, true},
        {"integer_literal", "42", yields::integer, false},
        {"string_literal", "'abc'", yields::value, false},
        {"arithmetic", "amount + 1", yields::value, true},
        {"unary_minus", "-amount", yields::value, true},
        {"bitwise", "amount & 1", yields::value, true},
        {"comparison", "amount > 5", yields::boolean, true},
        {"is_null", "name IS NULL", yields::boolean, true},
        {"boolean_and", "amount > 1 AND id < 100", yields::boolean, true},
        {"negation", "NOT (amount > 5)", yields::boolean, true},
        {"like", "name LIKE 'a%'", yields::boolean, true},
        {"in_list", "id IN (1, 2, 3)", yields::boolean, true},
        {"between", "amount BETWEEN 1 AND 100", yields::boolean, true},
        {"case_when", "CASE WHEN amount > 5 THEN 1 ELSE 0 END", yields::value, true},
        {"coalesce", "COALESCE(name, 'x')", yields::value, true},
        {"cast", "amount::TEXT", yields::value, true},
        {"function_numeric", "abs(amount)", yields::value, true},
        {"function_text", "length(name)", yields::value, true},
        {"function_case", "upper(name)", yields::value, true},
        {"aggregate", "sum(amount)", yields::aggregate, true},
        {"subscript", "tags[1]", yields::value, true},
        {"array_literal", "ARRAY[1, 2, 3]", yields::array_value, false},
        {"scalar_subquery", "(SELECT max(cid) FROM ddb.customers)", yields::value, false},
        {"exists_subquery", "EXISTS (SELECT 1 FROM ddb.customers)", yields::boolean, false},
        {"star", "*", yields::star, true},
        {"qualified_star", "orders.*", yields::table_star, true},
    };

    // `#E#` is the expression under test; `#ID#` a row id unique to the cell, so the DML
    // placements do not collide with each other.
    struct placement_t {
        const char* name;
        const char* prepare; // run first, not the verdict; nullptr when not needed
        const char* statement;
    };

    constexpr placement_t placements[] = {
        {"select", nullptr, "SELECT #E# FROM ddb.orders;"},
        {"select_aliased", nullptr, "SELECT #E# AS out FROM ddb.orders;"},
        {"predicate", nullptr, "SELECT id FROM ddb.orders WHERE #E#;"},
        {"predicate_operand", nullptr, "SELECT id FROM ddb.orders WHERE (#E#) IS NOT NULL;"},
        {"group_by", nullptr, "SELECT count(*) FROM ddb.orders GROUP BY #E#;"},
        {"having", nullptr, "SELECT count(*) FROM ddb.orders HAVING (#E#) IS NOT NULL;"},
        {"order_by", nullptr, "SELECT id FROM ddb.orders ORDER BY #E#;"},
        {"aggregate_argument", nullptr, "SELECT max(#E#) FROM ddb.orders;"},
        {"case_arm", nullptr, "SELECT CASE WHEN id = 1 THEN #E# ELSE NULL END FROM ddb.orders;"},
        {"join_on",
         nullptr,
         "SELECT orders.id FROM ddb.orders JOIN ddb.customers ON orders.id = customers.cid "
         "AND (#E#) IS NOT NULL;"},
        {"subquery", nullptr, "SELECT * FROM (SELECT #E# AS out FROM ddb.orders) AS t;"},
        {"cte", nullptr, "WITH t AS (SELECT #E# AS out FROM ddb.orders) SELECT * FROM t;"},
        {"update_set", nullptr, "UPDATE ddb.orders SET name = CAST(#E# AS TEXT) WHERE id = 1;"},
        {"update_where", nullptr, "UPDATE ddb.orders SET amount = amount WHERE (#E#) IS NOT NULL;"},
        {"insert_returning",
         nullptr,
         "INSERT INTO ddb.orders (id, name, amount) VALUES (#ID#, 'ins', 5) RETURNING #E#;"},
        {"update_returning", nullptr, "UPDATE ddb.orders SET amount = amount WHERE id = 1 RETURNING #E#;"},
        {"delete_returning",
         "INSERT INTO ddb.orders (id, name, amount) VALUES (#ID#, 'del', 7);",
         "DELETE FROM ddb.orders WHERE id = #ID# RETURNING #E#;"},
    };

    bool check_behavior(const placement_t& placement, const shape_t& shape) {
        const yields kind = shape.kind;
        const std::string_view clause{placement.name};
        // A target list projects anything, including a whole row and an aggregate.
        if (clause == "select") {
            return true;
        }
        // `AS` names one column, so it cannot name a row that expands to many.
        if (clause == "select_aliased" || clause == "subquery" || clause == "cte") {
            return !is_row(kind);
        }
        // WHERE takes a boolean, and cannot reduce the group it is filtering the rows of.
        if (clause == "predicate") {
            return kind == yields::boolean;
        }
        // Any single value can be an operand — but no aggregate and no whole row.
        if (clause == "predicate_operand" || clause == "join_on" || clause == "update_where" ||
            clause == "update_set" || clause == "aggregate_argument" || clause == "case_arm") {
            return is_single_value(kind);
        }
        // A grouping key is any single value. A bare integer is an ordinal, and this query has
        // one output column, so 42 addresses nothing.
        if (clause == "group_by") {
            return is_single_value(kind) && kind != yields::integer;
        }
        // No GROUP BY, so the whole table is one group: only a reduction of it, or an
        // expression that reads no column at all, has a defined value here.
        if (clause == "having") {
            return kind == yields::aggregate || (is_single_value(kind) && !shape.reads_columns);
        }
        // An ordinal over a single-column select list, and no grouping to reduce.
        if (clause == "order_by") {
            return kind != yields::integer && kind != yields::aggregate && !is_row(kind);
        }
        // RETURNING projects the affected rows one at a time — a row expands, but there is no
        // group to reduce.
        if (clause == "insert_returning" || clause == "update_returning" || clause == "delete_returning") {
            return kind != yields::aggregate;
        }
        FAIL("no rule stated for clause '" << placement.name << "'");
        return false;
    }

    //! This is what we do not support for now
    // Returns why a legal cell is not built, or nullptr when it is. Each entry is required to
    // be rejected today; when one starts working the test says so, so it can be promoted.
    const char* unbuilt(const placement_t& placement, const shape_t& shape) {
        const std::string_view clause{placement.name};
        // The SET value is cast to the target's type by this placement.
        if (clause == "update_set" && shape.kind == yields::array_value) {
            return "no implicit or assignment cast from a whole array to text";
        }
        if (clause == "aggregate_argument" && shape.kind == yields::array_value) {
            return "no aggregate kernel accumulates a whole array";
        }
        return nullptr;
    }

    std::string substitute(const char* text, const char* expression, int row_id) {
        std::string out{text};
        for (auto at = out.find("#E#"); at != std::string::npos; at = out.find("#E#")) {
            out.replace(at, 3, expression);
        }
        for (auto at = out.find("#ID#"); at != std::string::npos; at = out.find("#ID#")) {
            out.replace(at, 4, std::to_string(row_id));
        }
        return out;
    }

    class matrix_t {
    public:
        explicit matrix_t(otterbrix::wrapper_dispatcher_t* dispatcher)
            : dispatcher_(dispatcher) {
            const char* schema[] = {
                "CREATE DATABASE ddb;",
                "CREATE TABLE ddb.orders (id BIGINT, name TEXT, amount BIGINT, tags BIGINT[]);",
                "CREATE TABLE ddb.customers (cid BIGINT, cname TEXT);",
                "INSERT INTO ddb.orders (id, name, amount) VALUES (1, 'alice', 10);",
                "INSERT INTO ddb.customers (cid, cname) VALUES (1, 'acme');",
            };
            for (const char* statement : schema) {
                INFO(statement);
                REQUIRE(run(statement)->is_success());
            }
        }

        void check(const shape_t& shape, const placement_t& placement) {
            const int row_id = next_row_id_++;
            if (placement.prepare) {
                run(substitute(placement.prepare, shape.sql, row_id));
            }
            const std::string statement = substitute(placement.statement, shape.sql, row_id);
            auto cursor = run(statement);

            INFO(shape.name << " in " << placement.name);
            INFO(statement);
            INFO("engine: " << (cursor->is_success() ? std::string{"accepted"}
                                                     : std::string{"rejected — "} + cursor->get_error().what.c_str()));

            if (!check_behavior(placement, shape)) {
                CHECK(cursor->is_error());
                return;
            }
            const char* gap = unbuilt(placement, shape);
            if (gap == nullptr) {
                CHECK(cursor->is_success());
                return;
            }
            INFO("not built yet: " << gap);
            INFO("correct SQL allows this. If it now succeeds the gap has closed — move this "
                 "cell out of unbuilt() so the behavior becomes required.");
            CHECK_FALSE(cursor->is_success());
        }

    private:
        components::cursor::cursor_t_ptr run(const std::string& statement) {
            auto session = otterbrix::session_id_t();
            return dispatcher_->execute_sql(session, statement);
        }

        otterbrix::wrapper_dispatcher_t* dispatcher_;
        int next_row_id_{1000};
    };

} // namespace

TEST_CASE("integration::cpp::expression_matrix::every_expression_in_every_clause") {
    auto config = test_create_config("/tmp/test_expression_matrix");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    matrix_t matrix(space.dispatcher());

    for (const auto& shape : shapes) {
        for (const auto& placement : placements) {
            matrix.check(shape, placement);
        }
    }
}
