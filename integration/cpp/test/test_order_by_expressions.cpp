#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <core/operations_helper.hpp>

// ORDER BY over expression keys: unary operators and mixed plain/computed key lists.
// Issue #558 (A: `ORDER BY -v` crashed; B: a leading arithmetic key was demoted to a
// tie-breaker so `ORDER BY g + 0 DESC, v ASC` silently sorted by v).

namespace {

    // The shared 4-row fixture: v and g deliberately disagree on order so a dropped or
    // demoted key is visible in the output, not just in timing.
    //   id: 1   2   3   4
    //   v : 30  10  20  40
    //   g : 2   1   2   1
    void setup(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.t (id INT, v BIGINT, g INT);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(session,
                                      "INSERT INTO db.t (id, v, g) VALUES (1, 30, 2), (2, 10, 1), "
                                      "(3, 20, 2), (4, 40, 1);")
                        ->is_success());
        }
    }

    // Assert a single-BIGINT-column result equals `expected` top to bottom.
    void check_order(const components::cursor::cursor_t_ptr& cur, const std::vector<int64_t>& expected) {
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            auto cell0 = cur->get<int64_t>(0, i);
            REQUIRE_FALSE(cell0.has_error());
            REQUIRE(cell0.value() == expected[i]);
        }
    }

} // namespace

TEST_CASE("integration::cpp::order_by_expressions::unary_minus_evaluation") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/unary_eval"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.m (id INT, v BIGINT, s TEXT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher
                ->execute_sql(session, "INSERT INTO db.m (id, v, s) VALUES (1, 5, 'a'), (2, NULL, 'b'), (3, 7, 'c');")
                ->is_success());
    }

    {
        INFO("negating a TEXT column is a clean error, not a crash");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT -s FROM db.m;");
        REQUIRE(cur->is_error());
    }

    {
        INFO("negating a NULL keeps it NULL");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT -v FROM db.m;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto cell1 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell1.has_error());
        REQUIRE(cell1.value() == -5);
        auto cell2 = cur->is_null(0, 1);
        REQUIRE_FALSE(cell2.has_error());
        REQUIRE(cell2.value());
        auto cell3 = cur->get<int64_t>(0, 2);
        REQUIRE_FALSE(cell3.has_error());
        REQUIRE(cell3.value() == -7);
    }
}

TEST_CASE("integration::cpp::order_by_expressions::unary_minus") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/unary_minus"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    {
        INFO("ORDER BY -v sorts by the negated value (descending v)");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY -v;");
        check_order(cur, {40, 30, 20, 10});
    }

    {
        INFO("ORDER BY -v DESC inverts back to ascending v");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY -v DESC;");
        check_order(cur, {10, 20, 30, 40});
    }

    {
        INFO("unary plus is the identity: ORDER BY +v is ascending v");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY +v;");
        check_order(cur, {10, 20, 30, 40});
    }

    {
        INFO("nested operand: ORDER BY -(v + 1)");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY -(v + 1);");
        check_order(cur, {40, 30, 20, 10});
    }

    {
        INFO("unary minus over an INT key column");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.t ORDER BY -id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        auto cell4 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell4.has_error());
        REQUIRE(cell4.value() == 4);
        auto cell5 = cur->get<int32_t>(0, 3);
        REQUIRE_FALSE(cell5.has_error());
        REQUIRE(cell5.value() == 1);
    }

    {
        INFO("stacked unary plus strips down to the operand: ORDER BY ++v");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY ++v;");
        check_order(cur, {10, 20, 30, 40});
    }

    {
        INFO("unary plus over an expression: ORDER BY +(v + 1)");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY +(v + 1);");
        check_order(cur, {10, 20, 30, 40});
    }

    {
        INFO("double negation: ORDER BY -(-v)");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY -(-v);");
        check_order(cur, {10, 20, 30, 40});
    }

    {
        INFO("negating a TEXT sort key is a clean error, not a crash");
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.txt (s TEXT);")->is_success());
        auto session2 = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session2, "INSERT INTO db.txt (s) VALUES ('a');")->is_success());
        auto session3 = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session3, "SELECT s FROM db.txt ORDER BY -s;");
        REQUIRE(cur->is_error());
    }

    {
        INFO("ORDER BY -2 folds to a negative positional constant: clean error, not a crash");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY -2;");
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::order_by_expressions::unary_minus_double_column") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/unary_double"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.d (x DOUBLE);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.d (x) VALUES (1.5), (-2.5), (0.5);")->is_success());
    }

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT x FROM db.d ORDER BY -x;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell6 = cur->get<double>(0, 0);
    REQUIRE_FALSE(cell6.has_error());
    REQUIRE((cell6.value() > 1.4 && cell6.value() < 1.6));
    auto cell7 = cur->get<double>(0, 1);
    REQUIRE_FALSE(cell7.has_error());
    REQUIRE((cell7.value() > 0.4 && cell7.value() < 0.6));
    auto cell8 = cur->get<double>(0, 2);
    REQUIRE_FALSE(cell8.has_error());
    REQUIRE((cell8.value() > -2.6 && cell8.value() < -2.4));
}

TEST_CASE("integration::cpp::order_by_expressions::mixed_key_priority") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/mixed_keys"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    {
        INFO("a leading computed key outranks a plain tie-breaker: ORDER BY g + 0 DESC, v ASC");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY g + 0 DESC, v ASC;");
        // g DESC groups {2,2} before {1,1}; v ASC inside each group.
        check_order(cur, {20, 30, 10, 40});
    }

    {
        INFO("the same ordering with a plain leading key is the reference");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY g DESC, v ASC;");
        check_order(cur, {20, 30, 10, 40});
    }

    {
        INFO("a trailing computed key stays the tie-breaker: ORDER BY g ASC, v + 0 DESC");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY g ASC, v + 0 DESC;");
        // g ASC groups {1,1} before {2,2}; v DESC inside each group.
        check_order(cur, {40, 10, 30, 20});
    }

    {
        INFO("a computed key sandwiched between plain keys keeps its rank");
        auto session = otterbrix::session_id_t();
        // id % never ties, so use g twice: g ASC, then -v (computed) breaks ties descending v last.
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY g ASC, -v ASC, id ASC;");
        // g ASC: {10(g1), 40(g1)} then {30(g2), 20(g2)}; -v ASC = v DESC inside each group.
        check_order(cur, {40, 10, 30, 20});
    }

    {
        INFO("two computed keys keep their relative priority: ORDER BY g + 0 ASC, v + 0 DESC");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.t ORDER BY g + 0 ASC, v + 0 DESC;");
        check_order(cur, {40, 10, 30, 20});
    }

    {
        INFO("computed temp columns never leak into the result schema");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT v, g FROM db.t ORDER BY g + 0 DESC, v ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(cur->chunks().front().column_count() == 2);
    }
}

TEST_CASE("integration::cpp::order_by_expressions::mixed_key_priority_multichunk") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/mixed_multichunk"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.big (id INT, grp INT);")->is_success());
    }
    // > DEFAULT_VECTOR_CAPACITY rows so the k-way merge runs with computed keys at
    // interleaved priorities across several chunks.
    {
        std::string values;
        for (int i = 0; i < 3000; ++i) {
            if (i) {
                values += ", ";
            }
            values += "(" + std::to_string(i) + ", " + std::to_string(i % 3) + ")";
        }
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.big (id, grp) VALUES " + values + ";")->is_success());
    }

    {
        INFO("leading computed key over 3000 rows: ORDER BY grp + 0 DESC, id ASC");
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT id, grp FROM db.big ORDER BY grp + 0 DESC, id ASC LIMIT 4;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        // grp DESC puts grp==2 first; within it id ASC: 2, 5, 8, 11.
        auto cell9 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell9.has_error());
        REQUIRE(cell9.value() == 2);
        auto cell10 = cur->get<int32_t>(0, 1);
        REQUIRE_FALSE(cell10.has_error());
        REQUIRE(cell10.value() == 5);
        auto cell11 = cur->get<int32_t>(0, 2);
        REQUIRE_FALSE(cell11.has_error());
        REQUIRE(cell11.value() == 8);
        auto cell12 = cur->get<int32_t>(0, 3);
        REQUIRE_FALSE(cell12.has_error());
        REQUIRE(cell12.value() == 11);
    }
}

TEST_CASE("integration::cpp::order_by_expressions::unary_minus_empty_table") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/unary_empty"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.e (v BIGINT);")->is_success());
    }

    // The #558.A crash fired during SQL->plan transformation, before any row was read,
    // so the empty table is the minimal reproduction.
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT v FROM db.e ORDER BY -v;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 0);
}

TEST_CASE("integration::cpp::order_by_expressions::unary_over_null_rows") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/unary_null"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.n (id INT, v BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.n (id, v) VALUES (1, 5), (2, NULL), (3, 7);")
                    ->is_success());
    }

    // A NULL sort key sorts LAST (the sorter's ordering convention): -v over {5, NULL, 7}
    // is {-5, NULL, -7}, so ascending order is -7 (id 3), -5 (id 1), NULL (id 2).
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n ORDER BY -v;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell13 = cur->get<int32_t>(0, 0);
    REQUIRE_FALSE(cell13.has_error());
    REQUIRE(cell13.value() == 3);
    auto cell14 = cur->get<int32_t>(0, 1);
    REQUIRE_FALSE(cell14.has_error());
    REQUIRE(cell14.value() == 1);
    auto cell15 = cur->get<int32_t>(0, 2);
    REQUIRE_FALSE(cell15.has_error());
    REQUIRE(cell15.value() == 2);
}

TEST_CASE("integration::cpp::order_by_expressions::null_literal_arithmetic_keys") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/null_literal_keys"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.n (id INT, v BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.n (id, v) VALUES (1, 5), (2, 7);")->is_success());
    }

    // A NULL literal in arithmetic answers NULL (three-valued logic), never an operator
    // error: every key compares equal and the statement succeeds with all rows.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n ORDER BY v + NULL;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n ORDER BY -(v + NULL);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n ORDER BY -(NULL);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::order_by_expressions::computed_key_over_group_output") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/group_computed"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // The grouped validation path used to cast every ORDER BY child to sort_expression_t;
    // a computed key's direction encoding then reached column-path resolution and the
    // whole statement crashed before execution. A computed key over a GROUP BY output
    // column is valid SQL and must sort the aggregated rows.
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT g, COUNT(*) FROM db.t GROUP BY g ORDER BY g + 0 DESC;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    auto cell16 = cur->get<int32_t>(0, 0);
    REQUIRE_FALSE(cell16.has_error());
    REQUIRE(cell16.value() == 2);
    auto cell17 = cur->get<int32_t>(0, 1);
    REQUIRE_FALSE(cell17.has_error());
    REQUIRE(cell17.value() == 1);
}

TEST_CASE("integration::cpp::order_by_expressions::computed_key_over_ungrouped_column_is_error") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/group_computed_bad"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    setup(dispatcher);

    // PostgreSQL parity: an ORDER BY expression over a column that is neither grouped nor
    // aggregated must be rejected cleanly.
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT g, COUNT(*) FROM db.t GROUP BY g ORDER BY v + 0 DESC;");
    REQUIRE(cur->is_error());
}

TEST_CASE("integration::cpp::order_by_expressions::unary_minus_nulls_first") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/unary_nulls_first"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.n (id INT, v BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.n (id, v) VALUES (1, 5), (2, NULL), (3, 7);")
                    ->is_success());
    }

    // Explicit NULLS FIRST rides key.path()[1] through the computed-key spec: the NULL row
    // leads, then -v ascending = v descending (id 3 before id 1).
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n ORDER BY -v NULLS FIRST;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    auto cell18 = cur->get<int32_t>(0, 0);
    REQUIRE_FALSE(cell18.has_error());
    REQUIRE(cell18.value() == 2);
    auto cell19 = cur->get<int32_t>(0, 1);
    REQUIRE_FALSE(cell19.has_error());
    REQUIRE(cell19.value() == 3);
    auto cell20 = cur->get<int32_t>(0, 2);
    REQUIRE_FALSE(cell20.has_error());
    REQUIRE(cell20.value() == 1);
}

TEST_CASE("integration::cpp::order_by_expressions::computed_desc_nulls_placement") {
    auto config = test_create_config(integration_fixture_path("test_order_by_expressions/computed_desc_nulls"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.n (id INT, v BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db.n (id, v) VALUES (1, 5), (2, NULL), (3, 7);")
                    ->is_success());
    }

    // v + 0 is NULL for the NULL row (three-valued logic); a descending computed key
    // defaults to NULLS FIRST, and an explicit NULLS LAST overrides it.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n ORDER BY v + 0 DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto cell21 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell21.has_error());
        REQUIRE(cell21.value() == 2);
        auto cell22 = cur->get<int32_t>(0, 1);
        REQUIRE_FALSE(cell22.has_error());
        REQUIRE(cell22.value() == 3);
        auto cell23 = cur->get<int32_t>(0, 2);
        REQUIRE_FALSE(cell23.has_error());
        REQUIRE(cell23.value() == 1);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n ORDER BY v + 0 DESC NULLS LAST;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto cell24 = cur->get<int32_t>(0, 0);
        REQUIRE_FALSE(cell24.has_error());
        REQUIRE(cell24.value() == 3);
        auto cell25 = cur->get<int32_t>(0, 1);
        REQUIRE_FALSE(cell25.has_error());
        REQUIRE(cell25.value() == 1);
        auto cell26 = cur->get<int32_t>(0, 2);
        REQUIRE_FALSE(cell26.has_error());
        REQUIRE(cell26.value() == 2);
    }
}
