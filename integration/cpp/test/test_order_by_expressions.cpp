#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>
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
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
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
            REQUIRE(cur->value(0, i).value<int64_t>() == expected[i]);
        }
    }

} // namespace

TEST_CASE("integration::cpp::order_by_expressions::unary_minus_evaluation") {
    auto config = test_create_config("/tmp/test_order_by_expressions/unary_eval");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.m (id INT, v BIGINT, s TEXT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "INSERT INTO db.m (id, v, s) VALUES (1, 5, 'a'), (2, NULL, 'b'), (3, 7, 'c');")
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
        REQUIRE(cur->value(0, 0).value<int64_t>() == -5);
        REQUIRE(cur->value(0, 1).is_null());
        REQUIRE(cur->value(0, 2).value<int64_t>() == -7);
    }
}

TEST_CASE("integration::cpp::order_by_expressions::unary_minus") {
    auto config = test_create_config("/tmp/test_order_by_expressions/unary_minus");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
        REQUIRE(cur->value(0, 0).value<int32_t>() == 4);
        REQUIRE(cur->value(0, 3).value<int32_t>() == 1);
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
    auto config = test_create_config("/tmp/test_order_by_expressions/unary_double");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
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
    REQUIRE((cur->value(0, 0).value<double>() > 1.4 && cur->value(0, 0).value<double>() < 1.6));
    REQUIRE((cur->value(0, 1).value<double>() > 0.4 && cur->value(0, 1).value<double>() < 0.6));
    REQUIRE((cur->value(0, 2).value<double>() > -2.6 && cur->value(0, 2).value<double>() < -2.4));
}

TEST_CASE("integration::cpp::order_by_expressions::mixed_key_priority") {
    auto config = test_create_config("/tmp/test_order_by_expressions/mixed_keys");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
    auto config = test_create_config("/tmp/test_order_by_expressions/mixed_multichunk");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
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
        REQUIRE(cur->value(0, 0).value<int32_t>() == 2);
        REQUIRE(cur->value(0, 1).value<int32_t>() == 5);
        REQUIRE(cur->value(0, 2).value<int32_t>() == 8);
        REQUIRE(cur->value(0, 3).value<int32_t>() == 11);
    }
}

TEST_CASE("integration::cpp::order_by_expressions::unary_minus_empty_table") {
    auto config = test_create_config("/tmp/test_order_by_expressions/unary_empty");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
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
    auto config = test_create_config("/tmp/test_order_by_expressions/unary_null");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.n (id INT, v BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session, "INSERT INTO db.n (id, v) VALUES (1, 5), (2, NULL), (3, 7);")
                    ->is_success());
    }

    // A NULL sort key sorts LAST (the sorter's ordering convention): -v over {5, NULL, 7}
    // is {-5, NULL, -7}, so ascending order is -7 (id 3), -5 (id 1), NULL (id 2).
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, "SELECT id FROM db.n ORDER BY -v;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
    REQUIRE(cur->value(0, 0).value<int32_t>() == 3);
    REQUIRE(cur->value(0, 1).value<int32_t>() == 1);
    REQUIRE(cur->value(0, 2).value<int32_t>() == 2);
}

TEST_CASE("integration::cpp::order_by_expressions::null_literal_arithmetic_keys") {
    auto config = test_create_config("/tmp/test_order_by_expressions/null_literal_keys");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db.n (id INT, v BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session, "INSERT INTO db.n (id, v) VALUES (1, 5), (2, 7);")
                    ->is_success());
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
