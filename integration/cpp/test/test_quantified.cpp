#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <string>
#include <vector>

components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
    auto session = otterbrix::session_id_t();
    return dispatcher->execute_sql(session, sql);
}

void run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
    auto cur = exec(dispatcher, sql);
    INFO(sql << (cur->is_error() ? std::string(": ") + cur->get_error().what.c_str() : std::string{}));
    REQUIRE(cur->is_success());
}

// The ids `SELECT id FROM <table> WHERE <predicate>` answers, in order
std::vector<int64_t>
ids_where(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& predicate, const std::string& table = "q.t") {
    auto cur = exec(dispatcher, "SELECT id FROM " + table + " WHERE " + predicate + ";");
    INFO(predicate << (cur->is_error() ? std::string(": ") + cur->get_error().what.c_str() : std::string{}));
    REQUIRE(cur->is_success());
    std::vector<int64_t> ids;
    for (std::size_t row = 0; row < cur->size(); ++row) {
        auto cell = cur->get<int64_t>(0, row);
        REQUIRE_FALSE(cell.has_error());
        ids.push_back(cell.value());
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

using ids_t = std::vector<int64_t>;

TEST_CASE("integration::cpp::quantified") {
    auto config = test_create_config(integration_fixture_path("test_quantified"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run(dispatcher, "CREATE SCHEMA q;");
    run(dispatcher, "CREATE TABLE q.t (id bigint, x bigint, l bigint[], a bigint[3], m int[]);");
    run(dispatcher, "INSERT INTO q.t (id, x, l, a, m) VALUES (1, 1, ARRAY[1,2,3], ARRAY[1,2,3], ARRAY[1]);");
    run(dispatcher, "INSERT INTO q.t (id, x, l, a, m) VALUES (2, 5, ARRAY[1,2,3], ARRAY[4,5,6], ARRAY[5]);");
    run(dispatcher, "INSERT INTO q.t (id, x, l, a, m) VALUES (3, NULL, ARRAY[1,2], ARRAY[1,2,3], NULL);");
    run(dispatcher, "INSERT INTO q.t (id, x, l, a, m) VALUES (4, 10, ARRAY[], ARRAY[10,NULL,30], NULL);");
    run(dispatcher, "INSERT INTO q.t (id, x, l, a, m) VALUES (5, 7, NULL, NULL, NULL);");
    run(dispatcher, "INSERT INTO q.t (id, x, l, a, m) VALUES (6, 2, ARRAY[2,NULL], ARRAY[7,8,9], NULL);");
    run(dispatcher, "CREATE TABLE q.s (v bigint);");
    run(dispatcher, "INSERT INTO q.s (v) VALUES (1), (5), (NULL);");

    SECTION("list column") {
        CHECK(ids_where(dispatcher, "x = ANY(l)") == ids_t{1, 6});
        CHECK(ids_where(dispatcher, "x <> ALL(l)") == ids_t{2, 4});
        CHECK(ids_where(dispatcher, "NOT (x = ANY(l))") == ids_t{2, 4});
        CHECK(ids_where(dispatcher, "x > ANY(l)") == ids_t{2});
        CHECK(ids_where(dispatcher, "x >= ALL(l)") == ids_t{2, 4});
    }

    SECTION("array column") {
        CHECK(ids_where(dispatcher, "x = ANY(a)") == ids_t{1, 2, 4});
        CHECK(ids_where(dispatcher, "x < ANY(a)") == ids_t{1, 2, 4, 6});
        CHECK(ids_where(dispatcher, "x < ALL(a)") == ids_t{6});
    }

    SECTION("NULL element") {
        // 20 is in no set: FALSE where every element is known, NULL where one is NULL or the set is
        CHECK(ids_where(dispatcher, "20 = ANY(a)").empty());
        CHECK(ids_where(dispatcher, "NOT (20 = ANY(a))") == ids_t{1, 2, 3, 6});
    }

    SECTION("literal set") {
        CHECK(ids_where(dispatcher, "x = ANY(ARRAY[5,10])") == ids_t{2, 4});
        CHECK(ids_where(dispatcher, "x <> ALL(ARRAY[5,10])") == ids_t{1, 5, 6});
        CHECK(ids_where(dispatcher, "1 = ANY(ARRAY[1,2])") == ids_t{1, 2, 3, 4, 5, 6});
        CHECK(ids_where(dispatcher, "3 = ANY(ARRAY[1,2])").empty());
    }

    SECTION("cast probe") { CHECK(ids_where(dispatcher, "x = ANY(ARRAY[1.0, 5.0])") == ids_t{1, 2}); }

    SECTION("cast set") { CHECK(ids_where(dispatcher, "x = ANY(m)") == ids_t{1, 2}); }

    SECTION("sub-query") {
        CHECK(ids_where(dispatcher, "x IN (SELECT v FROM q.s)") == ids_t{1, 2});
        // the NULL in the set leaves no row TRUE
        CHECK(ids_where(dispatcher, "x NOT IN (SELECT v FROM q.s)").empty());
        CHECK(ids_where(dispatcher, "x NOT IN (SELECT v FROM q.s WHERE v IS NOT NULL)") == ids_t{4, 5, 6});
        CHECK(ids_where(dispatcher, "x > ALL (SELECT v FROM q.s WHERE v IS NOT NULL)") == ids_t{4, 5});
    }

    SECTION("empty sub-query") {
        CHECK(ids_where(dispatcher, "x = ANY (SELECT v FROM q.s WHERE v > 100)").empty());
        CHECK(ids_where(dispatcher, "x <> ALL (SELECT v FROM q.s WHERE v > 100)") == ids_t{1, 2, 3, 4, 5, 6});
        CHECK(ids_where(dispatcher, "NOT (x = ANY (SELECT v FROM q.s WHERE v > 100))") == ids_t{1, 2, 3, 4, 5, 6});
    }

    SECTION("sub-query over several chunks") {
        run(dispatcher, "CREATE TABLE q.big (v bigint);");
        for (int base = 0; base < 3000; base += 1000) {
            std::string sql = "INSERT INTO q.big (v) VALUES ";
            for (int i = 0; i < 1000; ++i) {
                sql += (i == 0 ? "(" : ", (") + std::to_string(base + i) + ")";
            }
            run(dispatcher, sql + ";");
        }
        CHECK(ids_where(dispatcher, "x IN (SELECT v FROM q.big)") == ids_t{1, 2, 4, 5, 6});
        CHECK(ids_where(dispatcher, "x IN (SELECT v FROM q.big WHERE v > 2990)").empty());
        CHECK(ids_where(dispatcher, "x < ALL (SELECT v FROM q.big WHERE v > 5)") == ids_t{1, 2, 6});
    }

    SECTION("like") {
        run(dispatcher, "CREATE TABLE q.w (id bigint, s text, p text[]);");
        run(dispatcher, "INSERT INTO q.w (id, s, p) VALUES (1, 'apple', ARRAY['a%', '%z']);");
        run(dispatcher, "INSERT INTO q.w (id, s, p) VALUES (2, 'Banana', ARRAY['b%']);");
        run(dispatcher, "INSERT INTO q.w (id, s, p) VALUES (3, NULL, ARRAY['%']);");
        run(dispatcher, "INSERT INTO q.w (id, s, p) VALUES (4, 'cherry', ARRAY[]);");
        run(dispatcher, "INSERT INTO q.w (id, s, p) VALUES (5, 'date', NULL);");
        run(dispatcher, "INSERT INTO q.w (id, s, p) VALUES (6, 'egg', ARRAY['e%', NULL]);");
        run(dispatcher, "CREATE TABLE q.pats (v text);");
        run(dispatcher, "INSERT INTO q.pats (v) VALUES ('a%'), ('%na%');");
        run(dispatcher, "CREATE TABLE q.null_pats (v text);");
        run(dispatcher, "INSERT INTO q.null_pats (v) VALUES ('a%'), (NULL);");

        SECTION("list column") {
            CHECK(ids_where(dispatcher, "s LIKE ANY(p)", "q.w") == ids_t{1, 6});
            CHECK(ids_where(dispatcher, "s ILIKE ANY(p)", "q.w") == ids_t{1, 2, 6});
            CHECK(ids_where(dispatcher, "s LIKE ALL(p)", "q.w") == ids_t{4});
            CHECK(ids_where(dispatcher, "s NOT LIKE ALL(p)", "q.w") == ids_t{2, 4});
        }

        SECTION("literal set") {
            CHECK(ids_where(dispatcher, "s LIKE ANY(ARRAY['%an%', 'e%'])", "q.w") == ids_t{2, 6});
            CHECK(ids_where(dispatcher, "s NOT ILIKE ALL(ARRAY['A%', 'B%'])", "q.w") == ids_t{4, 5, 6});
        }

        SECTION("sub-query") {
            CHECK(ids_where(dispatcher, "s LIKE ANY (SELECT v FROM q.pats)", "q.w") == ids_t{1, 2});
            CHECK(ids_where(dispatcher, "s NOT LIKE ALL (SELECT v FROM q.pats)", "q.w") == ids_t{4, 5, 6});
        }

        SECTION("NULL pattern") {
            CHECK(ids_where(dispatcher, "s LIKE ANY (SELECT v FROM q.null_pats)", "q.w") == ids_t{1});
            CHECK(ids_where(dispatcher, "NOT (s LIKE ANY (SELECT v FROM q.null_pats))", "q.w").empty());
        }

        SECTION("empty sub-query") {
            CHECK(ids_where(dispatcher, "s LIKE ANY (SELECT v FROM q.pats WHERE v = 'none')", "q.w").empty());
            CHECK(ids_where(dispatcher, "s NOT LIKE ALL (SELECT v FROM q.pats WHERE v = 'none')", "q.w") ==
                  ids_t{1, 2, 3, 4, 5, 6});
        }

        SECTION("non-text set") {
            CHECK(exec(dispatcher, "SELECT id FROM q.w WHERE s LIKE ANY(ARRAY[1, 2]);")->is_error());
            CHECK(exec(dispatcher, "SELECT id FROM q.w WHERE s LIKE ANY (SELECT id FROM q.w);")->is_error());
        }
    }
}
