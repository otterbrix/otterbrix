#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/tests/temp_dir.hpp>
#include <string>
#include <variant>
#include <vector>

using namespace components;
using expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static const std::string database_name = "testdatabase";
static const std::string collection_name_1 = "testcollection_1";
static const std::string collection_name_2 = "testcollection_2";

// NOTE: JOIN conditions here use the range form `a >= b AND a <= b` rather than a
// plain `a = b`. For these (non-NULL, integer) keys the two are semantically
// identical, so every expected size/value below is unchanged — but a compound AND
// condition is not a single equi-comparison, so the optimizer keeps the nested-loop
// operator_join_t. This file therefore exercises the general join operator; the
// equi hash-join fast path is covered by test_hash_join.cpp.
TEST_CASE("integration::cpp::test_join") {
    auto config = test_create_config(test_temp_path("test_join/base"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();

    INFO("initialization");
    {
        auto session = otterbrix::session_id_t();
        {
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
            dispatcher->execute_sql(session, "CREATE TABLE " + database_name + "." + collection_name_1 + "();");
            dispatcher->execute_sql(session, "CREATE TABLE " + database_name + "." + collection_name_2 + "();");
        }
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << collection_name_1 << " (name, key_1, key_2) VALUES ";
            for (int num = 0, reversed = 100; num < 101; ++num, --reversed) {
                query << "('Name " << num << "', " << num << ", " << reversed << ")" << (reversed == 0 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);
        }
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << collection_name_2 << " (value, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "(" << (num + 25) * 2 * 10 << ", " << (num + 25) * 2 << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
    }

    INFO("inner join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_2 + ".key"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (size_t num = 0; num < 26; ++num) {
                REQUIRE(cur->value(1, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2);
                REQUIRE(cur->value(4, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2);
                REQUIRE(cur->value(3, num).value<int64_t>() == (static_cast<int64_t>(num) + 25) * 2 * 10);
                REQUIRE(cur->value(0, num).value<std::string_view>() == "Name " + std::to_string((num + 25) * 2));
            }
        }
    }

    INFO("left outer join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " LEFT OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_2 + ".key"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);

            for (size_t num = 0; num < 50; ++num) {
                REQUIRE(cur->value(1, num).value<int64_t>() == static_cast<int64_t>(num));
                REQUIRE(cur->value(4, num).is_null());
                REQUIRE(cur->value(3, num).is_null());
                REQUIRE(cur->value(0, num).value<std::string_view>() == "Name " + std::to_string(num));
            }
            size_t row = 50;
            for (int num = 0; num < 50; num += 2) {
                REQUIRE(cur->value(1, row).value<int64_t>() == num + 50);
                REQUIRE(cur->value(4, row).value<int64_t>() == num + 50);
                REQUIRE(cur->value(3, row).value<int64_t>() == (num + 50) * 10);
                REQUIRE(cur->value(0, row).value<std::string_view>() == "Name " + std::to_string(num + 50));
                ++row;
                REQUIRE(cur->value(1, row).value<int64_t>() == num + 51);
                REQUIRE(cur->value(4, row).is_null());
                REQUIRE(cur->value(3, row).is_null());
                REQUIRE(cur->value(0, row).value<std::string_view>() == "Name " + std::to_string(num + 51));
                ++row;
            }
            REQUIRE(cur->value(1, 100).value<int64_t>() == 100);
            REQUIRE(cur->value(4, 100).value<int64_t>() == 100);
            REQUIRE(cur->value(3, 100).value<int64_t>() == 1000);
            REQUIRE(cur->value(0, 100).value<std::string_view>() == "Name 100");
        }
    }

    INFO("right outer join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " RIGHT OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_2 + ".key"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC, key ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);

            for (size_t num = 0; num < 26; ++num) {
                REQUIRE(cur->value(1, num).value<int64_t>() == static_cast<int64_t>(num) * 2 + 50);
                REQUIRE(cur->value(4, num).value<int64_t>() == static_cast<int64_t>(num) * 2 + 50);
                REQUIRE(cur->value(3, num).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 50) * 10);
                REQUIRE(cur->value(0, num).value<std::string_view>() == "Name " + std::to_string(num * 2 + 50));
            }
            for (size_t num = 0; num < 74; ++num) {
                size_t row = 26 + num;
                REQUIRE(cur->value(1, row).is_null());
                REQUIRE(cur->value(4, row).value<int64_t>() == static_cast<int64_t>(num) * 2 + 102);
                REQUIRE(cur->value(3, row).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 102) * 10);
                REQUIRE(cur->value(0, row).is_null());
            }
        }
    }

    INFO("full outer join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " FULL OUTER JOIN "
                  << database_name << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_2 + ".key"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_2 + ".key"
                  << " ORDER BY key_1 ASC, key ASC;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 175);

            for (size_t num = 0; num < 50; ++num) {
                REQUIRE(cur->value(1, num).value<int64_t>() == static_cast<int64_t>(num));
                REQUIRE(cur->value(4, num).is_null());
                REQUIRE(cur->value(3, num).is_null());
                REQUIRE(cur->value(0, num).value<std::string_view>() == "Name " + std::to_string(num));
            }
            size_t row = 50;
            for (int num = 0; num < 50; num += 2) {
                REQUIRE(cur->value(1, row).value<int64_t>() == num + 50);
                REQUIRE(cur->value(4, row).value<int64_t>() == num + 50);
                REQUIRE(cur->value(3, row).value<int64_t>() == (num + 50) * 10);
                REQUIRE(cur->value(0, row).value<std::string_view>() == "Name " + std::to_string(num + 50));
                ++row;
                REQUIRE(cur->value(1, row).value<int64_t>() == num + 51);
                REQUIRE(cur->value(4, row).is_null());
                REQUIRE(cur->value(3, row).is_null());
                REQUIRE(cur->value(0, row).value<std::string_view>() == "Name " + std::to_string(num + 51));
                ++row;
            }
            REQUIRE(cur->value(1, 100).value<int64_t>() == 100);
            REQUIRE(cur->value(4, 100).value<int64_t>() == 100);
            REQUIRE(cur->value(3, 100).value<int64_t>() == 1000);
            REQUIRE(cur->value(0, 100).value<std::string_view>() == "Name 100");
            for (size_t num = 0; num < 74; ++num) {
                row = 101 + num;
                REQUIRE(cur->value(1, row).is_null());
                REQUIRE(cur->value(4, row).value<int64_t>() == static_cast<int64_t>(num) * 2 + 102);
                REQUIRE(cur->value(3, row).value<int64_t>() == (static_cast<int64_t>(num) * 2 + 102) * 10);
                REQUIRE(cur->value(0, row).is_null());
            }
        }
    }

    INFO("cross join");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " CROSS JOIN " << database_name
                  << "." << collection_name_2 << ";";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10100);
        }
    }

    INFO("inner join ON true");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON TRUE;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10100);
        }
    }

    INFO("left join ON true");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " LEFT JOIN " << database_name
                  << "." << collection_name_2 << " ON TRUE;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10100);
        }
    }

    INFO("two join predicates");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key AND " << collection_name_1 << ".key_2"
                  << " = " << collection_name_2 + ".key;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("two join predicates, with const");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_2 << " ON " << collection_name_1 << ".key_1"
                  << " = " << collection_name_2 + ".key AND " << collection_name_2 << ".key"
                  << " > "
                  << "75;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);
        }
    }

    INFO("self join ");
    {
        auto session = otterbrix::session_id_t();
        {
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " INNER JOIN " << database_name
                  << "." << collection_name_1 << " ON " << collection_name_1 << ".key_1"
                  << " >= " << collection_name_1 + ".key_2"
                  << " AND " << collection_name_1 << ".key_1"
                  << " <= " << collection_name_1 + ".key_2;";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 101);
        }
    }

    INFO("inner join + group by + aggregates + order + limit");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT c1.name, COUNT(c2.value) AS cnt, AVG(c2.key) AS avg_key "
                                           "FROM testdatabase.testcollection_1 c1 "
                                           "INNER JOIN testdatabase.testcollection_2 c2 "
                                           "  ON c1.key_1 >= c2.key AND c1.key_1 <= c2.key "
                                           "GROUP BY c1.name "
                                           "ORDER BY avg_key DESC "
                                           "LIMIT 10;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
    }

    INFO("triple inner join with shared-name keys in both joins");
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE " + database_name + ".col_mid();");
        dispatcher->execute_sql(session, "CREATE TABLE " + database_name + ".col_end();");
        {
            std::stringstream query;
            query << "INSERT INTO " << database_name << ".col_mid (key_1, linker) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "(" << (num + 25) * 2 << ", " << (num + 25) * 2 * 10 << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
        }
        {
            auto cur = dispatcher->execute_sql(session,
                                               "INSERT INTO " + database_name +
                                                   ".col_end (linker, extra) VALUES "
                                                   "(500, 1), (700, 2), (1000, 3), (1500, 4), (2000, 5);");
            REQUIRE(cur->is_success());
        }

        // First join: 26 rows (key_1 in {50,52,..,100}, linker in {500,520,..,1000}).
        // Second join: intersect linker with {500,700,1000,1500,2000} → 3 rows.
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT testcollection_1.name, col_end.extra "
                                           "FROM testdatabase.testcollection_1 "
                                           "INNER JOIN testdatabase.col_mid "
                                           "  ON testcollection_1.key_1 >= col_mid.key_1 "
                                           "     AND testcollection_1.key_1 <= col_mid.key_1 "
                                           "INNER JOIN testdatabase.col_end "
                                           "  ON col_mid.linker >= col_end.linker "
                                           "     AND col_mid.linker <= col_end.linker "
                                           "ORDER BY col_end.extra ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("triple inner join — aliases");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT camp.name, ord.extra "
                                           "FROM testdatabase.testcollection_1 camp "
                                           "INNER JOIN testdatabase.col_mid mid "
                                           "  ON camp.key_1 >= mid.key_1 AND camp.key_1 <= mid.key_1 "
                                           "INNER JOIN testdatabase.col_end ord "
                                           "  ON mid.linker >= ord.linker AND mid.linker <= ord.linker "
                                           "ORDER BY ord.extra ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("triple inner join — second JOIN references first table");
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE " + database_name + ".col_aux();");
        {
            auto cur =
                dispatcher->execute_sql(session,
                                        "INSERT INTO " + database_name +
                                            ".col_aux (k, tag) VALUES (50, 'a'), (54, 'b'), (60, 'c'), (200, 'd');");
            REQUIRE(cur->is_success());
        }
        {
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT c.name, x.tag "
                                               "FROM testdatabase.testcollection_1 c "
                                               "INNER JOIN testdatabase.col_mid m "
                                               "  ON c.key_1 >= m.key_1 AND c.key_1 <= m.key_1 "
                                               "INNER JOIN testdatabase.col_aux x "
                                               "  ON c.key_1 >= x.k AND c.key_1 <= x.k "
                                               "ORDER BY x.tag ASC;");
            REQUIRE(cur->is_success());
            // c.key_1 = {50,52,...,100}, x.k = {50,54,60,200} = {50,54,60} → 3 rows.
            REQUIRE(cur->size() == 3);
        }
    }
}

namespace {
    // The result's column names, in output order.
    std::vector<std::string> result_column_names(const cursor::cursor_t& cur) {
        std::vector<std::string> names;
        names.reserve(cur.columns().size());
        for (const auto& column : cur.columns()) {
            names.emplace_back(column.name.data(), column.name.size());
        }
        return names;
    }
} // namespace

// A join MERGES two inputs into ONE chunk, and the merged chunk records the split
// nowhere: the only user-visible trace of which column came from which side is the
// column NAME. The join's output-schema currency is what carries those names from the
// two input chunks to the output chunk, so this pins them for every join type the
// nested-loop operator serves — matched rows, left-only NULL padding and the
// right-only drain that runs in finalize(), which is a separate builder instance and
// therefore a separate chance to lose them.
//
// The ON conditions use the range form (see the file note) so the optimizer leaves
// the nested-loop operator_join_t in place; test_hash_join.cpp pins the equi path.
TEST_CASE("integration::cpp::test_join::output_column_names") {
    auto config = test_create_config(test_temp_path("test_join/names"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();

    const std::string db = "joinnamesdb";
    REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE " + db + ";")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + db + ".jl();")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE " + db + ".jr();")->is_success());
    REQUIRE(dispatcher->execute_sql(session, "INSERT INTO " + db + ".jl (lk, lv) VALUES (1, 10), (2, 20);")
                ->is_success());
    REQUIRE(dispatcher->execute_sql(session, "INSERT INTO " + db + ".jr (rk, rv) VALUES (1, 100), (3, 300);")
                ->is_success());

    // Logical [left, right] output order, both sides named from their own table.
    const std::vector<std::string> expected{"lk", "lv", "rk", "rv"};
    const std::string on = " ON jl.lk >= jr.rk AND jl.lk <= jr.rk";

    INFO("inner — matched rows only");
    {
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + db + ".jl INNER JOIN " + db + ".jr" + on + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(result_column_names(*cur) == expected);
    }

    INFO("left — matched rows plus a left-only row NULL-padded on the right");
    {
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + db + ".jl LEFT JOIN " + db + ".jr" + on + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(result_column_names(*cur) == expected);
    }

    INFO("right — the unmatched build row is drained by finalize(), a second builder");
    {
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + db + ".jl RIGHT JOIN " + db + ".jr" + on + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(result_column_names(*cur) == expected);
    }

    INFO("full — both padding directions in one result");
    {
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + db + ".jl FULL JOIN " + db + ".jr" + on + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(result_column_names(*cur) == expected);
    }

    INFO("cross — every pair, no predicate");
    {
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + db + ".jl CROSS JOIN " + db + ".jr;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        REQUIRE(result_column_names(*cur) == expected);
    }

    // The shape the scans answer with make_drain_chunk: a user-visible result with no
    // rows. A join has no such chunk — join_builder::flush() returns without emitting
    // when nothing was buffered — so a join that matches nothing answers with NO
    // columns at all, not with unnamed ones. Pinned so that the difference between
    // "names lost" and "schema never produced" stays visible.
    INFO("zero matches — the result carries no column descriptors at all");
    {
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM " + db + ".jl INNER JOIN " + db + ".jr ON jl.lk >= jr.rv AND jl.lk <= jr.rv;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
        REQUIRE(result_column_names(*cur).empty());
    }

    // The contrast that makes the line above worth pinning: the same "no rows" answer
    // from a SCAN does name its columns, because a scan answers an exhausted stream
    // with make_drain_chunk — an empty chunk built from its type list. A join has no
    // equivalent, so the two disagree on what an empty result looks like. Recorded as
    // an asymmetry, not endorsed: closing it is a change to WHAT an empty join emits,
    // which is a different question from what its schema currency carries.
    INFO("contrast — a zero-row SCAN does name its columns");
    {
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + db + ".jl WHERE jl.lk = 9999;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
        const std::vector<std::string> left_only{"lk", "lv"};
        REQUIRE(result_column_names(*cur) == left_only);
    }
}

// A join output column is its source column, gathered, so it owes that column BOTH
// halves of its identity — the name and the attoid (M3-B4/B5).
//
// That is worth a guard because attoid is not inert at the storage boundary: the append
// matcher resolves an incoming write-set column to a table column by attoid FIRST and
// falls back to the name only for columns that have none (agent_disk.cpp, "identity
// outranks name"). A join feeding an INSERT therefore arrives carrying identities, and
// the values still have to land where the target column list says.
//
// They do, and the reason is structural rather than lucky: the identities a join emits
// belong to its SOURCE tables, an INSERT target's attoids are allocated separately, and
// the one projection shape that preserves identity — the unprojected star — is also the
// one that forces the FULL joined width, which can never match the arity of a single
// source table. So an incoming identity can never collide with a target column of the
// table being written. The arms below hold both ends of that: identity present on the
// join output, values routed by the target list.
TEST_CASE("integration::cpp::test_join::joined_write_set_column_routing") {
    auto config = test_create_config(test_temp_path("test_join/write_set_routing"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };

    REQUIRE(run("CREATE DATABASE wsdb;")->is_success());
    REQUIRE(run("CREATE TABLE wsdb.b (k BIGINT, p BIGINT);")->is_success());
    REQUIRE(run("INSERT INTO wsdb.b (k, p) VALUES (1, 77);")->is_success());
    REQUIRE(run("CREATE TABLE wsdb.u (x BIGINT, y BIGINT);")->is_success());
    REQUIRE(run("INSERT INTO wsdb.u (x, y) VALUES (1, 2);")->is_success());

    // The identity is compared against the same column read through a plain scan, so the
    // assertion says "the join preserved it", not "it happens to be some number".
    INFO("a joined column carries the SAME catalog identity as the column it gathers");
    {
        auto scan_u = run("SELECT * FROM wsdb.u;");
        REQUIRE(scan_u->is_success());
        REQUIRE(scan_u->columns().size() == 2);
        auto scan_b = run("SELECT * FROM wsdb.b;");
        REQUIRE(scan_b->is_success());
        REQUIRE(scan_b->columns().size() == 2);
        auto joined = run("SELECT * FROM wsdb.u INNER JOIN wsdb.b ON u.x = b.k;");
        REQUIRE(joined->is_success());
        REQUIRE(joined->columns().size() == 4);
        // Logical [left, right]: u.x, u.y, b.k, b.p.
        CHECK(joined->columns()[0].attoid == scan_u->columns()[0].attoid);
        CHECK(joined->columns()[1].attoid == scan_u->columns()[1].attoid);
        CHECK(joined->columns()[2].attoid == scan_b->columns()[0].attoid);
        CHECK(joined->columns()[3].attoid == scan_b->columns()[1].attoid);
        // And they are real identities, not four copies of "none".
        CHECK(joined->columns()[0].attoid != catalog::INVALID_OID);
        CHECK(joined->columns()[3].attoid != catalog::INVALID_OID);
    }

    INFO("values taken from both sides of a join land in the named target columns");
    {
        // x <- u.y (2), y <- b.p (77). u is BOTH a join source and the insert target.
        REQUIRE(run("INSERT INTO wsdb.u (x, y) SELECT u.y, b.p FROM wsdb.u u INNER JOIN wsdb.b b ON u.x = b.k;")
                    ->is_success());
        auto cur = run("SELECT x, y FROM wsdb.u ORDER BY y ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        CHECK(cur->value(0, 0).value<int64_t>() == 1);
        CHECK(cur->value(1, 0).value<int64_t>() == 2);
        CHECK(cur->value(0, 1).value<int64_t>() == 2);
        CHECK(cur->value(1, 1).value<int64_t>() == 77);
    }

    // A QUALIFIED star is a projection, and a projection rebuilds its columns as
    // expression results with no identity — which is why this reversed target list is
    // routed by name and lands correctly. Asserted rather than assumed, because it is
    // half of why the collision above is unreachable.
    INFO("a qualified star through a join is projected: identity-free, so name-routed");
    {
        REQUIRE(run("CREATE TABLE wsdb.z4 (a BIGINT, c BIGINT);")->is_success());
        REQUIRE(run("INSERT INTO wsdb.z4 (a, c) VALUES (1, 2);")->is_success());
        REQUIRE(run("CREATE TABLE wsdb.z5 (k BIGINT);")->is_success());
        REQUIRE(run("INSERT INTO wsdb.z5 (k) VALUES (1);")->is_success());
        auto qual = run("SELECT z4.* FROM wsdb.z4 INNER JOIN wsdb.z5 ON z4.a = z5.k;");
        REQUIRE(qual->is_success());
        REQUIRE(qual->columns().size() == 2);
        CHECK(qual->columns()[0].attoid == catalog::INVALID_OID);
        CHECK(qual->columns()[1].attoid == catalog::INVALID_OID);

        REQUIRE(run("INSERT INTO wsdb.z4 (c, a) SELECT z4.* FROM wsdb.z4 INNER JOIN wsdb.z5 ON z4.a = z5.k;")
                    ->is_success());
        auto cur = run("SELECT a, c FROM wsdb.z4 ORDER BY a ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // The star row (a=1, c=2) written into (c, a) must land as (a=2, c=1).
        CHECK(cur->value(0, 1).value<int64_t>() == 2);
        CHECK(cur->value(1, 1).value<int64_t>() == 1);
    }
}

// An INSERT with an explicit target column list routes by that list, whatever the source
// columns were called or which catalog columns they came from.
//
// The shape this guards is `INSERT INTO z (c, a) SELECT * FROM z`, which used to write
// both values straight back into the columns they were read from and report success. The
// chain: a storage scan stamps each output column with its table's attoid
// (data_table_t::stamp_column_identity); an UNPROJECTED star carries those columns
// through untouched, so the write set arrived at storage still identified as z's own
// columns; operator_insert renamed the columns positionally onto the target list but left
// the source attoid on them; and the append matcher prefers identity over name
// (agent_disk.cpp, pass 1), so the stale identity beat the corrected name.
//
// operator_insert now clears the identity of every column of a renamed write set: a
// positional rename decides what a column MEANS, so the identity it arrived with no longer
// describes it, and a target-listed write set is exactly the identity-free input that the
// matcher's name pass exists to serve. "Identity outranks name" is untouched.
//
// It was specifically the identity, not the star: the same statement with an EXPLICIT
// projection was always correct, because a projection rebuilds its columns as expression
// results with no attoid. That arm is kept below as the control.
TEST_CASE("integration::cpp::test_join::unprojected_star_insert_honours_target_list") {
    auto config = test_create_config(test_temp_path("test_join/star_insert_gap"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto dispatcher = space.dispatcher();
    auto session = otterbrix::session_id_t();
    auto run = [&](const std::string& sql) { return dispatcher->execute_sql(session, sql); };

    REQUIRE(run("CREATE DATABASE stardb;")->is_success());

    INFO("an unprojected star, self-insert: the target list wins over the source identity");
    {
        REQUIRE(run("CREATE TABLE stardb.z (a BIGINT, c BIGINT);")->is_success());
        REQUIRE(run("INSERT INTO stardb.z (a, c) VALUES (1, 2);")->is_success());
        REQUIRE(run("INSERT INTO stardb.z (c, a) SELECT * FROM stardb.z;")->is_success());
        auto cur = run("SELECT a, c FROM stardb.z ORDER BY a ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // The star row (a=1, c=2) written into (c, a) lands as (a=2, c=1). Before the fix
        // BOTH rows read (1,2): the inserted row went back into the columns it came from.
        CHECK(cur->value(0, 0).value<int64_t>() == 1);
        CHECK(cur->value(1, 0).value<int64_t>() == 2);
        CHECK(cur->value(1, 1).value<int64_t>() == 1);
        CHECK(cur->value(0, 1).value<int64_t>() == 2);
    }

    INFO("the control: the same statement with an explicit projection routes correctly");
    {
        REQUIRE(run("CREATE TABLE stardb.z3 (a BIGINT, c BIGINT);")->is_success());
        REQUIRE(run("INSERT INTO stardb.z3 (a, c) VALUES (1, 2);")->is_success());
        REQUIRE(run("INSERT INTO stardb.z3 (c, a) SELECT z3.a, z3.c FROM stardb.z3;")->is_success());
        auto cur = run("SELECT a, c FROM stardb.z3 ORDER BY a ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        CHECK(cur->value(0, 1).value<int64_t>() == 2);
        CHECK(cur->value(1, 1).value<int64_t>() == 1);
    }

    // Two columns swap symmetrically: a mis-route that sends every value home reads the
    // same as one that swaps them twice. A THREE-column rotation has no such symmetry —
    // (c, a, b) maps 1→c, 2→a, 3→b, so every column ends up holding a value no other
    // routing would put there, and "wrong" cannot look like "right".
    INFO("a three-column rotation: no symmetry for a mis-route to hide behind");
    {
        REQUIRE(run("CREATE TABLE stardb.p (a BIGINT, b BIGINT, c BIGINT);")->is_success());
        REQUIRE(run("INSERT INTO stardb.p (a, b, c) VALUES (10, 20, 30);")->is_success());
        REQUIRE(run("INSERT INTO stardb.p (c, a, b) SELECT * FROM stardb.p;")->is_success());
        auto cur = run("SELECT a, b, c FROM stardb.p ORDER BY a ASC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // Source row (a=10, b=20, c=30) into (c, a, b) => (a=20, b=30, c=10).
        CHECK(cur->value(0, 0).value<int64_t>() == 10);
        CHECK(cur->value(1, 0).value<int64_t>() == 20);
        CHECK(cur->value(2, 0).value<int64_t>() == 30);
        CHECK(cur->value(0, 1).value<int64_t>() == 20);
        CHECK(cur->value(1, 1).value<int64_t>() == 30);
        CHECK(cur->value(2, 1).value<int64_t>() == 10);
    }

    // The other half of the same rule, and the arm that says the fix did not simply
    // disable identity routing: a star from a DIFFERENT table carries identities that
    // belong to no column of the target, so the target list was always the only thing
    // that could route it. It has to still route it.
    INFO("an unprojected star from a different table still routes by the target list");
    {
        REQUIRE(run("CREATE TABLE stardb.src (m BIGINT, n BIGINT);")->is_success());
        REQUIRE(run("INSERT INTO stardb.src (m, n) VALUES (7, 8);")->is_success());
        REQUIRE(run("CREATE TABLE stardb.dst (a BIGINT, c BIGINT);")->is_success());
        REQUIRE(run("INSERT INTO stardb.dst (c, a) SELECT * FROM stardb.src;")->is_success());
        auto cur = run("SELECT a, c FROM stardb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == 8);
        CHECK(cur->value(1, 0).value<int64_t>() == 7);
    }
}

