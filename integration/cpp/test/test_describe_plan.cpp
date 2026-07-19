// describe_plan: "what columns/types will this plan return" WITHOUT executing.
// Runs the full pre-execution pipeline (resolve → validate → enrich → optimize →
// physical build), stops before execution and answers a zero-row cursor typed
// from the root's resolved output schema. This is the engine-side answer wire
// protocols need BEFORE execution (PG Parse/Describe, MySQL COM_STMT_PREPARE,
// Flight GetFlightInfo, Spark AnalyzePlan).

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>

using namespace components;

namespace {

    // Parse SQL and describe it through the wrapper (finalize_for_describe path —
    // parameters may stay unbound, exactly like PG Describe before Bind).
    cursor::cursor_t_ptr describe_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto* res = dispatcher->resource();
        std::pmr::monotonic_buffer_resource arena(res);
        sql::transform::transformer transformer(res);
        auto* raw = raw_parser(&arena, sql.c_str());
        REQUIRE(raw != nullptr);
        auto& ast_ref = sql::transform::pg_cell_to_node_cast(linitial(raw));
        auto binder = transformer.transform(ast_ref);
        REQUIRE_FALSE(binder.has_error());
        auto plan = binder.finalize_for_describe();
        REQUIRE_FALSE(plan.has_error());
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_plan(session, std::move(plan.value()));
    }

    // Execute for real (all params must be inline) — the reference answer.
    cursor::cursor_t_ptr run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    std::vector<std::pair<std::string, types::logical_type>> cursor_columns(const cursor::cursor_t_ptr& cur) {
        std::vector<std::pair<std::string, types::logical_type>> out;
        if (cur->chunks().empty()) {
            return out;
        }
        for (const auto& t : cur->chunks().front().types()) {
            out.emplace_back(t.has_alias() ? t.alias() : std::string{}, t.type());
        }
        return out;
    }

} // namespace

TEST_CASE("integration::cpp::describe_plan") {
    auto config = test_create_config("/tmp/test_describe_plan/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup schema + data");
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE ddb;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "CREATE TABLE ddb.orders (id BIGINT, name TEXT, amount BIGINT, price DOUBLE);")
                    ->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "INSERT INTO ddb.orders (id, name, amount, price) VALUES "
                                  "(1, 'a', 10, 1.5), (2, 'b', 20, 2.5), (3, 'a', 30, 3.5);")
                    ->is_success());
    }

    INFO("describe == execution types: projection");
    {
        const std::string q = "SELECT name, amount FROM ddb.orders;";
        auto desc = describe_sql(dispatcher, q);
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 0); // zero rows — describe never executes
        auto ran = run_sql(dispatcher, q);
        REQUIRE(ran->is_success());
        REQUIRE(cursor_columns(desc) == cursor_columns(ran));
        REQUIRE(cursor_columns(desc).size() == 2);
    }

    INFO("describe == execution types: GROUP BY + aggregate");
    {
        const std::string q = "SELECT name, SUM(amount) AS total FROM ddb.orders GROUP BY name;";
        auto desc = describe_sql(dispatcher, q);
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 0);
        auto ran = run_sql(dispatcher, q);
        REQUIRE(ran->is_success());
        REQUIRE(cursor_columns(desc) == cursor_columns(ran));
    }

    INFO("describe == execution types: JOIN");
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE ddb.users (id BIGINT, uname TEXT);")->is_success());
        auto session2 = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session2, "INSERT INTO ddb.users (id, uname) VALUES (1, 'u1');")->is_success());
        const std::string q =
            "SELECT o.name, u.uname FROM ddb.orders AS o JOIN ddb.users AS u ON o.id = u.id;";
        auto desc = describe_sql(dispatcher, q);
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 0);
        auto ran = run_sql(dispatcher, q);
        REQUIRE(ran->is_success());
        REQUIRE(cursor_columns(desc) == cursor_columns(ran));
    }

    INFO("pre-bind describe: unbound $1 (PG Describe-before-Bind)");
    {
        auto desc = describe_sql(dispatcher, "SELECT name, price FROM ddb.orders WHERE id = $1;");
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 0);
        auto cols = cursor_columns(desc);
        REQUIRE(cols.size() == 2);
        REQUIRE(cols[0].first == "name");
        REQUIRE(cols[1].first == "price");
    }

    INFO("describe INSERT ... RETURNING: schema without inserting");
    {
        auto desc = describe_sql(dispatcher,
                                 "INSERT INTO ddb.orders (id, name, amount, price) VALUES (9, 'z', 90, 9.5) "
                                 "RETURNING id, name;");
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 0);
        auto cols = cursor_columns(desc);
        REQUIRE(cols.size() == 2);
        REQUIRE(cols[0].first == "id");
        REQUIRE(cols[0].second == types::logical_type::BIGINT);
        REQUIRE(cols[1].first == "name");
        // No side effects: the row was NOT inserted.
        auto check = run_sql(dispatcher, "SELECT * FROM ddb.orders WHERE id = 9;");
        REQUIRE(check->is_success());
        REQUIRE(check->size() == 0);
    }

    INFO("describe RETURNING with a constant/computed expr: ERRORS, not silent NoData (otterbrix#582)");
    {
        // The RETURNING projection has a constant column validate cannot yet type;
        // describe must NOT silently answer NoData for a row-returning statement.
        auto desc = describe_sql(dispatcher,
                                 "INSERT INTO ddb.orders (id, name, amount, price) VALUES (8, 'y', 80, 8.5) "
                                 "RETURNING id, 42;");
        REQUIRE(desc->is_error());
        // Still no side effects — the row was NOT inserted.
        auto check = run_sql(dispatcher, "SELECT * FROM ddb.orders WHERE id = 8;");
        REQUIRE(check->is_success());
        REQUIRE(check->size() == 0);
    }

    INFO("describe plain DML (no RETURNING): NoData — zero columns, no side effects");
    {
        auto desc = describe_sql(dispatcher, "DELETE FROM ddb.orders WHERE id = 1;");
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 0);
        REQUIRE(cursor_columns(desc).empty());
        auto check = run_sql(dispatcher, "SELECT * FROM ddb.orders WHERE id = 1;");
        REQUIRE(check->is_success());
        REQUIRE(check->size() == 1); // the row is still there
    }

    INFO("describe DDL: empty cursor, no side effects (table NOT created)");
    {
        auto desc = describe_sql(dispatcher, "CREATE TABLE ddb.phantom (x BIGINT);");
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 0);
        REQUIRE(cursor_columns(desc).empty());
        auto check = run_sql(dispatcher, "SELECT * FROM ddb.phantom;");
        REQUIRE(check->is_error()); // phantom must NOT exist
    }

    INFO("describe error parity: unknown table errors like execution");
    {
        auto desc = describe_sql(dispatcher, "SELECT * FROM ddb.no_such_table;");
        REQUIRE(desc->is_error());
    }

    INFO("describe scalar sub-query");
    {
        const std::string q = "SELECT name FROM ddb.orders WHERE amount > (SELECT MIN(amount) FROM ddb.orders);";
        auto desc = describe_sql(dispatcher, q);
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 0);
        auto ran = run_sql(dispatcher, q);
        REQUIRE(ran->is_success());
        REQUIRE(cursor_columns(desc) == cursor_columns(ran));
    }
}
