// describe_sql answers what PostgreSQL 18's Parse and Describe answer: the result columns and the types of the $n,
// with no rows, nothing planned and nothing sent to a storage of a table (docs/embedding-host-api.md, "Describe").

#include "session_sql_fixture.hpp"

#include <components/vector/vector_builders.hpp>

#include <cstdint>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

using components::session::transaction_status_t;
using components::types::complex_logical_type;
using components::types::logical_type;
using code = core::error_code_t;
using params_t = std::vector<std::pair<size_t, components::vector::vector_t>>;

namespace {

    struct column_t {
        std::string name;
        logical_type type;
        bool operator==(const column_t&) const = default;
    };

    std::ostream& operator<<(std::ostream& out, const column_t& column) {
        return out << column.name << ":" << static_cast<int>(column.type);
    }

    std::vector<column_t> columns_of(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<column_t> columns;
        for (const auto& type : cursor->type_data()) {
            columns.push_back({type.has_alias() ? type.alias() : std::string{}, type.type()});
        }
        return columns;
    }

    std::vector<logical_type> parameters_of(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<logical_type> types;
        for (const auto& type : cursor->parameter_types()) {
            types.push_back(type.type());
        }
        return types;
    }

    struct db_t : session_sql::fixture_t {
        using session_sql::fixture_t::fixture_t;

        components::cursor::cursor_t_ptr
        describe(const otterbrix::session_id_t& session, const std::string& sql, const params_t& params = {}) {
            return dispatcher()->describe_sql(session, sql, params);
        }

        components::cursor::cursor_t_ptr
        described(const otterbrix::session_id_t& session, const std::string& sql, const params_t& params = {}) {
            auto cursor = describe(session, sql, params);
            INFO("describe " << sql << " -> " << session_sql::error_of(cursor));
            REQUIRE(cursor->is_success());
            CHECK(cursor->size() == 0);
            return cursor;
        }

        components::cursor::cursor_t_ptr
        executed(const otterbrix::session_id_t& session, const std::string& sql, const params_t& params) {
            auto cursor = dispatcher()->execute_sql_with_params(session, sql, params);
            INFO(sql << " -> " << session_sql::error_of(cursor));
            REQUIRE(cursor->is_success());
            return cursor;
        }

        // describe agrees with the type_data() of the same statement run.
        void same_as_run(const std::string& sql, const params_t& params = {}) {
            const auto session = otterbrix::session_id_t();
            const auto columns = columns_of(described(session, sql));
            INFO(sql);
            CHECK(columns == columns_of(executed(session, sql, params)));
        }

        std::int64_t rows(const std::string& table) {
            return ints(otterbrix::session_id_t(), "SELECT count(*) FROM " + table + ";").front();
        }
    };

    // s.t (a BIGINT, b INTEGER) with one row, s.t2 (a BIGINT) and s.n (a BIGINT, name TEXT).
    void tables(db_t& db) {
        const auto session = otterbrix::session_id_t();
        db.ok(session, "CREATE SCHEMA s;");
        db.ok(session, "CREATE TABLE s.t (a BIGINT, b INTEGER);");
        db.ok(session, "INSERT INTO s.t (a, b) VALUES (1, 2);");
        db.ok(session, "CREATE TABLE s.t2 (a BIGINT);");
        db.ok(session, "INSERT INTO s.t2 (a) VALUES (7);");
        db.ok(session, "CREATE TABLE s.n (a BIGINT, name TEXT);");
        db.ok(session, "INSERT INTO s.n (a, name) VALUES (1, 'one');");
    }

    params_t one(std::pmr::memory_resource* resource, std::int64_t value) {
        params_t params;
        params.emplace_back(1, components::vector::make_scalar(resource, value));
        return params;
    }

    params_t untyped_null(std::pmr::memory_resource* resource, size_t number = 1) {
        params_t params;
        params.emplace_back(number, components::vector::make_null(resource, complex_logical_type{logical_type::NA}));
        return params;
    }

} // namespace

TEST_CASE("integration::cpp::describe::the columns of a query are those its run answers") {
    db_t db("test_describe/columns");
    tables(db);
    const auto session = otterbrix::session_id_t();
    auto described = db.described(session, "SELECT a, b + 1 AS c FROM s.t;");
    CHECK(columns_of(described) == std::vector<column_t>{{"a", logical_type::BIGINT}, {"c", logical_type::BIGINT}});
    CHECK(described->parameter_types().empty());
    const auto c = described->column_index("c");
    REQUIRE_FALSE(c.has_error());
    CHECK(c.value() == 1);
    db.same_as_run("SELECT a, b + 1 AS c FROM s.t;");
}

TEST_CASE("integration::cpp::describe::joins, aggregates, views, CTE and UNION answer as they run") {
    db_t db("test_describe/shapes");
    tables(db);
    db.ok(otterbrix::session_id_t(), "CREATE VIEW s.v AS SELECT a, b * 2 AS twice FROM s.t;");
    db.same_as_run("SELECT t.a, t2.a AS other FROM s.t AS t JOIN s.t2 AS t2 ON t.a < t2.a;");
    db.same_as_run("SELECT b, count(*) AS n, sum(a) AS total, max(a) AS top FROM s.t GROUP BY b;");
    db.same_as_run("SELECT * FROM s.v;");
    db.same_as_run("WITH w AS (SELECT a FROM s.t2) SELECT a FROM w;");
    db.same_as_run("SELECT a FROM s.t UNION ALL SELECT a FROM s.t2;");
}

TEST_CASE("integration::cpp::describe::a parameter takes the type of its context") {
    db_t db("test_describe/parameters");
    tables(db);
    const auto session = otterbrix::session_id_t();

    SECTION("in the select list it is text") {
        auto described = db.described(session, "SELECT $1;");
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::STRING_LITERAL});
        REQUIRE(described->type_data().size() == 1);
        CHECK(described->type_data().front().type() == logical_type::STRING_LITERAL);
    }
    SECTION("compared with a column it is the column's type") {
        auto described = db.described(session, "SELECT a FROM s.t WHERE a = $1;");
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::BIGINT});
        CHECK(columns_of(described) == std::vector<column_t>{{"a", logical_type::BIGINT}});
    }
    SECTION("a cast, an operator, a function and COALESCE type it") {
        const std::vector<std::pair<std::string, logical_type>> cases{
            {"SELECT CAST($1 AS INTEGER);", logical_type::INTEGER},
            {"SELECT a FROM s.t WHERE a + $1 > 0;", logical_type::BIGINT},
            {"SELECT upper($1);", logical_type::STRING_LITERAL},
            {"SELECT coalesce($1, a) AS c FROM s.t;", logical_type::BIGINT},
            {"SELECT a FROM s.t WHERE a = $1 AND b < $1;", logical_type::BIGINT}};
        for (const auto& [sql, type] : cases) {
            INFO(sql);
            CHECK(parameters_of(db.described(session, sql)) == std::vector<logical_type>{type});
        }
    }
    SECTION("a use left untyped beside a typed one is refused") {
        auto cursor = db.describe(session, "SELECT a FROM s.t WHERE $1 IS NULL AND a = $1;");
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == code::ambiguous_parameter);
        CHECK(std::string{cursor->get_error().what} == "could not determine data type of parameter $1");
    }
    SECTION("LIMIT takes a bigint") {
        auto described = db.described(session, "SELECT a FROM s.t LIMIT $1;");
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::BIGINT});
        CHECK(columns_of(described) == std::vector<column_t>{{"a", logical_type::BIGINT}});
    }
    SECTION("a bound value keeps its own type") {
        auto described = db.described(session, "SELECT $1;", one(db.dispatcher()->resource(), 5));
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::BIGINT});
        REQUIRE(described->type_data().size() == 1);
        CHECK(described->type_data().front().type() == logical_type::BIGINT);
    }
    SECTION("inside a scalar sub-query the type reaches the statement") {
        auto outer = db.described(session, "SELECT a, (SELECT max(a) FROM s.t2) AS top FROM s.t WHERE a = $1;");
        CHECK(columns_of(outer) == std::vector<column_t>{{"a", logical_type::BIGINT}, {"top", logical_type::BIGINT}});
        CHECK(parameters_of(outer) == std::vector<logical_type>{logical_type::BIGINT});
        auto inner = db.described(session, "SELECT a, (SELECT max(a) FROM s.t2 WHERE a < $1) AS top FROM s.t;");
        CHECK(columns_of(inner) == std::vector<column_t>{{"a", logical_type::BIGINT}, {"top", logical_type::BIGINT}});
        CHECK(parameters_of(inner) == std::vector<logical_type>{logical_type::BIGINT});
    }
}

TEST_CASE("integration::cpp::describe::a parameter no context types is refused as in PostgreSQL 18") {
    db_t db("test_describe/undetermined");
    tables(db);
    const auto session = otterbrix::session_id_t();
    auto refused = [&](const std::string& sql) {
        auto cursor = db.describe(session, sql);
        INFO(sql);
        REQUIRE(cursor->is_error());
        return cursor->get_error();
    };

    SECTION("a parameter nothing gives a type") {
        const auto error = refused("SELECT 1 FROM s.t WHERE $1 IS NULL;");
        CHECK(error.type == code::indeterminate_datatype);
        CHECK(std::string{error.what} == "could not determine data type of parameter $1");
    }
    SECTION("a number the statement skips") {
        const auto error = refused("SELECT $2;");
        CHECK(error.type == code::indeterminate_datatype);
        CHECK(std::string{error.what} == "could not determine data type of parameter $1");
    }
    SECTION("an output column of a parameter another clause typed") {
        const auto error = refused("SELECT $1, a FROM s.t WHERE a = $1;");
        CHECK(error.type == code::ambiguous_parameter);
        CHECK(std::string{error.what} == "inconsistent types deduced for parameter $1");
    }
    SECTION("a second use reads the type the first gave, as a typed value would") {
        const std::string sql = "SELECT a FROM s.n WHERE a = $1 AND name = $1;";
        const auto error = refused(sql);
        auto typed = db.dispatcher()->execute_sql_with_params(session, sql, one(db.dispatcher()->resource(), 5));
        REQUIRE(typed->is_error());
        CHECK(error.type == typed->get_error().type);
        CHECK(std::string{error.what} == std::string{typed->get_error().what});
    }
}

TEST_CASE("integration::cpp::describe::the rules a run follows too") {
    db_t db("test_describe/same_rules");
    tables(db);
    const auto session = otterbrix::session_id_t();
    auto* resource = db.dispatcher()->resource();
    db.ok(session, "CREATE TABLE s.p (i INTEGER, t TEXT);");
    auto same_error = [&](const std::string& sql, const params_t& params, code type, const std::string& what) {
        INFO(sql);
        auto described = db.describe(session, sql);
        auto run = db.dispatcher()->execute_sql_with_params(session, sql, params);
        REQUIRE(described->is_error());
        REQUIRE(run->is_error());
        CHECK(described->get_error().type == type);
        CHECK(std::string{described->get_error().what} == what);
        CHECK(run->get_error().type == type);
        CHECK(std::string{run->get_error().what} == what);
    };

    SECTION("a number the statement skips has no type") {
        params_t params;
        params.emplace_back(1, components::vector::make_scalar(resource, std::int64_t{1}));
        params.emplace_back(3, components::vector::make_scalar(resource, std::int64_t{3}));
        same_error("SELECT $1, $3;",
                   params,
                   code::indeterminate_datatype,
                   "could not determine data type of parameter $2");
    }
    SECTION("one $n in VALUES columns of two types") {
        same_error("INSERT INTO s.p (i, t) VALUES ($1, $1);",
                   untyped_null(resource),
                   code::ambiguous_parameter,
                   "inconsistent types deduced for parameter $1");
        CHECK(db.rows("s.p") == 0);
    }
    SECTION("an output column of a $n an earlier column typed keeps the type") {
        const std::string sql = "SELECT a = $1 AS same, $1 AS value FROM s.t;";
        auto described = db.described(session, sql);
        CHECK(columns_of(described) ==
              std::vector<column_t>{{"same", logical_type::BOOLEAN}, {"value", logical_type::BIGINT}});
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::BIGINT});
        CHECK(columns_of(described) == columns_of(db.executed(session, sql, untyped_null(resource))));
    }
    SECTION("a $n of VALUES used again takes the VALUES column's type") {
        const std::string returning = "INSERT INTO s.t (a, b) VALUES ($1, 2) RETURNING $1 AS again;";
        auto described = db.described(session, returning);
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::BIGINT});
        CHECK(columns_of(described) == std::vector<column_t>{{"again", logical_type::BIGINT}});
        CHECK(columns_of(described) == columns_of(db.executed(session, returning, untyped_null(resource))));
    }
    SECTION("a $n of VALUES used again in ON CONFLICT DO UPDATE SET takes the VALUES column's type") {
        db.ok(session, "CREATE TABLE s.up (id BIGINT, v INTEGER);");
        db.ok(session, "ALTER TABLE s.up ADD CONSTRAINT pk_up PRIMARY KEY (id);");
        db.ok(session, "INSERT INTO s.up (id, v) VALUES (5, 1);");
        const std::string upsert =
            "INSERT INTO s.up (id, v) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET v = $2 + 1 RETURNING v;";
        auto upserted = db.described(session, upsert);
        CHECK(parameters_of(upserted) == std::vector<logical_type>{logical_type::BIGINT, logical_type::INTEGER});
        params_t params;
        params.emplace_back(1, components::vector::make_scalar(resource, std::int64_t{5}));
        params.emplace_back(2, components::vector::make_null(resource, complex_logical_type{logical_type::NA}));
        CHECK(columns_of(upserted) == columns_of(db.executed(session, upsert, params)));
    }
    SECTION("RETURNING of an upsert is its output") {
        db.ok(session, "CREATE TABLE s.up (id BIGINT, v INTEGER);");
        db.ok(session, "ALTER TABLE s.up ADD CONSTRAINT pk_up PRIMARY KEY (id);");
        db.ok(session, "INSERT INTO s.up (id, v) VALUES (5, 1);");
        auto alone = db.described(session,
                                  "INSERT INTO s.up (id, v) VALUES (5, 1) ON CONFLICT (id) DO UPDATE SET v = 2 "
                                  "RETURNING $1 AS r;");
        CHECK(parameters_of(alone) == std::vector<logical_type>{logical_type::STRING_LITERAL});
        CHECK(columns_of(alone) == std::vector<column_t>{{"r", logical_type::STRING_LITERAL}});
    }
    SECTION("RETURNING of an upsert reads the type its SET gave") {
        db.ok(session, "CREATE TABLE s.up (id BIGINT, v INTEGER);");
        db.ok(session, "ALTER TABLE s.up ADD CONSTRAINT pk_up PRIMARY KEY (id);");
        db.ok(session, "INSERT INTO s.up (id, v) VALUES (5, 1);");
        const std::string assigned =
            "INSERT INTO s.up (id, v) VALUES (5, 1) ON CONFLICT (id) DO UPDATE SET v = $1 RETURNING $1 AS r;";
        auto described = db.described(session, assigned);
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::INTEGER});
        CHECK(columns_of(described) == std::vector<column_t>{{"r", logical_type::INTEGER}});
        CHECK(columns_of(described) == columns_of(db.executed(session, assigned, untyped_null(resource))));
    }
    SECTION("an output column of a $n a later column types is refused") {
        same_error("SELECT $1 AS value, a = $1 AS same FROM s.t;",
                   untyped_null(resource),
                   code::ambiguous_parameter,
                   "inconsistent types deduced for parameter $1");
    }
}

TEST_CASE("integration::cpp::describe::a write is described and not done") {
    db_t db("test_describe/writes");
    tables(db);
    const auto session = otterbrix::session_id_t();

    SECTION("INSERT VALUES types its parameters by the columns") {
        auto described = db.described(session, "INSERT INTO s.t (a, b) VALUES ($1, $2);");
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::BIGINT, logical_type::INTEGER});
        CHECK(described->type_data().empty());
    }
    SECTION("UPDATE types SET by the column and WHERE by the comparison") {
        auto described = db.described(session, "UPDATE s.t SET b = $1 WHERE a = $2;");
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::INTEGER, logical_type::BIGINT});
        CHECK(described->type_data().empty());
    }
    SECTION("ON CONFLICT DO UPDATE SET types its parameter too") {
        db.ok(session, "ALTER TABLE s.t2 ADD CONSTRAINT pk_t2 PRIMARY KEY (a);");
        auto described =
            db.described(session, "INSERT INTO s.t2 (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET a = $2;");
        CHECK(parameters_of(described) == std::vector<logical_type>{logical_type::BIGINT, logical_type::BIGINT});
    }
    SECTION("RETURNING is the result") {
        const std::string sql = "DELETE FROM s.t WHERE a = 1 RETURNING a;";
        auto described = db.described(session, sql);
        CHECK(columns_of(described) == std::vector<column_t>{{"a", logical_type::BIGINT}});
        REQUIRE(db.rows("s.t") == 1);
        CHECK(columns_of(described) == columns_of(db.executed(session, sql, {})));
    }
    SECTION("RETURNING of every kind answers as it runs") {
        db.same_as_run("INSERT INTO s.t (a, b) VALUES (3, 4) RETURNING *;");
        db.same_as_run("INSERT INTO s.t (a, b) VALUES (5, 6) RETURNING b, a + 1 AS next;");
        db.same_as_run("UPDATE s.t SET b = 9 WHERE a = 3 RETURNING *;");
        db.same_as_run("UPDATE s.t SET b = 8 FROM s.t2 WHERE s.t.a < s.t2.a RETURNING s.t.a, s.t2.a AS other;");
        db.same_as_run("DELETE FROM s.t USING s.n WHERE s.t.a = s.n.a RETURNING s.t.b, s.n.name;");
    }
}

TEST_CASE("integration::cpp::describe::nothing of the statement stays") {
    db_t db("test_describe/nothing_stays");
    tables(db);
    const auto session = otterbrix::session_id_t();
    db.described(session, "INSERT INTO s.t (a, b) VALUES ($1, $2);");
    db.described(session, "UPDATE s.t SET b = $1 WHERE a = $2;");
    db.described(session, "DELETE FROM s.t RETURNING a;");
    CHECK(db.rows("s.t") == 1);
    CHECK(db.ints(session, "SELECT b FROM s.t;") == std::vector<std::int64_t>{2});
}

TEST_CASE("integration::cpp::describe::a statement that is not a query answers no columns and checks nothing") {
    db_t db("test_describe/utility");
    tables(db);
    const auto session = otterbrix::session_id_t();

    SECTION("CREATE TABLE of a table that exists, and no OID is taken") {
        const auto first = db.ok(session, "CREATE TABLE s.o1 (x INT);")->created_oid();
        const auto second = db.ok(session, "CREATE TABLE s.o2 (x INT);")->created_oid();
        auto described = db.described(session, "CREATE TABLE s.t (x INT);");
        CHECK(described->type_data().empty());
        CHECK(described->parameter_types().empty());
        const auto third = db.ok(session, "CREATE TABLE s.o3 (x INT);")->created_oid();
        CHECK(third - second == second - first);
    }
    SECTION("BEGIN opens nothing") {
        auto described = db.described(session, "BEGIN;");
        CHECK(described->type_data().empty());
        CHECK(described->transaction_status() == transaction_status_t::idle);
        CHECK(db.ok(session, "SELECT 1;")->transaction_status() == transaction_status_t::idle);
    }
    SECTION("SET changes no setting") {
        db.described(session, "SET autocommit = off;");
        CHECK(db.ok(session, "INSERT INTO s.t (a, b) VALUES (2, 2);")->transaction_status() ==
              transaction_status_t::idle);
    }
    SECTION("EXPLAIN is one text column QUERY PLAN") {
        auto described = db.described(session, "EXPLAIN SELECT a FROM s.t;");
        CHECK(columns_of(described) == std::vector<column_t>{{"QUERY PLAN", logical_type::STRING_LITERAL}});
    }
    SECTION("EXPLAIN ANALYZE runs nothing") {
        auto described = db.described(session, "EXPLAIN ANALYZE INSERT INTO s.t (a, b) VALUES (9, 9);");
        CHECK(columns_of(described) == std::vector<column_t>{{"QUERY PLAN", logical_type::STRING_LITERAL}});
        CHECK(db.rows("s.t") == 1);
    }
    SECTION("REFRESH MATERIALIZED VIEW refreshes nothing") {
        db.ok(session, "CREATE MATERIALIZED VIEW s.mv AS SELECT a FROM s.t WITH NO DATA;");
        db.ok(session, "REFRESH MATERIALIZED VIEW s.mv;");
        db.ok(session, "INSERT INTO s.t (a, b) VALUES (3, 3);");
        auto described = db.described(session, "REFRESH MATERIALIZED VIEW s.mv;");
        CHECK(described->type_data().empty());
        CHECK(db.rows("s.mv") == 1);
    }
}

TEST_CASE("integration::cpp::describe::an error is the run's error") {
    db_t db("test_describe/errors");
    tables(db);
    const auto session = otterbrix::session_id_t();
    auto described = db.describe(session, "SELECT * FROM s.nope;");
    auto run = db.run(session, "SELECT * FROM s.nope;");
    REQUIRE(described->is_error());
    REQUIRE(run->is_error());
    CHECK(described->get_error().type == run->get_error().type);
    CHECK(std::string{described->get_error().what} == std::string{run->get_error().what});
}

TEST_CASE("integration::cpp::describe::transactions") {
    db_t db("test_describe/transactions");
    tables(db);
    const auto session = otterbrix::session_id_t();

    SECTION("outside a block describe leaves none") {
        CHECK(db.described(session, "SELECT a FROM s.t;")->transaction_status() == transaction_status_t::idle);
    }
    SECTION("with autocommit off describe opens no block") {
        db.ok(session, "SET autocommit = off;");
        CHECK(db.described(session, "SELECT a FROM s.t;")->transaction_status() == transaction_status_t::idle);
        CHECK(db.described(session, "INSERT INTO s.t (a, b) VALUES ($1, $2);")->transaction_status() ==
              transaction_status_t::idle);
    }
    SECTION("inside a block describe sees the block's own DDL") {
        db.ok(session, "BEGIN;");
        db.ok(session, "CREATE TABLE s.u (x INT);");
        auto described = db.described(session, "SELECT * FROM s.u;");
        CHECK(columns_of(described) == std::vector<column_t>{{"x", logical_type::INTEGER}});
        CHECK(described->transaction_status() == transaction_status_t::in_transaction);
        db.ok(session, "ROLLBACK;");
    }
    SECTION("a failed block refuses a query and admits its end") {
        db.ok(session, "BEGIN;");
        REQUIRE(db.run(session, "SELECT * FROM s.nope;")->is_error());
        auto query = db.describe(session, "SELECT a FROM s.t;");
        REQUIRE(query->is_error());
        CHECK(query->get_error().type == code::transaction_finalized);
        CHECK(db.described(session, "COMMIT;")->transaction_status() == transaction_status_t::failed);
        CHECK(db.described(session, "ROLLBACK;")->transaction_status() == transaction_status_t::failed);
        CHECK(db.ok(session, "ROLLBACK;")->transaction_status() == transaction_status_t::idle);
    }
    SECTION("a failed block admits the describe of ROLLBACK TO and stays failed") {
        db.ok(session, "BEGIN;");
        db.ok(session, "SAVEPOINT p;");
        REQUIRE(db.run(session, "SELECT * FROM s.nope;")->is_error());
        auto described = db.described(session, "ROLLBACK TO SAVEPOINT p;");
        CHECK(described->type_data().empty());
        CHECK(described->transaction_status() == transaction_status_t::failed);
        CHECK(db.refused(session, "SELECT 1;").type == code::transaction_finalized);
        CHECK(db.ok(session, "ROLLBACK TO SAVEPOINT p;")->transaction_status() == transaction_status_t::in_transaction);
        db.ok(session, "ROLLBACK;");
    }
    SECTION("inside a block describe SAVEPOINT makes no savepoint") {
        db.ok(session, "BEGIN;");
        db.described(session, "SAVEPOINT p;");
        CHECK(db.refused(session, "ROLLBACK TO SAVEPOINT p;").type == code::missing_savepoint);
        db.ok(session, "ROLLBACK;");
    }
    SECTION("inside a block describe SAVEPOINT keeps no settings for a ROLLBACK TO") {
        db.ok(session, "BEGIN;");
        db.ok(session, "SAVEPOINT p;");
        db.ok(session, "SET autocommit = off;");
        db.described(session, "SAVEPOINT p;");
        db.ok(session, "ROLLBACK TO SAVEPOINT p;");
        db.ok(session, "COMMIT;");
        CHECK(db.ok(session, "INSERT INTO s.t (a, b) VALUES (2, 2);")->transaction_status() ==
              transaction_status_t::idle);
    }
    SECTION("inside a block describe of DDL leaves the block's work to its COMMIT") {
        db.ok(session, "BEGIN;");
        db.ok(session, "INSERT INTO s.t (a, b) VALUES (5, 5);");
        auto described = db.described(session, "CREATE TABLE s.t (x INT);");
        CHECK(described->transaction_status() == transaction_status_t::in_transaction);
        db.ok(session, "COMMIT;");
        CHECK(db.rows("s.t") == 2);
    }
    SECTION("a describe that fails inside a block fails the block") {
        db.ok(session, "BEGIN;");
        REQUIRE(db.describe(session, "SELECT * FROM s.nope;")->is_error());
        auto next = db.run(session, "SELECT 1;");
        REQUIRE(next->is_error());
        CHECK(next->get_error().type == code::transaction_finalized);
        db.ok(session, "ROLLBACK;");
    }
}

TEST_CASE("integration::cpp::describe::a run types an untyped NULL by the same rule") {
    db_t db("test_describe/untyped_null");
    tables(db);
    const auto session = otterbrix::session_id_t();
    auto* resource = db.dispatcher()->resource();

    SECTION("in the select list it is a text NULL") {
        auto cursor = db.executed(session, "SELECT $1;", untyped_null(resource));
        REQUIRE(cursor->type_data().size() == 1);
        CHECK(cursor->type_data().front().type() == logical_type::STRING_LITERAL);
        REQUIRE(cursor->size() == 1);
        const auto null = cursor->is_null(0, 0);
        REQUIRE_FALSE(null.has_error());
        CHECK(null.value());
    }
    SECTION("assigned to a column it is the column's NULL") {
        db.executed(session, "UPDATE s.t SET b = $1 WHERE a = 1;", untyped_null(resource));
        CHECK(db.ints(session, "SELECT count(*) FROM s.t WHERE b IS NULL;") == std::vector<std::int64_t>{1});
    }
    SECTION("where nothing types it the run is refused") {
        auto cursor = db.dispatcher()->execute_sql_with_params(session,
                                                               "SELECT 1 FROM s.t WHERE $1 IS NULL;",
                                                               untyped_null(resource));
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == code::indeterminate_datatype);
    }
}
