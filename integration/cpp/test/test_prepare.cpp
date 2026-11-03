// PREPARE / EXECUTE / DEALLOCATE as PostgreSQL 18 runs them: the engine keeps the statement's text per session and
// every EXECUTE runs it again the usual way (docs/embedding-host-api.md, "Prepared statements").

#include "host_fixture.hpp"

#include <components/logical_plan/node_deallocate.hpp>
#include <components/logical_plan/node_execute.hpp>
#include <components/logical_plan/node_prepare.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/tests/wait_ready.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <cstdint>
#include <string>
#include <thread>
#include <vector>

using code = core::error_code_t;
using components::session::transaction_status_t;
using components::types::logical_type;
using host_fixture::host_t;
using host_fixture::strings;
using ints = std::vector<std::int64_t>;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    // s.t(a BIGINT, b INT) with (1, 10), (2, 20), (3, 30).
    void seed(session_sql::fixture_t& db) {
        db.ok(any(), "CREATE SCHEMA s;");
        db.ok(any(), "CREATE TABLE s.t (a BIGINT, b INT);");
        db.ok(any(), "INSERT INTO s.t (a, b) VALUES (1, 10), (2, 20), (3, 30);");
    }

    std::vector<logical_type> types_of(const std::vector<components::types::complex_logical_type>& types) {
        std::vector<logical_type> out;
        for (const auto& type : types) {
            out.push_back(type.type());
        }
        return out;
    }

    std::vector<logical_type> columns_of(const components::cursor::cursor_t_ptr& cursor) {
        return types_of({cursor->type_data().begin(), cursor->type_data().end()});
    }

    std::vector<logical_type> parameters_of(const components::cursor::cursor_t_ptr& cursor) {
        return types_of({cursor->parameter_types().begin(), cursor->parameter_types().end()});
    }

    std::string plan_text(const components::cursor::cursor_t_ptr& cursor) {
        std::string text;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                text += std::string{chunk.get_value<std::string_view>(0, row)};
                text += '\n';
            }
        }
        return text;
    }

    const std::string missing_q = "prepared statement \"q\" does not exist";

} // namespace

TEST_CASE("integration::cpp::prepare::execute_returns_the_rows_of_the_direct_query") {
    session_sql::fixture_t db("test_prepare/direct");
    seed(db);
    const auto s = any();
    auto prepared = db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    CHECK(prepared->size() == 0);
    CHECK(prepared->type_data().empty());
    CHECK(db.ints(s, "EXECUTE q(1);") == db.ints(s, "SELECT a FROM s.t WHERE a = 1;"));
    CHECK(db.ints(s, "EXECUTE q(2);") == db.ints(s, "SELECT a FROM s.t WHERE a = 2;"));
    CHECK(db.ints(s, "EXECUTE q(2);") == ints{2});
}

TEST_CASE("integration::cpp::prepare::a_name_exists_only_in_its_session_until_deallocate") {
    session_sql::fixture_t db("test_prepare/names");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    db.ok(s, "PREPARE r AS SELECT b FROM s.t WHERE a = $1;");
    db.check_refused(any(), "EXECUTE q(1);", code::undefined_prepared_statement, missing_q);

    db.ok(s, "DEALLOCATE q;");
    db.check_refused(s, "EXECUTE q(1);", code::undefined_prepared_statement, missing_q);
    CHECK(db.ints(s, "EXECUTE r(3);") == ints{30});

    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    db.ok(s, "DEALLOCATE PREPARE q;");
    db.check_refused(s, "EXECUTE q(1);", code::undefined_prepared_statement, missing_q);
    db.check_refused(s,
                     "DEALLOCATE nope;",
                     code::undefined_prepared_statement,
                     "prepared statement \"nope\" does not exist");

    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    db.ok(s, "DEALLOCATE ALL;");
    db.check_refused(s, "EXECUTE q(1);", code::undefined_prepared_statement, missing_q);
    db.check_refused(s, "EXECUTE r(1);", code::undefined_prepared_statement, "prepared statement \"r\" does not exist");
    db.ok(s, "DEALLOCATE PREPARE ALL;");
}

TEST_CASE("integration::cpp::prepare::a_second_prepare_of_a_name_is_refused_after_its_body") {
    session_sql::fixture_t db("test_prepare/duplicate");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    db.check_refused(s,
                     "PREPARE q AS SELECT b FROM s.t;",
                     code::duplicate_prepared_statement,
                     "prepared statement \"q\" already exists");
    const auto body_error = db.refused(s, "PREPARE q AS SELECT * FROM s.nope;");
    CHECK(body_error.type == code::table_not_exists);
    CHECK(db.ints(s, "EXECUTE q(1);") == ints{1});
}

TEST_CASE("integration::cpp::prepare::a_refused_body_stores_nothing") {
    session_sql::fixture_t db("test_prepare/refused_body");
    seed(db);
    const auto s = any();
    CHECK(db.refused(s, "PREPARE x AS SELECT * FROM s.nope;").type == code::table_not_exists);
    db.check_refused(s, "EXECUTE x;", code::undefined_prepared_statement, "prepared statement \"x\" does not exist");
}

TEST_CASE("integration::cpp::prepare::the_number_of_arguments_is_checked_first") {
    session_sql::fixture_t db("test_prepare/count");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    const std::string wrong = "wrong number of parameters for prepared statement \"q\"\nDETAIL: Expected 1 parameters ";
    db.check_refused(s, "EXECUTE q(1, 2);", code::sql_parse_error, wrong + "but got 2.");
    db.check_refused(s, "EXECUTE q;", code::sql_parse_error, wrong + "but got 0.");
    db.check_refused(s, "EXECUTE q(1, (SELECT 1));", code::sql_parse_error, wrong + "but got 2.");
}

TEST_CASE("integration::cpp::prepare::an_argument_is_refused_as_postgresql_refuses_it") {
    session_sql::fixture_t db("test_prepare/argument_forms");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    db.check_refused(s, "EXECUTE q((SELECT 1));", code::unimplemented_yet, "cannot use subquery in EXECUTE parameter");
    for (const auto* sql : {"EXECUTE q(sum(1));", "EXECUTE q(count(*));", "EXECUTE q(1 + max(2));"}) {
        db.check_refused(s, sql, code::grouping_error, "aggregate functions are not allowed in EXECUTE parameters");
    }
    db.check_refused(s,
                     "EXECUTE q(row_number() OVER ());",
                     code::windowing_error,
                     "window functions are not allowed in EXECUTE parameters");
    db.check_refused(s,
                     "EXECUTE q(generate_series(1, 2));",
                     code::unimplemented_yet,
                     "set-returning functions are not allowed in EXECUTE parameters");
    db.check_refused(s, "EXECUTE q($1);", code::undefined_parameter, "there is no parameter $1");

    // PostgreSQL 18 EvaluateParams analyzes the arguments one by one: the first refused argument answers.
    db.ok(s, "PREPARE two(bigint, bigint) AS SELECT $1 + $2;");
    db.check_refused(s,
                     "EXECUTE two(sum(1), (SELECT 1));",
                     code::grouping_error,
                     "aggregate functions are not allowed in EXECUTE parameters");
    db.check_refused(s,
                     "EXECUTE two(generate_series(1, 2), row_number() OVER ());",
                     code::unimplemented_yet,
                     "set-returning functions are not allowed in EXECUTE parameters");
    db.check_refused(s,
                     "EXECUTE two((SELECT 1), sum(1));",
                     code::unimplemented_yet,
                     "cannot use subquery in EXECUTE parameter");
}

TEST_CASE("integration::cpp::prepare::an_argument_is_any_expression_coerced_to_its_parameter") {
    session_sql::fixture_t db("test_prepare/argument_values");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    CHECK(db.ints(s, "EXECUTE q(1 + 1);") == ints{2});
    CHECK(db.ints(s, "EXECUTE q(abs(-2));") == ints{2});
    CHECK(db.ints(s, "EXECUTE q(NULL);").empty());

    db.ok(s, "PREPARE p(bigint) AS SELECT $1;");
    auto five = db.ok(s, "EXECUTE p(5);");
    REQUIRE(five->size() == 1);
    CHECK(columns_of(five) == std::vector<logical_type>{logical_type::BIGINT});
    CHECK(five->chunks().front().get_value<std::int64_t>(0, 0) == 5);
    CHECK(db.ints(s, "EXECUTE p('7');") == ints{7});
    CHECK(db.ints(s, "EXECUTE p(1.5);") == ints{2});
    CHECK(db.refused(s, "EXECUTE p('x');").type == code::invalid_text_representation);
    // The value is coerced after it is evaluated: PostgreSQL 18 refuses the second argument's type first.
    db.ok(s, "PREPARE two(bigint, bigint) AS SELECT $1 + $2;");
    CHECK(db.refused(s, "EXECUTE two(1 / 0, '7'::text);").type == code::division_by_zero);
    db.check_refused(s,
                     "EXECUTE p('7'::text);",
                     code::datatype_mismatch,
                     "parameter $1 of type text cannot be coerced to the expected type bigint");
    // One rule with an INSERT's VALUES cell: a quoted literal reaches any type, a text value does not.
    db.ok(s, "CREATE TABLE s.u (a BIGINT);");
    CHECK(db.refused(s, "INSERT INTO s.u (a) VALUES ('5'::text), (6);").type == code::datatype_mismatch);
    db.ok(s, "INSERT INTO s.u (a) VALUES ('5'), (6);");
    CHECK(db.refused(s, "EXECUTE p('5'::text);").type == code::datatype_mismatch);
    CHECK(db.ints(s, "EXECUTE p('5');") == ints{5});
    db.ok(s, "PREPARE flag(boolean) AS SELECT $1;");
    auto flag = db.ok(s, "EXECUTE flag('t');");
    REQUIRE(flag->size() == 1);
    CHECK(flag->chunks().front().get_value<bool>(0, 0));
}

TEST_CASE("integration::cpp::prepare::insert_and_limit_take_each_execute_values") {
    session_sql::fixture_t db("test_prepare/insert_limit");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE i AS INSERT INTO s.t (a, b) VALUES ($1, $2);");
    db.ok(s, "EXECUTE i(10, 1);");
    db.ok(s, "EXECUTE i(11, 2);");
    CHECK(db.ints(s, "SELECT a FROM s.t WHERE a > 9 ORDER BY a;") == ints{10, 11});
    db.ok(s, "PREPARE l AS SELECT a FROM s.t ORDER BY a LIMIT $1;");
    CHECK(db.ints(s, "EXECUTE l(2);") == ints{1, 2});
    CHECK(db.ints(s, "EXECUTE l(1);") == ints{1});
}

TEST_CASE("integration::cpp::prepare::ddl_between_executes") {
    session_sql::fixture_t db("test_prepare/ddl");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT * FROM s.t WHERE a = $1;");
    db.ok(any(), "DROP TABLE s.t;");
    CHECK(db.refused(s, "EXECUTE q(1);").type == code::table_not_exists);
    db.ok(any(), "CREATE TABLE s.t (a BIGINT, b INT);");
    db.ok(any(), "INSERT INTO s.t (a, b) VALUES (1, 10);");
    CHECK(db.ints(s, "EXECUTE q(1);") == ints{1});

    const std::string changed = "cached plan must not change result type";
    db.ok(any(), "ALTER TABLE s.t ADD COLUMN c INT;");
    db.check_refused(s, "EXECUTE q(1);", code::unimplemented_yet, changed);
    db.check_refused(s, "EXPLAIN EXECUTE q(1);", code::unimplemented_yet, changed);

    // PostgreSQL 18 equalRowTypes compares the names too: a star that reads a renamed column is another row type.
    db.ok(s, "PREPARE r AS SELECT * FROM s.t WHERE a = $1;");
    CHECK(db.ints(s, "EXECUTE r(1);") == ints{1});
    db.ok(any(), "ALTER TABLE s.t RENAME COLUMN b TO bb;");
    db.check_refused(s, "EXECUTE r(1);", code::unimplemented_yet, changed);
}

TEST_CASE("integration::cpp::prepare::the_store_is_not_transactional") {
    session_sql::fixture_t db("test_prepare/transactions");
    seed(db);
    const auto s = any();
    db.ok(s, "BEGIN;");
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    db.ok(s, "ROLLBACK;");
    CHECK(db.ints(s, "EXECUTE q(1);") == ints{1});

    db.ok(s, "BEGIN;");
    CHECK(db.refused(s, "SELECT * FROM s.nope;").type == code::table_not_exists);
    for (const auto* sql : {"PREPARE q2 AS SELECT 1;", "EXECUTE q(1);", "DEALLOCATE q;"}) {
        auto refused = db.run(s, sql);
        INFO(sql);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == code::transaction_finalized);
        CHECK(refused->transaction_status() == transaction_status_t::failed);
    }
    db.ok(s, "ROLLBACK;");
    CHECK(db.ints(s, "EXECUTE q(1);") == ints{1});
    db.check_refused(s, "EXECUTE q2;", code::undefined_prepared_statement, "prepared statement \"q2\" does not exist");

    db.ok(s, "BEGIN;");
    CHECK(db.ok(s, "DEALLOCATE q;")->transaction_status() == transaction_status_t::in_transaction);
    db.ok(s, "COMMIT;");
    db.check_refused(s, "EXECUTE q(1);", code::undefined_prepared_statement, missing_q);
}

TEST_CASE("integration::cpp::prepare::close_session_forgets_the_statements") {
    session_sql::fixture_t db("test_prepare/close");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    db.dispatcher()->close_session(s);
    db.check_refused(s, "EXECUTE q(1);", code::undefined_prepared_statement, missing_q);
}

TEST_CASE("integration::cpp::prepare::a_host_table_is_read_by_each_execute_only") {
    host_t host("test_prepare/host");
    host.register_orders();
    host.backend().set_rows("m2.shop.public.orders", {{1, 100}, {2, 200}});
    host.backend().clear_journal();
    const auto s = any();
    const auto before = host.backend().messages();
    host.db.ok(s, "PREPARE r AS SELECT * FROM m2.shop.public.orders WHERE id = $1;");
    CHECK(host.backend().messages() == before);
    CHECK(host.backend().journal().empty());
    CHECK(host.db.ints(s, "EXECUTE r(1);") == ints{1});
    const auto after_first = host.backend().journal("scan").size();
    CHECK(after_first >= 1);
    CHECK(host.db.ints(s, "EXECUTE r(2);") == ints{2});
    CHECK(host.backend().journal("scan").size() == 2 * after_first);

    host.db.ok(any(), "ALTER TABLE m2.shop.public.orders ADD COLUMN note TEXT;");
    host.backend().clear_journal();
    host.db.check_refused(s, "EXECUTE r(1);", code::unimplemented_yet, "cached plan must not change result type");
    CHECK(host.backend().journal("scan").empty());
}

TEST_CASE("integration::cpp::prepare::a_body_of_a_host_grammar_is_refused_at_prepare") {
    session_sql::fixture_t db("test_prepare/grammar");
    seed(db);
    const auto s = any();
    CHECK(db.refused(s, "PREPARE d AS DEMO 1 + 20;").type == code::sql_parse_error);
    db.check_refused(s, "EXECUTE d;", code::undefined_prepared_statement, "prepared statement \"d\" does not exist");
}

TEST_CASE("integration::cpp::prepare::bodies_as_describe_types_them") {
    session_sql::fixture_t db("test_prepare/bodies");
    seed(db);
    const auto s = any();
    CHECK(db.refused(s, "PREPARE p AS SELECT 1 FROM s.t WHERE $1 IS NULL;").type == code::indeterminate_datatype);
    // PostgreSQL 18 keeps a declared $n the body skips.
    db.ok(s, "PREPARE skip(bigint, bigint) AS SELECT $2;");
    CHECK(db.ints(s, "EXECUTE skip(1, 2);") == ints{2});
    auto skipped = db.dispatcher()->describe_sql(s, "EXECUTE skip;");
    INFO(session_sql::error_of(skipped));
    REQUIRE(skipped->is_success());
    CHECK(parameters_of(skipped) == std::vector<logical_type>{logical_type::BIGINT, logical_type::BIGINT});
    db.ok(any(), "CREATE TYPE mood AS ENUM ('sad', 'ok');");
    CHECK(db.refused(s, "PREPARE m(mood) AS SELECT $1;").type == code::unimplemented_yet);
    db.ok(s, "PREPARE unused(bigint, bigint) AS SELECT $1;");
    CHECK(db.ints(s, "EXECUTE unused(4, 5);") == ints{4});
    db.ok(s, "PREPARE sub AS SELECT a FROM s.t WHERE a = (SELECT max(a) FROM s.t WHERE a < $1);");
    CHECK(db.ints(s, "EXECUTE sub(3);") == ints{2});
    db.ok(any(), "CREATE VIEW s.v AS SELECT a FROM s.t WHERE a > 1;");
    db.ok(s, "PREPARE overview AS SELECT a FROM s.v WHERE a < $1 ORDER BY a;");
    CHECK(db.ints(s, "EXECUTE overview(10);") == ints{2, 3});
}

TEST_CASE("integration::cpp::prepare::explain_execute") {
    session_sql::fixture_t db("test_prepare/explain");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    const auto explained = db.ok(s, "EXPLAIN EXECUTE q(1);");
    CHECK(plan_text(explained) == plan_text(db.ok(s, "EXPLAIN SELECT a FROM s.t WHERE a = 1;")));
    const auto analyzed = plan_text(db.ok(s, "EXPLAIN ANALYZE EXECUTE q(1);"));
    CHECK(analyzed.find("actual time") != std::string::npos);
    CHECK(analyzed.find("rows=") != std::string::npos);
    db.check_refused(s,
                     "EXPLAIN EXECUTE nope;",
                     code::undefined_prepared_statement,
                     "prepared statement \"nope\" does not exist");
    for (const auto* sql : {"EXPLAIN EXECUTE q(1);", "EXPLAIN EXECUTE nope;"}) {
        auto described = db.dispatcher()->describe_sql(s, sql);
        INFO(sql << " -> " << session_sql::error_of(described));
        REQUIRE(described->is_success());
        REQUIRE(described->type_data().size() == 1);
        CHECK(described->type_data().front().alias() == "QUERY PLAN");
    }
}

TEST_CASE("integration::cpp::prepare::describe") {
    session_sql::fixture_t db("test_prepare/describe");
    seed(db);
    const auto s = any();
    auto* dispatcher = db.dispatcher();

    auto prepare = dispatcher->describe_sql(s, "PREPARE z AS SELECT a FROM s.t WHERE a = $1;");
    REQUIRE(prepare->is_success());
    CHECK(prepare->type_data().empty());
    CHECK(prepare->parameter_types().empty());
    db.check_refused(s, "EXECUTE z(1);", code::undefined_prepared_statement, "prepared statement \"z\" does not exist");

    auto nope = dispatcher->describe_sql(s, "EXECUTE nope;");
    REQUIRE(nope->is_success());
    CHECK(nope->type_data().empty());

    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    auto q = dispatcher->describe_sql(s, "EXECUTE q;");
    INFO(session_sql::error_of(q));
    REQUIRE(q->is_success());
    CHECK(q->size() == 0);
    CHECK(columns_of(q) == std::vector<logical_type>{logical_type::BIGINT});
    CHECK(q->type_data().front().alias() == "a");
    CHECK(parameters_of(q) == std::vector<logical_type>{logical_type::BIGINT});

    auto deallocate = dispatcher->describe_sql(s, "DEALLOCATE q;");
    REQUIRE(deallocate->is_success());
    CHECK(deallocate->type_data().empty());
    CHECK(db.ints(s, "EXECUTE q(1);") == ints{1});
}

TEST_CASE("integration::cpp::prepare::create_table_as_execute_is_refused_as_create_table_as") {
    session_sql::fixture_t db("test_prepare/ctas");
    seed(db);
    const auto s = any();
    db.ok(s, "PREPARE q AS SELECT a FROM s.t WHERE a = $1;");
    db.check_refused(s,
                     "CREATE TABLE s.x AS EXECUTE q(1);",
                     code::sql_parse_error,
                     "CREATE TABLE AS is not supported yet (#675)");
}

// PREPARE in a block runs alone: an EXECUTE of the session sent while it runs waits for it.
TEST_CASE("integration::cpp::prepare::a_prepare_in_a_block_runs_alone") {
    host_t host("test_prepare/alone");
    host.register_orders();
    host.backend().set_rows("m2.shop.public.orders", {{1, 100}, {2, 200}});
    const auto s = any();
    host.db.ok(s, "BEGIN;");
    host.backend().answer_opens_together(2);

    components::cursor::cursor_t_ptr running;
    std::thread reader([&] { running = host.db.run(s, "SELECT id FROM m2.shop.public.orders;"); });
    REQUIRE(test_helpers::wait_until([&] { return host.backend().opens() == 1; }));

    const auto waited = services::dispatcher::dev_statements_waited_turn();
    components::cursor::cursor_t_ptr prepared;
    std::thread preparing([&] { prepared = host.db.run(s, "PREPARE q AS SELECT 1;"); });
    CHECK(test_helpers::wait_until([&] { return services::dispatcher::dev_statements_waited_turn() > waited; }));
    components::cursor::cursor_t_ptr executed;
    std::thread executing([&] { executed = host.db.run(s, "EXECUTE q;"); });
    CHECK(test_helpers::wait_until([&] { return services::dispatcher::dev_statements_waited_turn() > waited + 1; }));

    host.db.ok(any(), "SELECT id FROM m2.shop.public.orders;");
    reader.join();
    preparing.join();
    executing.join();
    INFO(session_sql::error_of(running));
    CHECK(running->is_success());
    INFO(session_sql::error_of(prepared));
    CHECK(prepared->is_success());
    INFO(session_sql::error_of(executed));
    REQUIRE(executed->is_success());
    CHECK(executed->size() == 1);
    host.db.ok(s, "ROLLBACK;");
}

// The host sends the nodes the SQL forms lower to: neither parses nor translates the body again.
TEST_CASE("integration::cpp::prepare::the_host_sends_the_nodes") {
    session_sql::fixture_t db("test_prepare/nodes");
    seed(db);
    const auto s = any();
    auto* resource = db.dispatcher()->resource();
    auto plan_of = [&](components::logical_plan::node_ptr node) {
        return components::logical_plan::execution_plan_t{resource,
                                                          std::move(node),
                                                          components::logical_plan::make_parameter_node(resource)};
    };

    std::pmr::vector<components::types::complex_logical_type> declared{resource};
    declared.emplace_back(logical_type::BIGINT);
    auto prepared = db.dispatcher()->execute_plan(
        s,
        plan_of(components::logical_plan::make_node_prepare(resource,
                                                            "q",
                                                            "SELECT a FROM s.t WHERE a = $1",
                                                            std::move(declared))));
    INFO(session_sql::error_of(prepared));
    REQUIRE(prepared->is_success());

    auto execute = components::logical_plan::execution_plan_t{resource,
                                                              nullptr,
                                                              components::logical_plan::make_parameter_node(resource)};
    const auto slot = execute.parameters->add_parameter(components::vector::make_scalar(resource, std::int64_t{2}),
                                                        components::vector::parameter_kind::external);
    std::pmr::vector<core::parameter_id_t> arguments{resource};
    arguments.push_back(slot);
    execute.sub_queries.back() = components::logical_plan::make_node_execute(resource, "q", std::move(arguments));
    auto executed = db.dispatcher()->execute_plan(s, std::move(execute));
    INFO(session_sql::error_of(executed));
    REQUIRE(executed->is_success());
    REQUIRE(executed->size() == 1);
    CHECK(executed->chunks().front().get_value<std::int64_t>(0, 0) == 2);

    CHECK(db.dispatcher()
              ->execute_plan(s, plan_of(components::logical_plan::make_node_deallocate(resource, "q")))
              ->is_success());
    db.check_refused(s, "EXECUTE q(2);", code::undefined_prepared_statement, missing_q);
    CHECK(db.dispatcher()
              ->execute_plan(s, plan_of(components::logical_plan::make_node_deallocate_all(resource)))
              ->is_success());

    auto unnamed =
        db.dispatcher()->execute_plan(s,
                                      plan_of(components::logical_plan::make_node_prepare(
                                          resource,
                                          "",
                                          "SELECT 1",
                                          std::pmr::vector<components::types::complex_logical_type>{resource})));
    REQUIRE(unnamed->is_error());
    CHECK(unnamed->get_error().type == code::protocol_violation);
    CHECK(std::string{unnamed->get_error().what} == "invalid statement name: must not be empty");
}

// The host's body is what PostgreSQL 18's grammar lets PREPARE hold (PreparableStmt): anything else is refused at
// PREPARE and nothing is stored.
TEST_CASE("integration::cpp::prepare::a_host_body_is_a_select_insert_update_or_delete") {
    session_sql::fixture_t db("test_prepare/host_body");
    seed(db);
    const auto s = any();
    auto* resource = db.dispatcher()->resource();
    for (const auto* body : {"EXECUTE q", "BEGIN", "PREPARE x AS SELECT 1"}) {
        INFO(body);
        auto prepared =
            db.dispatcher()->execute_plan(s,
                                          components::logical_plan::execution_plan_t{
                                              resource,
                                              components::logical_plan::make_node_prepare(
                                                  resource,
                                                  "q",
                                                  body,
                                                  std::pmr::vector<components::types::complex_logical_type>{resource}),
                                              components::logical_plan::make_parameter_node(resource)});
        REQUIRE(prepared->is_error());
        CHECK(prepared->get_error().type == code::protocol_violation);
        CHECK(std::string{prepared->get_error().what} ==
              "the body of a prepared statement must be SELECT, INSERT, UPDATE or DELETE");
        db.check_refused(s, "EXECUTE q;", code::undefined_prepared_statement, missing_q);
    }
    db.check_refused(s, "EXECUTE x;", code::undefined_prepared_statement, "prepared statement \"x\" does not exist");
}

// PostgreSQL 18 drops a declared typmod (numeric(10,2) is numeric); otterbrix keeps it: it has no NUMERIC without a
// precision.
TEST_CASE("integration::cpp::prepare::a_declared_type_keeps_its_typmod") {
    session_sql::fixture_t db("test_prepare/declared_typmod");
    const auto s = any();
    db.ok(s, "PREPARE dd2(numeric(10,2)) AS SELECT $1;");
    auto rounded = db.ok(s, "EXECUTE dd2(1.257);");
    REQUIRE(rounded->size() == 1);
    const auto& column = rounded->type_data().front();
    REQUIRE(column.type() == logical_type::DECIMAL);
    const auto* decimal = column.extension_as<components::types::decimal_logical_type_extension>();
    CHECK(decimal->width() == 10);
    CHECK(decimal->scale() == 2);
    auto unscaled = rounded->get<std::int64_t>(0, 0);
    REQUIRE_FALSE(unscaled.has_error());
    CHECK(unscaled.value() == 126);
}

// Two declarations of a name differ by any part of a declared type.
TEST_CASE("integration::cpp::prepare::the_node_hash_holds_the_whole_declared_types") {
    std::pmr::synchronized_pool_resource resource;
    auto node_of = [&](std::uint8_t width) {
        std::pmr::vector<components::types::complex_logical_type> declared{&resource};
        auto decimal = components::types::complex_logical_type::create_decimal(&resource, width, 2);
        REQUIRE(!decimal.has_error());
        declared.push_back(std::move(decimal.value()));
        return components::logical_plan::make_node_prepare(&resource, "q", "SELECT $1", std::move(declared));
    };
    CHECK(node_of(10)->hash() != node_of(12)->hash());
    CHECK(node_of(10)->hash() == node_of(10)->hash());
}
