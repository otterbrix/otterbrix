// ============================================================================
// A CHECK ROW WITH NO EXPRESSION MUST NOT QUIETLY REPEAL THE CHECK.
//
// operator_resolve_constraint decoded a 'c' pg_constraint row like this:
//
//     } else if (contype == 'c' && direction == direction_t::outgoing) {
//         if (con_chunk.is_null(conexpr, ci)) { continue; }
//         const auto conexpr_sv = con_chunk.get_value<std::string_view>(conexpr, ci);
//         if (conexpr_sv.empty()) { continue; }
//
// — two `continue`s that drop the row on the floor: check_exprs stays empty, the planner
// splices no operator_check_constraint, and the table takes every row while every
// statement reports success. Two branches up, IN THE SAME LOOP, an unreadable `contype`
// is refused out loud with exactly this argument — "what it declares cannot be determined,
// so it cannot be enforced or dismissed". A CHECK row with no expression is the same fact
// one field over.
//
// HOW THE ROW IS PRODUCED HERE. It is written by the ENGINE: the plan is a CREATE TABLE
// carrying an INLINE constraint node — the shape transform_table builds for
// `CREATE TABLE t (..., CONSTRAINT c CHECK (...))` — run through the same enrich ->
// rewrite_create_table -> build_create_constraint_writes path. The test only hands that
// node over with its check expression unset, the state any writer that lost the expression
// leaves behind: build_create_constraint_writes writes col 10 only `if (is_check &&
// !check_expr.empty())`, so contype lands as 'c' and conexpr lands NULL.
//
// PATH NOT NAMED FROM SQL, deliberately: both live SQL routes are closed one floor up —
// transform_table refuses an inline CHECK whose expression deparsed to nothing, and
// executor_t::execute_plan_full refuses a standalone ADD CONSTRAINT CHECK with an empty
// expression. What is left is a catalog written before those gates, which is what a floor
// is for.
//
// THE CONTROL PROVES THE STAND HAS TEETH: same helper, same node, same CREATE TABLE — with
// the expression present the CHECK is gathered, enforced, and the violating row stays out.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/table/column_definition.hpp>

#include <string>
#include <vector>

using namespace test_helpers;

namespace {

    std::vector<int64_t> column_i64(const components::cursor::cursor_t_ptr& cur, uint64_t col) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t row = 0; row < cur->size(); ++row) {
            out.push_back(cur->value(col, row).value<int64_t>());
        }
        return out;
    }

    // CREATE TABLE <rel> (id bigint, code bigint, CONSTRAINT <name> CHECK (<expr>))
    // as a plan: the production create node, the production INLINE constraint
    // child (set_inline_with_table, exactly as transform_table hangs it off the
    // create node), the production executor. `expr` empty is the defect shape —
    // conexpr is then written NULL.
    components::cursor::cursor_t_ptr create_table_with_check(otterbrix::wrapper_dispatcher_t* d,
                                                             const std::string& db,
                                                             const std::string& rel,
                                                             const std::string& con_name,
                                                             const std::string& expr) {
        auto* resource = d->resource();
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        cols.emplace_back("code", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto created = components::logical_plan::make_node_create_collection(resource,
                                                                             core::relname_t{rel},
                                                                             std::move(cols),
                                                                             {},
                                                                             false);
        static_cast<components::logical_plan::node_create_collection_t*>(created.get())->set_dbname(db);

        auto cstr =
            components::logical_plan::make_node_create_constraint(resource,
                                                                  db,
                                                                  rel,
                                                                  core::constraint_name_t{con_name},
                                                                  components::logical_plan::constraint_kind::check);
        cstr->set_inline_with_table(true);
        if (!expr.empty()) {
            cstr->set_check_expression_sql(expr);
        }
        created->append_child(components::logical_plan::node_ptr{std::move(cstr)});

        components::logical_plan::execution_plan_t plan{resource,
                                                        created,
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_namespace(resource, &plan.catalog_resolves, db);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    void seed(otterbrix::wrapper_dispatcher_t* d) { REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success()); }

} // namespace

// THE CONTROL. The same node with an expression on it: the CHECK is gathered
// from the same catalog row shape and enforced, so the violating row stays out.
TEST_CASE("integration::cpp::check_row_without_expression::a_check_with_an_expression_is_enforced") {
    auto config = make_test_config(integration_fixture_path("test_check_row_without_expression/control"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    auto ddl = create_table_with_check(d, "cur", "t", "chk_code", "code > 0");
    INFO("CREATE TABLE with CHECK (code > 0): " << (ddl->is_error() ? ddl->get_error().what : "accepted"));
    REQUIRE(ddl->is_success());

    auto ok = exec(d, "INSERT INTO cur.t (id, code) VALUES (1, 5);");
    INFO("conforming INSERT: " << (ok->is_error() ? ok->get_error().what : "accepted"));
    REQUIRE(ok->is_success());

    auto bad = exec(d, "INSERT INTO cur.t (id, code) VALUES (2, -1);");
    INFO("violating INSERT: " << (bad->is_error() ? bad->get_error().what : "accepted"));
    REQUIRE(bad->is_error());

    auto stored = exec(d, "SELECT id FROM cur.t ORDER BY id;");
    REQUIRE(stored->is_success());
    REQUIRE(column_i64(stored, 0) == std::vector<int64_t>{1});
}

// THE DEFECT. The same statement with the expression missing. The engine accepts
// the constraint and then cannot read it — so either the declaration or the write
// that rides on the unread constraint has to be refused. What must never happen
// is the third answer: both accepted, and the CHECK silently gone.
TEST_CASE("integration::cpp::check_row_without_expression::a_check_row_with_no_expression_is_not_passed_over") {
    auto config = make_test_config(integration_fixture_path("test_check_row_without_expression/no_expr"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    auto ddl = create_table_with_check(d, "cur", "t", "chk_code", "");
    INFO("CREATE TABLE with a CHECK carrying no expression: "
         << (ddl->is_error() ? ddl->get_error().what : "accepted"));

    auto ins = exec(d, "INSERT INTO cur.t (id, code) VALUES (1, 5);");
    INFO("INSERT under the unreadable CHECK: " << (ins->is_error() ? ins->get_error().what : "accepted"));

    INFO("a CHECK the engine accepted is enforced, or the write is refused — never quietly dismissed");
    REQUIRE((ddl->is_error() || ins->is_error()));

    if (ins->is_error()) {
        INFO("the refusal has to name the constraint it is about");
        REQUIRE(std::string(ins->get_error().what).find("chk_code") != std::string::npos);
    }
}

// LOUD IS NOT FATAL. The refusal rides on the statement that would have been
// written under the unread CHECK; the database around it still reads, still
// writes where no such row exists, and still drops.
TEST_CASE("integration::cpp::check_row_without_expression::an_unreadable_check_row_does_not_brick_the_database") {
    auto config = make_test_config(integration_fixture_path("test_check_row_without_expression/not_bricked"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);
    create_table_with_check(d, "cur", "t", "chk_code", "");
    // Rows written before the CHECK is ever read: a bricked database takes them
    // with it. This INSERT is itself the statement the unreadable row refuses, so
    // its status is not asserted — only that everything AROUND it still works.
    exec(d, "INSERT INTO cur.t (id, code) VALUES (1, 100), (2, 200);");
    REQUIRE(exec(d, "CREATE TABLE cur.other (id bigint);")->is_success());

    INFO("the table still reads");
    {
        auto cur = exec(d, "SELECT id FROM cur.t ORDER BY id;");
        INFO("read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    INFO("a table that never saw the bad row is untouched by it");
    REQUIRE(exec(d, "INSERT INTO cur.other (id) VALUES (7);")->is_success());
    INFO("and the way out is open");
    {
        auto drop = exec(d, "DROP TABLE cur.t;");
        INFO("error: " << (drop->is_error() ? drop->get_error().what : "none"));
        REQUIRE(drop->is_success());
    }
}
