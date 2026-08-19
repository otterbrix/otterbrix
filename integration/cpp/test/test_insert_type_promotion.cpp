#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/sql/transformer/utils.hpp>
#include <string>

// promote_column rebuilds a whole column, cell by cell through logical_value_t, every time a
// later row widens that column's type — the shape usually called quadratic promotion. A workload
// with one type per column never enters this path at all.
//
// This is a counter test, not a timing test: it says what the growth IS rather than how long it
// took on a busy machine. If the cost were quadratic in the row count, doubling the rows would
// roughly quadruple the rewrites. If it is bounded by the type lattice (int -> bigint -> double
// is only two steps), doubling the rows merely doubles them.
namespace {
    // WORST CASE on purpose: the widening literal comes LAST, so every row already parsed has
    // to be rewritten. Putting it early (the obvious way to write this test) measures nothing —
    // the column reaches its widest type on row 3 and never widens again.
    std::string late_widening_values(int rows) {
        std::string sql = "INSERT INTO p.t (id, v) VALUES ";
        for (int i = 0; i < rows; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", ";
            sql += (i == rows - 1) ? (std::to_string(i) + ".5") : std::to_string(i);
            sql += ")";
        }
        return sql + ";";
    }
} // namespace

TEST_CASE("integration::cpp::test_insert_type_promotion::growth_is_not_quadratic") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_insert_type_promotion/growth");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE p;")->is_success());
    REQUIRE(exec("CREATE TABLE p.t (id bigint, v double);")->is_success());

    auto rewrites_for = [&](int rows) {
        components::sql::transform::reset_insert_promote_rows();
        REQUIRE(exec(late_widening_values(rows))->is_success());
        return components::sql::transform::insert_promote_rows();
    };

    const auto small = rewrites_for(300);
    const auto large = rewrites_for(600);

    INFO("rows rewritten by promote_column (widening literal last): 300 values -> "
         << small << ", 600 values -> " << large);
    // Doubling the rows must not more than triple the rewrites. Quadratic growth would be ~4x;
    // the bound leaves room for the lattice-climb constant without admitting N^2.
    CHECK(large <= small * 3 + 16);
}
