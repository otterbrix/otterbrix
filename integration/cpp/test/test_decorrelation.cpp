// A correlated sub-query is turned into a join over its whole table, as PostgreSQL 18 pulls a sub-link up: [NOT]
// EXISTS and [NOT] IN of a WHERE conjunct become a semi / anti join, a scalar aggregate sub-query and a LATERAL
// aggregate a GROUP BY of the correlation keys joined LEFT. The answers are the sub-queries' own (SQL's NULL rules
// included); the sub-query is read once, not once per outer row.

#include "session_sql_fixture.hpp"

#include <components/compute/aggregate_state.hpp>
#include <components/compute/compute_kernel.hpp>
#include <components/compute/function.hpp>

#include <algorithm>
#include <string>
#include <vector>

using code = core::error_code_t;
using rows_t = std::vector<std::vector<std::string>>;
using strings = std::vector<std::string>;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    // d(id, name): departments 1..3 and one without an id; e(id, dept, sal): staff, one without a department.
    void staff(session_sql::fixture_t& db) {
        db.ok(any(), "CREATE TABLE d (id BIGINT, name TEXT);");
        db.ok(any(), "CREATE TABLE e (id BIGINT, dept BIGINT, sal BIGINT);");
        db.ok(any(), "INSERT INTO d (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (NULL, 'x');");
        db.ok(any(), "INSERT INTO e (id, dept, sal) VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, NULL, 5);");
    }

    // Every row, its cells as text (NULL as "null"), sorted.
    rows_t rows_of(session_sql::fixture_t& db, const std::string& sql) {
        auto cursor = db.ok(any(), sql);
        rows_t rows;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                std::vector<std::string> cells;
                for (std::uint64_t column = 0; column < chunk.column_count(); ++column) {
                    const auto& cell = chunk.data[column];
                    if (cell.is_null(row)) {
                        cells.emplace_back("null");
                    } else if (cell.type().type() == components::types::logical_type::STRING_LITERAL) {
                        cells.emplace_back(cell.get_value<std::string_view>(row));
                    } else if (cell.type().type() == components::types::logical_type::UBIGINT) {
                        cells.push_back(std::to_string(cell.get_value<std::uint64_t>(row)));
                    } else if (cell.type().type() == components::types::logical_type::BOOLEAN) {
                        cells.emplace_back(cell.get_value<bool>(row) ? "true" : "false");
                    } else if (cell.type().type() == components::types::logical_type::DOUBLE) {
                        cells.push_back(std::to_string(static_cast<std::int64_t>(cell.get_value<double>(row))));
                    } else {
                        cells.push_back(std::to_string(cell.get_value<std::int64_t>(row)));
                    }
                }
                rows.push_back(std::move(cells));
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    strings plan_of(session_sql::fixture_t& db, const std::string& sql) {
        auto cursor = db.ok(any(), "EXPLAIN " + sql);
        strings lines;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                lines.emplace_back(chunk.get_value<std::string_view>(0, row));
            }
        }
        return lines;
    }

    bool has_line(const strings& lines, const std::string& part) {
        return std::any_of(lines.begin(), lines.end(), [&part](const std::string& line) {
            return line.find(part) != std::string::npos;
        });
    }

} // namespace

TEST_CASE("integration::cpp::decorrelation::exists_and_not_exists") {
    session_sql::fixture_t db("test_decorrelation/exists");
    staff(db);
    const std::string exists = "SELECT d.id FROM d WHERE EXISTS (SELECT 1 FROM e WHERE e.dept = d.id);";
    CHECK(rows_of(db, exists) == rows_t{{"1"}, {"2"}});
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE NOT EXISTS (SELECT 1 FROM e WHERE e.dept = d.id);") ==
          rows_t{{"3"}, {"null"}});
    // Inside a larger WHERE, and with a condition of the sub-query's own.
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE d.id >= 2 AND EXISTS (SELECT 1 FROM e WHERE e.dept = d.id);") ==
          rows_t{{"2"}});
    CHECK(rows_of(db,
                  "SELECT d.id FROM d WHERE NOT EXISTS (SELECT 1 FROM e WHERE e.dept = d.id AND e.sal > 15) "
                  "AND d.id IS NOT NULL;") == rows_t{{"3"}});
    // Two correlations, and one that is not an equality.
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE EXISTS (SELECT 1 FROM e WHERE e.dept = d.id AND e.id = d.id);") ==
          rows_t{{"1"}});
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE EXISTS (SELECT 1 FROM e WHERE e.sal > d.id * 15);") == rows_t{{"1"}});
    // A correlation inside a list or a CASE is read through the joined sub-query too.
    CHECK(rows_of(db,
                  "SELECT d.id FROM d WHERE EXISTS (SELECT 1 FROM e WHERE d.id = CASE WHEN e.sal > 25 THEN e.id "
                  "ELSE e.dept END);") == rows_t{{"1"}, {"3"}});
    const auto lines = plan_of(db, exists);
    INFO(lines.size());
    CHECK(has_line(lines, "Hash Semi Join"));
    CHECK_FALSE(has_line(lines, "Nested Loop"));
    CHECK(has_line(plan_of(db, "SELECT d.id FROM d WHERE NOT EXISTS (SELECT 1 FROM e WHERE e.dept = d.id);"),
                   "Hash Anti Join"));
}

TEST_CASE("integration::cpp::decorrelation::in_and_not_in") {
    session_sql::fixture_t db("test_decorrelation/in");
    staff(db);
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE d.id IN (SELECT e.dept FROM e WHERE e.sal > d.id);") ==
          rows_t{{"1"}, {"2"}});
    // A NULL among the sub-query's rows makes NOT IN never true; without one, NOT IN keeps what is not there. The
    // department without an id compares with no salary: its set is empty, and anything is NOT IN an empty set.
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE d.id NOT IN (SELECT e.dept FROM e WHERE e.sal > d.id);") ==
          rows_t{{"null"}});
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE d.id NOT IN (SELECT e.dept FROM e WHERE e.sal > d.id + 5);") ==
          rows_t{{"3"}, {"null"}});
    // An outer NULL is not in a non-empty set, and is NOT IN an empty one.
    CHECK(rows_of(db,
                  "SELECT d.name FROM d WHERE d.id NOT IN (SELECT e.dept FROM e WHERE e.sal > 25 AND "
                  "e.id = 3 AND d.name = 'x');") == rows_t{{"a"}, {"b"}, {"c"}});
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE d.id IN (SELECT e.dept FROM e WHERE e.id = d.id);") == rows_t{{"1"}});
}

TEST_CASE("integration::cpp::decorrelation::scalar_sub_queries") {
    session_sql::fixture_t db("test_decorrelation/scalar");
    staff(db);
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE 15 < (SELECT max(e.sal) FROM e WHERE e.dept = d.id);") ==
          rows_t{{"1"}, {"2"}});
    // count(*) of no row is 0, not NULL; sum of no row is NULL.
    CHECK(rows_of(db, "SELECT d.id, (SELECT count(*) FROM e WHERE e.dept = d.id) AS c FROM d;") ==
          rows_t{{"1", "2"}, {"2", "1"}, {"3", "0"}, {"null", "0"}});
    CHECK(rows_of(db, "SELECT d.id, (SELECT sum(e.sal) FROM e WHERE e.dept = d.id) AS s FROM d;") ==
          rows_t{{"1", "30"}, {"2", "30"}, {"3", "null"}, {"null", "null"}});
    // A sub-query of no aggregate gives its one row, NULL for none, and an error for more.
    CHECK(rows_of(db, "SELECT e.id, (SELECT d.name FROM d WHERE d.id = e.dept) AS n FROM e;") ==
          rows_t{{"1", "a"}, {"2", "a"}, {"3", "b"}, {"4", "null"}});
    db.check_refused(any(),
                     "SELECT d.id, (SELECT e.sal FROM e WHERE e.dept = d.id) AS s FROM d;",
                     code::cardinality_violation,
                     "more than one row returned by a subquery used as an expression");
    // A departure from PostgreSQL 18 in the moment of the error: PostgreSQL computes the sub-query only for the rows
    // that reach the output and answers NULL here (the department without an id comes first and has no staff);
    // the join reads every department first, as Trino's TransformCorrelatedScalarSubquery does.
    db.check_refused(any(),
                     "SELECT d.id, (SELECT e.sal FROM e WHERE e.dept = d.id) AS s FROM d ORDER BY d.id DESC LIMIT 1;",
                     code::cardinality_violation,
                     "more than one row returned by a subquery used as an expression");
    const auto lines = plan_of(db, "SELECT d.id, (SELECT count(*) FROM e WHERE e.dept = d.id) AS c FROM d;");
    CHECK(has_line(lines, "Hash Join"));
    CHECK_FALSE(has_line(lines, "Nested Loop"));
}

// An expression over aggregates is computed over the group's values, the empty group of a department without staff
// included: its count is 0 and its sum, min and max are NULL, whatever stands around them.
TEST_CASE("integration::cpp::decorrelation::an_expression_over_the_aggregates_of_an_empty_group") {
    session_sql::fixture_t db("test_decorrelation/empty_group");
    staff(db);
    const auto of = [&db](const std::string& value) {
        return rows_of(db,
                       "SELECT d.id, (SELECT " + value + " FROM e WHERE e.dept = d.id) AS v FROM d WHERE d.id = 3;");
    };
    CHECK(of("coalesce(sum(e.sal), 0)") == rows_t{{"3", "0"}});
    CHECK(of("coalesce(count(*), 5)") == rows_t{{"3", "0"}});
    CHECK(of("CASE WHEN count(*) = 0 THEN 0 ELSE 1 END") == rows_t{{"3", "0"}});
    CHECK(of("max(e.sal) IS NULL") == rows_t{{"3", "true"}});
    CHECK(of("count(*) + 1") == rows_t{{"3", "1"}});
    // And over a group with rows.
    CHECK(rows_of(db,
                  "SELECT d.id, (SELECT coalesce(sum(e.sal), 0) + count(*) FROM e WHERE e.dept = d.id) AS v FROM d "
                  "WHERE d.id = 1;") == rows_t{{"1", "32"}});
}

// A LATERAL of bare sum / min / max / avg is a LEFT join of a GROUP BY: of no row each is NULL, as the join gives.
TEST_CASE("integration::cpp::decorrelation::a_lateral_aggregate") {
    session_sql::fixture_t db("test_decorrelation/lateral");
    staff(db);
    const std::string lateral =
        "SELECT d.id, s.m FROM d, LATERAL (SELECT max(e.sal) AS m FROM e WHERE e.dept = d.id) s;";
    CHECK(rows_of(db, lateral) == rows_t{{"1", "20"}, {"2", "30"}, {"3", "null"}, {"null", "null"}});
    const auto lines = plan_of(db, lateral);
    CHECK(has_line(lines, "Hash Join"));
    CHECK_FALSE(has_line(lines, "Nested Loop"));
}

// A LATERAL with a count, or an expression over an aggregate, is read per outer row: the count of an empty group is
// 0 however the outer query reads it.
TEST_CASE("integration::cpp::decorrelation::a_lateral_count_is_read_per_row") {
    session_sql::fixture_t db("test_decorrelation/lateral_count");
    staff(db);
    const std::string count = "LATERAL (SELECT count(*) AS c FROM e WHERE e.dept = d.id) s";
    CHECK(rows_of(db, "SELECT d.id, s.c FROM d, " + count + ";") ==
          rows_t{{"1", "2"}, {"2", "1"}, {"3", "0"}, {"null", "0"}});
    CHECK(rows_of(db, "SELECT d.id, c FROM d, " + count + ";") ==
          rows_t{{"1", "2"}, {"2", "1"}, {"3", "0"}, {"null", "0"}});
    CHECK(rows_of(db, "SELECT d.id, CASE WHEN s.c = 0 THEN 7 ELSE 8 END AS k FROM d, " + count + ";") ==
          rows_t{{"1", "8"}, {"2", "8"}, {"3", "7"}, {"null", "7"}});
    CHECK(rows_of(db, "SELECT d.id, s.c IS NULL AS n FROM d, " + count + ";") ==
          rows_t{{"1", "false"}, {"2", "false"}, {"3", "false"}, {"null", "false"}});
    CHECK(has_line(plan_of(db, "SELECT d.id, s.c FROM d, " + count + ";"), "Nested Loop"));
}

namespace {

    // A host's aggregate the pass does not know: the sum of squares of a BIGINT.
    struct my_agg_state_t {
        double value = 0.0;
    };

    components::compute::aggregate_state_layout_t
    my_agg_layout(const std::pmr::vector<components::types::complex_logical_type>&) {
        return components::compute::aggregate_state_of<my_agg_state_t>();
    }

    core::error_t my_agg_update(components::compute::kernel_context&,
                                const components::vector::data_chunk_t& in,
                                core::span<const uint32_t> groups,
                                components::compute::aggregate_states_t states) {
        const auto& column = in.data[0];
        for (size_t row = 0; row < in.size(); ++row) {
            if (!column.is_null(row)) {
                const auto value = static_cast<double>(column.data<int64_t>()[row]);
                states.at<my_agg_state_t>(groups[row]).value += value * value;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t my_agg_finalize(components::compute::kernel_context&,
                                  components::compute::aggregate_states_t states,
                                  uint64_t first,
                                  uint64_t count,
                                  components::vector::vector_t& out) {
        for (uint64_t row = 0; row < count; ++row) {
            out.data<double>()[row] = states.at<my_agg_state_t>(first + row).value;
        }
        return core::error_t::no_error();
    }

    core::pmr::polymorphic_unique_ptr<components::compute::aggregate_function>
    make_my_agg(std::pmr::memory_resource* resource) {
        using namespace components::compute;
        function_doc doc{resource, "my_agg", "sum of squares", {"arg"}, false};
        auto fn = core::pmr::make_polymorphic_unique<aggregate_function>(resource,
                                                                         "my_agg",
                                                                         arity::unary(),
                                                                         doc,
                                                                         size_t{1},
                                                                         false,
                                                                         volatility_t::immutable);
        kernel_signature_t sig(function_type_t::aggregate,
                               {parameter_type::exact(components::types::logical_type::BIGINT)},
                               {output_type::fixed(components::types::logical_type::DOUBLE)});
        aggregate_kernel kernel{std::move(sig), my_agg_layout, my_agg_update, my_agg_finalize};
        auto added = fn->add_kernel(resource, std::move(kernel));
        REQUIRE_FALSE(added.contains_error());
        return fn;
    }

} // namespace

// A sub-query of a function the pass does not know keeps its path: an aggregate without GROUP BY always gives a row,
// so NOT EXISTS of it is never true, whatever the host's aggregate is called.
TEST_CASE("integration::cpp::decorrelation::a_sub_query_of_an_unknown_function_keeps_its_path") {
    session_sql::fixture_t db("test_decorrelation/unknown_function");
    staff(db);
    REQUIRE_FALSE(db.dispatcher()->register_udf(any(), make_my_agg(db.dispatcher()->resource())).contains_error());
    CHECK(
        rows_of(db, "SELECT d.id FROM d WHERE NOT EXISTS (SELECT my_agg(e.sal) FROM e WHERE e.dept = d.id);").empty());
    CHECK(rows_of(db, "SELECT d.id FROM d WHERE EXISTS (SELECT my_agg(e.sal) FROM e WHERE e.dept = d.id);") ==
          rows_t{{"1"}, {"2"}, {"3"}, {"null"}});
}
