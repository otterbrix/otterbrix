#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/compute/function.hpp>
#include <components/vector/vector_builders.hpp>
#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>

#include <atomic>
#include <string>
#include <utility>
#include <vector>

// GROUP BY, HAVING, ORDER BY, LIMIT and JOIN answered over named tables, with and without an index and with the
// aggregate pushed to the owning agent: what the statement returns, whatever the clause nodes carry.

using namespace test_helpers;

namespace {

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE cl;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE cl.t (g bigint, v bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE cl.u (g bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO cl.t (g, v) VALUES (1, 10), (1, 20), (2, 30), (2, 50), (2, 40), (3, 5);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO cl.u (g, name) VALUES (1, 'a'), (2, 'b'), (4, 'd');")->is_success());
    }

    std::vector<std::pair<int64_t, int64_t>> pairs_of(const components::cursor::cursor_t_ptr& cur) {
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        std::vector<std::pair<int64_t, int64_t>> out;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto cell0 = cur->get<int64_t>(0, row);
            REQUIRE_FALSE(cell0.has_error());
            auto cell1 = cur->get<int64_t>(1, row);
            REQUIRE_FALSE(cell1.has_error());
            out.emplace_back(cell0.value(), cell1.value());
        }
        return out;
    }

    std::vector<int64_t> column_of(const components::cursor::cursor_t_ptr& cur) {
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        std::vector<int64_t> out;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto cell2 = cur->get<int64_t>(0, row);
            REQUIRE_FALSE(cell2.has_error());
            out.push_back(cell2.value());
        }
        return out;
    }

    std::string plan_of(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, "EXPLAIN " + sql);
        REQUIRE(cur->is_success());
        std::string text;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto cell3 = cur->get<std::string_view>(0, row);
            REQUIRE_FALSE(cell3.has_error());
            text += std::string(cell3.value());
            text += '\n';
        }
        return text;
    }

    using pairs = std::vector<std::pair<int64_t, int64_t>>;
    using values = std::vector<int64_t>;
    using names = std::vector<std::string>;

    void seed_ob(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(exec(dispatcher, "CREATE DATABASE ob;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE ob.c (id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE ob.o (cid bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE ob.t (a bigint, b bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE ob.d (k bigint, v bigint, ts bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE ob.big (id bigint, v bigint);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO ob.c (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO ob.o (cid, name) VALUES (1, 'z'), (2, 'y'), (3, 'x');")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO ob.t (a, b) VALUES (2, 20), (3, 10), (1, 30);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO ob.d (k, v, ts) VALUES (1, 10, 1), (1, 11, 2), (2, 20, 1), (2, 21, 3);")
                    ->is_success());
        REQUIRE(seed_rows(dispatcher, "ob.big", "id, v", 1000, [](unsigned i) {
                    return "(" + std::to_string(i + 1) + ", " + std::to_string(i + 1) + ")";
                })->is_success());
    }

    values ints_at(const components::cursor::cursor_t_ptr& cur, std::size_t col) {
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        values out;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto cell = cur->get<int64_t>(col, row);
            REQUIRE_FALSE(cell.has_error());
            out.push_back(cell.value());
        }
        return out;
    }

    names strings_at(const components::cursor::cursor_t_ptr& cur, std::size_t col) {
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        names out;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto cell = cur->get<std::string_view>(col, row);
            REQUIRE_FALSE(cell.has_error());
            out.emplace_back(cell.value());
        }
        return out;
    }

    std::size_t width_of(const components::cursor::cursor_t_ptr& cur) {
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return cur->column_count();
    }

    std::string refusal_of(const components::cursor::cursor_t_ptr& cur) {
        REQUIRE(cur->is_error());
        return std::string(cur->get_error().what);
    }

    bool mentions(const std::string& text, const std::string& part) { return text.find(part) != std::string::npos; }

    std::atomic<uint64_t> probe_calls{0};

    core::error_t probe_kernel(components::compute::kernel_context&,
                               const components::vector::data_chunk_t& in,
                               components::vector::vector_t& out) {
        probe_calls += in.size();
        const auto* source = in.data[0].data<int64_t>();
        auto* destination = out.data<int64_t>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = source[row] + 1;
        }
        return core::error_t::no_error();
    }

    components::compute::function_ptr make_probe(std::pmr::memory_resource* resource) {
        components::compute::function_doc doc{resource, "short_doc", "full_doc", {"arg"}, false};
        auto fn = core::pmr::make_polymorphic_unique<components::compute::vector_function>(
            resource,
            "probe",
            components::compute::arity::unary(),
            doc,
            size_t{1}, components::compute::volatility_t::volatile_);
        components::compute::kernel_signature_t sig(
            components::compute::function_type_t::vector,
            {components::compute::parameter_type::exact(components::types::logical_type::BIGINT)},
            {components::compute::output_type::fixed(components::types::logical_type::BIGINT)});
        components::compute::vector_kernel kernel{std::move(sig), probe_kernel};
        auto added = fn->add_kernel(resource, std::move(kernel));
        REQUIRE_FALSE(added.contains_error());
        return fn;
    }

    uint64_t probe_calls_of(otterbrix::wrapper_dispatcher_t* dispatcher,
                            const std::string& sql,
                            components::cursor::cursor_t_ptr& cur) {
        probe_calls = 0;
        cur = exec(dispatcher, sql);
        return probe_calls.load();
    }

} // namespace

TEST_CASE("integration::cpp::clause_answers::group_having_order_limit_over_a_table") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/table")));
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    INFO("GROUP BY ... ORDER BY, the aggregate answered by the owning agent");
    services::disk::reset_pushdown_reply_rows();
    CHECK(pairs_of(exec(dispatcher, "SELECT g, SUM(v) AS s FROM cl.t GROUP BY g ORDER BY g;")) ==
          pairs{{1, 30}, {2, 120}, {3, 5}});
    CHECK(services::disk::pushdown_reply_rows() > 0);

    INFO("HAVING keeps the groups it admits, ORDER BY DESC orders them");
    CHECK(
        pairs_of(exec(dispatcher, "SELECT g, SUM(v) AS s FROM cl.t GROUP BY g HAVING SUM(v) > 20 ORDER BY g DESC;")) ==
        pairs{{2, 120}, {1, 30}});

    INFO("ORDER BY ... LIMIT ... OFFSET over the rows");
    CHECK(column_of(exec(dispatcher, "SELECT v FROM cl.t ORDER BY v DESC LIMIT 2 OFFSET 1;")) == values{40, 30});

    INFO("LIMIT ... OFFSET over the groups");
    CHECK(pairs_of(exec(dispatcher, "SELECT g, SUM(v) AS s FROM cl.t GROUP BY g ORDER BY g LIMIT 1 OFFSET 1;")) ==
          pairs{{2, 120}});

    INFO("WHERE, GROUP BY, ORDER BY and LIMIT together");
    CHECK(pairs_of(exec(dispatcher,
                        "SELECT g, COUNT(*) AS c FROM cl.t WHERE v >= 20 GROUP BY g ORDER BY g DESC LIMIT 1;")) ==
          pairs{{2, 3}});
}

TEST_CASE("integration::cpp::clause_answers::an_index_answers_the_same") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/index")));
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);
    REQUIRE(exec(dispatcher, "CREATE INDEX t_g ON cl.t (g);")->is_success());

    const std::string ordered = "SELECT v FROM cl.t WHERE g = 2 ORDER BY v LIMIT 2;";
    const auto plan = plan_of(dispatcher, ordered);
    INFO("plan:\n" << plan);
    CHECK(plan.find("Index Scan") != std::string::npos);
    CHECK(plan.find("Limit") != std::string::npos);

    services::index::reset_index_agent_reads();
    CHECK(column_of(exec(dispatcher, ordered)) == values{30, 40});
    CHECK(services::index::index_agent_reads() >= 1);

    CHECK(pairs_of(exec(dispatcher, "SELECT g, SUM(v) AS s FROM cl.t WHERE g = 2 GROUP BY g HAVING COUNT(*) > 1;")) ==
          pairs{{2, 120}});
}

TEST_CASE("integration::cpp::clause_answers::a_join_groups_orders_and_limits") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/join")));
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);

    CHECK(pairs_of(exec(dispatcher,
                        "SELECT t.g, COUNT(*) AS c FROM cl.t JOIN cl.u ON t.g = u.g GROUP BY t.g ORDER BY t.g;")) ==
          pairs{{1, 2}, {2, 3}});
    CHECK(pairs_of(exec(dispatcher,
                        "SELECT t.g, SUM(t.v) AS s FROM cl.t JOIN cl.u ON t.g = u.g GROUP BY t.g HAVING SUM(t.v) > 50 "
                        "ORDER BY t.g DESC LIMIT 1;")) == pairs{{2, 120}});
}

TEST_CASE("integration::cpp::clause_answers::insert_from_a_grouped_select") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/insert_select")));
    auto* dispatcher = space.dispatcher();
    seed(dispatcher);
    REQUIRE(exec(dispatcher, "CREATE TABLE cl.s (g bigint, total bigint);")->is_success());

    auto inserted = exec(dispatcher, "INSERT INTO cl.s (g, total) SELECT g, SUM(v) FROM cl.t GROUP BY g ORDER BY g;");
    INFO("error: " << (inserted->is_error() ? inserted->get_error().what : "none"));
    REQUIRE(inserted->is_success());
    CHECK(pairs_of(exec(dispatcher, "SELECT g, total FROM cl.s ORDER BY g;")) == pairs{{1, 30}, {2, 120}, {3, 5}});
}

TEST_CASE("integration::cpp::clause_answers::order_by_a_grouping_key_the_select_list_does_not_show") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_grouped")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);
    const std::string from = " FROM ob.c JOIN ob.o ON c.id = o.cid ";

    SECTION("A1 a qualified grouping key outside the select list orders the groups and is not returned") {
        auto cur = exec(dispatcher, "SELECT c.name, count(*)" + from + "GROUP BY c.name, o.name ORDER BY o.name;");
        CHECK(strings_at(cur, 0) == names{"c", "b", "a"});
        CHECK(width_of(cur) == 2);
    }
    SECTION("A2 the same under an alias of the shown column") {
        auto cur =
            exec(dispatcher, "SELECT c.name AS cn, count(*)" + from + "GROUP BY c.name, o.name ORDER BY o.name;");
        CHECK(strings_at(cur, 0) == names{"c", "b", "a"});
        CHECK(width_of(cur) == 2);
    }
    SECTION("A3 two shown columns of one name: the qualifier picks the sort column") {
        auto cur =
            exec(dispatcher, "SELECT c.name, o.name, count(*)" + from + "GROUP BY c.name, o.name ORDER BY o.name;");
        CHECK(strings_at(cur, 0) == names{"c", "b", "a"});
        CHECK(strings_at(cur, 1) == names{"x", "y", "z"});
        CHECK(width_of(cur) == 3);
    }
    SECTION("A4 guard: ordering by the shown grouping key") {
        auto cur = exec(dispatcher, "SELECT c.name, count(*)" + from + "GROUP BY c.name ORDER BY c.name;");
        CHECK(strings_at(cur, 0) == names{"a", "b", "c"});
        CHECK(width_of(cur) == 2);
    }
    SECTION("A5 only an aggregate is shown") {
        auto cur = exec(dispatcher, "SELECT count(*)" + from + "GROUP BY o.name ORDER BY o.name;");
        CHECK(ints_at(cur, 0) == values{1, 1, 1});
        CHECK(width_of(cur) == 1);
    }
    SECTION("A6 a key that is neither grouped nor shown is refused") {
        const auto what =
            refusal_of(exec(dispatcher, "SELECT c.name, count(*)" + from + "GROUP BY c.name ORDER BY o.name;"));
        CHECK(mentions(what, "GROUP BY clause or be used in an aggregate function"));
    }
    SECTION("A7 the same over one table") {
        const auto what = refusal_of(exec(dispatcher, "SELECT a, count(*) FROM ob.t GROUP BY a ORDER BY b;"));
        CHECK(mentions(what, "GROUP BY clause or be used in an aggregate function"));
    }
    SECTION("A8 a hidden grouping key with the aggregate answered by the owning agent") {
        services::disk::reset_pushdown_reply_rows();
        auto cur = exec(dispatcher, "SELECT SUM(v) FROM ob.d GROUP BY k ORDER BY k DESC;");
        CHECK(column_of(cur) == values{41, 21});
        CHECK(width_of(cur) == 1);
        CHECK(services::disk::pushdown_reply_rows() > 0);
    }
    SECTION("A9 the same with HAVING") {
        auto cur = exec(dispatcher, "SELECT SUM(v) FROM ob.d GROUP BY k HAVING count(*) > 1 ORDER BY k DESC;");
        CHECK(column_of(cur) == values{41, 21});
        CHECK(width_of(cur) == 1);
    }
    SECTION("A10 the alias of a shown grouping key") {
        CHECK(column_of(exec(dispatcher, "SELECT a AS x, count(*) FROM ob.t GROUP BY a ORDER BY x;")) ==
              values{1, 2, 3});
    }
    SECTION("A11 a grouping key the binder hides does not settle an ambiguous name") {
        for (const auto* order : {"o.name, name", "name"}) {
            auto cur = exec(dispatcher, "SELECT count(*)" + from + "GROUP BY o.name ORDER BY " + order + ";");
            REQUIRE(cur->is_error());
            CHECK(cur->get_error().type == core::error_code_t::ambiguous_name);
        }
    }
}

TEST_CASE("integration::cpp::clause_answers::order_by_an_output_name") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_alias")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);

    SECTION("B1 the alias of a computed column") {
        CHECK(column_of(exec(dispatcher, "SELECT a + 1 AS x FROM ob.t ORDER BY x;")) == values{2, 3, 4});
    }
    SECTION("B2 the alias of a column") {
        CHECK(column_of(exec(dispatcher, "SELECT a AS x FROM ob.t ORDER BY x;")) == values{1, 2, 3});
    }
    SECTION("B3 the position of a computed column") {
        CHECK(column_of(exec(dispatcher, "SELECT a + 1 AS x FROM ob.t ORDER BY 1;")) == values{2, 3, 4});
    }
    SECTION("B4 an output name wins over the input column of the same name") {
        auto cur = exec(dispatcher, "SELECT a AS b, b AS a FROM ob.t ORDER BY a;");
        CHECK(ints_at(cur, 0) == values{3, 2, 1});
        CHECK(ints_at(cur, 1) == values{10, 20, 30});
    }
    SECTION("B5 guard: the position of swapped names") {
        CHECK(column_of(exec(dispatcher, "SELECT a AS b, b AS a FROM ob.t ORDER BY 1;")) == values{1, 2, 3});
    }
    SECTION("B6 guard: an input column outside the select list is not returned") {
        auto cur = exec(dispatcher, "SELECT a FROM ob.t ORDER BY b;");
        CHECK(column_of(cur) == values{3, 2, 1});
        CHECK(width_of(cur) == 1);
    }
    SECTION("B7 guard: a computed column ordered by an input column") {
        CHECK(column_of(exec(dispatcher, "SELECT a + 1 AS x FROM ob.t ORDER BY b;")) == values{4, 3, 2});
    }
    SECTION("B8 guard: a qualified key is the input column") {
        CHECK(column_of(exec(dispatcher, "SELECT b AS a FROM ob.t ORDER BY t.a;")) == values{30, 20, 10});
    }
    SECTION("B9 two outputs of one name are ambiguous") {
        auto cur = exec(dispatcher, "SELECT a AS x, b AS x FROM ob.t ORDER BY x;");
        REQUIRE(cur->is_error());
        CHECK(cur->get_error().type == core::error_code_t::ambiguous_name);
    }
    SECTION("B10 an output name over another input column") {
        CHECK(column_of(exec(dispatcher, "SELECT b AS a FROM ob.t ORDER BY a;")) == values{10, 20, 30});
    }
    SECTION("B11 an alias under DESC NULLS FIRST") {
        REQUIRE(exec(dispatcher, "CREATE TABLE ob.n (a bigint);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO ob.n (a) VALUES (1), (NULL), (3);")->is_success());
        auto cur = exec(dispatcher, "SELECT a + 1 AS x FROM ob.n ORDER BY x DESC NULLS FIRST;");
        REQUIRE(width_of(cur) == 1);
        REQUIRE(cur->size() == 3);
        auto first = cur->is_null(0, 0);
        REQUIRE_FALSE(first.has_error());
        CHECK(first.value());
        auto second = cur->get<int64_t>(0, 1);
        REQUIRE_FALSE(second.has_error());
        CHECK(second.value() == 4);
        auto third = cur->get<int64_t>(0, 2);
        REQUIRE_FALSE(third.has_error());
        CHECK(third.value() == 2);
    }
    SECTION("B12 a bound parameter in the sorted-on computation") {
        const std::vector<std::pair<size_t, components::vector::vector_t>> ten{
            {1, components::vector::make_scalar(dispatcher->resource(), static_cast<int64_t>(10))}};
        auto session = otterbrix::session_id_t();
        CHECK(column_of(dispatcher->execute_sql_with_params(session,
                                                            "SELECT a + $1 AS x FROM ob.t ORDER BY x DESC;",
                                                            ten)) == values{13, 12, 11});
        CHECK(column_of(dispatcher->execute_sql_with_params(session,
                                                            "SELECT a + $1 AS x FROM ob.t ORDER BY a + $1 DESC;",
                                                            ten)) == values{13, 12, 11});
        const std::vector<std::pair<size_t, components::vector::vector_t>> minus_one{
            {1, components::vector::make_scalar(dispatcher->resource(), static_cast<int64_t>(-1))}};
        CHECK(column_of(dispatcher->execute_sql_with_params(session,
                                                            "SELECT a FROM ob.t ORDER BY b * $1;",
                                                            minus_one)) == values{1, 2, 3});
    }
}

TEST_CASE("integration::cpp::clause_answers::order_by_an_output_position") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_position")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);

    SECTION("P1 the position, not the name, of the second column") {
        CHECK(ints_at(exec(dispatcher, "SELECT b AS a, a FROM ob.t ORDER BY 2;"), 1) == values{1, 2, 3});
    }
    SECTION("P2 the name both columns answer to is ambiguous") {
        auto cur = exec(dispatcher, "SELECT b AS a, a FROM ob.t ORDER BY a;");
        REQUIRE(cur->is_error());
        CHECK(cur->get_error().type == core::error_code_t::ambiguous_name);
    }
    SECTION("P3 the position of an unnamed computed column") {
        CHECK(column_of(exec(dispatcher, "SELECT a + 1, b FROM ob.t ORDER BY 1;")) == values{2, 3, 4});
    }
    SECTION("P4 a position counts the columns a star expands to") {
        CHECK(pairs_of(exec(dispatcher, "SELECT * FROM ob.t ORDER BY 2;")) == pairs{{3, 10}, {2, 20}, {1, 30}});
    }
    SECTION("P5 guard: positions outside the select list are refused") {
        CHECK(mentions(refusal_of(exec(dispatcher, "SELECT a, b FROM ob.t ORDER BY 0;")), "ORDER BY position"));
        CHECK(mentions(refusal_of(exec(dispatcher, "SELECT a, b FROM ob.t ORDER BY 3;")), "ORDER BY position"));
        CHECK(mentions(refusal_of(exec(dispatcher, "SELECT * FROM ob.t ORDER BY 3;")), "ORDER BY position"));
    }
    SECTION("P6 a position over a UNION") {
        CHECK(column_of(exec(dispatcher, "SELECT a AS x FROM ob.t UNION ALL SELECT b FROM ob.t ORDER BY 1;")) ==
              values{1, 2, 3, 10, 20, 30});
    }
    SECTION("P7 a star next to a computed column: the position of the computed column") {
        auto cur = exec(dispatcher, "SELECT *, a + 1 AS x FROM ob.t ORDER BY 3;");
        CHECK(ints_at(cur, 2) == values{2, 3, 4});
        CHECK(width_of(cur) == 3);
    }
    SECTION("P8 a star next to a computed column: the position of a star column") {
        auto cur = exec(dispatcher, "SELECT *, a + 1 AS x FROM ob.t ORDER BY 2;");
        CHECK(pairs_of(cur) == pairs{{3, 10}, {2, 20}, {1, 30}});
        CHECK(ints_at(cur, 2) == values{4, 3, 2});
    }
    SECTION("P9 a star next to a computed column: the first position") {
        CHECK(column_of(exec(dispatcher, "SELECT *, a + 1 AS x FROM ob.t ORDER BY 1;")) == values{1, 2, 3});
    }
    SECTION("P10 a star column and an alias of one name are ambiguous") {
        auto cur = exec(dispatcher, "SELECT *, b AS a FROM ob.t ORDER BY a;");
        REQUIRE(cur->is_error());
        CHECK(cur->get_error().type == core::error_code_t::ambiguous_name);
    }
    SECTION("P11 guard: a qualified star next to a computed column") {
        CHECK(ints_at(exec(dispatcher, "SELECT t.*, a + 1 AS x FROM ob.t ORDER BY 3;"), 2) == values{2, 3, 4});
    }
    SECTION("P12 a position over a UNION under DESC and LIMIT") {
        CHECK(column_of(exec(dispatcher, "SELECT a FROM ob.t UNION ALL SELECT b FROM ob.t ORDER BY 1 DESC LIMIT 2;")) ==
              values{30, 20});
    }
}

TEST_CASE("integration::cpp::clause_answers::order_by_under_distinct") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_distinct")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);

    SECTION("D1 a DISTINCT sort key must be shown") {
        const auto what = refusal_of(exec(dispatcher, "SELECT DISTINCT a FROM ob.t ORDER BY b;"));
        CHECK(mentions(what, "for SELECT DISTINCT, ORDER BY expressions must appear in select list"));
    }
    SECTION("D2 the alias of a shown computed column") {
        CHECK(column_of(exec(dispatcher, "SELECT DISTINCT a + 1 AS x FROM ob.t ORDER BY x DESC;")) == values{4, 3, 2});
    }
    SECTION("D3 the shown computation spelled again") {
        CHECK(column_of(exec(dispatcher, "SELECT DISTINCT a + 1 FROM ob.t ORDER BY a + 1;")) == values{2, 3, 4});
    }
    SECTION("D4 DISTINCT ON keeps the first row per key and does not return the key") {
        auto cur = exec(dispatcher, "SELECT DISTINCT ON (k) v FROM ob.d ORDER BY k, ts DESC;");
        CHECK(column_of(cur) == values{11, 21});
        CHECK(width_of(cur) == 1);
    }
    SECTION("D6 a grouped DISTINCT ordered by a shown computation over the groups") {
        CHECK(column_of(exec(dispatcher, "SELECT DISTINCT a + 1, count(*) FROM ob.t GROUP BY a ORDER BY a + 1;")) ==
              values{2, 3, 4});
        CHECK(column_of(exec(dispatcher,
                             "SELECT DISTINCT a + 1, count(*) FROM ob.t GROUP BY a ORDER BY a + 1 DESC;")) ==
              values{4, 3, 2});
    }
    SECTION("D7 a grouped DISTINCT ordered by a computation it does not show") {
        const auto what =
            refusal_of(exec(dispatcher, "SELECT DISTINCT a, count(*) FROM ob.t GROUP BY a ORDER BY a + 1;"));
        CHECK(mentions(what, "for SELECT DISTINCT, ORDER BY expressions must appear in select list"));
    }
    SECTION("D5 DISTINCT ON without ORDER BY") {
        auto cur = exec(dispatcher, "SELECT DISTINCT ON (k) v FROM ob.d;");
        CHECK(column_of(cur).size() == 2);
        CHECK(width_of(cur) == 1);
    }
}

TEST_CASE("integration::cpp::clause_answers::order_by_with_limit_and_in_nested_blocks") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_limit")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);
    const std::string from = " FROM ob.c JOIN ob.o ON c.id = o.cid ";

    SECTION("L1 an input key under LIMIT") {
        CHECK(column_of(exec(dispatcher, "SELECT a FROM ob.t ORDER BY b LIMIT 2;")) == values{3, 2});
    }
    SECTION("L2 an alias under LIMIT and OFFSET") {
        CHECK(column_of(exec(dispatcher, "SELECT a + 1 AS x FROM ob.t ORDER BY x DESC LIMIT 1 OFFSET 1;")) ==
              values{3});
    }
    SECTION("L3 a hidden grouping key under LIMIT") {
        auto cur = exec(dispatcher, "SELECT count(*)" + from + "GROUP BY o.name ORDER BY o.name LIMIT 2;");
        CHECK(column_of(cur) == values{1, 1});
        CHECK(width_of(cur) == 1);
    }
    SECTION("L4 a derived table shows only its select list") {
        CHECK(width_of(exec(dispatcher, "SELECT * FROM (SELECT a FROM ob.t ORDER BY b) s;")) == 1);
    }
    SECTION("L5 a CTE ordered by a column it does not show") {
        CHECK(column_of(exec(dispatcher, "WITH c AS (SELECT a FROM ob.t ORDER BY b LIMIT 2) SELECT * FROM c;")) ==
              values{3, 2});
    }
    SECTION("L6 an IN sub-query ordered by a column it does not show") {
        CHECK(column_of(exec(dispatcher, "SELECT a FROM ob.t WHERE a IN (SELECT a FROM ob.t ORDER BY b LIMIT 1);")) ==
              values{3});
    }
    SECTION("L7 INSERT ... SELECT ordered by a column it does not insert") {
        REQUIRE(exec(dispatcher, "CREATE TABLE ob.s (a bigint);")->is_success());
        auto inserted = exec(dispatcher, "INSERT INTO ob.s (a) SELECT a FROM ob.t ORDER BY b;");
        INFO("error: " << (inserted->is_error() ? inserted->get_error().what : "none"));
        REQUIRE(inserted->is_success());
        CHECK(column_of(exec(dispatcher, "SELECT a FROM ob.s ORDER BY a;")) == values{1, 2, 3});
    }
    SECTION("L8 an IN sub-query ordered by a grouping key it does not show") {
        CHECK(column_of(exec(dispatcher,
                             "SELECT a FROM ob.t WHERE a IN (SELECT count(*)" + from +
                                 "GROUP BY o.name ORDER BY o.name LIMIT 2);")) == values{1});
    }
    SECTION("L9 a joined derived table ordered by a grouping key it does not show") {
        auto cur = exec(dispatcher,
                        "SELECT t.b FROM (SELECT count(*) AS n" + from +
                            "GROUP BY o.name ORDER BY o.name LIMIT 2) s JOIN ob.t ON s.n = t.a;");
        CHECK(column_of(cur) == values{30, 30});
        CHECK(width_of(cur) == 1);
    }
    SECTION("L10 a derived table ordered by an alias under LIMIT") {
        auto cur = exec(dispatcher, "SELECT * FROM (SELECT a + 1 AS x FROM ob.t ORDER BY x LIMIT 2) s;");
        CHECK(column_of(cur) == values{2, 3});
        CHECK(width_of(cur) == 1);
    }
}

TEST_CASE("integration::cpp::clause_answers::select_list_is_evaluated_once_and_after_limit") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_evaluation")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);
    REQUIRE_FALSE(
        dispatcher->register_udf(otterbrix::session_id_t(), make_probe(dispatcher->resource())).contains_error());
    components::cursor::cursor_t_ptr cur;

    SECTION("E1 guard: a column not sorted on is computed for the LIMIT rows only") {
        CHECK(probe_calls_of(dispatcher, "SELECT id, probe(v) AS h FROM ob.big ORDER BY id LIMIT 10;", cur) == 10);
        CHECK(column_of(cur) == values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    }
    SECTION("E2 guard: and for the OFFSET rows") {
        CHECK(probe_calls_of(dispatcher, "SELECT id, probe(v) AS h FROM ob.big ORDER BY id LIMIT 10 OFFSET 5;", cur) ==
              15);
        CHECK(column_of(cur) == values{6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
    }
    SECTION("E3 guard: without LIMIT every row") {
        CHECK(probe_calls_of(dispatcher, "SELECT id, probe(v) AS h FROM ob.big ORDER BY id;", cur) == 1000);
    }
    SECTION("E4 a sorted-on alias is computed once per row") {
        CHECK(probe_calls_of(dispatcher, "SELECT id, probe(v) AS h FROM ob.big ORDER BY h LIMIT 10;", cur) == 1000);
        CHECK(column_of(cur) == values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        CHECK(ints_at(cur, 1) == values{2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
    }
    SECTION("E5 a sorted-on computation spelled again is computed once per row") {
        CHECK(probe_calls_of(dispatcher, "SELECT id, probe(v) AS h FROM ob.big ORDER BY probe(v) LIMIT 10;", cur) ==
              1000);
        CHECK(column_of(cur) == values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    }
    SECTION("E6 a sorted-on position is computed once per row") {
        CHECK(probe_calls_of(dispatcher, "SELECT probe(v) AS h FROM ob.big ORDER BY 1 LIMIT 10;", cur) == 1000);
        CHECK(column_of(cur) == values{2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
    }
    SECTION("E7 under DISTINCT ON the select list is computed for the kept rows") {
        CHECK(probe_calls_of(dispatcher, "SELECT DISTINCT ON (k) probe(v) AS p FROM ob.d ORDER BY k, ts DESC;", cur) ==
              2);
        CHECK(column_of(cur) == values{12, 22});
    }
    SECTION("E8 LIMIT reaches through DISTINCT ON") {
        CHECK(probe_calls_of(dispatcher,
                             "SELECT DISTINCT ON (k) probe(v) FROM ob.d ORDER BY k, ts DESC LIMIT 1;",
                             cur) == 1);
        CHECK(column_of(cur) == values{12});
        CHECK(probe_calls_of(dispatcher, "SELECT DISTINCT ON (v) probe(id) FROM ob.big ORDER BY v LIMIT 5;", cur) == 5);
        CHECK(column_of(cur) == values{2, 3, 4, 5, 6});
        CHECK(probe_calls_of(dispatcher,
                             "SELECT DISTINCT ON (v) probe(id) FROM ob.big ORDER BY v LIMIT 2 OFFSET 3;",
                             cur) == 5);
        CHECK(column_of(cur) == values{5, 6});
    }
    SECTION("E9 a sorted-on alias named twice is computed once per row") {
        CHECK(probe_calls_of(dispatcher, "SELECT probe(v) AS h FROM ob.big ORDER BY h, h LIMIT 10;", cur) == 1000);
        CHECK(column_of(cur) == values{2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
        CHECK(probe_calls_of(dispatcher, "SELECT id, probe(v) AS h FROM ob.big ORDER BY h, probe(v) LIMIT 10;", cur) ==
              1000);
        CHECK(column_of(cur) == values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    }
    SECTION("E11 an aggregate the select list shows is not computed again for ORDER BY") {
        CHECK(probe_calls_of(dispatcher,
                             "SELECT k, sum(probe(v)) AS s FROM ob.d GROUP BY k ORDER BY sum(probe(v)) DESC;",
                             cur) == 4);
        CHECK(column_of(cur) == values{2, 1});
        CHECK(ints_at(cur, 1) == values{43, 23});
        CHECK(width_of(cur) == 2);
    }
    SECTION("E10 LIMIT reaches through DISTINCT ON over duplicate keys") {
        CHECK(probe_calls_of(dispatcher,
                             "SELECT DISTINCT ON (k) probe(v) FROM ob.d ORDER BY k, ts DESC LIMIT 2;",
                             cur) == 2);
        CHECK(column_of(cur) == values{12, 22});
        REQUIRE(exec(dispatcher, "CREATE TABLE ob.dup (k bigint, v bigint);")->is_success());
        REQUIRE(seed_rows(dispatcher, "ob.dup", "k, v", 1000, [](unsigned i) {
                    return "(" + std::to_string(i % 100) + ", " + std::to_string(i) + ")";
                })->is_success());
        CHECK(probe_calls_of(dispatcher, "SELECT DISTINCT ON (k) probe(v) FROM ob.dup ORDER BY k, v LIMIT 5;", cur) ==
              5);
        CHECK(column_of(cur) == values{1, 2, 3, 4, 5});
    }
}

TEST_CASE("integration::cpp::clause_answers::hidden_keys_are_cut_without_a_projection") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_hidden")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);
    const std::string from = " FROM ob.c JOIN ob.o ON c.id = o.cid ";

    SECTION("H1 a hidden grouping key adds no Project") {
        const auto plan = plan_of(dispatcher, "SELECT count(*)" + from + "GROUP BY o.name ORDER BY o.name;");
        INFO("plan:\n" << plan);
        CHECK(plan.find("Project") == std::string::npos);
    }
    SECTION("H2 DISTINCT ON a hidden grouping key") {
        auto cur = exec(dispatcher, "SELECT DISTINCT ON (o.name) count(*)" + from + "GROUP BY o.name ORDER BY o.name;");
        CHECK(column_of(cur) == values{1, 1, 1});
        CHECK(width_of(cur) == 1);
    }
}

TEST_CASE("integration::cpp::clause_answers::order_by_over_a_union") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_union")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);

    SECTION("U1 an expression over the result is refused") {
        const auto what = refusal_of(exec(dispatcher, "SELECT a FROM ob.t UNION SELECT b FROM ob.t ORDER BY a + 1;"));
        CHECK(mentions(what, "invalid UNION/INTERSECT/EXCEPT ORDER BY clause"));
    }
    SECTION("U2 a result column") {
        CHECK(column_of(exec(dispatcher, "SELECT a FROM ob.t UNION SELECT b FROM ob.t ORDER BY a;")) ==
              values{1, 2, 3, 10, 20, 30});
    }
    SECTION("U3 guard: a column the result does not have is refused") {
        CHECK(exec(dispatcher, "SELECT a FROM ob.t UNION SELECT b FROM ob.t ORDER BY b;")->is_error());
    }
}

TEST_CASE("integration::cpp::clause_answers::a_sorted_on_computed_column_survives_column_narrowing") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_narrowing")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);
    REQUIRE(exec(dispatcher, "CREATE TABLE ob.w (a bigint, b bigint, c bigint);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO ob.w (a, b, c) VALUES (1, 10, 100), (3, 30, 300), (2, 20, 200);")
                ->is_success());

    SECTION("N1 the first column of a wider table") {
        auto cur = exec(dispatcher, "SELECT a + 1 AS x FROM ob.w ORDER BY x;");
        CHECK(column_of(cur) == values{2, 3, 4});
        CHECK(width_of(cur) == 1);
    }
    SECTION("N2 the last column of a wider table") {
        auto cur = exec(dispatcher, "SELECT c + 1 AS x FROM ob.w ORDER BY x DESC;");
        CHECK(column_of(cur) == values{301, 201, 101});
        CHECK(width_of(cur) == 1);
    }
    SECTION("N3 over a join") {
        auto cur = exec(dispatcher, "SELECT w.c + t.b AS x FROM ob.w JOIN ob.t ON w.a = t.a ORDER BY x;");
        CHECK(column_of(cur) == values{130, 220, 310});
        CHECK(width_of(cur) == 1);
    }
    SECTION("N4 over a join, next to a plain column") {
        auto cur =
            exec(dispatcher, "SELECT w.c + t.b AS x, t.a AS y FROM ob.w JOIN ob.t ON w.a = t.a ORDER BY x DESC;");
        CHECK(ints_at(cur, 0) == values{310, 220, 130});
        CHECK(ints_at(cur, 1) == values{3, 2, 1});
        CHECK(width_of(cur) == 2);
    }
}

TEST_CASE("integration::cpp::clause_answers::a_view_keeps_its_own_order_by_constants") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_view")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);
    REQUIRE(exec(dispatcher, "CREATE VIEW ob.v AS SELECT a FROM ob.t ORDER BY b * -1 LIMIT 3;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE VIEW ob.w AS SELECT a + 0 AS x FROM ob.t ORDER BY b * -1 LIMIT 3;")
                ->is_success());

    SECTION("V1 guard: the view alone") {
        CHECK(column_of(exec(dispatcher, "SELECT a FROM ob.v;")) == values{1, 2, 3});
    }
    SECTION("V2 an outer constant does not replace the view's sort constant") {
        CHECK(column_of(exec(dispatcher, "SELECT a FROM ob.v WHERE a < 100;")) == values{1, 2, 3});
    }
    SECTION("V3 the same under a computed column") {
        CHECK(column_of(exec(dispatcher, "SELECT x FROM ob.w WHERE x < 100;")) == values{1, 2, 3});
    }
}

TEST_CASE("integration::cpp::clause_answers::order_by_an_expression_over_groups") {
    test_spaces space(make_test_config(integration_fixture_path("test_clause_answers/order_group_expression")));
    auto* dispatcher = space.dispatcher();
    seed_ob(dispatcher);

    SECTION("G1 a grouping key the select list does not show, inside an expression") {
        auto cur = exec(dispatcher, "SELECT count(*) FROM ob.t GROUP BY a ORDER BY a + 1;");
        CHECK(column_of(cur) == values{1, 1, 1});
        CHECK(width_of(cur) == 1);
        CHECK(column_of(exec(dispatcher, "SELECT sum(b) FROM ob.t GROUP BY a ORDER BY a + 1;")) ==
              values{30, 20, 10});
    }
    SECTION("G2 the same expression shown") {
        auto cur = exec(dispatcher, "SELECT a + 1, count(*) FROM ob.t GROUP BY a ORDER BY a + 1 DESC;");
        CHECK(column_of(cur) == values{4, 3, 2});
        CHECK(width_of(cur) == 2);
    }
    SECTION("G3 an aggregate the select list does not show") {
        auto cur = exec(dispatcher, "SELECT k FROM ob.d WHERE v <> 10 GROUP BY k ORDER BY count(*) DESC;");
        CHECK(column_of(cur) == values{2, 1});
        CHECK(width_of(cur) == 1);
    }
    SECTION("G4 the same under LIMIT") {
        auto cur = exec(dispatcher, "SELECT k FROM ob.d WHERE v <> 10 GROUP BY k ORDER BY count(*) DESC LIMIT 1;");
        CHECK(column_of(cur) == values{2});
        CHECK(width_of(cur) == 1);
    }
    SECTION("G5 an aggregate the select list shows is reused") {
        const std::string sql = "SELECT k, count(*) FROM ob.d WHERE v <> 10 GROUP BY k ORDER BY count(*) DESC;";
        auto cur = exec(dispatcher, sql);
        CHECK(column_of(cur) == values{2, 1});
        CHECK(ints_at(cur, 1) == values{2, 1});
        CHECK(width_of(cur) == 2);
    }
    SECTION("G6 an aggregate and a grouping key in one expression") {
        auto cur = exec(dispatcher, "SELECT k FROM ob.d WHERE v <> 10 GROUP BY k ORDER BY k - count(*) * 10;");
        CHECK(column_of(cur) == values{2, 1});
        CHECK(width_of(cur) == 1);
    }
    SECTION("G7 a name inside an expression is the input column, not the output name") {
        CHECK(column_of(exec(dispatcher, "SELECT b AS a, count(*) FROM ob.t GROUP BY a, b ORDER BY a * -1;")) ==
              values{10, 20, 30});
    }
    SECTION("G8 an output name inside an expression is refused") {
        const auto aggregate =
            refusal_of(exec(dispatcher, "SELECT a, count(*) AS c FROM ob.t GROUP BY a ORDER BY c + 1;"));
        INFO(aggregate);
        CHECK(mentions(aggregate, "'c'"));
        const auto key =
            refusal_of(exec(dispatcher, "SELECT a AS x, count(*) FROM ob.t GROUP BY a ORDER BY x + 1 DESC;"));
        INFO(key);
        CHECK(mentions(key, "'x'"));
    }
    SECTION("G10 a column the binder hides cannot be named") {
        for (const auto* order : {"count(*), count", "sum(v), sum"}) {
            auto cur = exec(dispatcher, std::string{"SELECT k FROM ob.d GROUP BY k ORDER BY "} + order + ";");
            REQUIRE(cur->is_error());
            CHECK(cur->get_error().type == core::error_code_t::schema_error);
        }
    }
    SECTION("G11 two aggregates the select list does not show, under LIMIT and OFFSET") {
        REQUIRE(exec(dispatcher, "INSERT INTO ob.d (k, v, ts) VALUES (3, 1, 1), (3, 2, 1), (0, 0, 1);")->is_success());
        const std::string sql = "SELECT k FROM ob.d GROUP BY k ORDER BY count(*) DESC, sum(v) LIMIT 1 OFFSET 2;";
        const auto plan = plan_of(dispatcher, sql);
        INFO("plan:\n" << plan);
        CHECK(plan.find("Pushed Aggregate Scan") != std::string::npos);
        auto cur = exec(dispatcher, sql);
        CHECK(column_of(cur) == values{2});
        CHECK(width_of(cur) == 1);
    }
    SECTION("G12 a computed grouping key inside an expression") {
        auto cur = exec(dispatcher, "SELECT sum(b) FROM ob.t GROUP BY a + 1 ORDER BY (a + 1) * -1;");
        CHECK(column_of(cur) == values{10, 20, 30});
        CHECK(width_of(cur) == 1);
    }
    SECTION("G14 a derived table over a block that hides an aggregate shows only its own columns") {
        auto cur = exec(dispatcher, "SELECT * FROM (SELECT k FROM ob.d GROUP BY k ORDER BY count(*) LIMIT 2) s;");
        CHECK(width_of(cur) == 1);
        auto rows = column_of(cur);
        std::sort(rows.begin(), rows.end());
        CHECK(rows == values{1, 2});
    }
    SECTION("G9 guard: an input column neither grouped nor aggregated is refused") {
        CHECK(mentions(refusal_of(exec(dispatcher, "SELECT a FROM ob.t GROUP BY a ORDER BY b;")),
                       "must appear in the GROUP BY clause or be used in an aggregate function"));
        CHECK(mentions(refusal_of(exec(dispatcher, "SELECT a FROM ob.t GROUP BY a ORDER BY b + 1;")),
                       "must appear in the GROUP BY clause or be used in an aggregate function"));
    }
}
