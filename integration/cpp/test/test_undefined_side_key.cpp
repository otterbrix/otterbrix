#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <string>

// A host-built single-table plan whose key_t leaves the side undefined (the constructor's default).

namespace {
    using namespace components;
    using test_helpers::exec;
    using expressions::compare_type;
    using key = expressions::key_t;

    template<typename D>
    bool okq(D* d, const std::string& query) {
        auto c = exec(d, query);
        return c && c->is_success();
    }

    template<typename D>
    void seed(D* d) {
        REQUIRE(okq(d, "CREATE DATABASE m;"));
        REQUIRE(okq(d, "CREATE TABLE m.t (id BIGINT, v BIGINT);"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, v) VALUES (1,10),(2,20),(3,30);"));
    }

    template<typename D>
    logical_plan::node_match_ptr match_id_eq(D* d, core::parameter_id_t id) {
        auto* r = d->resource();
        auto predicate = expressions::make_compare_expression(r, compare_type::eq, key{r, "id"}, id);
        return logical_plan::make_node_match(r, core::dbname_t{"m"}, core::relname_t{"t"}, std::move(predicate));
    }
} // namespace

TEST_CASE("integration::cpp::undefined_side_key::select") {
    auto config = test_helpers::make_test_config(integration_fixture_path("undefined_side_key/select"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    auto* r = d->resource();

    auto params = logical_plan::make_parameter_node(r);
    const auto id = params->add_parameter(types::logical_value_t(r, int64_t{2}));
    auto agg = logical_plan::make_node_aggregate(r, core::dbname_t{"m"}, core::relname_t{"t"});
    agg->append_child(match_id_eq(d, id));
    auto cur = d->execute_plan(otterbrix::session_id_t(), logical_plan::execution_plan_t{r, agg, params});
    REQUIRE(cur);
    INFO((cur->is_success() ? std::string{} : std::string{cur->get_error().what}));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    REQUIRE(cur->column_count() == 2);
    CHECK(std::string{cur->type_data()[0].alias()} == "id");
    CHECK(std::string{cur->type_data()[1].alias()} == "v");
    CHECK(cur->value(1, 0).value<int64_t>() == 20);
}

TEST_CASE("integration::cpp::undefined_side_key::delete") {
    auto config = test_helpers::make_test_config(integration_fixture_path("undefined_side_key/delete"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    auto* r = d->resource();

    auto params = logical_plan::make_parameter_node(r);
    const auto id = params->add_parameter(types::logical_value_t(r, int64_t{2}));
    auto del = test_dml_target(
        logical_plan::make_node_delete(r,
                                       match_id_eq(d, id),
                                       logical_plan::make_node_limit(r, {}, {}, logical_plan::limit_t::unlimit())),
        "m",
        "t");
    auto cur = d->execute_plan(otterbrix::session_id_t(), logical_plan::execution_plan_t{r, del, params});
    REQUIRE(cur);
    INFO((cur->is_success() ? std::string{} : std::string{cur->get_error().what}));
    REQUIRE(cur->is_success());

    auto left = exec(d, "SELECT id FROM m.t ORDER BY id;");
    REQUIRE(left->is_success());
    REQUIRE(left->size() == 2);
    CHECK(left->value(0, 0).value<int64_t>() == 1);
    CHECK(left->value(0, 1).value<int64_t>() == 3);
}

TEST_CASE("integration::cpp::undefined_side_key::catalog_select") {
    auto config = test_helpers::make_test_config(integration_fixture_path("undefined_side_key/catalog_select"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    auto* r = d->resource();

    auto params = logical_plan::make_parameter_node(r);
    const auto name = params->add_parameter(types::logical_value_t(r, std::string{"t"}));
    auto agg = logical_plan::make_node_aggregate(r, core::dbname_t{"pg_catalog"}, core::relname_t{"pg_class"});
    auto predicate = expressions::make_compare_expression(r, compare_type::eq, key{r, "relname"}, name);
    agg->append_child(logical_plan::make_node_match(r,
                                                    core::dbname_t{"pg_catalog"},
                                                    core::relname_t{"pg_class"},
                                                    std::move(predicate)));
    auto cur = d->execute_plan(otterbrix::session_id_t(), logical_plan::execution_plan_t{r, agg, params});
    REQUIRE(cur);
    INFO((cur->is_success() ? std::string{} : std::string{cur->get_error().what}));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    auto relname = cur->column_index("relname");
    REQUIRE_FALSE(relname.has_error());
    CHECK(cur->value(relname.value(), 0).value<std::string_view>() == "t");
}
