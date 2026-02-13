#include <catch2/catch.hpp>

#include "test_operator_generaty.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/types/operations_helper.hpp>

using namespace components;
using namespace components::types;
using namespace components::expressions;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

compute::function* get_function_by_name(const std::string& name) {
    auto it = std::find_if(compute::DEFAULT_FUNCTIONS.begin(),
                           compute::DEFAULT_FUNCTIONS.end(),
                           [&name](const auto& pair) { return pair.first == name; });
    if (it != compute::DEFAULT_FUNCTIONS.end()) {
        return compute::function_registry_t::get_default()->get_function(it->second.uid);
    } else {
        return compute::function_registry_t::get_default()->get_function(compute::invalid_function_uid);
    }
}

TEST_CASE("components::physical_plan::aggregate::count") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("count::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        operators::aggregate::operator_func_t count(
            d(table),
            get_function_by_name("count"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        count.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        count.on_execute(nullptr);
        REQUIRE(count.value().value<uint64_t>() == 100);
    }

    SECTION("count::match") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lte,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(&resource, 10));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::aggregate::operator_func_t count(
            d(table),
            get_function_by_name("count"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        count.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        count.on_execute(&pipeline_context);
        REQUIRE(count.value().value<uint64_t>() == 10);
    }
}

TEST_CASE("components::physical_plan::aggregate::min") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("min::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        operators::aggregate::operator_func_t min_(
            d(table),
            get_function_by_name("min"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        min_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        min_.on_execute(nullptr);
        REQUIRE(min_.value().value<int64_t>() == 1);
    }

    SECTION("min::match") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::gt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(&resource, 80));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::aggregate::operator_func_t min_(
            d(table),
            get_function_by_name("min"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        min_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        min_.on_execute(&pipeline_context);
        REQUIRE(min_.value().value<int64_t>() == 81);
    }
}

TEST_CASE("components::physical_plan::aggregate::max") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("max::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        operators::aggregate::operator_func_t max_(
            d(table),
            get_function_by_name("max"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        max_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        max_.on_execute(nullptr);
        REQUIRE(max_.value().value<int64_t>() == 100);
    }

    SECTION("max::match") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(&resource, 20));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::aggregate::operator_func_t max_(
            d(table),
            get_function_by_name("max"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        max_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        max_.on_execute(&pipeline_context);
        REQUIRE(max_.value().value<int64_t>() == 19);
    }
}

TEST_CASE("components::physical_plan::aggregate::sum") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("sum::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        operators::aggregate::operator_func_t sum_(
            d(table),
            get_function_by_name("sum"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        sum_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        sum_.on_execute(nullptr);
        REQUIRE(sum_.value().value<int64_t>() == 5050);
    }

    SECTION("sum::match") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(&resource, 10));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::aggregate::operator_func_t sum_(
            d(table),
            get_function_by_name("sum"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        sum_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        sum_.on_execute(&pipeline_context);
        REQUIRE(sum_.value().value<int64_t>() == 45);
    }
}

TEST_CASE("components::physical_plan::aggregate::avg") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("avg::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        operators::aggregate::operator_func_t avg_(
            d(table),
            get_function_by_name("avg"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        avg_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        avg_.on_execute(nullptr);
        REQUIRE(core::is_equals(avg_.value().value<int64_t>(), 50));
    }

    SECTION("avg::match") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(&resource, 10));
        pipeline::context_t pipeline_context(std::move(parameters));

        operators::aggregate::operator_func_t avg_(
            d(table),
            get_function_by_name("avg"),
            std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource});
        avg_.set_children(boost::intrusive_ptr(
            new operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
        avg_.on_execute(&pipeline_context);
        REQUIRE(core::is_equals(avg_.value().value<int64_t>(), 5));
    }
}