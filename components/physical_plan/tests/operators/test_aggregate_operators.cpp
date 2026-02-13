#include <catch2/catch.hpp>

#include "test_operator_generaty.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/operators/aggregate/operator_max.hpp>
#include <components/physical_plan/operators/aggregate/operator_min.hpp>
#include <components/physical_plan/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/types/operations_helper.hpp>

using namespace components;
using namespace components::types;
using namespace components::expressions;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("components::physical_plan::aggregate::count") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);
    auto* res = table->resource_;
    auto* lg = &table->log_;
    auto& name = table->name_;

    SECTION("count::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);

        operators::aggregate::operator_count_t count(res, lg);
        count.set_children(boost::intrusive_ptr(scan_ptr));
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

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);
        auto* match_ptr = new operators::operator_match_t(res, lg, cond, logical_plan::limit_t::unlimit());
        match_ptr->set_children(boost::intrusive_ptr(scan_ptr));

        operators::aggregate::operator_count_t count(res, lg);
        count.set_children(boost::intrusive_ptr(match_ptr));
        count.on_execute(&pipeline_context);
        REQUIRE(count.value().value<uint64_t>() == 10);
    }
}

TEST_CASE("components::physical_plan::aggregate::min") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);
    auto* res = table->resource_;
    auto* lg = &table->log_;
    auto& name = table->name_;

    SECTION("min::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);

        operators::aggregate::operator_min_t min_(res, lg, key(&resource, "count"));
        min_.set_children(boost::intrusive_ptr(scan_ptr));
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

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);
        auto* match_ptr = new operators::operator_match_t(res, lg, cond, logical_plan::limit_t::unlimit());
        match_ptr->set_children(boost::intrusive_ptr(scan_ptr));

        operators::aggregate::operator_min_t min_(res, lg, key(&resource, "count"));
        min_.set_children(boost::intrusive_ptr(match_ptr));
        min_.on_execute(&pipeline_context);
        REQUIRE(min_.value().value<int64_t>() == 81);
    }
}

TEST_CASE("components::physical_plan::aggregate::max") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);
    auto* res = table->resource_;
    auto* lg = &table->log_;
    auto& name = table->name_;

    SECTION("max::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);

        operators::aggregate::operator_max_t max_(res, lg, key(&resource, "count"));
        max_.set_children(boost::intrusive_ptr(scan_ptr));
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

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);
        auto* match_ptr = new operators::operator_match_t(res, lg, cond, logical_plan::limit_t::unlimit());
        match_ptr->set_children(boost::intrusive_ptr(scan_ptr));

        operators::aggregate::operator_max_t max_(res, lg, key(&resource, "count"));
        max_.set_children(boost::intrusive_ptr(match_ptr));
        max_.on_execute(&pipeline_context);
        REQUIRE(max_.value().value<int64_t>() == 19);
    }
}

TEST_CASE("components::physical_plan::aggregate::sum") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);
    auto* res = table->resource_;
    auto* lg = &table->log_;
    auto& name = table->name_;

    SECTION("sum::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);

        operators::aggregate::operator_sum_t sum_(res, lg, key(&resource, "count"));
        sum_.set_children(boost::intrusive_ptr(scan_ptr));
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

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);
        auto* match_ptr = new operators::operator_match_t(res, lg, cond, logical_plan::limit_t::unlimit());
        match_ptr->set_children(boost::intrusive_ptr(scan_ptr));

        operators::aggregate::operator_sum_t sum_(res, lg, key(&resource, "count"));
        sum_.set_children(boost::intrusive_ptr(match_ptr));
        sum_.on_execute(&pipeline_context);
        REQUIRE(sum_.value().value<int64_t>() == 45);
    }
}

TEST_CASE("components::physical_plan::aggregate::avg") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);
    auto* res = table->resource_;
    auto* lg = &table->log_;
    auto& name = table->name_;

    SECTION("avg::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);

        operators::aggregate::operator_avg_t avg_(res, lg, key(&resource, "count"));
        avg_.set_children(boost::intrusive_ptr(scan_ptr));
        avg_.on_execute(nullptr);
        REQUIRE(core::is_equals(avg_.value().value<double>(), 50.5));
    }

    SECTION("avg::match") {
        auto cond = make_compare_expression(&resource,
                                            compare_type::lt,
                                            key(&resource, "count", side_t::left),
                                            core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(&resource, 10));
        pipeline::context_t pipeline_context(std::move(parameters));

        auto* scan_ptr = new operators::full_scan(res, lg, name, cond, logical_plan::limit_t::unlimit());
        inject_scan_data(table, *scan_ptr);
        auto* match_ptr = new operators::operator_match_t(res, lg, cond, logical_plan::limit_t::unlimit());
        match_ptr->set_children(boost::intrusive_ptr(scan_ptr));

        operators::aggregate::operator_avg_t avg_(res, lg, key(&resource, "count"));
        avg_.set_children(boost::intrusive_ptr(match_ptr));
        avg_.on_execute(&pipeline_context);
        REQUIRE(core::is_equals(avg_.value().value<double>(), 5.0));
    }
}
