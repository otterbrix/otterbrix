#include <catch2/catch.hpp>

#include "test_operator_generaty.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_max.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_min.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/collection/operators/scan/full_scan.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_max.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_min.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/table/operators/scan/full_scan.hpp>
#include <components/types/operations_helper.hpp>

using namespace components;
using namespace components::types;
using namespace components::expressions;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("operator::aggregate::count") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("count::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        SECTION("documents") {
            collection::operators::aggregate::operator_count_t count(d(collection));
            count.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            count.on_execute(nullptr);
            REQUIRE(count.value().as_unsigned() == 100);
        }
        SECTION("table") {
            table::operators::aggregate::operator_count_t count(d(table));
            count.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            count.on_execute(nullptr);
            REQUIRE(count.value().value<uint64_t>() == 100);
        }
    }

    SECTION("count::match") {
        auto cond =
            make_compare_expression(&resource, compare_type::lte, side_t::left, key("count"), core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(10));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::aggregate::operator_count_t count(d(collection));
            count.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            count.on_execute(&pipeline_context);
            REQUIRE(count.value().as_unsigned() == 10);
        }
        SECTION("table") {
            table::operators::aggregate::operator_count_t count(d(table));
            count.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            count.on_execute(&pipeline_context);
            REQUIRE(count.value().value<uint64_t>() == 10);
        }
    }
}

TEST_CASE("operator::aggregate::min") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("min::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        SECTION("documents") {
            collection::operators::aggregate::operator_min_t min_(d(collection), key("count"));
            min_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            min_.on_execute(nullptr);
            REQUIRE(min_.value().as_unsigned() == 1);
        }
        SECTION("table") {
            table::operators::aggregate::operator_min_t min_(d(table), key("count"));
            min_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            min_.on_execute(nullptr);
            REQUIRE(min_.value().value<int64_t>() == 1);
        }
    }

    SECTION("min::match") {
        auto cond =
            make_compare_expression(&resource, compare_type::gt, side_t::left, key("count"), core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(80));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::aggregate::operator_min_t min_(d(collection), key("count"));
            min_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            min_.on_execute(&pipeline_context);
            REQUIRE(min_.value().as_unsigned() == 81);
        }
        SECTION("table") {
            table::operators::aggregate::operator_min_t min_(d(table), key("count"));
            min_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            min_.on_execute(&pipeline_context);
            REQUIRE(min_.value().value<int64_t>() == 81);
        }
    }
}

TEST_CASE("operator::aggregate::max") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("max::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        SECTION("documents") {
            collection::operators::aggregate::operator_max_t max_(d(collection), key("count"));
            max_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            max_.on_execute(nullptr);
            REQUIRE(max_.value().as_unsigned() == 100);
        }
        SECTION("table") {
            table::operators::aggregate::operator_max_t max_(d(table), key("count"));
            max_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            max_.on_execute(nullptr);
            REQUIRE(max_.value().value<int64_t>() == 100);
        }
    }

    SECTION("max::match") {
        auto cond =
            make_compare_expression(&resource, compare_type::lt, side_t::left, key("count"), core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(20));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::aggregate::operator_max_t max_(d(collection), key("count"));
            max_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            max_.on_execute(&pipeline_context);
            REQUIRE(max_.value().as_unsigned() == 19);
        }
        SECTION("table") {
            table::operators::aggregate::operator_max_t max_(d(table), key("count"));
            max_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            max_.on_execute(&pipeline_context);
            REQUIRE(max_.value().value<int64_t>() == 19);
        }
    }
}

TEST_CASE("operator::aggregate::sum") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("sum::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        SECTION("documents") {
            collection::operators::aggregate::operator_sum_t sum_(d(collection), key("count"));
            sum_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            sum_.on_execute(nullptr);
            REQUIRE(sum_.value().as_unsigned() == 5050);
        }
        SECTION("table") {
            table::operators::aggregate::operator_sum_t sum_(d(table), key("count"));
            sum_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            sum_.on_execute(nullptr);
            REQUIRE(sum_.value().value<int64_t>() == 5050);
        }
    }

    SECTION("sum::match") {
        auto cond =
            make_compare_expression(&resource, compare_type::lt, side_t::left, key("count"), core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(10));
        pipeline::context_t pipeline_context(std::move(parameters));

        SECTION("documents") {
            collection::operators::aggregate::operator_sum_t sum_(d(collection), key("count"));
            sum_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            sum_.on_execute(&pipeline_context);
            REQUIRE(sum_.value().as_unsigned() == 45);
        }
        SECTION("table") {
            table::operators::aggregate::operator_sum_t sum_(d(table), key("count"));
            sum_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            sum_.on_execute(&pipeline_context);
            REQUIRE(sum_.value().value<int64_t>() == 45);
        }
    }
}

TEST_CASE("operator::aggregate::avg") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("avg::all") {
        auto cond = make_compare_expression(&resource, compare_type::all_true);

        SECTION("documents") {
            collection::operators::aggregate::operator_avg_t avg_(d(collection), key("count"));
            avg_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            avg_.on_execute(nullptr);
            REQUIRE(avg_.value().as_double() == 50.5);
        }
        SECTION("table") {
            table::operators::aggregate::operator_avg_t avg_(d(table), key("count"));
            avg_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            avg_.on_execute(nullptr);
            REQUIRE(avg_.value().value<double>() == 50.5);
        }
    }

    SECTION("avg::match") {
        auto cond =
            make_compare_expression(&resource, compare_type::lt, side_t::left, key("count"), core::parameter_id_t(1));
        logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), logical_value_t(10));
        pipeline::context_t pipeline_context(std::move(parameters));
        SECTION("documents") {
            collection::operators::aggregate::operator_avg_t avg_(d(collection), key("count"));
            avg_.set_children(boost::intrusive_ptr(
                new collection::operators::full_scan(d(collection),
                                                     collection::operators::predicates::create_predicate(cond),
                                                     logical_plan::limit_t::unlimit())));
            avg_.on_execute(&pipeline_context);
            REQUIRE(avg_.value().as_double() == 5.0);
        }
        SECTION("table") {
            table::operators::aggregate::operator_avg_t avg_(d(table), key("count"));
            avg_.set_children(boost::intrusive_ptr(
                new table::operators::full_scan(d(table), cond, logical_plan::limit_t::unlimit())));
            avg_.on_execute(&pipeline_context);
            REQUIRE(avg_.value().value<double>() == 5.0);
        }
    }
}
