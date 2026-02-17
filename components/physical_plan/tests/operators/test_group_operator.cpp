#include <catch2/catch.hpp>

#include "test_operator_generaty.hpp"

#include <components/compute/function.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/get/simple_value.hpp>
#include <components/physical_plan/operators/operator_group.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>

using namespace components;
using namespace components::expressions;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("components::physical_plan::group::base") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("base::all::no_valid") {
        operators::operator_group_t group(d(table));
        group.set_children(
            boost::intrusive_ptr(new operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group.add_key("id_", operators::get::simple_value_t::create(key(&resource, "id_")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 0);
    }

    SECTION("base::all::id") {
        operators::operator_group_t group(d(table));
        group.set_children(
            boost::intrusive_ptr(new operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group.add_key("_id", operators::get::simple_value_t::create(key(&resource, "_id")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 100);
    }

    SECTION("base::all::count_bool") {
        operators::operator_group_t group(d(table));
        group.set_children(
            boost::intrusive_ptr(new operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group.add_key("count_bool", operators::get::simple_value_t::create(key(&resource, "count_bool")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 2);
    }
}

TEST_CASE("components::physical_plan::group::sort") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("sort::all") {
        auto group = boost::intrusive_ptr(new operators::operator_group_t(d(table)));
        group->set_children(
            boost::intrusive_ptr(new operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group->add_key("count_bool", operators::get::simple_value_t::create(key(&resource, "count_bool")));
        auto sort = boost::intrusive_ptr(new operators::operator_sort_t(d(table)));
        sort->set_children(std::move(group));
        sort->add({"count_bool"});
        sort->on_execute(nullptr);
        REQUIRE(sort->output()->size() == 2);

        const auto& chunk = sort->output()->data_chunk();
        auto val0 = chunk.value(0, 0); // First row, first column (count_bool)
        auto val1 = chunk.value(0, 1); // Second row, first column (count_bool)
        REQUIRE(val0.value<bool>() == false);
        REQUIRE(val1.value<bool>() == true);
    }
}

TEST_CASE("components::physical_plan::group::all") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("aggregate::all") {
        auto group = boost::intrusive_ptr(new operators::operator_group_t(d(table)));
        group->set_children(
            boost::intrusive_ptr(new operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group->add_key("count_bool", operators::get::simple_value_t::create(key(&resource, "count_bool")));

        group->add_value("cnt",
                         boost::intrusive_ptr(new operators::aggregate::operator_func_t(
                             d(table),
                             get_function_by_name("count"),
                             std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource})));
        group->add_value("sum",
                         boost::intrusive_ptr(new operators::aggregate::operator_func_t(
                             d(table),
                             get_function_by_name("sum"),
                             std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource})));
        group->add_value("avg",
                         boost::intrusive_ptr(new operators::aggregate::operator_func_t(
                             d(table),
                             get_function_by_name("avg"),
                             std::pmr::vector<param_storage>{{key(&resource, "count")}, &resource})));

        auto sort = boost::intrusive_ptr(new operators::operator_sort_t(d(table)));
        sort->set_children(std::move(group));
        sort->add({"count_bool"});
        sort->on_execute(nullptr);
        REQUIRE(sort->output()->size() == 2);

        const auto& chunk = sort->output()->data_chunk();

        REQUIRE(chunk.value(0, 0).value<bool>() == false);   // count_bool
        REQUIRE(chunk.value(1, 0).value<uint64_t>() == 50);  // cnt
        REQUIRE(chunk.value(2, 0).value<int64_t>() == 2550); // sum

        REQUIRE(chunk.value(0, 1).value<bool>() == true);    // count_bool
        REQUIRE(chunk.value(1, 1).value<uint64_t>() == 50);  // cnt
        REQUIRE(chunk.value(2, 1).value<int64_t>() == 2500); // sum
    }
}