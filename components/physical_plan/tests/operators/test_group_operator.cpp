#include <catch2/catch.hpp>

#include "test_operator_generaty.hpp"

#include <components/physical_plan/table/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/table/operators/get/simple_value.hpp>
#include <components/physical_plan/table/operators/operator_group.hpp>
#include <components/physical_plan/table/operators/operator_sort.hpp>
#include <components/physical_plan/table/operators/scan/transfer_scan.hpp>

using namespace components;
using namespace components::expressions;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("components::physical_plan::group::base") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("base::all::no_valid") {
        table::operators::operator_group_t group(d(table));
        group.set_children(
            boost::intrusive_ptr(new table::operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group.add_key("id_", table::operators::get::simple_value_t::create(key(&resource, "id_")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 0);
    }

    SECTION("base::all::id") {
        table::operators::operator_group_t group(d(table));
        group.set_children(
            boost::intrusive_ptr(new table::operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group.add_key("_id", table::operators::get::simple_value_t::create(key(&resource, "_id")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 100);
    }

    SECTION("base::all::count_bool") {
        table::operators::operator_group_t group(d(table));
        group.set_children(
            boost::intrusive_ptr(new table::operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group.add_key("count_bool", table::operators::get::simple_value_t::create(key(&resource, "count_bool")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 2);
    }
}

TEST_CASE("components::physical_plan::group::sort") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("sort::all") {
        auto group = boost::intrusive_ptr(new table::operators::operator_group_t(d(table)));
        group->set_children(
            boost::intrusive_ptr(new table::operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group->add_key("count_bool", table::operators::get::simple_value_t::create(key(&resource, "count_bool")));
        auto sort = boost::intrusive_ptr(new table::operators::operator_sort_t(d(table)));
        sort->set_children(std::move(group));
        sort->add({"count_bool"});
        sort->on_execute(nullptr);
        REQUIRE(sort->output()->size() == 2);

        // Check sorted values using data_chunk
        const auto& chunk = sort->output()->data_chunk();
        // count_bool column should be sorted: false (0) first, then true (1)
        auto val0 = chunk.value(0, 0);  // First row, first column (count_bool)
        auto val1 = chunk.value(0, 1);  // Second row, first column (count_bool)
        REQUIRE(val0.value<bool>() == false);
        REQUIRE(val1.value<bool>() == true);
    }
}

TEST_CASE("components::physical_plan::group::all") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto table = init_table(&resource);

    SECTION("aggregate::all") {
        auto group = boost::intrusive_ptr(new table::operators::operator_group_t(d(table)));
        group->set_children(
            boost::intrusive_ptr(new table::operators::transfer_scan(d(table), logical_plan::limit_t::unlimit())));
        group->add_key("count_bool", table::operators::get::simple_value_t::create(key(&resource, "count_bool")));

        group->add_value("cnt", boost::intrusive_ptr(new table::operators::aggregate::operator_count_t(d(table))));
        group->add_value(
            "sum",
            boost::intrusive_ptr(new table::operators::aggregate::operator_sum_t(d(table), key(&resource, "count"))));
        group->add_value(
            "avg",
            boost::intrusive_ptr(new table::operators::aggregate::operator_avg_t(d(table), key(&resource, "count"))));

        auto sort = boost::intrusive_ptr(new table::operators::operator_sort_t(d(table)));
        sort->set_children(std::move(group));
        sort->add({"count_bool"});
        sort->on_execute(nullptr);
        REQUIRE(sort->output()->size() == 2);

        // Check aggregate values using data_chunk
        // Output columns should be: count_bool, cnt, sum, avg
        const auto& chunk = sort->output()->data_chunk();

        // First row (count_bool = false): even numbers 2,4,6,...,100 (50 items, sum=2550, avg=51)
        REQUIRE(chunk.value(0, 0).value<bool>() == false);  // count_bool
        REQUIRE(chunk.value(1, 0).value<uint64_t>() == 50);       // cnt
        REQUIRE(chunk.value(2, 0).value<int64_t>() == 2550);     // sum

        // Second row (count_bool = true): odd numbers 1,3,5,...,99 (50 items, sum=2500, avg=50)
        REQUIRE(chunk.value(0, 1).value<bool>() == true);   // count_bool
        REQUIRE(chunk.value(1, 1).value<uint64_t>() == 50);       // cnt
        REQUIRE(chunk.value(2, 1).value<int64_t>() == 2500);     // sum
    }
}