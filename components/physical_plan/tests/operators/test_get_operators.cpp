#include <catch2/catch.hpp>
#include <components/physical_plan/collection/operators/get/simple_value.hpp>
#include <components/tests/generaty.hpp>

using namespace components;
using namespace components::collection::operators;
using key = components::expressions::key_t;

TEST_CASE("components::physical_plan::get::get_value") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto doc = gen_doc(1, &resource);

    auto getter = get::simple_value_t::create(key(&resource, "count"));
    REQUIRE(getter->value(doc));
    REQUIRE(getter->value(doc).as_int() == 1);

    getter = get::simple_value_t::create(key(&resource, "count_str"));
    REQUIRE(getter->value(doc));
    REQUIRE(getter->value(doc).as_string() == "1");

    getter = get::simple_value_t::create(key(&resource, "count_array/0"));
    REQUIRE(getter->value(doc));
    REQUIRE(getter->value(doc).as_int() == 1);

    getter = get::simple_value_t::create(key(&resource, "count_dict/even"));
    REQUIRE(getter->value(doc));
    REQUIRE(getter->value(doc).as_bool() == false);

    getter = get::simple_value_t::create(key(&resource, "invalid"));
    REQUIRE_FALSE(getter->value(doc));

    getter = get::simple_value_t::create(key(&resource, "count_array/10"));
    REQUIRE_FALSE(getter->value(doc));

    getter = get::simple_value_t::create(key(&resource, "count_dict/invalid"));
    REQUIRE_FALSE(getter->value(doc));
}
