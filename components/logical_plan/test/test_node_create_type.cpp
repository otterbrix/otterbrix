#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/node_create_type.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

#include <memory_resource>
#include <string>
#include <vector>

using namespace components::logical_plan;
using namespace components::types;

// node_create_type_t::to_string() renders an ENUM's entry labels and a STRUCT's field
// names — two DIFFERENT name slots on the member's type (label_ vs field_name_), so
// the rendered text is pinned here. It has no other coverage: the transformer wraps
// create_type in a sequence, and node_sequence_t::to_string_impl() prints only
// "$sequence[N]", so test_create_drop.cpp never reaches this node.

TEST_CASE("logical_plan::node_create_type_t renders enum entry labels") {
    std::pmr::monotonic_buffer_resource resource;

    std::vector<logical_value_t> entries;
    entries.emplace_back(&resource, 0);
    entries.back().set_label("even");
    entries.emplace_back(&resource, 1);
    entries.back().set_label("odd");
    auto enum_type = complex_logical_type::create_enum("oddness_t", std::move(entries));

    auto node = make_node_create_type(&resource, std::move(enum_type));
    CHECK(node->to_string() == "$create_type: name: oddness_t, fields:[ even=0 odd=1 ]");
}

TEST_CASE("logical_plan::node_create_type_t renders struct field names") {
    std::pmr::monotonic_buffer_resource resource;

    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::INTEGER);
    fields.back().set_field_name("f1");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_field_name("f2");
    auto struct_type = complex_logical_type::create_struct("custom_type_name", fields);

    auto node = make_node_create_type(&resource, std::move(struct_type));
    CHECK(node->to_string() == "$create_type: name: custom_type_name, fields:[ f1 f2 ]");
}
