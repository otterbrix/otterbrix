#include <catch2/catch_test_macros.hpp>

#include <native/python_conversion.hpp>
#include <pybind11/embed.h>
#include <pybind11/pybind_wrapper.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

#include <memory_resource>
#include <string>

using namespace otterbrix;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    complex_logical_type named(logical_type t, const std::string& alias) {
        complex_logical_type type{t};
        type.set_field_name(alias);
        return type;
    }

} // namespace

// A dict converted against a TARGET STRUCT type is matched field-by-field BY NAME.
// Only the field COUNT is checked up front, so a dict of the right size whose keys do
// not cover the struct's fields still reaches the per-field loop. Looking the missing
// name up must not answer with another field's value: unordered_map::operator[]
// default-inserts index 0 on a miss, which silently presents values[0] as the missing
// field. The conversion has an error channel — a field the dict does not carry is an
// error, not a copy of the first value.
TEST_CASE("python_conversion::dict_to_struct_missing_field_is_an_error") {
    py::scoped_interpreter guard{};
    std::pmr::monotonic_buffer_resource resource;

    std::pmr::vector<complex_logical_type> fields{&resource};
    fields.push_back(named(logical_type::INTEGER, "a"));
    fields.push_back(named(logical_type::INTEGER, "b"));
    auto target = complex_logical_type::create_struct("s", fields);

    // Same arity as the target struct, but the second key is 'c', not 'b'.
    py::dict dict;
    dict["a"] = py::int_(1);
    dict["c"] = py::int_(2);

    auto res = transform_python_value(&resource, dict, target);
    if (!res.has_error()) {
        // Diagnostic for the failure below: report what the missing field was filled with.
        const auto& children = res.value().children();
        INFO("converted struct: " << children.size() << " fields, field 'b' = "
                                  << (children.size() > 1 ? std::to_string(children[1].value<int32_t>())
                                                          : std::string{"<none>"}));
        FAIL("a struct field absent from the dict was converted instead of reported");
    }
    REQUIRE(std::string(res.error().what).find('b') != std::string::npos);
}

// The name-matched path stays intact: every target field present in the dict converts
// to that field's value, whatever the dict's own key order is.
TEST_CASE("python_conversion::dict_to_struct_matches_fields_by_name") {
    py::scoped_interpreter guard{};
    std::pmr::monotonic_buffer_resource resource;

    std::pmr::vector<complex_logical_type> fields{&resource};
    fields.push_back(named(logical_type::INTEGER, "a"));
    fields.push_back(named(logical_type::INTEGER, "b"));
    auto target = complex_logical_type::create_struct("s", fields);

    py::dict dict;
    dict["b"] = py::int_(20); // reversed insertion order on purpose
    dict["a"] = py::int_(10);

    auto res = transform_python_value(&resource, dict, target);
    REQUIRE_FALSE(res.has_error());
    const auto& children = res.value().children();
    REQUIRE(children.size() == 2);
    REQUIRE(children[0].value<int32_t>() == 10);
    REQUIRE(children[1].value<int32_t>() == 20);
}

// Without a target type the dict IS the struct: each key keeps its own value. Field
// names are matched case-INSENSITIVELY, so 'a' and 'A' collide into one map entry --
// resolving each key's value through that map handed both fields the last colliding
// key's value. keys()/values() are parallel lists, so the pairing is positional.
TEST_CASE("python_conversion::dict_to_struct_keeps_case_colliding_keys_apart") {
    py::scoped_interpreter guard{};
    std::pmr::monotonic_buffer_resource resource;

    py::dict dict;
    dict["a"] = py::int_(1);
    dict["A"] = py::int_(2);

    auto res = transform_python_value(&resource, dict);
    REQUIRE_FALSE(res.has_error());
    const auto& children = res.value().children();
    REQUIRE(children.size() == 2);
    REQUIRE(children[0].value<int32_t>() == 1);
    REQUIRE(children[1].value<int32_t>() == 2);
}
