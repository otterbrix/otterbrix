#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

// Reading a nested cell back out of a vector must answer the column's DECLARED type.
// A NULL inside the cell -- a struct field, a list element, an array element -- is the
// absence of a value, not the absence of a type: it must not rewrite the cell's type.

using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;
using components::vector::vector_t;

namespace {

    logical_value_t null_value(std::pmr::memory_resource* resource) {
        return logical_value_t(resource, complex_logical_type{logical_type::NA});
    }

    complex_logical_type point_type(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::BIGINT, "a");
        fields.emplace_back(logical_type::BIGINT, "b");
        return complex_logical_type::create_struct("point", fields);
    }

} // namespace

TEST_CASE("vector_t::value: struct cell with a NULL field") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = point_type(&resource);
    vector_t v(&resource, declared, 4);

    std::vector<logical_value_t> fields;
    fields.emplace_back(&resource, int64_t{1});
    fields.emplace_back(null_value(&resource));
    v.set_value(0, logical_value_t::create_struct(&resource, declared, fields));

    const auto value = v.value(0);
    REQUIRE_FALSE(value.is_null());
    REQUIRE(value.type() == declared);
    REQUIRE(value.type().child_types().size() == 2);
    REQUIRE(value.type().child_types()[1].type() == logical_type::BIGINT);
    REQUIRE(value.children().size() == 2);
    REQUIRE(value.children()[0].value<int64_t>() == 1);
    REQUIRE(value.children()[1].is_null());
}

TEST_CASE("vector_t::value: struct cell whose every field is NULL is still not a NULL cell") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = point_type(&resource);
    vector_t v(&resource, declared, 4);

    std::vector<logical_value_t> fields;
    fields.emplace_back(null_value(&resource));
    fields.emplace_back(null_value(&resource));
    v.set_value(0, logical_value_t::create_struct(&resource, declared, fields));

    const auto value = v.value(0);
    REQUIRE_FALSE(value.is_null());
    REQUIRE(value.type() == declared);
    REQUIRE(value.children().size() == 2);
    REQUIRE(value.children()[0].is_null());
    REQUIRE(value.children()[1].is_null());
}

TEST_CASE("vector_t::value: an entirely NULL struct cell answers NULL, not a struct of NULLs") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = point_type(&resource);
    vector_t v(&resource, declared, 4);

    v.set_value(0, null_value(&resource));

    const auto value = v.value(0);
    REQUIRE(value.is_null());
    REQUIRE(value.children().empty());
    REQUIRE(v.is_null(0));
}

TEST_CASE("vector_t::value: nested struct with a NULL on the second level") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> inner_fields(&resource);
    inner_fields.emplace_back(logical_type::BIGINT, "x");
    inner_fields.emplace_back(logical_type::BIGINT, "y");
    const auto inner = complex_logical_type::create_struct("inner", inner_fields);

    std::pmr::vector<complex_logical_type> outer_fields(&resource);
    outer_fields.emplace_back(logical_type::BIGINT, "a");
    outer_fields.emplace_back(inner);
    outer_fields.back().set_alias("nested");
    const auto declared = complex_logical_type::create_struct("outer", outer_fields);

    vector_t v(&resource, declared, 4);

    std::vector<logical_value_t> inner_values;
    inner_values.emplace_back(&resource, int64_t{7});
    inner_values.emplace_back(null_value(&resource));
    auto inner_value = logical_value_t::create_struct(&resource, inner, inner_values);
    inner_value.set_alias("nested");

    std::vector<logical_value_t> outer_values;
    outer_values.emplace_back(&resource, int64_t{1});
    outer_values.emplace_back(std::move(inner_value));
    v.set_value(0, logical_value_t::create_struct(&resource, declared, outer_values));

    const auto value = v.value(0);
    REQUIRE_FALSE(value.is_null());
    REQUIRE(value.type() == declared);
    REQUIRE(value.children().size() == 2);
    REQUIRE(value.children()[0].value<int64_t>() == 1);

    const auto& nested = value.children()[1];
    REQUIRE_FALSE(nested.is_null());
    REQUIRE(nested.type() == inner);
    REQUIRE(nested.children().size() == 2);
    REQUIRE(nested.children()[0].value<int64_t>() == 7);
    REQUIRE(nested.children()[1].is_null());
}

TEST_CASE("vector_t::value: struct cell whose LIST field is entirely NULL") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(complex_logical_type::create_list(complex_logical_type{logical_type::BIGINT}, "tags"));
    const auto declared = complex_logical_type::create_struct("bag", fields);

    vector_t v(&resource, declared, 4);

    std::vector<logical_value_t> values;
    values.emplace_back(&resource, int64_t{5});
    values.emplace_back(null_value(&resource));
    v.set_value(0, logical_value_t::create_struct(&resource, declared, values));

    const auto value = v.value(0);
    REQUIRE_FALSE(value.is_null());
    REQUIRE(value.type() == declared);
    REQUIRE(value.children().size() == 2);
    REQUIRE(value.children()[0].value<int64_t>() == 5);
    REQUIRE(value.children()[1].is_null());
}

TEST_CASE("vector_t::value: list cell with a NULL element") {
    auto resource = core::pmr::otterbrix_resource();
    const auto element = complex_logical_type{logical_type::BIGINT};
    const auto declared = complex_logical_type::create_list(element);
    vector_t v(&resource, declared, 4);

    std::vector<logical_value_t> elements;
    elements.emplace_back(&resource, int64_t{10});
    elements.emplace_back(null_value(&resource));
    elements.emplace_back(&resource, int64_t{30});
    v.set_value(0, logical_value_t::create_list(&resource, element, elements));

    const auto value = v.value(0);
    REQUIRE_FALSE(value.is_null());
    REQUIRE(value.type() == declared);
    REQUIRE(value.children().size() == 3);
    REQUIRE(value.children()[0].value<int64_t>() == 10);
    REQUIRE(value.children()[1].is_null());
    REQUIRE(value.children()[2].value<int64_t>() == 30);
}

TEST_CASE("vector_t::value: an entirely NULL list cell answers NULL") {
    auto resource = core::pmr::otterbrix_resource();
    const auto element = complex_logical_type{logical_type::BIGINT};
    const auto declared = complex_logical_type::create_list(element);
    vector_t v(&resource, declared, 4);

    v.set_value(0, null_value(&resource));

    const auto value = v.value(0);
    REQUIRE(value.is_null());
    REQUIRE(value.children().empty());
    REQUIRE(v.is_null(0));
}

TEST_CASE("vector_t::value: array cell with a NULL element") {
    auto resource = core::pmr::otterbrix_resource();
    const auto element = complex_logical_type{logical_type::BIGINT};
    const auto declared = complex_logical_type::create_array(element, 3);
    vector_t v(&resource, declared, 4);

    std::vector<logical_value_t> elements;
    elements.emplace_back(&resource, int64_t{10});
    elements.emplace_back(null_value(&resource));
    elements.emplace_back(&resource, int64_t{30});
    v.set_value(0, logical_value_t::create_array(&resource, element, elements));

    const auto value = v.value(0);
    REQUIRE_FALSE(value.is_null());
    REQUIRE(value.type() == declared);
    REQUIRE(value.children().size() == 3);
    REQUIRE(value.children()[0].value<int64_t>() == 10);
    REQUIRE(value.children()[1].is_null());
    REQUIRE(value.children()[2].value<int64_t>() == 30);
}

TEST_CASE("vector_t::value: an entirely NULL array cell answers NULL") {
    auto resource = core::pmr::otterbrix_resource();
    const auto element = complex_logical_type{logical_type::BIGINT};
    const auto declared = complex_logical_type::create_array(element, 3);
    vector_t v(&resource, declared, 4);

    v.set_value(0, null_value(&resource));

    const auto value = v.value(0);
    REQUIRE(value.is_null());
    REQUIRE(value.children().empty());
    REQUIRE(v.is_null(0));
}

TEST_CASE("vector_t::value: a struct cell with a NULL field survives a read-write round trip") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = point_type(&resource);
    vector_t source(&resource, declared, 4);

    std::vector<logical_value_t> fields;
    fields.emplace_back(&resource, int64_t{1});
    fields.emplace_back(null_value(&resource));
    source.set_value(0, logical_value_t::create_struct(&resource, declared, fields));

    vector_t sink(&resource, declared, 4);
    sink.set_value(0, source.value(0));

    const auto value = sink.value(0);
    REQUIRE_FALSE(value.is_null());
    REQUIRE(value.type() == declared);
    REQUIRE(value.children().size() == 2);
    REQUIRE(value.children()[0].value<int64_t>() == 1);
    REQUIRE(value.children()[1].is_null());
}

// A DICTIONARY (sliced) vector and a CONSTANT vector both address their payload through
// a mapping, so the row a caller names is not the row the validity bit lives at.

TEST_CASE("vector_t::value: a sliced vector answers the NULL of the row it selected") {
    auto resource = core::pmr::otterbrix_resource();
    vector_t source(&resource, complex_logical_type{logical_type::BIGINT}, 4);
    source.set_value(0, logical_value_t(&resource, int64_t{10}));
    source.set_value(1, null_value(&resource));
    source.set_value(2, logical_value_t(&resource, int64_t{30}));

    components::vector::indexing_vector_t indexing(&resource, 3);
    indexing.set_index(0, 1); // the NULL row
    indexing.set_index(1, 2);
    indexing.set_index(2, 0);

    vector_t sliced(&resource, complex_logical_type{logical_type::BIGINT}, 4);
    sliced.slice(source, indexing, 3);

    REQUIRE(sliced.is_null(0));
    REQUIRE(sliced.value(0).is_null());
    REQUIRE_FALSE(sliced.value(1).is_null());
    REQUIRE(sliced.value(1).value<int64_t>() == 30);
    REQUIRE_FALSE(sliced.value(2).is_null());
    REQUIRE(sliced.value(2).value<int64_t>() == 10);
}

TEST_CASE("vector_t::value: a constant NULL vector answers NULL at every index") {
    auto resource = core::pmr::otterbrix_resource();
    vector_t v(&resource, complex_logical_type{logical_type::BIGINT}, 4);
    v.set_vector_type(components::vector::vector_type::CONSTANT);
    v.set_null(true);

    REQUIRE(v.is_null(0));
    REQUIRE(v.is_null(3));
    REQUIRE(v.value(0).is_null());
    REQUIRE(v.value(3).is_null());
}
