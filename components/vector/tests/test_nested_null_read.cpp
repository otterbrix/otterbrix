#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <array>
#include <components/types/types.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_builders.hpp>
#include <components/vector/vector_operations.hpp>

// A nested cell stored in a vector keeps the column's DECLARED type. A NULL inside the cell -- a struct
// field, a list element, an array element -- is the absence of a value, not the absence of a type: the
// cell stays valid and only the child is NULL. A wholly NULL cell is NULL itself.

using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::vector_t;
using namespace components::vector;

namespace {

    template<typename... Vs>
    std::array<vector_t, sizeof...(Vs)> pack(Vs&&... vs) {
        return {std::forward<Vs>(vs)...};
    }

    complex_logical_type point_type(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::BIGINT, "a");
        fields.emplace_back(logical_type::BIGINT, "b");
        return complex_logical_type::create_struct("point", fields);
    }

    complex_logical_type bigint() { return complex_logical_type{logical_type::BIGINT}; }

    void put(const vector_t& cell, vector_t& target, uint64_t row) { vector_ops::copy(cell, target, 1, 0, row); }

} // namespace

TEST_CASE("nested null read: struct cell with a NULL field") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = point_type(&resource);
    vector_t v(&resource, declared, 4);

    put(make_struct(&resource, declared, pack(make_scalar<int64_t>(&resource, 1), make_null(&resource, bigint()))), v, 0);

    REQUIRE_FALSE(v.is_null(0));
    REQUIRE(v.type() == declared);
    REQUIRE(v.entries().size() == 2);
    REQUIRE_FALSE(v.entries()[0]->is_null(0));
    REQUIRE(v.entries()[0]->get_value<int64_t>(0) == 1);
    REQUIRE(v.entries()[1]->type().type() == logical_type::BIGINT);
    REQUIRE(v.entries()[1]->is_null(0));
}

TEST_CASE("nested null read: struct cell whose every field is NULL is still not a NULL cell") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = point_type(&resource);
    vector_t v(&resource, declared, 4);

    put(make_struct(&resource, declared, pack(make_null(&resource, bigint()), make_null(&resource, bigint()))), v, 0);

    REQUIRE_FALSE(v.is_null(0));
    REQUIRE(v.type() == declared);
    REQUIRE(v.entries()[0]->is_null(0));
    REQUIRE(v.entries()[1]->is_null(0));
}

TEST_CASE("nested null read: an entirely NULL struct cell is NULL") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = point_type(&resource);
    vector_t v(&resource, declared, 4);

    put(make_null(&resource, declared), v, 0);

    REQUIRE(v.is_null(0));
    REQUIRE(v.type() == declared);
}

TEST_CASE("nested null read: nested struct with a NULL on the second level") {
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

    auto inner_value =
        make_struct(&resource, inner, pack(make_scalar<int64_t>(&resource, 7), make_null(&resource, bigint())));
    put(make_struct(&resource, declared, pack(make_scalar<int64_t>(&resource, 1), std::move(inner_value))), v, 0);

    REQUIRE_FALSE(v.is_null(0));
    REQUIRE(v.type() == declared);
    REQUIRE(v.entries()[0]->get_value<int64_t>(0) == 1);

    const auto& nested = *v.entries()[1];
    REQUIRE_FALSE(nested.is_null(0));
    REQUIRE(nested.entries().size() == 2);
    REQUIRE(nested.entries()[0]->get_value<int64_t>(0) == 7);
    REQUIRE(nested.entries()[1]->is_null(0));
}

TEST_CASE("nested null read: struct cell whose LIST field is entirely NULL") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(complex_logical_type::create_list(bigint(), "tags"));
    const auto declared = complex_logical_type::create_struct("bag", fields);

    vector_t v(&resource, declared, 4);

    put(make_struct(&resource,
                    declared,
                    pack(make_scalar<int64_t>(&resource, 5), make_null(&resource, fields[1]))),
        v,
        0);

    REQUIRE_FALSE(v.is_null(0));
    REQUIRE(v.type() == declared);
    REQUIRE(v.entries()[0]->get_value<int64_t>(0) == 5);
    REQUIRE(v.entries()[1]->is_null(0));
}

TEST_CASE("nested null read: list cell with a NULL element") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = complex_logical_type::create_list(bigint());
    vector_t v(&resource, declared, 4);

    put(make_list(&resource,
                  bigint(),
                  pack(make_scalar<int64_t>(&resource, 10),
                       make_null(&resource, bigint()),
                       make_scalar<int64_t>(&resource, 30))),
        v,
        0);

    REQUIRE_FALSE(v.is_null(0));
    REQUIRE(v.type() == declared);
    const auto seq = sequence_of(v, 0);
    REQUIRE(seq.length == 3);
    REQUIRE(seq.child->get_value<int64_t>(seq.offset) == 10);
    REQUIRE(seq.child->is_null(seq.offset + 1));
    REQUIRE(seq.child->get_value<int64_t>(seq.offset + 2) == 30);
}

TEST_CASE("nested null read: an entirely NULL list cell is NULL") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = complex_logical_type::create_list(bigint());
    vector_t v(&resource, declared, 4);

    put(make_null(&resource, declared), v, 0);

    REQUIRE(v.is_null(0));
    REQUIRE(v.type() == declared);
}

TEST_CASE("nested null read: array cell with a NULL element") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = complex_logical_type::create_array(bigint(), 3);
    vector_t v(&resource, declared, 4);

    put(make_array(&resource,
                   bigint(),
                   pack(make_scalar<int64_t>(&resource, 10),
                        make_null(&resource, bigint()),
                        make_scalar<int64_t>(&resource, 30))),
        v,
        0);

    REQUIRE_FALSE(v.is_null(0));
    REQUIRE(v.type() == declared);
    const auto seq = sequence_of(v, 0);
    REQUIRE(seq.length == 3);
    REQUIRE(seq.child->get_value<int64_t>(seq.offset) == 10);
    REQUIRE(seq.child->is_null(seq.offset + 1));
    REQUIRE(seq.child->get_value<int64_t>(seq.offset + 2) == 30);
}

TEST_CASE("nested null read: an entirely NULL array cell is NULL") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = complex_logical_type::create_array(bigint(), 3);
    vector_t v(&resource, declared, 4);

    put(make_null(&resource, declared), v, 0);

    REQUIRE(v.is_null(0));
    REQUIRE(v.type() == declared);
}

TEST_CASE("nested null read: a struct cell with a NULL field survives a copy") {
    auto resource = core::pmr::otterbrix_resource();
    const auto declared = point_type(&resource);
    vector_t source(&resource, declared, 4);
    put(make_struct(&resource, declared, pack(make_scalar<int64_t>(&resource, 1), make_null(&resource, bigint()))),
        source,
        0);

    vector_t sink(&resource, declared, 4);
    put(source, sink, 0);

    REQUIRE_FALSE(sink.is_null(0));
    REQUIRE(sink.type() == declared);
    REQUIRE(sink.entries()[0]->get_value<int64_t>(0) == 1);
    REQUIRE(sink.entries()[1]->is_null(0));
}

// A DICTIONARY (sliced) vector and a CONSTANT vector both address their payload through
// a mapping, so the row a caller names is not the row the validity bit lives at.

TEST_CASE("nested null read: a sliced vector answers the NULL of the row it selected") {
    auto resource = core::pmr::otterbrix_resource();
    vector_t source(&resource, bigint(), 4);
    source.set_value(0, int64_t{10});
    source.set_null(1, true);
    source.set_value(2, int64_t{30});

    components::vector::indexing_vector_t indexing(&resource, 3);
    indexing.set_index(0, 1); // the NULL row
    indexing.set_index(1, 2);
    indexing.set_index(2, 0);

    vector_t sliced(&resource, bigint(), 4);
    sliced.slice(source, indexing, 3);

    REQUIRE(sliced.is_null(0));
    REQUIRE_FALSE(sliced.is_null(1));
    const auto second = resolve_cell(sliced, 1);
    REQUIRE(second.vec->get_value<int64_t>(second.index) == 30);
    REQUIRE_FALSE(sliced.is_null(2));
    const auto third = resolve_cell(sliced, 2);
    REQUIRE(third.vec->get_value<int64_t>(third.index) == 10);
}

// fails until GitHub issue #764 is fixed: get_value on a DICTIONARY vector checks the outer row's validity
// (hidden: the failing assert aborts the process in debug builds)
TEST_CASE("nested null read: get_value on a sliced vector reads the row it selected", "[.][!shouldfail]") {
    auto resource = core::pmr::otterbrix_resource();
    vector_t source(&resource, bigint(), 4);
    source.set_value(0, int64_t{10});
    source.set_null(1, true);
    source.set_value(2, int64_t{30});

    components::vector::indexing_vector_t indexing(&resource, 3);
    indexing.set_index(0, 1); // the NULL row
    indexing.set_index(1, 2);
    indexing.set_index(2, 0);

    vector_t sliced(&resource, bigint(), 4);
    sliced.slice(source, indexing, 3);

    REQUIRE(sliced.is_null(0));
    REQUIRE_FALSE(sliced.is_null(1));
    REQUIRE(sliced.get_value<int64_t>(1) == 30);
    REQUIRE_FALSE(sliced.is_null(2));
    REQUIRE(sliced.get_value<int64_t>(2) == 10);
}

TEST_CASE("nested null read: a constant NULL vector answers NULL at every index") {
    auto resource = core::pmr::otterbrix_resource();
    vector_t v(&resource, bigint(), 4);
    v.set_vector_type(components::vector::vector_type::CONSTANT);
    v.set_null(true);

    REQUIRE(v.is_null(0));
    REQUIRE(v.is_null(3));
}
