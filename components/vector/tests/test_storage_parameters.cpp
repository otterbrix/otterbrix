#include <catch2/catch_test_macros.hpp>

#include <components/types/types.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/storage_parameters.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/resource_tracer.hpp>

using namespace components::vector;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    complex_logical_type na() { return complex_logical_type{logical_type::NA}; }
    complex_logical_type bigint() { return complex_logical_type{logical_type::BIGINT}; }

    core::parameter_id_t id(size_t index) { return core::parameter_id_t(static_cast<uint32_t>(index)); }

    vector_t bigint_column(std::pmr::memory_resource* resource, std::initializer_list<int64_t> values) {
        vector_t column(resource, bigint(), values.size());
        uint64_t row = 0;
        for (const auto value : values) {
            column.set_value(row++, value);
        }
        return column;
    }

} // namespace

TEST_CASE("storage_parameters: add hands out dense ids with their kind and type") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    CHECK(params.size() == 0);

    const auto first = params.add(make_scalar(&resource, int64_t{7}), parameter_kind::literal);
    const auto second = params.add(make_null(&resource, na()), parameter_kind::external);
    const auto third = params.add(make_scalar(&resource, std::string_view{"abc"}), parameter_kind::execution);

    CHECK(first == id(0));
    CHECK(second == id(1));
    CHECK(third == id(2));
    CHECK(params.size() == 3);
    CHECK(params.kind(first) == parameter_kind::literal);
    CHECK(params.kind(second) == parameter_kind::external);
    CHECK(params.kind(third) == parameter_kind::execution);
    CHECK(params.type(first) == bigint());
    CHECK(params.type(second).type() == logical_type::NA);
    CHECK(params.type(third).type() == logical_type::TEXT);
    CHECK_FALSE(params.is_null(first));
    CHECK(params.is_null(second));
    CHECK(params.at(first).get_value<int64_t>(0) == 7);
    CHECK(params.at(first).get_vector_type() == vector_type::CONSTANT);
    CHECK(params.resource() == &resource);
}

TEST_CASE("storage_parameters: set replaces the vector and leaves the old buffer alone") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    const auto slot = params.add(make_scalar(&resource, int64_t{1}), parameter_kind::literal);

    const vector_t held = params.at(slot);
    params.set(slot, make_scalar(&resource, int64_t{2}));

    CHECK(params.at(slot).get_value<int64_t>(0) == 2);
    CHECK(held.get_value<int64_t>(0) == 1);
}

TEST_CASE("storage_parameters: an NA slot takes the type of the first value set") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    const auto slot = params.add(make_null(&resource, na()), parameter_kind::external);

    params.set(slot, make_scalar(&resource, int64_t{5}));
    CHECK(params.type(slot) == bigint());
    CHECK(params.at(slot).get_value<int64_t>(0) == 5);

    params.set(slot, make_scalar(&resource, int64_t{6}));
    CHECK(params.at(slot).get_value<int64_t>(0) == 6);
}

TEST_CASE("storage_parameters: an external slot takes the last bind whatever its type") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    const auto slot = params.add(make_null(&resource, na()), parameter_kind::external);

    params.set(slot, make_scalar(&resource, int64_t{1}));
    CHECK(params.type(slot) == bigint());

    params.set(slot, make_scalar(&resource, std::string_view{"abc"}));
    CHECK(params.type(slot).type() == logical_type::TEXT);
    CHECK(params.at(slot).get_value<std::string_view>(0) == "abc");
}

TEST_CASE("storage_parameters: a sub-query result takes its array in the NA slot") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    const auto slot = params.add(make_null(&resource, na()), parameter_kind::execution);

    const vector_t elements[] = {make_scalar(&resource, int64_t{1}),
                                 make_null(&resource, bigint()),
                                 make_scalar(&resource, int64_t{3})};
    params.set(slot, make_array(&resource, bigint(), elements));

    REQUIRE(params.type(slot).type() == logical_type::ARRAY);
    const auto& array = params.at(slot);
    CHECK(static_cast<const components::types::array_logical_type_extension*>(array.type().extension())->size() == 3);
    CHECK(array.entry().get_value<int64_t>(0) == 1);
    CHECK(array.entry().is_null(1));
    CHECK(array.entry().get_value<int64_t>(2) == 3);
}

TEST_CASE("storage_parameters: a LIST<NA> placeholder") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    auto placeholder = complex_logical_type::create_list(na());
    auto slot = params.add(make_null(&resource, placeholder), parameter_kind::execution);
    REQUIRE(is_placeholder(params.type(slot)));
    CHECK(filled_placeholder(placeholder, bigint()) == complex_logical_type::create_list(bigint()));

    vector_t elements[] = {make_scalar(&resource, int64_t{1}), make_scalar(&resource, int64_t{2})};
    params.set(slot, make_list(&resource, bigint(), elements));
    CHECK(params.type(slot) == complex_logical_type::create_list(bigint()));
    CHECK_FALSE(is_placeholder(params.type(slot)));
}

TEST_CASE("storage_parameters: set_cell takes one row of a column and keeps the column's type") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    const auto slot = params.add(make_null(&resource, na()), parameter_kind::execution);

    auto column = bigint_column(&resource, {10, 20, 30});
    column.set_null(1, true);

    params.set_cell(slot, column, 2);
    CHECK(params.type(slot) == bigint());
    CHECK_FALSE(params.is_null(slot));
    CHECK(params.at(slot).get_value<int64_t>(0) == 30);
    CHECK(params.at(slot).get_vector_type() == vector_type::CONSTANT);

    params.set_cell(slot, column, 1);
    CHECK(params.type(slot) == bigint());
    CHECK(params.is_null(slot));

    params.set_cell(slot, column, 0);
    CHECK(params.at(slot).get_value<int64_t>(0) == 10);
}

TEST_CASE("storage_parameters: set_cell reads a row of a DICTIONARY column") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    const auto slot = params.add(make_null(&resource, bigint()), parameter_kind::execution);

    const auto base = bigint_column(&resource, {10, 20, 30, 40});
    indexing_vector_t indexing(&resource, 2);
    indexing.set_index(0, 3);
    indexing.set_index(1, 1);
    const vector_t picked(base, indexing, 2);
    REQUIRE(picked.get_vector_type() == vector_type::DICTIONARY);

    params.set_cell(slot, picked, 1);
    CHECK(params.at(slot).get_value<int64_t>(0) == 20);
    params.set_cell(slot, picked, 0);
    CHECK(params.at(slot).get_value<int64_t>(0) == 40);
}

TEST_CASE("storage_parameters: set_cell copies a string out of the column") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    const auto slot = params.add(make_null(&resource, na()), parameter_kind::execution);

    vector_t column(&resource, complex_logical_type{logical_type::TEXT}, 3);
    column.set_value(0, std::string_view{"first"});
    column.set_value(1, std::string_view{"a string long enough to leave the inline buffer"});
    column.set_value(2, std::string_view{"third"});

    params.set_cell(slot, column, 1);
    CHECK(params.type(slot).type() == logical_type::TEXT);
    CHECK(params.at(slot).get_value<std::string_view>(0) == "a string long enough to leave the inline buffer");
}

TEST_CASE("storage_parameters: add_cell appends the next id") {
    resource_tracer_t resource;
    storage_parameters params(&resource);
    const auto column = bigint_column(&resource, {10, 20});

    const auto first = params.add_cell(column, 1, parameter_kind::literal);
    const auto second = params.add_cell(column, 0, parameter_kind::literal);

    CHECK(first == id(0));
    CHECK(second == id(1));
    CHECK(params.at(first).get_value<int64_t>(0) == 20);
    CHECK(params.at(second).get_value<int64_t>(0) == 10);
}

namespace {

    vector_t struct_value(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(complex_logical_type{logical_type::BIGINT});
        fields.back().set_alias("n");
        fields.emplace_back(complex_logical_type{logical_type::TEXT});
        fields.back().set_alias("s");
        const auto type = complex_logical_type::create_struct("row", fields);
        const vector_t members[] = {make_scalar(resource, int64_t{4}),
                                    make_scalar(resource, std::string_view{"a string past the inline buffer"})};
        return make_struct(resource, type, members);
    }

} // namespace

TEST_CASE("storage_parameters: clone is a deep copy of every type family on the target resource") {
    resource_tracer_t source_resource;
    resource_tracer_t target_resource;

    const vector_t list_elements[] = {make_scalar(&source_resource, int64_t{1}), make_null(&source_resource, bigint())};
    const vector_t array_elements[] = {
        make_scalar(&source_resource, std::string_view{"long enough to be heap-allocated"}),
        make_scalar(&source_resource, std::string_view{"b"})};
    const vector_t keys[] = {make_scalar(&source_resource, int64_t{1})};
    const vector_t values[] = {make_scalar(&source_resource, std::string_view{"mapped to a long string value"})};
    auto decimal = make_decimal(&source_resource, 12345, 10, 2);
    REQUIRE_FALSE(decimal.has_error());

    storage_parameters source(&source_resource);
    const auto scalar = source.add(make_scalar(&source_resource, int64_t{42}), parameter_kind::literal);
    const auto text =
        source.add(make_scalar(&source_resource, std::string_view{"a string long enough to be on the heap"}),
                   parameter_kind::external);
    const auto decimal_id = source.add(std::move(decimal.value()), parameter_kind::literal);
    const auto list = source.add(make_list(&source_resource, bigint(), list_elements), parameter_kind::execution);
    const auto array =
        source.add(make_array(&source_resource, complex_logical_type{logical_type::TEXT}, array_elements),
                   parameter_kind::execution);
    const auto structure = source.add(struct_value(&source_resource), parameter_kind::literal);
    const auto map =
        source.add(make_map(&source_resource, bigint(), complex_logical_type{logical_type::TEXT}, keys, values),
                   parameter_kind::literal);
    const auto null_bigint = source.add(make_null(&source_resource, bigint()), parameter_kind::literal);
    const auto null_na = source.add(make_null(&source_resource, na()), parameter_kind::external);

    std::optional<storage_parameters> clone;
    clone.emplace(source.clone(&target_resource));
    const size_t source_bytes_before = source_resource.total_allocated();

    REQUIRE(clone->size() == source.size());
    CHECK(clone->resource() == &target_resource);
    for (size_t index = 0; index < source.size(); ++index) {
        CHECK(clone->kind(id(index)) == source.kind(id(index)));
        CHECK(clone->type(id(index)) == source.type(id(index)));
        CHECK(clone->at(id(index)).resource() == &target_resource);
        CHECK(clone->at(id(index)).get_vector_type() == vector_type::CONSTANT);
        CHECK(cells_equal(clone->at(id(index)), 0, source.at(id(index)), 0));
    }
    CHECK(clone->at(null_bigint).is_null(0));
    CHECK(clone->type(null_bigint) == bigint());
    CHECK(clone->at(null_na).is_null(0));
    CHECK(clone->type(null_na).type() == logical_type::NA);
    CHECK(clone->at(list).entry().resource() == &target_resource);
    CHECK(clone->at(array).entry().resource() == &target_resource);
    CHECK(clone->at(structure).entries().front()->resource() == &target_resource);
    CHECK(clone->at(map).entry().resource() == &target_resource);
    CHECK(source_resource.total_allocated() == source_bytes_before);

    // The clone owns its buffers: the source can go away first.
    { storage_parameters doomed(std::move(source)); }
    CHECK(clone->at(scalar).get_value<int64_t>(0) == 42);
    CHECK(clone->at(text).get_value<std::string_view>(0) == "a string long enough to be on the heap");
    CHECK(clone->at(decimal_id).type().type() == logical_type::DECIMAL);
    CHECK(clone->at(list).entry().get_value<int64_t>(0) == 1);
    CHECK(clone->at(list).entry().is_null(1));
    CHECK(clone->at(array).entry().get_value<std::string_view>(0) == "long enough to be heap-allocated");
    CHECK(clone->at(structure).entries()[1]->get_value<std::string_view>(0) == "a string past the inline buffer");
    CHECK(clone->at(map).entry().entries()[1]->get_value<std::string_view>(0) == "mapped to a long string value");
}

TEST_CASE("storage_parameters: setting a slot of a clone does not touch the original") {
    resource_tracer_t resource;
    storage_parameters source(&resource);
    const auto slot = source.add(make_scalar(&resource, int64_t{1}), parameter_kind::literal);

    auto clone = source.clone(&resource);
    clone.set(slot, make_scalar(&resource, int64_t{2}));

    CHECK(source.at(slot).get_value<int64_t>(0) == 1);
    CHECK(clone.at(slot).get_value<int64_t>(0) == 2);
}

namespace {

    components::vector::data_chunk_t bigint_chunk(std::pmr::memory_resource* resource, int64_t first, uint64_t rows) {
        std::pmr::vector<complex_logical_type> types(resource);
        types.emplace_back(bigint());
        data_chunk_t chunk(resource, types, rows);
        for (uint64_t row = 0; row < rows; ++row) {
            chunk.data[0].set_value(row, first + static_cast<int64_t>(row));
        }
        chunk.set_cardinality(rows);
        return chunk;
    }

} // namespace

TEST_CASE("compact_to_single_value: one cell keeps the column's type") {
    resource_tracer_t resource;
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, 9, 1));

    auto compacted = compact_to_single_value(&resource, chunks, bigint());
    REQUIRE_FALSE(compacted.has_error());
    CHECK(compacted.value().type() == bigint());
    CHECK(compacted.value().get_vector_type() == vector_type::CONSTANT);
    CHECK(compacted.value().get_value<int64_t>(0) == 9);
}

TEST_CASE("compact_to_single_value: a NULL cell stays NULL and typed") {
    resource_tracer_t resource;
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, 9, 1));
    chunks.back().data[0].set_null(0, true);

    auto compacted = compact_to_single_value(&resource, chunks, bigint());
    REQUIRE_FALSE(compacted.has_error());
    CHECK(compacted.value().type() == bigint());
    CHECK(compacted.value().is_null(0));
}

TEST_CASE("compact_to_single_value: no row") {
    resource_tracer_t resource;
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, 0, 0));

    auto compacted = compact_to_single_value(&resource, chunks, bigint());
    REQUIRE_FALSE(compacted.has_error());
    CHECK(compacted.value().type() == bigint());
    CHECK(compacted.value().is_null(0));
}

TEST_CASE("compact_to_single_value: two rows are refused") {
    resource_tracer_t resource;
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, 1, 1));
    chunks.emplace_back(bigint_chunk(&resource, 2, 1));

    CHECK(compact_to_single_value(&resource, chunks, bigint()).has_error());
}

TEST_CASE("compact_to_bool_value: any row is true") {
    resource_tracer_t resource;
    const complex_logical_type boolean{logical_type::BOOLEAN};
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, 0, 0));
    {
        auto none = compact_to_bool_value(&resource, chunks, boolean);
        REQUIRE_FALSE(none.has_error());
        CHECK(none.value().type().type() == logical_type::BOOLEAN);
        CHECK_FALSE(none.value().get_value<bool>(0));
    }
    chunks.emplace_back(bigint_chunk(&resource, 1, 2));
    auto some = compact_to_bool_value(&resource, chunks, boolean);
    REQUIRE_FALSE(some.has_error());
    CHECK(some.value().get_value<bool>(0));
}

TEST_CASE("compact_to_list_value: every row of every chunk lands in one list, NULL elements included") {
    resource_tracer_t resource;
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, 0, 1024));
    chunks.back().data[0].set_null(5, true);
    chunks.emplace_back(bigint_chunk(&resource, 1024, 300));

    auto compacted = compact_to_list_value(&resource, chunks, complex_logical_type::create_list(bigint()));
    REQUIRE_FALSE(compacted.has_error());
    const auto& list = compacted.value();
    REQUIRE(list.type() == complex_logical_type::create_list(bigint()));
    const auto elements = sequence_of(list, 0);
    REQUIRE(elements.length == 1324);
    CHECK(elements.child->get_value<int64_t>(elements.offset) == 0);
    CHECK(elements.child->is_null(elements.offset + 5));
    CHECK(elements.child->get_value<int64_t>(elements.offset + 1023) == 1023);
    CHECK(elements.child->get_value<int64_t>(elements.offset + 1024) == 1024);
    CHECK(elements.child->get_value<int64_t>(elements.offset + 1323) == 1323);
}

TEST_CASE("compact_to_list_value: no row") {
    resource_tracer_t resource;
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, 0, 0));

    auto compacted = compact_to_list_value(&resource, chunks, complex_logical_type::create_list(bigint()));
    REQUIRE_FALSE(compacted.has_error());
    CHECK(compacted.value().type() == complex_logical_type::create_list(bigint()));
    CHECK_FALSE(compacted.value().is_null(0));
    CHECK(sequence_of(compacted.value(), 0).length == 0);
}

TEST_CASE("compact_to_list_value: a column that changes its type between chunks is refused") {
    resource_tracer_t resource;
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(bigint_chunk(&resource, 0, 2));
    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(complex_logical_type{logical_type::INTEGER});
    data_chunk_t other(&resource, types, 2);
    other.set_cardinality(2);
    chunks.emplace_back(std::move(other));

    CHECK(compact_to_list_value(&resource, chunks, complex_logical_type::create_list(bigint())).has_error());
}

TEST_CASE("compact_to_list_value: more than one column is refused") {
    resource_tracer_t resource;
    std::pmr::vector<complex_logical_type> types(&resource);
    types.emplace_back(bigint());
    types.emplace_back(bigint());
    std::pmr::vector<data_chunk_t> chunks(&resource);
    chunks.emplace_back(&resource, types, 2);
    chunks.back().set_cardinality(1);

    CHECK(compact_to_list_value(&resource, chunks, complex_logical_type::create_list(bigint())).has_error());
}
