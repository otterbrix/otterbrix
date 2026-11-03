#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/table/pushed_filter.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/resource_tracer.hpp>

#include <optional>

using namespace components;

// The disk builds its filter of a taken WHERE on its own arena: nothing the filter holds shares a buffer with the
// parameters it was built from.
TEST_CASE("components::table::pushed_filter::the_parameters_of_a_built_filter_are_copies_on_its_arena") {
    resource_tracer_t sender;
    resource_tracer_t builder;

    std::optional<vector::storage_parameters> params;
    params.emplace(&sender);
    const auto id = params->add(vector::make_scalar(&sender, std::string_view{"a string past the inline buffer"}),
                                vector::parameter_kind::literal);

    expressions::key_t column{&sender};
    column.set_path(std::pmr::vector<size_t>{{0}, std::pmr::polymorphic_allocator<size_t>{&sender}});
    auto predicate = expressions::make_compare_expression(&sender, expressions::compare_type::eq, column, id);

    std::pmr::vector<types::complex_logical_type> types(&builder);
    types.emplace_back(types::complex_logical_type{types::logical_type::STRING_LITERAL});
    auto built = table::build_table_filter(&builder, predicate, *params, graph_execution_context{}, types);
    REQUIRE_FALSE(built.has_error());
    params.reset();
    CHECK(built.value()->parameters.resource() == &builder);
    CHECK(built.value()->parameters.at(id).resource() == &builder);
    CHECK(built.value()->parameters.at(id).get_value<std::string_view>(0) == "a string past the inline buffer");
}
