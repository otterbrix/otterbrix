#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/table/pushed_filter.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/resource_tracer.hpp>

#include <optional>

using namespace components;

// The filter crosses from the executor to a disk agent in a mailbox, so nothing it carries may share a
// buffer with the sender's parameters.
TEST_CASE("components::table::pushed_filter::parameters are a deep copy on the arena of the receiver") {
    resource_tracer_t sender;
    resource_tracer_t target;
    resource_tracer_t builder;

    std::optional<vector::storage_parameters> params;
    params.emplace(&sender);
    const auto id = params->add(vector::make_scalar(&sender, std::string_view{"a string past the inline buffer"}),
                                vector::parameter_kind::literal);

    expressions::key_t column{&sender};
    column.set_path(std::pmr::vector<size_t>{{0}, std::pmr::polymorphic_allocator<size_t>{&sender}});
    auto predicate = expressions::make_compare_expression(&sender, expressions::compare_type::eq, column, id);

    auto filter = table::make_pushed_filter(&target, predicate, *params, graph_execution_context{});
    REQUIRE(filter != nullptr);
    CHECK(filter->parameters.resource() == &target);
    CHECK(filter->parameters.at(id).resource() == &target);
    CHECK(filter->parameters.at(id).data() != params->at(id).data());

    params.reset();
    CHECK(filter->parameters.at(id).get_value<std::string_view>(0) == "a string past the inline buffer");

    std::pmr::vector<types::complex_logical_type> types(&builder);
    types.emplace_back(types::complex_logical_type{types::logical_type::STRING_LITERAL});
    auto built = table::build_table_filter(&builder, *filter, types);
    REQUIRE_FALSE(built.has_error());
    CHECK(built.value()->parameters.resource() == &builder);
    CHECK(built.value()->parameters.at(id).resource() == &builder);
    CHECK(built.value()->parameters.at(id).get_value<std::string_view>(0) == "a string past the inline buffer");
}
