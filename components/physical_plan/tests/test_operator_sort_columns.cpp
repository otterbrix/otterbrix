#include <catch2/catch_test_macros.hpp>

#include <components/context/context.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <core/pmr.hpp>

using namespace components;

TEST_CASE("components::physical_plan::operator_sort::kept_computed_keys_follow_the_cut_columns") {
    core::pmr::otterbrix_resource resource;
    logical_plan::storage_parameters parameters{&resource};
    pipeline::context_t ctx{parameters, pipeline::no_mailbox(), pipeline::no_mailbox(), pipeline::no_mailbox()};

    std::pmr::vector<types::complex_logical_type> types{&resource};
    types.emplace_back(types::logical_type::BIGINT);
    types.emplace_back(types::logical_type::BIGINT);
    types.emplace_back(types::logical_type::BIGINT);
    vector::data_chunk_t input(&resource, types, 3);
    const int64_t a[] = {3, 1, 2};
    for (uint64_t row = 0; row < 3; ++row) {
        input.set_value(0, row, a[row]);
        input.set_value(1, row, a[row] * 10);
        input.set_value(2, row, a[row] * 100);
    }
    input.set_cardinality(3);

    expressions::key_t column{&resource, "a"};
    std::pmr::vector<size_t> path{&resource};
    path.push_back(0);
    column.set_path(std::move(path));
    auto key = expressions::make_scalar_expression(&resource, expressions::scalar_type::get_field, column);
    key->set_result_type(types::complex_logical_type{types::logical_type::BIGINT});

    operators::operator_sort_t sort{&resource, log_t{}};
    operators::sort_key_spec_t spec{&resource};
    spec.expression = key;
    sort.add_computed(std::move(spec));
    sort.set_expected_output_count(2);
    sort.set_keep_computed_keys(true);

    operators::chunks_vector_t out{&resource};
    REQUIRE_FALSE(sort.push(&ctx, std::move(input), out).contains_error());
    REQUIRE_FALSE(sort.finalize(&ctx, out).contains_error());
    REQUIRE(out.size() == 1);
    const auto& sorted = out.front();
    REQUIRE(sorted.column_count() == 3);
    for (uint64_t row = 0; row < 3; ++row) {
        const auto expected = static_cast<int64_t>(row + 1);
        CHECK(sorted.data[0].data<int64_t>()[row] == expected);
        CHECK(sorted.data[1].data<int64_t>()[row] == expected * 10);
        CHECK(sorted.data[2].data<int64_t>()[row] == expected);
    }
}
