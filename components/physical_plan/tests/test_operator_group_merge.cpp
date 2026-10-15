#include <catch2/catch_test_macros.hpp>

#include <components/physical_plan/operators/operator_group_merge.hpp>
#include <core/pmr.hpp>

using namespace components;
using components::operators::operator_group_merge_t;

namespace {
    operator_group_merge_t scalar_count(std::pmr::memory_resource* resource, types::complex_logical_type output_type) {
        std::pmr::vector<types::complex_logical_type> output_types{resource};
        output_types.push_back(std::move(output_type));
        return operator_group_merge_t{resource, log_t{}, true, std::move(output_types), {{"cnt", "count"}}};
    }
} // namespace

TEST_CASE("components::physical_plan::group_merge::scalar_count_over_no_rows_is_ubigint_zero") {
    core::pmr::otterbrix_resource resource;
    auto op = scalar_count(&resource, types::complex_logical_type{types::logical_type::UBIGINT});

    components::operators::chunks_vector_t out{&resource};
    auto error = op.finalize(nullptr, out);
    REQUIRE_FALSE(error.contains_error());
    REQUIRE(out.size() == 1);
    REQUIRE(out.front().size() == 1);
    REQUIRE_FALSE(out.front().is_null(0, 0));
    CHECK(out.front().get_value<uint64_t>(0, 0) == 0);
}

TEST_CASE("components::physical_plan::group_merge::scalar_count_over_no_rows_refuses_a_non_ubigint_column") {
    core::pmr::otterbrix_resource resource;
    auto op = scalar_count(&resource, types::complex_logical_type{types::logical_type::BIGINT});

    components::operators::chunks_vector_t out{&resource};
    auto error = op.finalize(nullptr, out);
    CHECK(error.contains_error());
    CHECK(out.empty());
}
