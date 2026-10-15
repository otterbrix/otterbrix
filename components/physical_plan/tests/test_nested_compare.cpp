#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <array>
#include <compare>
#include <components/physical_plan/operators/sort/sort.hpp>
#include <components/types/types.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/operations/apply_operator.hpp>
#include <components/vector/vector_builders.hpp>

// STRUCT / LIST(STRUCT) / MAP cells through the three comparison paths: cells_equal (GROUP BY, UNION,
// DISTINCT), operations::compare_cells (list compare, MIN/MAX) and the ORDER BY comparator. SQL cannot
// build a MAP or an ENUM inside a STRUCT, nor reach MIN/MAX over a STRUCT, so the cells come from the builders.

using namespace components::vector;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    complex_logical_type bigint() { return complex_logical_type{logical_type::BIGINT}; }
    complex_logical_type varchar() { return complex_logical_type{logical_type::STRING_LITERAL}; }

    template<typename... Vectors>
    std::array<vector_t, sizeof...(Vectors)> pack(Vectors&&... vectors) {
        return {std::forward<Vectors>(vectors)...};
    }

    vector_t map_of(std::pmr::memory_resource* resource, std::string_view k1, vector_t v1, std::string_view k2, vector_t v2) {
        auto keys = pack(make_scalar<std::string_view>(resource, k1), make_scalar<std::string_view>(resource, k2));
        auto values = pack(std::move(v1), std::move(v2));
        return make_map(resource, varchar(), bigint(), keys, values);
    }

    // ORDER BY comparator over a one-row chunk of `a` and a one-row chunk of `b`.
    int sort_compare(std::pmr::memory_resource* resource, const vector_t& a, const vector_t& b) {
        std::pmr::vector<complex_logical_type> types(resource);
        types.push_back(a.type());
        data_chunk_t left(resource, types, 1);
        data_chunk_t right(resource, types, 1);
        left.data[0].reference(a);
        right.data[0].reference(b);
        left.set_cardinality(1);
        right.set_cardinality(1);
        components::sort::columnar_sorter_t sorter(resource, 0);
        return sorter.compare_cross(left, 0, right, 0);
    }

} // namespace

TEST_CASE("nested compare: MAP") {
    auto resource = core::pmr::otterbrix_resource();
    auto ab = map_of(&resource, "a", make_scalar<int64_t>(&resource, 1), "b", make_scalar<int64_t>(&resource, 2));
    auto ab_again = map_of(&resource, "a", make_scalar<int64_t>(&resource, 1), "b", make_scalar<int64_t>(&resource, 2));
    auto ba = map_of(&resource, "b", make_scalar<int64_t>(&resource, 2), "a", make_scalar<int64_t>(&resource, 1));

    SECTION("equal maps") {
        CHECK(cells_equal(ab, 0, ab_again, 0));
        CHECK(operations::compare_cells(ab, 0, ab_again, 0) == std::partial_ordering::equivalent);
        CHECK(sort_compare(&resource, ab, ab_again) == 0);
    }
    SECTION("the same entries in another order are another map") {
        CHECK_FALSE(cells_equal(ab, 0, ba, 0));
        CHECK(operations::compare_cells(ab, 0, ba, 0) == std::partial_ordering::less);
        CHECK(sort_compare(&resource, ab, ba) < 0);
        CHECK(sort_compare(&resource, ba, ab) > 0);
    }
    SECTION("a NULL value") {
        auto with_null = map_of(&resource, "a", make_null(&resource, bigint()), "b", make_scalar<int64_t>(&resource, 2));
        auto with_null_again =
            map_of(&resource, "a", make_null(&resource, bigint()), "b", make_scalar<int64_t>(&resource, 2));
        CHECK(cells_equal(with_null, 0, with_null_again, 0));
        CHECK(operations::compare_cells(with_null, 0, with_null_again, 0) == std::partial_ordering::equivalent);
        CHECK(sort_compare(&resource, with_null, with_null_again) == 0);

        CHECK_FALSE(cells_equal(with_null, 0, ab, 0));
        // a NULL sorts after every value
        CHECK(operations::compare_cells(with_null, 0, ab, 0) == std::partial_ordering::greater);
        CHECK(sort_compare(&resource, with_null, ab) > 0);
        CHECK(sort_compare(&resource, ab, with_null) < 0);
    }
}

TEST_CASE("nested compare: STRUCT and LIST of STRUCT") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(logical_type::STRING_LITERAL, "b");
    const auto point = complex_logical_type::create_struct("point", fields);
    auto make_point = [&](int64_t a, std::string_view b) {
        return make_struct(&resource,
                           point,
                           pack(make_scalar<int64_t>(&resource, a), make_scalar<std::string_view>(&resource, b)));
    };
    auto one_b = make_point(1, "b");
    auto one_z = make_point(1, "z");
    auto two_a = make_point(2, "a");

    SECTION("STRUCT: the first unequal field decides") {
        CHECK(cells_equal(one_b, 0, make_point(1, "b"), 0));
        CHECK_FALSE(cells_equal(one_b, 0, one_z, 0));
        CHECK(operations::compare_cells(one_b, 0, one_z, 0) == std::partial_ordering::less);
        CHECK(operations::compare_cells(two_a, 0, one_z, 0) == std::partial_ordering::greater);
        CHECK(sort_compare(&resource, one_b, two_a) < 0);
        CHECK(sort_compare(&resource, two_a, one_z) > 0);
        CHECK(sort_compare(&resource, one_b, make_point(1, "b")) == 0);
    }
    SECTION("LIST(STRUCT): element-wise ordering") {
        auto list_of = [&](vector_t element) {
            return make_list(&resource, point, pack(std::move(element)));
        };
        auto smallest = list_of(make_point(1, "b"));
        auto middle = list_of(make_point(1, "z"));
        auto largest = list_of(make_point(2, "a"));
        CHECK(cells_equal(smallest, 0, list_of(make_point(1, "b")), 0));
        CHECK(operations::compare_cells(smallest, 0, middle, 0) == std::partial_ordering::less);
        CHECK(operations::compare_cells(largest, 0, middle, 0) == std::partial_ordering::greater);
        CHECK(sort_compare(&resource, smallest, largest) < 0);
    }
}

TEST_CASE("nested compare: a STRUCT holding an ENUM orders by position") {
    auto resource = core::pmr::otterbrix_resource();
    // position order (sad < ok) is the opposite of the alphabetical one
    const auto mood = complex_logical_type::create_enum("mood", {{"sad", 0}, {"ok", 1}});
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.push_back(mood);
    const auto holder = complex_logical_type::create_struct("holder", fields);
    auto holding = [&](int32_t ordinal) {
        return make_struct(&resource, holder, pack(make_scalar<int32_t>(&resource, mood, ordinal)));
    };
    auto sad = holding(0);
    auto ok = holding(1);
    CHECK(operations::compare_cells(sad, 0, ok, 0) == std::partial_ordering::less);
    CHECK(sort_compare(&resource, sad, ok) < 0);
    CHECK(sort_compare(&resource, ok, sad) > 0);
    CHECK_FALSE(cells_equal(sad, 0, ok, 0));
}
