#include <catch2/catch.hpp>
#include <components/expressions/forward.hpp>
#include <components/table/column_state.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

using namespace components::table;
using namespace components::types;

// 1. Basic IN-list — values stored verbatim, contains() returns truth for member values.
TEST_CASE("table::set_membership_filter::basic_contains") {
    std::pmr::synchronized_pool_resource resource;
    std::pmr::vector<logical_value_t> vals(&resource);
    vals.emplace_back(&resource, int64_t(10));
    vals.emplace_back(&resource, int64_t(20));
    vals.emplace_back(&resource, int64_t(30));
    std::pmr::vector<uint64_t> indices(&resource);
    indices.push_back(0);

    set_membership_filter_t f(std::move(vals), std::move(indices));
    REQUIRE(f.contains(logical_value_t{&resource, int64_t(10)}));
    REQUIRE(f.contains(logical_value_t{&resource, int64_t(20)}));
    REQUIRE(f.contains(logical_value_t{&resource, int64_t(30)}));
    REQUIRE_FALSE(f.contains(logical_value_t{&resource, int64_t(40)}));
    REQUIRE_FALSE(f.contains(logical_value_t{&resource, int64_t(0)}));
}

// 2. Empty value set never matches — used by M4 batch resolve when nothing to look up.
TEST_CASE("table::set_membership_filter::empty_set_never_matches") {
    std::pmr::synchronized_pool_resource resource;
    std::pmr::vector<logical_value_t> vals(&resource);
    std::pmr::vector<uint64_t> indices(&resource);
    indices.push_back(0);

    set_membership_filter_t f(std::move(vals), std::move(indices));
    REQUIRE_FALSE(f.contains(logical_value_t{&resource, int64_t(0)}));
    REQUIRE_FALSE(f.contains(logical_value_t{&resource, int64_t(123)}));
}

// 3. copy() preserves values and table_indices (deep semantic copy via copy ctor).
TEST_CASE("table::set_membership_filter::copy_preserves_values") {
    std::pmr::synchronized_pool_resource resource;
    std::pmr::vector<logical_value_t> vals(&resource);
    vals.emplace_back(&resource, int64_t(7));
    vals.emplace_back(&resource, int64_t(8));
    std::pmr::vector<uint64_t> indices(&resource);
    indices.push_back(2);
    indices.push_back(5);

    set_membership_filter_t orig(std::move(vals), std::move(indices));
    auto copy = orig.copy();
    REQUIRE(copy != nullptr);

    auto* casted = dynamic_cast<set_membership_filter_t*>(copy.get());
    REQUIRE(casted != nullptr);
    REQUIRE(casted->values.size() == 2);
    REQUIRE(casted->table_indices.size() == 2);
    REQUIRE(casted->table_indices[0] == 2);
    REQUIRE(casted->table_indices[1] == 5);
    REQUIRE(casted->contains(logical_value_t{&resource, int64_t(7)}));
}

// 4. equals() — same values + indices match; different sizes don't match.
TEST_CASE("table::set_membership_filter::equals") {
    std::pmr::synchronized_pool_resource resource;

    auto make = [&](std::initializer_list<int64_t> ns) {
        std::pmr::vector<logical_value_t> vals(&resource);
        for (int64_t n : ns) {
            vals.emplace_back(&resource, n);
        }
        std::pmr::vector<uint64_t> indices(&resource);
        indices.push_back(0);
        return set_membership_filter_t(std::move(vals), std::move(indices));
    };

    auto a = make({1, 2, 3});
    auto b = make({1, 2, 3});
    auto c = make({1, 2, 4});
    auto d = make({1, 2});

    REQUIRE(a.equals(b));
    REQUIRE_FALSE(a.equals(c));
    REQUIRE_FALSE(a.equals(d));
}

// 5. The filter type is EQUALS (treated as multi-valued equality by dispatch sites).
TEST_CASE("table::set_membership_filter::filter_type_is_eq") {
    std::pmr::synchronized_pool_resource resource;
    std::pmr::vector<logical_value_t> vals(&resource);
    vals.emplace_back(&resource, int64_t(1));
    std::pmr::vector<uint64_t> indices(&resource);
    indices.push_back(0);

    set_membership_filter_t f(std::move(vals), std::move(indices));
    REQUIRE(f.filter_type == components::expressions::compare_type::eq);
}
