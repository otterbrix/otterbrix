#include <catch2/catch.hpp>

#include "components/index/index_engine.hpp"
#include "components/index/single_field_index.hpp"
#include "components/tests/generaty.hpp"

using namespace components::index;
using key = components::expressions::key_t;

TEST_CASE("single_field_index:base") {
    auto resource = std::pmr::synchronized_pool_resource();
    single_field_index_t index(&resource, "single_count", {key(&resource, "count")});

    // Insert row indices with corresponding values
    // Values: 0, 1, 10, 5, 6, 2, 8, 13
    // Row indices: 0, 1, 2, 3, 4, 5, 6, 7
    std::vector<std::pair<int64_t, int64_t>> data = {{0, 0}, {1, 1}, {10, 2}, {5, 3}, {6, 4}, {2, 5}, {8, 6}, {13, 7}};

    for (const auto& [value, row_idx] : data) {
        components::types::logical_value_t val(&resource, value);
        index.insert(val, row_idx);
    }

    SECTION("find existing value") {
        components::types::logical_value_t value(&resource, 10);
        auto find_range = index.find(value);
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(find_range.first->row_index == 2); // Row index for value 10
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("find non-existing value") {
        components::types::logical_value_t value(&resource, 11);
        auto find_range = index.find(value);
        REQUIRE(find_range.first == find_range.second);
    }

    SECTION("lower_bound query") {
        components::types::logical_value_t value(&resource, 4);
        auto find_range = index.lower_bound(value);
        REQUIRE(find_range.first == index.cbegin());
        // Values less than 4 are: 0, 1, 2 (sorted)
        // Row indices for 0, 1, 2 are: 0, 1, 5
        REQUIRE(find_range.first->row_index == 0);     // value 0
        REQUIRE((++find_range.first)->row_index == 1); // value 1
        REQUIRE((++find_range.first)->row_index == 5); // value 2
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("lower_bound query at boundary") {
        components::types::logical_value_t value(&resource, 5);
        auto find_range = index.lower_bound(value);
        REQUIRE(find_range.first == index.cbegin());
        // Values less than 5 are: 0, 1, 2 (sorted)
        REQUIRE(find_range.first->row_index == 0);     // value 0
        REQUIRE((++find_range.first)->row_index == 1); // value 1
        REQUIRE((++find_range.first)->row_index == 5); // value 2
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("upper_bound query") {
        components::types::logical_value_t value(&resource, 6);
        auto find_range = index.upper_bound(value);
        REQUIRE(find_range.second == index.cend());
        // Values greater than 6 are: 8, 10, 13 (sorted)
        // Row indices for 8, 10, 13 are: 6, 2, 7
        REQUIRE(find_range.first->row_index == 6);     // value 8
        REQUIRE((++find_range.first)->row_index == 2); // value 10
        REQUIRE((++find_range.first)->row_index == 7); // value 13
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("upper_bound query between values") {
        components::types::logical_value_t value(&resource, 7);
        auto find_range = index.upper_bound(value);
        REQUIRE(find_range.second == index.cend());
        // Values greater than 7 are: 8, 10, 13 (sorted)
        REQUIRE(find_range.first->row_index == 6);     // value 8
        REQUIRE((++find_range.first)->row_index == 2); // value 10
        REQUIRE((++find_range.first)->row_index == 7); // value 13
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("duplicate values") {
        // Insert duplicate values with different row indices
        for (const auto& [value, row_idx] : data) {
            components::types::logical_value_t val(&resource, value);
            index.insert(val, row_idx + 100); // Different row indices
        }
        components::types::logical_value_t value(&resource, 10);
        auto find_range = index.find(value);
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(std::distance(find_range.first, find_range.second) == 2);
        // Both entries have value 10, row indices 2 and 102
        auto row1 = find_range.first->row_index;
        ++find_range.first;
        auto row2 = find_range.first->row_index;
        REQUIRE(((row1 == 2 && row2 == 102) || (row1 == 102 && row2 == 2)));
        REQUIRE(++find_range.first == find_range.second);
    }
}

TEST_CASE("single_field_index:engine") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto index_engine = make_index_engine(&resource);
    auto id = make_index<single_field_index_t>(index_engine, "single_count", {key(&resource, "count")});

    // Get the index and insert values directly
    auto* idx = search_index(index_engine, id);
    REQUIRE(idx != nullptr);

    // Insert row 0 with value 0
    idx->insert(components::types::logical_value_t(&resource, 0), int64_t(0));

    // Insert rows 1-10 with values 10, 9, 8, ..., 1
    for (int i = 10; i >= 1; --i) {
        idx->insert(components::types::logical_value_t(&resource, i), int64_t(11 - i));
    }

    // Verify the index has 11 entries by iterating
    int count = 0;
    for (auto it = idx->cbegin(); it != idx->cend(); ++it) {
        count++;
    }
    REQUIRE(count == 11);

    components::types::logical_value_t value(&resource, 5);
    auto find_range = idx->find(value);
    REQUIRE(find_range.first != find_range.second);
    REQUIRE(find_range.first->row_index == 6); // Row 6 has value 5 (11-5=6)
}
