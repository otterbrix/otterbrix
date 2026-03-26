#include <catch2/catch.hpp>
#include <algorithm>

#include "components/index/hash_single_field_index.hpp"
#include "components/index/index_engine.hpp"
#include "components/index/single_field_index.hpp"
#include "components/tests/generaty.hpp"

using namespace components::index;
using key = components::expressions::key_t;

TEST_CASE("hash_single_field_index:base") {
    auto resource = std::pmr::synchronized_pool_resource();
    hash_single_field_index_t index(&resource, "hash_count", {key(&resource, "count")});

    std::vector<std::pair<int64_t, int64_t>> data = {{0, 0}, {1, 1}, {10, 2}, {5, 3}, {6, 4}, {2, 5}, {8, 6}, {13, 7}};

    for (const auto& [value, row_idx] : data) {
        components::types::logical_value_t val(&resource, value);
        index.insert(val, row_idx);
    }

    SECTION("find existing value") {
        components::types::logical_value_t value(&resource, static_cast<int64_t>(10));
        auto find_range = index.find(value);
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(std::distance(find_range.first, find_range.second) == 1);
        REQUIRE(find_range.first->row_index == 2);
    }

    SECTION("find non-existing value") {
        components::types::logical_value_t value(&resource, static_cast<int64_t>(11));
        auto find_range = index.find(value);
        REQUIRE(find_range.first == find_range.second);
    }

    SECTION("duplicate values") {
        for (const auto& [value, row_idx] : data) {
            components::types::logical_value_t val(&resource, value);
            index.insert(val, row_idx + 100);
        }
        components::types::logical_value_t value(&resource, static_cast<int64_t>(10));
        auto find_range = index.find(value);
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(std::distance(find_range.first, find_range.second) == 2);

        std::vector<int64_t> rows;
        for (auto it = find_range.first; it != find_range.second; ++it) {
            rows.push_back(it->row_index);
        }
        REQUIRE(std::find(rows.begin(), rows.end(), static_cast<int64_t>(2)) != rows.end());
        REQUIRE(std::find(rows.begin(), rows.end(), static_cast<int64_t>(102)) != rows.end());
    }
}

TEST_CASE("hash_single_field_index:engine") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto index_engine = make_index_engine(&resource);
    auto id = make_index<hash_single_field_index_t>(index_engine, "hash_count", {key(&resource, "count")});

    auto* idx = search_index(index_engine, id);
    REQUIRE(idx != nullptr);

    idx->insert(components::types::logical_value_t(&resource, 0), int64_t(0));
    for (int i = 10; i >= 1; --i) {
        idx->insert(components::types::logical_value_t(&resource, i), int64_t(11 - i));
    }

    int count = 0;
    for (auto it = idx->cbegin(); it != idx->cend(); ++it) {
        count++;
    }
    REQUIRE(count == 11);

    components::types::logical_value_t value(&resource, 5);
    auto find_range = idx->find(value);
    REQUIRE(find_range.first != find_range.second);
    REQUIRE(find_range.first->row_index == 6);
}
