#include <algorithm>
#include <catch2/catch_test_macros.hpp>

#include "components/index/hash_single_field_index.hpp"
#include "components/index/index_engine.hpp"
#include "components/index/single_field_index.hpp"
#include "components/tests/generaty.hpp"

using namespace components::index;
using key = components::expressions::key_t;

TEST_CASE("single_field_index:base") {
    auto resource = core::pmr::otterbrix_resource();
    single_field_index_t index(&resource, "single_count", {key(&resource, "count")});

    // Insert row indices with corresponding values
    // Values: 0, 1, 10, 5, 6, 2, 8, 13
    // Row indices: 0, 1, 2, 3, 4, 5, 6, 7
    std::vector<std::pair<int64_t, int64_t>> data = {{0, 0}, {1, 1}, {10, 2}, {5, 3}, {6, 4}, {2, 5}, {8, 6}, {13, 7}};

    for (const auto& [value, row_idx] : data) {
        components::types::logical_value_t val(&resource, value);
        index.insert(val, row_idx, {});
    }

    SECTION("find existing value") {
        components::types::logical_value_t value(&resource, 10l);
        auto find_range = index.find(value, {});
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(find_range.first->row_index == 2); // Row index for value 10
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("find non-existing value") {
        components::types::logical_value_t value(&resource, 11l);
        auto find_range = index.find(value, {});
        REQUIRE(find_range.first == find_range.second);
    }

    SECTION("lower_bound query") {
        components::types::logical_value_t value(&resource, 4l);
        auto find_range = index.lower_bound(value, {});
        REQUIRE(find_range.first == index.cbegin());
        // Values less than 4 are: 0, 1, 2 (sorted)
        // Row indices for 0, 1, 2 are: 0, 1, 5
        REQUIRE(find_range.first->row_index == 0);     // value 0
        REQUIRE((++find_range.first)->row_index == 1); // value 1
        REQUIRE((++find_range.first)->row_index == 5); // value 2
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("lower_bound query at boundary") {
        components::types::logical_value_t value(&resource, 5l);
        auto find_range = index.lower_bound(value, {});
        REQUIRE(find_range.first == index.cbegin());
        // Values less than 5 are: 0, 1, 2 (sorted)
        REQUIRE(find_range.first->row_index == 0);     // value 0
        REQUIRE((++find_range.first)->row_index == 1); // value 1
        REQUIRE((++find_range.first)->row_index == 5); // value 2
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("upper_bound query") {
        components::types::logical_value_t value(&resource, 6l);
        auto find_range = index.upper_bound(value, {});
        REQUIRE(find_range.second == index.cend());
        // Values greater than 6 are: 8, 10, 13 (sorted)
        // Row indices for 8, 10, 13 are: 6, 2, 7
        REQUIRE(find_range.first->row_index == 6);     // value 8
        REQUIRE((++find_range.first)->row_index == 2); // value 10
        REQUIRE((++find_range.first)->row_index == 7); // value 13
        REQUIRE(++find_range.first == find_range.second);
    }

    SECTION("upper_bound query between values") {
        components::types::logical_value_t value(&resource, 7l);
        auto find_range = index.upper_bound(value, {});
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
            index.insert(val, row_idx + 100, {}); // Different row indices
        }
        components::types::logical_value_t value(&resource, 10l);
        auto find_range = index.find(value, {});
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
    auto resource = core::pmr::otterbrix_resource();
    auto index_engine = make_index_engine(&resource);
    auto id = make_index<single_field_index_t>(index_engine, "single_count", {key(&resource, "count")});

    // Get the index and insert values directly
    auto* idx = search_index(index_engine, id);
    REQUIRE(idx != nullptr);

    // Insert row 0 with value 0
    idx->insert(components::types::logical_value_t(&resource, 0), int64_t(0), {});

    // Insert rows 1-10 with values 10, 9, 8, ..., 1
    for (int i = 10; i >= 1; --i) {
        idx->insert(components::types::logical_value_t(&resource, i), int64_t(11 - i), {});
    }

    // Verify the index has 11 entries by iterating
    int count = 0;
    for (auto it = idx->cbegin(); it != idx->cend(); ++it) {
        count++;
    }
    REQUIRE(count == 11);

    components::types::logical_value_t value(&resource, 5);
    auto find_range = idx->find(value, {});
    REQUIRE(find_range.first != find_range.second);
    REQUIRE(find_range.first->row_index == 6); // Row 6 has value 5 (11-5=6)
}

// The btree holds keys of ONE type, so every key is normalised into stored_type_ (the first
// inserted key's type) on the way in and on the way out. The hazard is that logical_value_t::cast_as
// is a SQL CAST, not a domain check: STRING -> BIGINT runs std::atoll, which maps every non-numeric
// string to 0 and reports success. Taking that at face value made the index INVENT keys — 'hello'
// and 'world' both landed under the BIGINT key 0, and an equality probe, collapsing the same way,
// matched them. index_scan carries no operator_match above it, so the invented match was the answer.
//
// The contract these cases pin: a value the index can not represent faithfully is left OUT, never
// folded onto a key that means something else. Incomplete is recoverable; wrong is not.
TEST_CASE("single_field_index:out_of_domain_keys_are_not_invented") {
    auto resource = core::pmr::otterbrix_resource();
    single_field_index_t index(&resource, "single_val", {key(&resource, "val")});

    // stored_type_ locks to BIGINT here.
    index.insert(components::types::logical_value_t(&resource, int64_t{10}), int64_t{0}, {});
    index.insert(components::types::logical_value_t(&resource, int64_t{20}), int64_t{1}, {});
    // A dynamic-schema field whose type later evolved: these are not representable as BIGINT.
    index.insert(components::types::logical_value_t(&resource, "hello"), int64_t{2}, {});
    index.insert(components::types::logical_value_t(&resource, "world"), int64_t{3}, {});

    SECTION("the un-representable values were not stored under an invented key") {
        int count = 0;
        for (auto it = index.cbegin(); it != index.cend(); ++it) {
            ++count;
        }
        REQUIRE(count == 2); // only the two genuine BIGINTs
    }

    SECTION("a BIGINT probe does not collect the strings") {
        // atoll('hello') == atoll('world') == 0, so before the domain check this returned both.
        auto range = index.find(components::types::logical_value_t(&resource, int64_t{0}), {});
        REQUIRE(range.first == range.second);
    }

    SECTION("a STRING probe finds nothing rather than everything") {
        auto range = index.find(components::types::logical_value_t(&resource, "hello"), {});
        REQUIRE(range.first == range.second);
    }

    SECTION("the in-domain keys still answer exactly") {
        auto range = index.find(components::types::logical_value_t(&resource, int64_t{20}), {});
        REQUIRE(range.first != range.second);
        REQUIRE(range.first->row_index == 1);
        REQUIRE(++range.first == range.second);
    }
}

// The domain check must not reject a conversion that IS faithful, or an index over a widened
// integer would silently lose rows. Round-tripping is what separates the two: INTEGER 7 -> BIGINT 7
// comes back as 7, whereas 'hello' -> 0 comes back as '0'.
TEST_CASE("single_field_index:lossless_widening_stays_indexed") {
    auto resource = core::pmr::otterbrix_resource();
    single_field_index_t index(&resource, "single_w", {key(&resource, "w")});

    index.insert(components::types::logical_value_t(&resource, int64_t{5}), int64_t{0}, {}); // locks BIGINT
    index.insert(components::types::logical_value_t(&resource, int32_t{7}), int64_t{1}, {});
    index.insert(components::types::logical_value_t(&resource, int16_t{9}), int64_t{2}, {});

    int count = 0;
    for (auto it = index.cbegin(); it != index.cend(); ++it) {
        ++count;
    }
    REQUIRE(count == 3);

    auto seven = index.find(components::types::logical_value_t(&resource, int64_t{7}), {});
    REQUIRE(seven.first != seven.second);
    REQUIRE(seven.first->row_index == 1);

    auto nine = index.find(components::types::logical_value_t(&resource, int64_t{9}), {});
    REQUIRE(nine.first != nine.second);
    REQUIRE(nine.first->row_index == 2);
}
