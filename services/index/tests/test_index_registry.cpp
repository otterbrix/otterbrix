// Pure lookups over manager_index_t's per-oid record vector -- no actor, no store; rows and
// search stay with the storage agent.

#include <catch2/catch_test_macros.hpp>

#include <components/expressions/key.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <core/pmr.hpp>
#include <services/index/manager_index.hpp>

using components::logical_plan::index_type;
using services::index::index_record_t;
using services::index::index_records_t;
using services::index::indexed_descriptions;
using services::index::indexed_keys;
using services::index::match_index;
using services::index::match_index_relid;

namespace {

    components::index::keys_base_storage_t one_key(std::pmr::memory_resource* resource, const char* name) {
        components::index::keys_base_storage_t keys(resource);
        keys.emplace_back(components::expressions::key_t{resource, name});
        return keys;
    }

    // The address is never dereferenced: every decision here is taken off the record's own fields.
    index_record_t make_record(components::catalog::oid_t index_oid,
                               components::index::keys_base_storage_t keys,
                               index_type type) {
        return index_record_t{index_oid,
                              std::move(keys),
                              type,
                              type != index_type::hashed,
                              actor_zeta::address_t::empty_address()};
    }

} // namespace

// A key-keyed record map (one slot per key set) would break every observable here: the second
// registration couldn't claim its own slot, and dropping either index would erase both.
TEST_CASE("services::index::dropping_one_index_over_a_key_keeps_its_twin") {
    auto resource = core::pmr::otterbrix_resource();
    index_records_t records(&resource);

    const auto k = one_key(&resource, "k");
    records.push_back(make_record(201u, one_key(&resource, "k"), index_type::single));
    records.push_back(make_record(202u, one_key(&resource, "k"), index_type::hashed));
    REQUIRE(records.size() == 2);

    REQUIRE(match_index_relid(records, 201u) != nullptr);
    REQUIRE(match_index_relid(records, 202u) != nullptr);
    REQUIRE(match_index_relid(records, 201u) != match_index_relid(records, 202u));

    records.erase(records.begin() + 1);

    INFO("the ordered index is still registered, so it still counts");
    CHECK(records.size() == 1);
    CHECK(match_index_relid(records, 201u) != nullptr);
    CHECK(match_index_relid(records, 202u) == nullptr);

    INFO("a table holding an index must not report as having no indexed keys");
    auto keys = indexed_keys(records, &resource);
    REQUIRE(keys.size() == 1);
    CHECK(keys[0] == k);

    CHECK(match_index(records, k, index_type::single) != nullptr);
    CHECK(match_index(records, k, index_type::hashed) == nullptr);
    CHECK(match_index(records, k) == match_index_relid(records, 201u));

    auto descriptions = indexed_descriptions(records, &resource);
    REQUIRE(descriptions.size() == 1);
    CHECK(descriptions[0].type == index_type::single);
}

// indexed_keys() is a SET of key sets: two indexes on the same column count once here, and
// indexed_descriptions() is what still tells them apart by backend type.
TEST_CASE("services::index::indexed_keys_are_a_set_not_a_bag") {
    auto resource = core::pmr::otterbrix_resource();
    index_records_t records(&resource);

    const auto k = one_key(&resource, "k");
    const auto j = one_key(&resource, "j");
    records.push_back(make_record(301u, one_key(&resource, "k"), index_type::single));
    records.push_back(make_record(302u, one_key(&resource, "k"), index_type::hashed));
    records.push_back(make_record(303u, one_key(&resource, "j"), index_type::single));

    CHECK(records.size() == 3);
    auto keys = indexed_keys(records, &resource);
    REQUIRE(keys.size() == 2);
    CHECK(keys[0] == k);
    CHECK(keys[1] == j);
    CHECK(indexed_descriptions(records, &resource).size() == 3);
}

// The untyped lookup must prefer the ordered index regardless of registration order, since only
// an ordered index answers range predicates (an unordered one errors on them).
TEST_CASE("services::index::untyped_lookup_prefers_the_ordered_index") {
    auto resource = core::pmr::otterbrix_resource();
    index_records_t records(&resource);

    const auto k = one_key(&resource, "k");
    records.push_back(make_record(401u, one_key(&resource, "k"), index_type::hashed));
    records.push_back(make_record(402u, one_key(&resource, "k"), index_type::single));

    const auto* hashed = match_index_relid(records, 401u);
    const auto* ordered = match_index_relid(records, 402u);
    REQUIRE(hashed != nullptr);
    REQUIRE(ordered != nullptr);
    REQUIRE_FALSE(hashed->ordered);
    REQUIRE(ordered->ordered);

    CHECK(match_index(records, k) == ordered);

    records.erase(records.begin() + 1);
    CHECK(match_index(records, k) == match_index_relid(records, 401u));
    CHECK(records.size() == 1);
    CHECK(indexed_keys(records, &resource).size() == 1);
}

// no_valid must match nothing, or search_with_preferred_type's typed-then-untyped fallback
// becomes unreachable dead code.
TEST_CASE("services::index::no_valid_names_no_backend") {
    auto resource = core::pmr::otterbrix_resource();
    index_records_t records(&resource);

    const auto k = one_key(&resource, "k");
    records.push_back(make_record(501u, one_key(&resource, "k"), index_type::single));

    CHECK(match_index(records, k, index_type::no_valid) == nullptr);
    CHECK(match_index(records, k) != nullptr);
}
