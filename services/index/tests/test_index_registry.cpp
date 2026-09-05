// THE ROUTING DECISIONS manager_index_t makes over its per-oid record map.
//
// An index's rows, its search and its per-transaction buffer are all the storage agent's, so what
// the manager keeps is a record per index (indexrelid, key set, backend, ordering, address); the
// lookups below are the whole of what it decides with them. They are driven directly, with no actor
// and no store: match_index / indexed_keys / indexed_descriptions are pure functions over the
// record vector.

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

    // A record with no live agent behind it: every decision under test is taken off the
    // record's own fields, and the address is never dereferenced.
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

// TWO INDEXES OVER ONE COLUMN, and dropping one of them.
//
// The pair is legal and reachable from SQL: create_index rejects a duplicate only on the PAIR
// (keys, type), so `CREATE INDEX i ON t (k)` and `CREATE INDEX j ON t USING hash (k)` both
// register. Dropping ONE of them must leave the OTHER visible in every observable the registry
// publishes, because production reads all of them and each decides something different:
//
//   * the record COUNT   -- the compact gate (manager_index_t::tables_without_indexes) and the
//                          repopulate driver (all_indexed_oids). Reading 0 over a live index
//                          lets a compact renumber rows underneath it AND skips the rebuild
//                          that would have repaired them;
//   * indexed_keys()     -- what the planner sees (context_storage_t::has_index_on) and what
//                          stamps DML index mirroring (stamp_table_has_indexes). Reading empty
//                          makes the surviving index invisible to reads and unfed by writes at
//                          the same time;
//   * match_index(keys)  -- the untyped read-path lookup.
//
// A key-keyed map holding ONE slot per key set breaks all three: the second registration cannot
// write its slot, and the first drop erases it on the way out.
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

    // DROP INDEX trims the ONE record out of the table's vector (manager_index_t's
    // detach_index) and leaves the table registered.
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

// indexed_keys() publishes a SET of key sets, not a bag. Two indexes over one column are
// two indexes (the record count says so) but ONE indexed key set, and the multiplicity
// that distinguishes them — the backend type — is what indexed_descriptions() is for.
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

// The untyped lookup's priority is DECLARED, not inherited from registration order: the
// hashed index is registered FIRST here and the ordered one must still be the answer.
// An ordered index answers all six comparison predicates; an unordered one answers eq and
// nothing else, and manager_index_t turns a range on it into an error — so handing back
// the hashed twin would fail a probe the neighbouring index could have answered.
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

    // With the ordered one gone the hashed one is the only answer left — priority, not
    // exclusion.
    records.erase(records.begin() + 1);
    CHECK(match_index(records, k) == match_index_relid(records, 401u));
    CHECK(records.size() == 1);
    CHECK(indexed_keys(records, &resource).size() == 1);
}

// index_type::no_valid is what the planner sends when it named NO backend, and it must
// match nothing: search_with_preferred_type tries the typed lookup first and falls to the
// untyped one only when it comes back empty. A no_valid that matched anything would make
// the fallback unreachable and the ordered-first priority above dead code.
TEST_CASE("services::index::no_valid_names_no_backend") {
    auto resource = core::pmr::otterbrix_resource();
    index_records_t records(&resource);

    const auto k = one_key(&resource, "k");
    records.push_back(make_record(501u, one_key(&resource, "k"), index_type::single));

    CHECK(match_index(records, k, index_type::no_valid) == nullptr);
    CHECK(match_index(records, k) != nullptr);
}
