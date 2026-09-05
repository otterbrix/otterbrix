#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/node_create_index.hpp>

// WHAT USED TO BE IN THIS FILE, and where it went.
//
// It carried a hand-written index_t subclass (`dummy`) and four cases over
// index_engine_t -- the per-table registry that owned a list of index objects. Both types
// are gone: an index's rows and its search were always the storage agent's, its
// per-transaction buffer moved down to sit beside them, and the ROUTING the registry did
// is now one per-oid record map inside manager_index_t.
//
//   * `base_index_created` (registration + lookup by the engine's positional id) is
//     deleted with the id itself: an index is named by its pg_index.indexrelid (rule 16),
//     never by a position in a list.
//   * `dropping_one_index_over_a_key_keeps_its_twin`,
//     `indexed_keys_are_a_set_not_a_bag` and `untyped_lookup_prefers_the_ordered_index`
//     kept their subjects and moved to services/index/tests/test_index_registry.cpp,
//     retargeted onto the record map's lookups.
//
// What stays here is the one case that was never about the engine: pg_index.indtype is a
// CATALOG encoding, read back at bootstrap to pick a storage family, and it belongs to
// components/logical_plan.

// pg_index.indtype code alphabet: every writable index_type round-trips through its
// single-char catalog code; an unknown code decodes to no_valid (the bootstrap reader
// treats that as catalog corruption and fails LOUDLY — error log + abort — instead of
// guessing a backend); no_valid itself has no writable code (the encoder returns 0).
TEST_CASE("components::index::indtype_code_roundtrip") {
    using components::logical_plan::index_type;
    using components::logical_plan::index_type_from_indtype_code;
    using components::logical_plan::index_type_to_indtype_code;
    for (auto t : {index_type::single,
                   index_type::composite,
                   index_type::multikey,
                   index_type::hashed,
                   index_type::wildcard}) {
        const char code = index_type_to_indtype_code(t);
        REQUIRE(code != 0);
        REQUIRE(index_type_from_indtype_code(code) == t);
    }
    REQUIRE(index_type_from_indtype_code('x') == index_type::no_valid);
    REQUIRE(index_type_from_indtype_code('\0') == index_type::no_valid);
    REQUIRE(index_type_to_indtype_code(index_type::no_valid) == 0);
}
