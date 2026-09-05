#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/node_create_index.hpp>

// The index_t hierarchy and index_engine_t -- the per-table registry that owned a list of index objects
// -- are gone: an index's rows and its search were always the storage agent's, its per-transaction buffer
// sits beside them, and the ROUTING the registry did is one per-oid record map inside manager_index_t.
// The registry cases live in services/index/tests/test_index_registry.cpp, retargeted onto that map's
// lookups, and lookup by a positional id went with the id itself (an index is named by its
// pg_index.indexrelid, rule 16).
//
// What stays here was never about the engine: pg_index.indtype is a CATALOG encoding, read back at
// bootstrap to pick a storage family, and it belongs to components/logical_plan.

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
