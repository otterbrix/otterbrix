#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/node_create_index.hpp>

// Index registry tests live in services/index/tests/test_index_registry.cpp. This file only
// covers pg_index.indtype's catalog-code round-trip (components/logical_plan).

// Every writable index_type round-trips through its single-char catalog code; an unknown code
// decodes to no_valid, which manager_disk_bootstrap.cpp treats as catalog corruption and refuses
// to start rather than guess a backend. no_valid itself has no writable code (encoder returns 0).
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
