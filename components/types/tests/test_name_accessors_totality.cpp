// alias()/type_name()/is_unnamed() must be total: an extension-less complex_logical_type is
// legitimate (default ctor, catalog builders before naming, decode_type_spec("") readers), not
// a broken object. The old assert(extension_)/no-check versions crashed `SELECT * FROM
// pg_class` on this read path: the refusal must be loud, not fatal.

#include <catch2/catch_test_macros.hpp>

#include <components/types/types.hpp>

using namespace components::types;

TEST_CASE("types::name_accessors::a_nameless_type_answers_instead_of_aborting") {
    // Same shape as system_table_schemas.cpp's oid_col(): a bare scalar, no extension.
    const complex_logical_type bare{logical_type::UINTEGER};
    REQUIRE_FALSE(bare.has_alias());
    REQUIRE(bare.alias().empty());
    REQUIRE(bare.is_unnamed());
    REQUIRE(bare.type_name().empty());
}

TEST_CASE("types::name_accessors::a_bare_UNKNOWN_can_be_asked_for_its_name") {
    // What catalog::decode_type_spec("") and oid_to_builtin_type() hand a reader back.
    const complex_logical_type bare_unknown{logical_type::UNKNOWN};
    REQUIRE(bare_unknown.type_name().empty());
    REQUIRE(bare_unknown.alias().empty());
    REQUIRE(bare_unknown.is_unnamed());
}

TEST_CASE("types::name_accessors::a_default_constructed_type_is_nameless_not_undefined") {
    const complex_logical_type na{};
    REQUIRE(na.type() == logical_type::NA);
    REQUIRE(na.alias().empty());
    REQUIRE(na.is_unnamed());
    REQUIRE(na.type_name().empty());
}

TEST_CASE("types::name_accessors::a_STRUCT_without_its_extension_does_not_dereference_it") {
    // A STRUCT-tagged type that never went through create_struct has no struct extension.
    const complex_logical_type bare_struct{logical_type::STRUCT};
    REQUIRE(bare_struct.child_name(0).empty());
    REQUIRE(bare_struct.child_name(7).empty());
}

TEST_CASE("types::name_accessors::totality_does_not_swallow_a_real_name") {
    // Totality must not swallow a real name.
    complex_logical_type named{logical_type::UINTEGER};
    named.set_alias("oid");
    REQUIRE(named.has_alias());
    REQUIRE(named.alias() == "oid");
    REQUIRE_FALSE(named.is_unnamed());

    const auto udt = complex_logical_type::create_unknown("myudt");
    REQUIRE(udt.type_name() == "myudt");

    std::pmr::vector<complex_logical_type> fields{std::pmr::new_delete_resource()};
    fields.emplace_back(logical_type::INTEGER, "a");
    fields.emplace_back(logical_type::BIGINT, "b");
    const auto st = complex_logical_type::create_struct("point", fields);
    REQUIRE(st.type_name() == "point");
    REQUIRE(st.child_name(0) == "a");
    REQUIRE(st.child_name(1) == "b");
}
