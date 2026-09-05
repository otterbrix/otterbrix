// THE THREE NAME ACCESSORS OF complex_logical_type MUST BE TOTAL FUNCTIONS.
//
// An extension-less complex_logical_type is not a broken object: it is what the DEFAULT
// constructor builds, what `complex_logical_type{logical_type::UINTEGER}` builds, what
// components/catalog/system_table_schemas.cpp builds for every system-table column before
// column_definition_t names it, and what catalog::decode_type_spec("") /
// catalog::oid_to_builtin_type() hand a reader back. `has_alias()` already answers "no
// name" for it without complaining.
//
// alias(), type_name() and is_unnamed() used to claim that state impossible:
//   * alias()      assert(extension_)  -> Debug abort, and under NDEBUG a null
//                                         unique_ptr dereference instead;
//   * type_name()  assert(extension_)  -> the same pair;
//   * is_unnamed() no check at all     -> a null dereference in EVERY build.
// That is fatal on a READ path — `SELECT * FROM pg_class` walked into alias() and killed
// the process (queue idx 417), and the flat catalog type codec walked into type_name()
// on a bare UNKNOWN while the write gate next door had just declared that same type
// persistable (queue idx 408). Rule 6: a refusal must be LOUD, not FATAL — an abort on a
// read path leaves a database nobody can open.
//
// The honest answer for a nameless type is the empty name, which is exactly what the ~15
// production sites already spell by hand as `has_alias() ? alias() : std::string{}`.

#include <catch2/catch_test_macros.hpp>

#include <components/types/types.hpp>

using namespace components::types;

TEST_CASE("types::name_accessors::a_nameless_type_answers_instead_of_aborting") {
    // Built exactly as system_table_schemas.cpp's oid_col() builds the pg_class "oid"
    // column: a bare scalar, no extension, no alias.
    const complex_logical_type bare{logical_type::UINTEGER};
    REQUIRE_FALSE(bare.has_alias());
    REQUIRE(bare.alias().empty());
    REQUIRE(bare.is_unnamed());
    REQUIRE(bare.type_name().empty());
}

TEST_CASE("types::name_accessors::a_bare_UNKNOWN_can_be_asked_for_its_name") {
    // catalog::decode_type_spec("") returns precisely this value ("a builtin scalar
    // stored without a spec"), and so does oid_to_builtin_type() for any non-builtin oid.
    // The binary type-spec codec persists it happily (has_type_name = 0), so every
    // consumer downstream of the write gate is entitled to ask it for a name.
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
    // child_name() static_cast-ed the extension pointer with no check at all; on a
    // STRUCT-tagged type that never went through create_struct that is a null
    // dereference, in Debug and Release alike.
    const complex_logical_type bare_struct{logical_type::STRUCT};
    REQUIRE(bare_struct.child_name(0).empty());
    REQUIRE(bare_struct.child_name(7).empty());
}

TEST_CASE("types::name_accessors::totality_does_not_swallow_a_real_name") {
    // Sensitivity in the other direction: making the accessors total must not turn a
    // named type into a nameless one.
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
