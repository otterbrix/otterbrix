// The flat-text type codec (pg_attribute.atttypspec / pg_type.typdefspec) refuses
// what it cannot read, through core::result_wrapper_t — never by shrugging the
// input into a plausible type.
//
// RED (proven pre-fix, 2026-09-02):
//   * 2^20 nested LIST(...)  → SIGSEGV: the parser had NO depth limit at all while
//     the binary codec next door (components/types/type_spec_codec.cpp) refuses
//     beyond MAX_SPEC_DEPTH = 64 on both encode and decode;
//   * "numeric(10,2)garbage" → decoded to a CLEAN DECIMAL(10,2), trailing bytes
//     silently dropped;
//   * "FROBNICATE(int4,7)"   → the argument list was consumed to the matching ')'
//     and the answer was UNKNOWN named "FROBNICATE" — indistinguishable from a
//     legitimate named user-type reference (UNKNOWN(name));
//   * everything else unreadable — malformed or out-of-window DECIMAL, broken ENUM
//     entries, missing separators — collapsed into UNKNOWN with a catch(...) arm
//     swallowing even bad_alloc.

#include <catch2/catch_test_macros.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/types/types.hpp>

#include <string>

using namespace components::catalog;
using namespace components::types;

namespace {
    auto* g_resource = std::pmr::new_delete_resource();

    core::error_code_t decode_refusal(std::string_view spec) {
        auto decoded = decode_type_spec(g_resource, spec);
        REQUIRE(decoded.has_error());
        return decoded.error().type;
    }

    std::string nested_lists(std::size_t levels) {
        std::string spec;
        spec.reserve(levels * 6 + 8);
        for (std::size_t i = 0; i < levels; ++i) {
            spec += "LIST(";
        }
        spec += "int4";
        spec.append(levels, ')');
        return spec;
    }
} // namespace

TEST_CASE("catalog::type_spec::a_spec_deeper_than_the_shared_window_is_refused_not_a_stack_overflow") {
    // Pre-fix this input was an unbounded recursion (SIGSEGV at 2^20 levels).
    REQUIRE(decode_refusal(nested_lists(1u << 20)) == core::error_code_t::data_corruption);
    // The refusal starts exactly past the window shared with the binary codec.
    REQUIRE(decode_refusal(nested_lists(65)) == core::error_code_t::data_corruption);
}

TEST_CASE("catalog::type_spec::the_window_boundary_itself_still_decodes") {
    auto decoded = decode_type_spec(g_resource, nested_lists(64));
    REQUIRE_FALSE(decoded.has_error());
    REQUIRE(decoded.value().type() == logical_type::LIST);
}

TEST_CASE("catalog::type_spec::trailing_garbage_is_not_a_clean_type") {
    // Pre-fix: decoded to DECIMAL(10,2), the garbage silently dropped.
    REQUIRE(decode_refusal("numeric(10,2)garbage") == core::error_code_t::data_corruption);
}

TEST_CASE("catalog::type_spec::an_unreadable_keyword_must_not_become_a_plausible_type") {
    // Pre-fix: answered UNKNOWN named "FROBNICATE" — the exact shape of a valid
    // named user-type reference, so corruption travelled on as a resolvable name.
    REQUIRE(decode_refusal("FROBNICATE(int4,7)") == core::error_code_t::data_corruption);
}

TEST_CASE("catalog::type_spec::decimal_outside_the_window_is_a_refusal_not_unknown") {
    // The comment used to claim "callers that care refuse an UNKNOWN column type
    // loudly" — no production caller ever did. The refusal lives HERE now.
    REQUIRE(decode_refusal("numeric(300,5)") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("numeric(10,)") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("numeric(1e2,0)") == core::error_code_t::data_corruption);
}

TEST_CASE("catalog::type_spec::broken_enum_entries_are_refused") {
    REQUIRE(decode_refusal("ENUM:color:red=zz") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("ENUM:color:red") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("ENUM:color") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("ENUM:color:red=1,") == core::error_code_t::data_corruption);
}

TEST_CASE("catalog::type_spec::missing_separators_are_refused") {
    REQUIRE(decode_refusal("LIST(int4") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("ARRAY(int4)") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("ARRAY(int4,12x)") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("MAP(int4)") == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal("STRUCT(point,x:int4") == core::error_code_t::data_corruption);
}

TEST_CASE("catalog::type_spec::the_two_legitimate_unknown_answers_stay_answers") {
    // Empty spec: a builtin scalar stored without one; atttypid reconstructs it.
    auto empty = decode_type_spec(g_resource, "");
    REQUIRE_FALSE(empty.has_error());
    REQUIRE(empty.value().type() == logical_type::UNKNOWN);
    // Explicit UNKNOWN(name): a named user-type reference the resolver chases.
    auto named = decode_type_spec(g_resource, "UNKNOWN(myudt)");
    REQUIRE_FALSE(named.has_error());
    REQUIRE(named.value().type() == logical_type::UNKNOWN);
    REQUIRE(named.value().type_name() == "myudt");
    // A zero-entry ENUM is what the encoder writes for one: still legal.
    auto empty_enum = decode_type_spec(g_resource, "ENUM:mood:");
    REQUIRE_FALSE(empty_enum.has_error());
    REQUIRE(empty_enum.value().type() == logical_type::ENUM);
}
