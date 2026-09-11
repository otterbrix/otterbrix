// The codec must refuse what it can't read, not guess a plausible type: unbounded LIST nesting
// SIGSEGVs, the shared depth window is MAX_SPEC_DEPTH=64 (the binary codec's cap), trailing
// garbage silently drops, and an unreadable keyword would decode as an UNKNOWN named after itself.

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
    // Unbounded, this recurses to a SIGSEGV at 2^20 levels; 65 is exactly past the shared window.
    REQUIRE(decode_refusal(nested_lists(1u << 20)) == core::error_code_t::data_corruption);
    REQUIRE(decode_refusal(nested_lists(65)) == core::error_code_t::data_corruption);
}

TEST_CASE("catalog::type_spec::the_window_boundary_itself_still_decodes") {
    auto decoded = decode_type_spec(g_resource, nested_lists(64));
    REQUIRE_FALSE(decoded.has_error());
    REQUIRE(decoded.value().type() == logical_type::LIST);
}

TEST_CASE("catalog::type_spec::trailing_garbage_is_not_a_clean_type") {
    REQUIRE(decode_refusal("numeric(10,2)garbage") == core::error_code_t::data_corruption);
}

// FROBNICATE(int4,7) has the exact shape of a valid UNKNOWN(name) reference, so unrefused
// corruption would travel on as a resolvable type name.
TEST_CASE("catalog::type_spec::an_unreadable_keyword_must_not_become_a_plausible_type") {
    REQUIRE(decode_refusal("FROBNICATE(int4,7)") == core::error_code_t::data_corruption);
}

// No production caller refuses an UNKNOWN column type on its own, so this refusal must live here.
TEST_CASE("catalog::type_spec::decimal_outside_the_window_is_a_refusal_not_unknown") {
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

// Three legitimate answers: an empty spec (atttypid reconstructs the builtin), an explicit
// UNKNOWN(name) the resolver chases, and a zero-entry ENUM (what the encoder writes for one).
TEST_CASE("catalog::type_spec::the_two_legitimate_unknown_answers_stay_answers") {
    auto empty = decode_type_spec(g_resource, "");
    REQUIRE_FALSE(empty.has_error());
    REQUIRE(empty.value().type() == logical_type::UNKNOWN);
    auto named = decode_type_spec(g_resource, "UNKNOWN(myudt)");
    REQUIRE_FALSE(named.has_error());
    REQUIRE(named.value().type() == logical_type::UNKNOWN);
    REQUIRE(named.value().type_name() == "myudt");
    auto empty_enum = decode_type_spec(g_resource, "ENUM:mood:");
    REQUIRE_FALSE(empty_enum.has_error());
    REQUIRE(empty_enum.value().type() == logical_type::ENUM);
}
