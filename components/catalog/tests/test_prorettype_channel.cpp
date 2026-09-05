// pg_proc.prorettype: what encode_prorettype writes is the function's declared
// return-type contract as it will be read back after restart. A `custom` resolver
// has no introspectable form — persisting it as "s:0" (same type as argument 0)
// writes a DIFFERENT contract than the one declared, silently.
//
// The answer is a TRUTHFUL TAG, not a refusal: registering a computed(...) output is
// pinned legal behaviour (integration test_udfs registers
// computed(same_type_resolver(0))), and the runtime resolver is reconstructed
// through pg_proc.prouid → compute::function_registry, never by parsing this
// column — so "c" states what the row actually carries.

#include <catch2/catch_test_macros.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/compute/kernel_signature.hpp>

using namespace components::catalog;
using components::compute::output_type;

TEST_CASE("catalog::prorettype::a_custom_resolver_is_tagged_not_downgraded") {
    auto resolver = [](std::pmr::memory_resource*, const std::pmr::vector<components::compute::fixed_t>& in)
        -> core::result_wrapper_t<components::compute::fixed_t> {
        return in.empty() ? components::compute::fixed_t{components::types::logical_type::BIGINT} : in.front();
    };
    std::vector<output_type> outs;
    outs.push_back(output_type::computed(resolver));

    const auto encoded = encode_prorettype(outs);
    // Not the identity contract the function never declared — the explicit marker.
    REQUIRE(encoded != "s:0");
    REQUIRE(encoded == "c");
}

TEST_CASE("catalog::prorettype::introspectable_outputs_still_encode") {
    std::vector<output_type> outs;
    outs.push_back(output_type::fixed(components::types::complex_logical_type{components::types::logical_type::BIGINT}));
    outs.push_back(output_type::same_type_at(1));

    const auto expected = std::string{"f:"} +
                          std::to_string(static_cast<int>(components::types::logical_type::BIGINT)) + ",s:1";
    REQUIRE(encode_prorettype(outs) == expected);
}

TEST_CASE("catalog::prorettype::a_mixed_list_keeps_every_position") {
    auto resolver = [](std::pmr::memory_resource*, const std::pmr::vector<components::compute::fixed_t>& in)
        -> core::result_wrapper_t<components::compute::fixed_t> {
        return in.empty() ? components::compute::fixed_t{components::types::logical_type::BIGINT} : in.front();
    };
    std::vector<output_type> outs;
    outs.push_back(output_type::same_type_at(0));
    outs.push_back(output_type::computed(resolver));

    // The custom entry must not collapse into its neighbour's syntax: positions
    // stay readable so a future decoder can tell which outputs it may trust.
    REQUIRE(encode_prorettype(outs) == "s:0,c");
}
