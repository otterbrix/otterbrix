// unregister_udf of a function no process holds matches the inputs against its pg_proc row, so the text the encoder
// writes has to come back as the same matchers.

#include <catch2/catch_test_macros.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/compute/kernel_signature.hpp>
#include <core/pmr.hpp>

using namespace components::catalog;
using components::compute::parameter_type;
using components::types::complex_logical_type;
using components::types::logical_type;

TEST_CASE("catalog::proargmatchers::exact_and_variable_matchers_round_trip") {
    core::pmr::otterbrix_resource resource;
    std::vector<parameter_type> parameters;
    parameters.push_back(parameter_type::exact(complex_logical_type{logical_type::BIGINT}));
    parameters.push_back(parameter_type::variable(
        0,
        std::pmr::vector<complex_logical_type>{{complex_logical_type{logical_type::INTEGER},
                                                complex_logical_type{logical_type::DOUBLE}},
                                               &resource}));
    parameters.push_back(parameter_type::variable(1));
    const auto encoded = encode_proargmatchers(parameters);

    auto decoded = decode_proargmatchers(&resource, encoded);
    REQUIRE_FALSE(decoded.has_error());
    REQUIRE(encode_proargmatchers(decoded.value()) == encoded);

    const components::compute::kernel_signature_t stored(components::compute::function_type_t::vector,
                                                         std::move(decoded.value()),
                                                         std::pmr::vector<components::compute::output_type>{&resource});
    CHECK(stored.matches_inputs({{logical_type::BIGINT, logical_type::DOUBLE, logical_type::STRING_LITERAL}, &resource}));
    CHECK_FALSE(stored.matches_inputs({{logical_type::BIGINT, logical_type::BIGINT, logical_type::BIGINT}, &resource}));
}

TEST_CASE("catalog::proargmatchers::no_arguments_is_the_empty_text") {
    core::pmr::otterbrix_resource resource;
    auto decoded = decode_proargmatchers(&resource, "");
    REQUIRE_FALSE(decoded.has_error());
    CHECK(decoded.value().empty());
}

TEST_CASE("catalog::proargmatchers::text_outside_the_grammar_is_refused") {
    core::pmr::otterbrix_resource resource;
    for (const char* text : {"x:1", "e:", "e:1|", "v:a", "v:0:", "e:1;e:2"}) {
        INFO(text);
        auto decoded = decode_proargmatchers(&resource, text);
        REQUIRE(decoded.has_error());
        CHECK(decoded.error().type == core::error_code_t::data_corruption);
    }
}
