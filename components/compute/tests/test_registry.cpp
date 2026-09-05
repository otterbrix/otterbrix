#include <catch2/catch_test_macros.hpp>
#include <components/compute/function.hpp>

using namespace components::compute;

TEST_CASE("components::compute::registry::basic") {
    core::pmr::otterbrix_resource resource;
    auto* reg = function_registry_t::get_default();
    REQUIRE(reg != nullptr);
    auto registered_functions = reg->get_functions();

    SECTION("singleton") {
        auto* reg2 = function_registry_t::get_default();
        REQUIRE(reg == reg2);
    }

    SECTION("all function names present") { REQUIRE(registered_functions.size() >= 5); }

    SECTION("aggregate functions exist") {
        for (const auto& [name, uid] : registered_functions) {
            auto* fn = reg->get_function(uid);
            REQUIRE(fn != nullptr);
            REQUIRE(fn->name() == name);
            if (name == "count") {
                REQUIRE(fn->fn_arity().num_args == 0);
                REQUIRE(fn->fn_arity().varargs == true);
            } else if (name == "substring") {
                // SUBSTRING(s, start[, len]) — 2 or 3 args
                REQUIRE(fn->fn_arity().num_args == 2);
                REQUIRE(fn->fn_arity().varargs == true);
            } else if (name == "regexp_replace") {
                REQUIRE(fn->fn_arity().num_args == 3);
            } else if (name == "regexp_like") {
                // regexp_like(subject, pattern[, flags]) — 2 or 3 args
                REQUIRE(fn->fn_arity().num_args == 2);
                REQUIRE(fn->fn_arity().varargs == true);
            } else if (name == "generate_series") {
                // generate_series(start, stop[, step]) — 2 or 3 args
                REQUIRE(fn->fn_arity().num_args == 2);
                REQUIRE(fn->fn_arity().varargs == true);
            } else if (name == "pow") {
                // pow(base, exponent)
                REQUIRE(fn->fn_arity().num_args == 2);
            } else {
                // sum, min, max, avg, length, abs, sqrt, cbrt, factorial
                REQUIRE(fn->fn_arity().num_args == 1);
            }
        }
    }

    SECTION("non-existent function") { REQUIRE(reg->get_function(invalid_function_uid) == nullptr); }
}
// add_function() built the refusal for a null payload but never returned it, so the null landed
// in functions_ and the caller was handed a uid. Two independent claims — what the call REPORTS
// and what the registry HOLDS — so two cases, each able to go red on its own.
TEST_CASE("components::compute::registry::add_function_refuses_a_null_payload") {
    core::pmr::otterbrix_resource resource;
    function_registry_t registry(&resource);

    auto added = registry.add_function(nullptr);
    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::function_registry_error);
}

TEST_CASE("components::compute::registry::a_refused_null_payload_never_enters_the_registry") {
    core::pmr::otterbrix_resource resource;
    function_registry_t registry(&resource);

    auto added = registry.add_function(nullptr);
    INFO("add_function reported " << (added.has_error() ? "a refusal" : "a uid"));

    // A fresh registry hands out uid 0 first, so that is where a wrongly-accepted null lands.
    // Probed through remove_function() and not through get_functions(): get_functions()
    // dereferences every stored pointer, so on the broken build it takes the whole test binary
    // down instead of failing this assertion.
    REQUIRE_FALSE(registry.remove_function(0));
    REQUIRE(registry.get_functions().empty());
}
