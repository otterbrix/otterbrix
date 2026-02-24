#include <catch2/catch.hpp>
#include <components/compute/function.hpp>

using namespace components::compute;

TEST_CASE("components::compute::registry::basic") {
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
            REQUIRE(fn->fn_arity().num_args == 1);
        }
    }

    SECTION("non-existent function") { REQUIRE(reg->get_function(invalid_function_uid) == nullptr); }
}