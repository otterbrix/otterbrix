#include <catch2/catch.hpp>
#include <set>
#include <string>
#include <components/compute/function.hpp>

using namespace components::compute;

TEST_CASE("components::compute::registry::basic") {
    std::pmr::synchronized_pool_resource resource;
    auto* reg = function_registry_t::get_default();
    REQUIRE(reg != nullptr);
    auto registered_functions = reg->get_functions();

    SECTION("singleton") {
        auto* reg2 = function_registry_t::get_default();
        REQUIRE(reg == reg2);
    }

    SECTION("all function names present") { REQUIRE(registered_functions.size() >= 9); }

    SECTION("builtin functions exist") {
        const std::set<std::string> binary_distance_funcs = {"l2_distance",
                                                             "cosine_distance",
                                                             "inner_product",
                                                             "negative_inner_product"};
        for (const auto& [name, uid] : registered_functions) {
            auto* fn = reg->get_function(uid);
            REQUIRE(fn != nullptr);
            REQUIRE(fn->name() == name);
            if (name == "count") {
                REQUIRE(fn->fn_arity().num_args == 0);
                REQUIRE(fn->fn_arity().varargs == true);
            } else if (binary_distance_funcs.count(name) != 0) {
                REQUIRE(fn->fn_arity().num_args == 2);
            } else if (name == "substring") {
                // SUBSTRING(s, start[, len]) — 2 or 3 args
                REQUIRE(fn->fn_arity().num_args == 2);
                REQUIRE(fn->fn_arity().varargs == true);
            } else if (name == "regexp_replace") {
                REQUIRE(fn->fn_arity().num_args == 3);
            } else {
                // sum, min, max, avg, length
                REQUIRE(fn->fn_arity().num_args == 1);
            }
        }
    }

    SECTION("non-existent function") { REQUIRE(reg->get_function(invalid_function_uid) == nullptr); }
}