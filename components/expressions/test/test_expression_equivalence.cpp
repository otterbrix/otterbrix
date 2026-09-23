#include <catch2/catch_test_macros.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression_equivalence.hpp>
#include <components/expressions/function_expression.hpp>

using namespace components::expressions;
using key = components::expressions::key_t;

namespace {
    function_expression_ptr call(std::pmr::memory_resource* resource,
                                 qualified_name_t written,
                                 components::compute::function_uid uid = components::compute::invalid_function_uid) {
        auto expr = make_function_expression(resource, std::move(written));
        expr->args().emplace_back(key(resource, "x"));
        expr->add_function_uid(uid);
        return expr;
    }
} // namespace

TEST_CASE("components::expression::same_computation::function_identity") {
    auto resource = core::pmr::otterbrix_resource();
    const components::types::parameter_map_t parameters(&resource);
    const auto same = [&](const expression_ptr& lhs, const expression_ptr& rhs) {
        return same_computation(lhs, rhs, parameters);
    };

    SECTION("unresolved calls compare by what was written") {
        CHECK(same(call(&resource, {"", "upper"}), call(&resource, {"", "upper"})));
        CHECK(same(call(&resource, {"ns", "f1"}), call(&resource, {"ns", "f1"})));
        CHECK_FALSE(same(call(&resource, {"ns", "f1"}), call(&resource, {"ns2", "f1"})));
        CHECK_FALSE(same(call(&resource, {"pg_catalog", "upper"}), call(&resource, {"", "upper"})));
    }

    SECTION("a slot beyond the database is part of the spelling too") {
        CHECK_FALSE(same(call(&resource, {"ns", "s", "f1"}), call(&resource, {"ns", "f1"})));
    }

    SECTION("resolved calls compare by uid") {
        CHECK(same(call(&resource, {"pg_catalog", "upper"}, 9), call(&resource, {"", "upper"}, 9)));
        CHECK_FALSE(same(call(&resource, {"ns", "f1"}, 20), call(&resource, {"ns2", "f1"}, 21)));
    }

    SECTION("an aggregate compares its call the same way") {
        const auto aggregate = [&](std::string dbname, components::compute::function_uid uid) {
            return expression_ptr{
                make_aggregate_over(call(&resource, {std::move(dbname), "count"}, uid), key(&resource))};
        };
        CHECK_FALSE(same(aggregate("db1", components::compute::invalid_function_uid),
                         aggregate("", components::compute::invalid_function_uid)));
        CHECK(same(aggregate("pg_catalog", 3), aggregate("", 3)));
    }
}
