#include <catch2/catch_test_macros.hpp>

#include <components/casts/default_casts.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>

#include <memory_resource>
#include <string>

using namespace components;

namespace {

    struct registries_t {
        casts::cast_registry_t cast_registry{std::pmr::new_delete_resource()};
        compute::function_registry_t function_registry{std::pmr::new_delete_resource()};

        registries_t() {
            casts::register_default_casts(cast_registry);
            compute::register_default_functions(function_registry);
        }
    };

    const registries_t& registries() {
        static const registries_t loaded;
        return loaded;
    }

    logical_plan::node_ptr validated(std::pmr::memory_resource* resource, const char* sql) {
        std::pmr::monotonic_buffer_resource arena(resource);
        auto* raw = raw_parser(&arena, sql);
        REQUIRE(raw != nullptr);
        sql::transform::transformer transformer(resource);
        auto binder =
            transformer.transform(sql::transform::pg_cell_to_node_cast(reinterpret_cast<::Node*>(linitial(raw))));
        REQUIRE_FALSE(binder.has_error());
        auto finalized = binder.finalize();
        REQUIRE_FALSE(finalized.has_error());
        auto plan = std::move(finalized.value());
        auto root = plan.sub_queries.back();
        REQUIRE(root->type() == logical_plan::node_type::aggregate_t);

        std::pmr::vector<types::complex_logical_type> columns(resource);
        for (const auto* name : {"a", "b"}) {
            columns.emplace_back(types::logical_type::BIGINT);
            columns.back().set_alias(name);
        }
        vector::data_chunk_t chunk(resource, columns, 2);
        for (size_t row = 0; row < 2; ++row) {
            chunk.set_value(0, row, static_cast<int64_t>(row));
            chunk.set_value(1, row, static_cast<int64_t>(10 - row));
        }
        chunk.set_cardinality(2);
        root->append_child(logical_plan::make_node_raw_data(resource, std::move(chunk)));

        static const graph_execution_context execution_context{};
        const services::dispatcher::validation::validation_context_t context{resource,
                                                                             nullptr,
                                                                             registries().cast_registry,
                                                                             registries().function_registry,
                                                                             execution_context};
        auto schema = services::dispatcher::validate_schema(context, root.get(), plan.parameters->parameters());
        INFO("error: " << (schema.has_error() ? schema.error().what : "none"));
        REQUIRE_FALSE(schema.has_error());
        return root;
    }

    const logical_plan::node_sort_t* sort_of(const logical_plan::node_ptr& root) {
        for (const auto& child : root->children()) {
            if (child->type() == logical_plan::node_type::sort_t) {
                return static_cast<const logical_plan::node_sort_t*>(child.get());
            }
        }
        return nullptr;
    }

} // namespace

TEST_CASE("services::dispatcher::validate_order_keys::a_constant_key_leaves_the_sort") {
    std::pmr::synchronized_pool_resource resource;

    SECTION("next to a column only the column is left") {
        auto root = validated(&resource, "SELECT a FROM t ORDER BY 1 + 1, b;");
        const auto* sort = sort_of(root);
        REQUIRE(sort != nullptr);
        REQUIRE(sort->expressions().size() == 1);
        const auto* key = static_cast<const expressions::sort_expression_t*>(sort->expressions().front().get());
        REQUIRE(expressions::is_key(key->operand()));
        const auto& path = expressions::as_key(key->operand()).path();
        REQUIRE(path.size() == 1);
        CHECK(path.front() == 1);
    }
    SECTION("alone it takes the sort with it") {
        auto root = validated(&resource, "SELECT a FROM t ORDER BY 1 + 1;");
        CHECK(sort_of(root) == nullptr);
    }
    SECTION("guard: a column key stays") {
        auto root = validated(&resource, "SELECT a FROM t ORDER BY b;");
        const auto* sort = sort_of(root);
        REQUIRE(sort != nullptr);
        CHECK(sort->expressions().size() == 1);
    }
}
