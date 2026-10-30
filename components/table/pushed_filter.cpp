#include "pushed_filter.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/execution_dag_builder.hpp>

namespace components::table {

    core::result_wrapper_t<std::unique_ptr<table_filter_t>>
    build_table_filter(std::pmr::memory_resource* resource,
                       const expressions::expression_ptr& expression,
                       const vector::storage_parameters& parameters,
                       const graph_execution_context& context,
                       const std::pmr::vector<types::complex_logical_type>& types) {
        const auto condition = expressions::classify_condition(expression);
        std::unique_ptr<execution_dag::execution_dag_t> graph;
        auto owned = parameters.clone(resource);
        if (condition == expressions::condition_kind::computed) {
            auto built = expressions::build_condition_graph(resource, owned, expression.get(), types);
            if (built.has_error()) {
                return built.error();
            }
            graph = std::move(built.value());
        }
        return std::make_unique<table_filter_t>(std::move(owned), context, std::move(graph), condition);
    }

} // namespace components::table
