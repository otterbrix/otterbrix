#include "pushed_filter.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/execution_dag_builder.hpp>

namespace components::table {

    pushed_filter_t::pushed_filter_t(expressions::detached_expression_t&& expression,
                                     vector::storage_parameters&& parameters,
                                     const graph_execution_context& context)
        : expression(std::move(expression))
        , parameters(std::move(parameters))
        , timezone_offset(context.timezone_offset) {}

    std::unique_ptr<pushed_filter_t> make_pushed_filter(std::pmr::memory_resource* target,
                                                        const expressions::expression_ptr& expression,
                                                        const vector::storage_parameters& parameters,
                                                        const graph_execution_context& context) {
        return std::make_unique<pushed_filter_t>(expressions::detached_expression_t::detach(target, expression),
                                                 parameters.clone(target),
                                                 context);
    }

    core::result_wrapper_t<std::unique_ptr<table_filter_t>>
    build_table_filter(std::pmr::memory_resource* resource,
                       const pushed_filter_t& filter,
                       const std::pmr::vector<types::complex_logical_type>& types) {
        auto expression = filter.expression.attach(resource);
        const auto condition = expressions::classify_condition(expression);
        std::unique_ptr<execution_dag::execution_dag_t> graph;
        auto parameters = filter.parameters.clone(resource);
        if (condition == expressions::condition_kind::computed) {
            auto built = expressions::build_condition_graph(resource, parameters, expression.get(), types);
            if (built.has_error()) {
                return built.error();
            }
            graph = std::move(built.value());
        }
        const graph_execution_context context{filter.timezone_offset};
        return std::make_unique<table_filter_t>(std::move(parameters), context, std::move(graph), condition);
    }

} // namespace components::table
