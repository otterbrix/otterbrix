#include "execution_plan.hpp"

namespace components::logical_plan {

    execution_plan_t::execution_plan_t(std::pmr::memory_resource* resource)
        : sub_queries(resource)
        , sub_query_results(resource)
        , parameters(make_parameter_node(resource))
        , if_exists_subcommands(resource) {}

    execution_plan_t::execution_plan_t(std::pmr::memory_resource* resource, node_ptr node, parameter_node_ptr params)
        : sub_queries({node}, resource)
        , sub_query_results(resource)
        , parameters(params)
        , if_exists_subcommands(resource) {}

} // namespace components::logical_plan