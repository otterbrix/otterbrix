#include "execution_plan.hpp"

namespace components::logical_plan {

    execution_plan_t::execution_plan_t(std::pmr::memory_resource* resource)
        : sub_queries(resource)
        , sub_query_results(resource)
        , parameters(make_parameter_node(resource))
        , external_parameters(resource) {}

    execution_plan_t::execution_plan_t(std::pmr::memory_resource* resource, node_ptr node, parameter_node_ptr params)
        : sub_queries({node}, resource)
        , sub_query_results(resource)
        , parameters(params)
        , external_parameters(resource) {}

} // namespace components::logical_plan