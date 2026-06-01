#pragma once

#include "node.hpp"
#include "param_storage.hpp"

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

namespace components::logical_plan {

    using subquery_compacter = core::result_wrapper_t<types::logical_value_t> (*)(const vector::data_chunk_t& data);

    struct id_result_mapping {
        subquery_compacter compacter = nullptr;
        core::parameter_id_t id;
    };

    struct execution_plan_t {
        // default is null_memory_resource to make it non-usable, but also be able to send over actor-zeta
        explicit execution_plan_t(std::pmr::memory_resource* resource);
        explicit execution_plan_t(std::pmr::memory_resource* resource, node_ptr node, parameter_node_ptr params);
        // ordered collection of subqueries that can not be directly chained into one logical_plan
        // always has at least one entry
        // last one -> main query
        // executed front to back
        std::pmr::vector<node_ptr> sub_queries;

        // ordered collection of subquery results and their parameter id for mapping
        // always has 1 less entry than 'sub_queries'
        // maps 1:1 for every actual sub_query, e.g. sub_queries[1] -> sub_query_results[1]
        std::pmr::vector<id_result_mapping> sub_query_results;

        // hold various parameters for the whole execution_plan_t, including subquery mapping
        parameter_node_ptr parameters;
    };

} // namespace components::logical_plan