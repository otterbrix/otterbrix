#pragma once

#include <cstdint>

#include "node.hpp"
#include "param_storage.hpp"

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

namespace components::logical_plan {

    // Compacts a sub-query result (ALL its cursor chunks) into a bound parameter value. Spanning every
    // chunk — not just the first — is what keeps an IN-list / scalar sub-query from being truncated at
    // DEFAULT_VECTOR_CAPACITY (1024) or to a single UNION-ALL branch.
    using subquery_compacter =
        core::result_wrapper_t<types::logical_value_t> (*)(const std::pmr::vector<vector::data_chunk_t>& chunks);

    struct id_result_mapping {
        subquery_compacter compacter = nullptr;
        core::parameter_id_t id;
        // A bare boolean-context scalar sub-query: `WHERE (SELECT ...)` /
        // `HAVING (SELECT ...)`. PostgreSQL requires the argument of WHERE/HAVING to be
        // type boolean, so the executor rejects a non-boolean static output type before
        // binding (else a numeric scalar would silently coerce to bool). false for every
        // other sub-query form (IN / scalar comparison / EXISTS), where any type is legal.
        bool boolean_required = false;
    };

    // EXPLAIN mode for the whole plan. `plan` renders the physical plan without executing;
    // `analyze` executes with per-operator instrumentation and renders the annotated plan.
    // Set by the transformer's T_ExplainStmt case; read by the executor. Scalar enum, so it
    // copies trivially (no pmr re-anchor).
    enum class explain_type
    {
        none,
        plan,
        analyze
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

        // EXPLAIN / EXPLAIN ANALYZE mode (none for a normal query). Only the main (top-level)
        // plan carries it; flattened sub-queries are built fresh and stay `none`.
        explain_type explain{explain_type::none};

        // EXPLAIN ANALYZE only: set by the executor's sub-query loop on each flattened sub-plan so it
        // instruments + BUILDS its IR (returned via execute_result_t::captured_explain_ir) while KEEPING
        // its data cursor for the loop's compaction. The main plan never sets it. Scalar → copies trivially.
        bool explain_capture_ir{false};

        // Host-selected EXPLAIN renderer slot: an index into the executor's renderer registry
        // (see set_explain_renderer). 0 = the built-in postgres renderer (default). Set only from
        // the host C++ API, never SQL; rides the plan by value into the executor like `explain`.
        uint32_t explain_render_id{0};
    };

} // namespace components::logical_plan