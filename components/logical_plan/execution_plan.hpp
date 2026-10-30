#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "node.hpp"
#include "node_catalog_resolve.hpp"
#include "param_storage.hpp"

#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

namespace components::logical_plan {

    // Compacts a sub-query result (ALL its cursor chunks) into a bound parameter value. Spanning every
    // chunk — not just the first — is what keeps an IN-list / scalar sub-query from being truncated at
    // DEFAULT_VECTOR_CAPACITY (1024) or to a single UNION-ALL branch.
    // `result_type` is the type of the value, from the sub-query's schema.
    using subquery_compacter =
        core::result_wrapper_t<vector::vector_t> (*)(std::pmr::memory_resource* resource,
                                                     const std::pmr::vector<vector::data_chunk_t>& chunks,
                                                     const types::complex_logical_type& result_type);

    struct id_result_mapping {
        subquery_compacter compacter = nullptr;
        core::parameter_id_t id{};
        // A bare boolean-context scalar sub-query: `WHERE (SELECT ...)` /
        // `HAVING (SELECT ...)`. PostgreSQL requires the argument of WHERE/HAVING to be
        // type boolean, so the executor rejects a non-boolean static output type before
        // binding (else a numeric scalar would silently coerce to bool). false for every
        // other sub-query form (IN / scalar comparison / EXISTS), where any type is legal.
        bool boolean_required = false;
        // ARRAY(SELECT ...): the order of the rows is the order of the elements
        // if ordering can not be observed, we can drop ordering node in sub-query
        bool ordered = false;
    };

    // EXPLAIN mode for the whole plan. `plan` renders the physical plan without executing;
    // `analyze` executes with per-operator instrumentation and renders the annotated plan.
    // Set by the transformer's T_ExplainStmt case; read by the executor. Scalar enum, so it
    // copies trivially (no pmr re-anchor).
    // `describe` answers the statement's result columns and the types of its $n, as PostgreSQL 18's Parse and
    // Describe do: the statement is analyzed, nothing is planned, nothing reaches a storage of a table.
    // `describe_explain` is that of an EXPLAIN [ANALYZE] of the statement: its one text column QUERY PLAN.
    enum class explain_type
    {
        none,
        plan,
        analyze,
        describe,
        describe_explain
    };

    [[nodiscard]] constexpr bool is_describe(explain_type explain) noexcept {
        return explain == explain_type::describe || explain == explain_type::describe_explain;
    }

    // A $n of the statement: the slot its other clauses read, the columns of the INSERT's VALUES chunk that hold it,
    // and the type of the value bound to it (NA while unbound or bound to an untyped NULL).
    struct external_parameter_t {
        explicit external_parameter_t(std::pmr::memory_resource* resource)
            : insert_columns(resource) {}

        std::size_t number{0};
        std::optional<core::parameter_id_t> slot;
        std::pmr::vector<std::size_t> insert_columns;
        types::complex_logical_type bound_type{types::logical_type::NA};
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

        // Every catalog lookup this plan depends on
        catalog_resolves_t catalog_resolves;

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

        bool commits_when_done{false};

        // Every $n the statement names, by number. Empty for a plan whose parameters the host wrote itself.
        std::pmr::vector<external_parameter_t> external_parameters;

        // The result columns PREPARE described, for the statement an EXECUTE runs: a statement whose columns differ is
        // refused before it runs (PostgreSQL 18 RevalidateCachedQuery, equalRowTypes: names and types).
        std::optional<std::pmr::vector<types::complex_logical_type>> expected_columns;
    };

} // namespace components::logical_plan