#pragma once

#include <components/compute/function.hpp>          // compute::function_uid / invalid_function_uid
#include <components/types/types.hpp>                // types::complex_logical_type

#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

// Aggregate-pushdown SPEC. A POD, mailbox-safe description of the reduce the
// owning agent runs over its OWN slice: plain-column GROUP BY keys + builtin
// SUM/COUNT/MIN/MAX/AVG. The coordinator lowers the stamped aggregate into a
// pushed_reduce_scan that CARRIES this spec, and the agent rebuilds the EXISTING
// operator_group from it.
//
// R10/R14: NO node_ptr / expression_ptr / variant / any / tuple / shared_ptr anywhere —
// only POD scalars + std::pmr containers of them + a resolved column-index path. So it may
// cross the mailbox by value without a non-atomic intrusive-refcount hazard. Every pmr
// member is constructed on an explicit resource (NO get_default_resource): the struct is
// NOT default-constructible on purpose (storage_parameters ships the same way —
// actor_zeta value-args need no default ctor).
//
// The WHERE predicate is NOT carried here: it rides the mailbox-safe table_filter_t on
// storage_reduce's `filter` param (built once via transform_predicate). The scan
// projection likewise rides that call's projected_cols param (single source of truth —
// NOT duplicated in the POD).

namespace components::operators {

    // One pushed aggregate: a builtin SUM/COUNT/MIN/MAX/AVG over a single resolved column
    // (arg_col_path), or COUNT(*) when arg_col_path is empty. func_uid resolves against the
    // agent's OWN register_default_functions registry (the optimizer already refused any UDF
    // via is_udf_uid, so a builtin uid always resolves there). alias is the group->add_value
    // output name (== the aggregate expression key's as_pmr_string, byte-identical with the
    // coordinator-side create_plan_group naming).
    struct pushed_aggregate_t {
        std::pmr::string function_name;          // "sum"/"count"/"min"/"max"/"avg" (agent classify())
        std::pmr::vector<uint64_t> arg_col_path; // resolved column-index path; EMPTY => COUNT(*)
        components::compute::function_uid func_uid{components::compute::invalid_function_uid};
        bool distinct{false};                    // always false in scope (optimizer skips DISTINCT)
        std::pmr::string alias;                  // group->add_value output name

        explicit pushed_aggregate_t(std::pmr::memory_resource* resource)
            : function_name(resource)
            , arg_col_path(resource)
            , alias(resource) {}
    };

    // One pushed GROUP BY key: a plain column with its resolved column-index path (group_key_t
    // ::full_path) + output name (group_key_t::name). Both are needed — operator_group's
    // build_plan REQUIRES a resolved full_path on every column key, and the name feeds the
    // per-cell alias so a coordinator-side sort/select can still reference the key by name.
    struct pushed_group_key_t {
        std::pmr::string name;           // group_key_t::name (output alias)
        std::pmr::vector<uint64_t> path; // resolved column-index path (group_key_t::full_path)

        explicit pushed_group_key_t(std::pmr::memory_resource* resource)
            : name(resource)
            , path(resource) {}
    };

    struct pushed_aggregate_spec_t {
        std::pmr::vector<pushed_group_key_t> group_keys;
        std::pmr::vector<pushed_aggregate_t> aggregates;
        // FINAL output column types (keys first, then aggregate values), forwarded from the
        // aggregate node's output_types(). MANDATORY: operator_group::set_output_types uses it
        // to type an empty-slice scalar result instead of the 0-byte NA sentinel (gcc -O3 crash).
        std::pmr::vector<types::complex_logical_type> output_types;
        explicit pushed_aggregate_spec_t(std::pmr::memory_resource* resource)
            : group_keys(resource)
            , aggregates(resource)
            , output_types(resource) {}

        // "A reduce is armed": an all-empty spec (no keys, no aggregates) describes no
        // reduce at all — build_pushed_spec rejects it.
        [[nodiscard]] bool active() const noexcept { return !aggregates.empty() || !group_keys.empty(); }
    };

} // namespace components::operators
