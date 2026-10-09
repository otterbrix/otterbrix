#pragma once

#include <components/compute/function.hpp>
#include <components/expressions/clone_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/types/types.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

// The coordinator ships this inside pushed_reduce_scan; the agent rebuilds operator_hash_group from it.
//
// Crosses the mailbox by value and owns everything in it: pmr containers, and the output expressions
// as detached deep copies the agent attaches onto its own resource. services/disk/mailbox_payload.hpp
// refuses to compile a disk message carrying anything else. Not default-constructible on purpose.
//
// The WHERE predicate and scan projection ride storage_reduce's own filter/projected_cols params, not this POD.

namespace components::operators {

    // The function itself rides in `outputs`, stamped into its aggregate expression. alias must be
    // byte-identical with create_plan_group's coordinator-side naming.
    struct pushed_aggregate_t {
        std::pmr::string function_name;          // "sum"/"count"/"min"/"max"/"avg" (agent classify())
        std::pmr::vector<uint64_t> arg_col_path; // resolved column-index path; EMPTY => COUNT(*)
        bool distinct{false};                    // always false in scope (optimizer skips DISTINCT)
        std::pmr::string alias;                  // group->add_value output name
        types::complex_logical_type result_type;

        explicit pushed_aggregate_t(std::pmr::memory_resource* resource)
            : function_name(resource)
            , arg_col_path(resource)
            , alias(resource) {}
    };

    // Mirrors group_key_t::full_path/name. Both are required: operator_hash_group's build_plan
    // needs a resolved full_path, and name lets a coordinator-side sort/select reference the key.
    struct pushed_group_key_t {
        std::pmr::string name;
        std::pmr::vector<uint64_t> path;

        explicit pushed_group_key_t(std::pmr::memory_resource* resource)
            : name(resource)
            , path(resource) {}
    };

    struct pushed_aggregate_spec_t {
        std::pmr::vector<pushed_group_key_t> group_keys;
        std::pmr::vector<pushed_aggregate_t> aggregates;
        std::pmr::vector<expressions::detached_expression_t> outputs;
        std::pmr::vector<types::complex_logical_type> output_types;
        std::pmr::vector<types::complex_logical_type> input_types;
        explicit pushed_aggregate_spec_t(std::pmr::memory_resource* resource)
            : group_keys(resource)
            , aggregates(resource)
            , outputs(resource)
            , output_types(resource)
            , input_types(resource) {}

        // All-empty (no keys, no aggregates) means no reduce is armed; build_pushed_spec rejects it.
        [[nodiscard]] bool active() const noexcept { return !aggregates.empty() || !group_keys.empty(); }
    };

} // namespace components::operators
