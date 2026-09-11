#pragma once

#include <components/compute/function.hpp>
#include <components/expressions/expression.hpp>
#include <components/types/types.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

// The coordinator ships this inside pushed_reduce_scan; the agent rebuilds operator_hash_group from it.
//
// R10/R14: no node_ptr/expression_ptr/variant/any/tuple/shared_ptr — only POD + pmr containers,
// so it crosses the mailbox by value without a non-atomic refcount hazard; not default-constructible on purpose.
//
// The WHERE predicate and scan projection ride storage_reduce's own filter/projected_cols params, not this POD.

namespace components::operators {

    // func_uid resolves against the agent's OWN registry — the optimizer already refused any UDF
    // via is_udf_uid. alias must be byte-identical with create_plan_group's coordinator-side naming.
    struct pushed_aggregate_t {
        std::pmr::string function_name;          // "sum"/"count"/"min"/"max"/"avg" (agent classify())
        std::pmr::vector<uint64_t> arg_col_path; // resolved column-index path; EMPTY => COUNT(*)
        components::compute::function_uid func_uid{components::compute::invalid_function_uid};
        bool distinct{false};   // always false in scope (optimizer skips DISTINCT)
        std::pmr::string alias; // group->add_value output name
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
        std::pmr::vector<expressions::expression_ptr> outputs;
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
