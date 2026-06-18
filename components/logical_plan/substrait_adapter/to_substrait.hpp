#pragma once

#include <components/catalog/catalog.hpp>
#include <components/logical_plan/node.hpp>
#include <string>

namespace substrait {
    class Plan;
}

namespace components::logical_plan::substrait_adapter {

    enum class export_profile_t { internal_roundtrip, external_canonical };

    substrait::Plan to_substrait_plan(const node_ptr& plan);
    substrait::Plan to_substrait_plan(const node_ptr& plan, export_profile_t profile);
    substrait::Plan to_substrait_plan(const node_ptr& plan, const components::catalog::catalog* catalog);
    substrait::Plan
    to_substrait_plan(const node_ptr& plan, export_profile_t profile, const components::catalog::catalog* catalog);
    std::string to_substrait_binary(const node_ptr& plan);
    std::string to_substrait_binary(const node_ptr& plan, export_profile_t profile);
    std::string to_substrait_binary(const node_ptr& plan, const components::catalog::catalog* catalog);
    std::string
    to_substrait_binary(const node_ptr& plan, export_profile_t profile, const components::catalog::catalog* catalog);
    std::string to_substrait_json(const node_ptr& plan);
    std::string to_substrait_json(const node_ptr& plan, export_profile_t profile);
    std::string to_substrait_json(const node_ptr& plan, const components::catalog::catalog* catalog);
    std::string
    to_substrait_json(const node_ptr& plan, export_profile_t profile, const components::catalog::catalog* catalog);

} // namespace components::logical_plan::substrait_adapter
