#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <string>

namespace substrait {
    class Plan;
}


namespace components::logical_plan::substrait_adapter {

    enum class import_profile_t { internal_roundtrip, external_canonical };

    struct plan_with_params_t {
        node_ptr plan;
        parameter_node_ptr params;
    };

    plan_with_params_t from_substrait_plan(std::pmr::memory_resource* resource, const substrait::Plan& plan);
    plan_with_params_t
    from_substrait_plan(std::pmr::memory_resource* resource, const substrait::Plan& plan, import_profile_t profile);
    plan_with_params_t from_substrait_binary(std::pmr::memory_resource* resource, const std::string& binary);
    plan_with_params_t from_substrait_binary(std::pmr::memory_resource* resource,
                                             const std::string& binary,
                                             import_profile_t profile);
    plan_with_params_t from_substrait_json(std::pmr::memory_resource* resource, const std::string& json);
    plan_with_params_t
    from_substrait_json(std::pmr::memory_resource* resource, const std::string& json, import_profile_t profile);

} // namespace components::logical_plan::substrait_adapter
