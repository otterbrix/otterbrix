#pragma once

#include "node.hpp"
#include <components/base/identifier_types.hpp>

#include <components/catalog/results/ddl_result.hpp>
#include <components/types/types.hpp>

#include <string>

namespace components::logical_plan {

    // UNREGISTER_UDF — leaf carrying the function name + argument types that
    // identify a single overload to drop. The operator probes the global
    // default function_registry_t to confirm the overload exists, removes it
    // from the registry, and purges all matching pg_proc rows.
    class node_unregister_udf_t final : public node_t {
    public:
        node_unregister_udf_t(std::pmr::memory_resource* resource,
                              core::function_name_t function_name,
                              std::pmr::vector<types::complex_logical_type> inputs,
                              components::catalog::drop_behavior_t behavior);

        const std::string& function_name() const noexcept { return function_name_; }
        const std::pmr::vector<types::complex_logical_type>& inputs() const noexcept { return inputs_; }
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string function_name_;
        std::pmr::vector<types::complex_logical_type> inputs_;
        components::catalog::drop_behavior_t behavior_;
    };

    using node_unregister_udf_ptr = boost::intrusive_ptr<node_unregister_udf_t>;

} // namespace components::logical_plan
