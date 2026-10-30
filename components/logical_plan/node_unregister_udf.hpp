#pragma once

#include "node.hpp"
#include <components/base/identifier_types.hpp>

#include <components/catalog/results/ddl_result.hpp>
#include <components/types/types.hpp>

#include <string>

namespace components::logical_plan {

    // UNREGISTER_UDF: one overload by its name and argument types, whose pg_proc rows the statement drops.
    class node_unregister_udf_t final : public node_t {
    public:
        node_unregister_udf_t(std::pmr::memory_resource* resource,
                              core::function_name_t function_name,
                              std::pmr::vector<types::complex_logical_type> inputs,
                              components::catalog::drop_behavior_t behavior);

        const std::string& function_name() const noexcept { return function_name_; }
        // The schema of the session's place, the one the overload is dropped from: the executor stamps it.
        void set_schema_oid(std::uint32_t schema_oid) noexcept { schema_oid_ = schema_oid; }
        std::uint32_t schema_oid() const noexcept { return schema_oid_; }
        const std::pmr::vector<types::complex_logical_type>& inputs() const noexcept { return inputs_; }
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string function_name_;
        std::pmr::vector<types::complex_logical_type> inputs_;
        components::catalog::drop_behavior_t behavior_;
        std::uint32_t schema_oid_{0};
    };

    using node_unregister_udf_ptr = boost::intrusive_ptr<node_unregister_udf_t>;

} // namespace components::logical_plan
