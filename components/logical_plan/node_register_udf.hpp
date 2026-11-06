#pragma once

#include "node.hpp"

#include <components/compute/function.hpp>

namespace components::logical_plan {

    // REGISTER_UDF: a host's function, whose pg_proc rows the statement writes in its transaction; the registries of
    // the other sessions take it when that transaction commits. The node owns the function: a copy is get_copy's.
    class node_register_udf_t final : public node_t {
    public:
        node_register_udf_t(std::pmr::memory_resource* resource, components::compute::function_ptr function);

        const components::compute::function* function() const noexcept { return function_.get(); }

        // The uid the dispatcher reserved for the function and the schema of the session's place, which the
        // executor stamps before the plan is lowered.
        void set_identity(components::compute::function_uid uid, std::uint32_t schema_oid) noexcept {
            uid_ = uid;
            schema_oid_ = schema_oid;
        }
        components::compute::function_uid uid() const noexcept { return uid_; }
        std::uint32_t schema_oid() const noexcept { return schema_oid_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::compute::function_ptr function_;
        components::compute::function_uid uid_{components::compute::invalid_function_uid};
        std::uint32_t schema_oid_{0};
    };

    using node_register_udf_ptr = boost::intrusive_ptr<node_register_udf_t>;

} // namespace components::logical_plan
