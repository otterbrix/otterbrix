#pragma once

#include "node.hpp"

#include <components/compute/function.hpp>

namespace components::logical_plan {

    // REGISTER_UDF — leaf carrying a user-defined function object that should be
    // installed across all executor-local registries, the global default
    // function_registry_t, and persisted into pg_proc. Phase 4 #55: replaces
    // inline manager_dispatcher_t::register_udf with the operator pipeline,
    // mirroring the get_schema migration done in #54.
    //
    // The function is held by std::shared_ptr<compute::function> rather than the
    // canonical std::unique_ptr alias so the planner can clone/copy the node
    // without consuming the payload (the operator deep-copies via get_copy()
    // when fanning out to per-executor registries).
    class node_register_udf_t final : public node_t {
    public:
        node_register_udf_t(std::pmr::memory_resource* resource,
                            std::shared_ptr<components::compute::function> function);

        const std::shared_ptr<components::compute::function>& function() const noexcept { return function_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::shared_ptr<components::compute::function> function_;
    };

    using node_register_udf_ptr = boost::intrusive_ptr<node_register_udf_t>;

} // namespace components::logical_plan
