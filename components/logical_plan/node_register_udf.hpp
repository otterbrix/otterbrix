#pragma once

#include "node.hpp"

#include <components/compute/function.hpp>

namespace components::logical_plan {

    // REGISTER_UDF — leaf carrying a user-defined function object that should be
    // installed across all executor-local registries, the global default
    // function_registry_t, and persisted into pg_proc.
    //
    // The function is owned as the canonical components::compute::function_ptr
    // (a std::unique_ptr alias): the node is the sole owner of the payload.
    // Holding a unique member inside an intrusive_ptr-managed node is safe on
    // this path because logical_plan nodes are always passed by intrusive_ptr
    // (boost::intrusive_ptr<node_t>) — the planner clones references, never the
    // node object, so node_register_udf_t is never value-copied and the unique
    // member is never required to be copyable. When fan-out needs an independent
    // copy of the payload it calls function()->get_copy(resource) (a deep copy),
    // leaving this owned payload intact for the operator's pg_proc encode step.
    class node_register_udf_t final : public node_t {
    public:
        node_register_udf_t(std::pmr::memory_resource* resource, components::compute::function_ptr function);

        // Borrowed view of the owned payload. The node retains ownership; callers
        // that need an independent instance must deep-copy via get_copy(resource).
        const components::compute::function* function() const noexcept { return function_.get(); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::compute::function_ptr function_;
    };

    using node_register_udf_ptr = boost::intrusive_ptr<node_register_udf_t>;

} // namespace components::logical_plan
