#include "optimizer.hpp"

#include "optimizer/rules/constant_folding.hpp"

namespace components::planner {

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    const catalog::catalog* /*catalog*/,
                                    logical_plan::parameter_node_t* parameters) {
        if (!node) {
            return nullptr;
        }

        // Constant folding: resolve arithmetic on parameters at plan time
        if (parameters) {
            optimizer::fold_constants(resource, node, parameters);
        }

        return node;
    }

} // namespace components::planner
