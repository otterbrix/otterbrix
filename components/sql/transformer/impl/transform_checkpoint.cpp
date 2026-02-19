#include <components/logical_plan/node_checkpoint.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_checkpoint(CheckPointStmt& /*node*/) {
        return logical_plan::make_node_checkpoint(resource_);
    }

} // namespace components::sql::transform
