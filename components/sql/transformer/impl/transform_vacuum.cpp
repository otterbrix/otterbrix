#include <components/logical_plan/node_vacuum.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_vacuum(VacuumStmt& /*node*/) {
        return logical_plan::make_node_vacuum(resource_);
    }

} // namespace components::sql::transform
