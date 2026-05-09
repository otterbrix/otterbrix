#include "create_plan_get_schema.hpp"

#include <components/logical_plan/node_get_schema.hpp>
#include <components/physical_plan/operators/operator_get_schema.hpp>

#include <utility>
#include <vector>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_get_schema(const context_storage_t& context,
                            const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_get_schema_t*>(node.get());

        // Re-pack pmr-vector<pair> → std::vector<pair> so the operator owns a
        // self-contained id list (it does not need pmr semantics for the
        // string contents — only the per-id reads via read_rows_by_key matter,
        // and those construct fresh logical_value_t per call).
        std::vector<std::pair<std::string, std::string>> ids;
        ids.reserve(n->ids().size());
        for (const auto& [db, name] : n->ids()) {
            ids.emplace_back(db, name);
        }

        return boost::intrusive_ptr(new components::operators::operator_get_schema_t(
            context.resource,
            context.log.clone(),
            std::move(ids)));
    }

} // namespace services::planner::impl
