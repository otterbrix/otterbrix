#include "create_plan_create_matview.hpp"

#include <components/logical_plan/node_create_matview.hpp>
#include <components/physical_plan/operators/operator_create_matview.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_create_matview(const context_storage_t& context,
                               [[maybe_unused]] const components::compute::function_registry_t& function_registry,
                               const components::logical_plan::node_ptr& node,
                               [[maybe_unused]] const components::logical_plan::storage_parameters* params) {
        using namespace components::logical_plan;
        auto* cm = static_cast<node_create_matview_t*>(node.get());
        if (cm->inferred_columns().empty() || cm->catalog_writes().empty()) {
            // enrich/planner couldn't derive schema or build writes — surface
            // as "invalid query plan" via the standard executor error path.
            return nullptr;
        }
        // Body plan is NOT compiled (only WITH NO DATA reaches here, so it has no consumer); it
        // still shapes the matview via enrich's inferred_columns and the planner's pg_rewrite SQL.
        // Move catalog_writes out of the node into the operator.
        auto writes_vec = const_cast<node_create_matview_t*>(cm)->take_catalog_writes();
        std::vector<components::operators::operator_create_matview_t::catalog_write_t> writes;
        writes.reserve(writes_vec.size());
        for (auto& w : writes_vec) {
            writes.emplace_back(w.table_oid, std::move(w.row));
        }

        return boost::intrusive_ptr(new components::operators::operator_create_matview_t(
            context.resource,
            context.log.clone(),
            cm->matview_oid(),
            cm->namespace_oid(),
            std::vector<components::table::column_definition_t>(cm->inferred_columns()),
            std::move(writes)));
    }

} // namespace services::planner::impl
