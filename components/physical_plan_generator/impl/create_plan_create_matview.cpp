#include "create_plan_create_matview.hpp"

#include <components/logical_plan/node_create_matview.hpp>
#include <components/physical_plan/operators/operator_create_matview.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_create_matview(const context_storage_t& context,
                               const components::compute::function_registry_t& function_registry,
                               const components::logical_plan::node_ptr& node,
                               const components::logical_plan::storage_parameters* params) {
        using namespace components::logical_plan;
        auto* cm = static_cast<node_create_matview_t*>(node.get());
        if (cm->inferred_columns().empty() || cm->catalog_writes().empty()) {
            // enrich/planner couldn't derive schema or build writes — surface
            // as "invalid query plan" via the standard executor error path.
            return nullptr;
        }
        // Recursively compile body_plan (matview's child[0]) through the
        // standard pipeline. The compiled operator chain produces the data
        // chunk that operator_create_matview_t will storage_append into the
        // new matview's heap.
        auto body_op = create_plan(context,
                                   function_registry,
                                   cm->body_plan(),
                                   components::logical_plan::limit_t::unlimit(),
                                   params);
        if (!body_op) {
            return nullptr;
        }

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
            /*is_disk_storage=*/true,
            std::move(writes),
            std::move(body_op)));
    }

} // namespace services::planner::impl
