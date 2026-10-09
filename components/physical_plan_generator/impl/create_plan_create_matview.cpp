#include "create_plan_create_matview.hpp"

#include <components/logical_plan/node_create_matview.hpp>
#include <components/physical_plan/operators/operator_create_matview.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    plan_result_t create_plan_create_matview(const context_storage_t& context,
                                             const components::logical_plan::node_ptr& node) {
        using namespace components::logical_plan;
        auto* cm = static_cast<node_create_matview_t*>(node.get());
        auto columns = cm->take_columns();
        auto writes_vec = cm->take_catalog_writes();
        if (columns.empty() || writes_vec.empty()) {
            return plan_refusal(context.resource, "materialized view has no columns or no catalog rows to write");
        }
        std::vector<components::operators::operator_create_matview_t::catalog_write_t> writes;
        writes.reserve(writes_vec.size());
        for (auto& w : writes_vec) {
            writes.emplace_back(w.table_oid, std::move(w.row));
        }

        return boost::intrusive_ptr(new components::operators::operator_create_matview_t(context.resource,
                                                                                         context.log.clone(),
                                                                                         cm->matview_oid(),
                                                                                         cm->namespace_oid(),
                                                                                         std::move(columns),
                                                                                         std::move(writes)));
    }

} // namespace services::planner::impl
