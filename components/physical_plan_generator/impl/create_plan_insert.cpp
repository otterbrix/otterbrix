#include "create_plan_insert.hpp"

#include "create_plan_select.hpp"
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_insert(const context_storage_t& context,
                       const components::compute::function_registry_t& function_registry,
                       const components::logical_plan::node_ptr& node,
                       const components::logical_plan::storage_parameters* params) {
        const auto* node_insert = static_cast<const components::logical_plan::node_insert_t*>(node.get());
        std::pmr::vector<components::operators::select_column_t> returning(context.resource);
        if (!build_returning_columns(context.resource, node_insert->returning(), params, returning)) {
            // Defensive guard tripped: return nullptr -> executor surfaces the error
            // (rule 9: no throw on the operator-build path).
            return nullptr;
        }
        // Forward the plan-resolved RETURNING output types (stamped on the insert node by
        // validate_schema) onto the projection columns, in projection order, so a
        // CASE/COALESCE/deep-field RETURNING column over the appended rows stays correctly
        // typed instead of being dropped as an untyped (NA) placeholder. evaluate_projection
        // reads col.result_type authoritatively. Mirrors create_plan_aggregate's select path.
        // No RETURNING -> node->produces_rows() is false and there is nothing to forward.
        // No data-derived fallback (rule 6): a column without a resolved type stays unset.
        if (node->produces_rows()) {
            const auto& out_schema = node->output_schema();
            for (size_t i = 0; i < returning.size() && i < out_schema.size(); ++i) {
                returning[i].result_type = out_schema[i].type;
            }
        }
        auto plan = boost::intrusive_ptr(new components::operators::operator_insert(context.resource,
                                                                                    context.log.clone(),
                                                                                    node->table_oid(),
                                                                                    std::move(returning)));
        // INSERT ... SELECT: the SELECT projection column names need not match the
        // target columns (e.g. SELECT 5, 55 into (id, a.b)), and the append is
        // name-based, so hand the operator the target names to rename the streamed
        // columns positionally. INSERT ... VALUES has a raw-data (data_t) child whose
        // columns fill_row already named, so it is left untouched.
        const auto& kt = node_insert->key_translation();
        if (!kt.empty() && !node->children().empty() &&
            node->children().front()->type() != components::logical_plan::node_type::data_t) {
            std::pmr::vector<std::pmr::string> names(context.resource);
            names.reserve(kt.size());
            for (const auto& k : kt) {
                names.emplace_back(std::pmr::string{k.as_string().c_str(), context.resource});
            }
            plan->set_rename_targets(std::move(names));
        }
        plan->set_children(create_plan(context,
                                       function_registry,
                                       node->children().front(),
                                       components::logical_plan::limit_t::unlimit(),
                                       params));

        return plan;
    }

} // namespace services::planner::impl
