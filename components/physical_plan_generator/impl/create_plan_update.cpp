#include "create_plan_update.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>

#include "create_plan_data.hpp"

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_update(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node,
                                                           const components::logical_plan::storage_parameters* params) {
        const auto* node_update = static_cast<const components::logical_plan::node_update_t*>(node.get());
        auto returning = build_returning_columns(context.resource, node_update->returning(), params);

        // Forward the plan-resolved RETURNING output types (stamped on the update node by
        // validate_schema) onto the RETURNING projection columns, mirroring the SELECT path
        // (create_plan_aggregate -> operator_select_t::set_output_types). evaluate_projection
        // reads select_column_t::result_type AUTHORITATIVELY for coalesce/case_when/deep-field
        // columns, so a RETURNING projection over zero affected rows stays correctly typed
        // instead of collapsing to an untyped (NA) column. No RETURNING -> output_types() empty
        // -> guard skips (no-op). Column order is aligned by construction: build_returning_columns
        // walks node_update->returning() in order producing one select_column_t per scalar expr,
        // and validate_schema resolves/expands the SAME returning() vector in the SAME order, so
        // returning[i] corresponds to output_types()[i]. No data-derived fallback (rule 6).
        if (node->has_output_types()) {
            const auto& output_types = node->output_types();
            for (size_t i = 0; i < returning.size() && i < output_types.size(); ++i) {
                returning[i].result_type = output_types[i];
            }
        }

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_raw_data = nullptr;
        for (auto child : node_update->children()) {
            if (child->type() == components::logical_plan::node_type::match_t) {
                node_match = child;
            } else if (child->type() == components::logical_plan::node_type::limit_t) {
                node_limit = child;
            } else if (child->type() == components::logical_plan::node_type::data_t) {
                node_raw_data = child;
            }
        }
        auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        auto table_oid = node->table_oid();
        // Target the simple (no FROM) path when neither a FROM-side
        // table_oid nor raw data join input is present.
        if (node_update->table_oid_from() == components::catalog::INVALID_OID && !node_raw_data) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        node_update->updates(),
                                                                                        node_update->upsert(),
                                                                                        std::move(returning)));
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        } else {
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        node_update->updates(),
                                                                                        node_update->upsert(),
                                                                                        std::move(returning),
                                                                                        node_match->expressions()[0]));
            if (node_raw_data) {
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             table_oid,
                                                                                             nullptr,
                                                                                             limit)),
                                   create_plan_data(node_raw_data));
            } else {
                // Read the FROM-side table_oid from the node (enrich
                // stamps it via the sibling resolve_table for the FROM source).
                const auto from_oid = node_update->table_oid_from();
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             table_oid,
                                                                                             nullptr,
                                                                                             limit)),
                                   boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             from_oid,
                                                                                             nullptr,
                                                                                             limit)));
            }

            return plan;
        }
    }

} // namespace services::planner::impl
