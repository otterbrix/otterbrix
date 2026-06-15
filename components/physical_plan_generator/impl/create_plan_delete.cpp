#include "create_plan_delete.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>

#include <algorithm>
#include <limits>

#include "create_plan_data.hpp"

namespace services::planner::impl {

    namespace {

        void append_unique_projected_col(std::vector<size_t>& projected_cols, size_t column_index) {
            if (column_index == std::numeric_limits<size_t>::max()) {
                return;
            }
            if (std::find(projected_cols.begin(), projected_cols.end(), column_index) == projected_cols.end()) {
                projected_cols.push_back(column_index);
            }
        }

        void collect_delete_scan_projection_from_expression(const components::expressions::expression_ptr& expression,
                                                            std::vector<size_t>& projected_cols);

        void collect_delete_scan_projection_from_param(const components::expressions::param_storage& param,
                                                       std::vector<size_t>& projected_cols) {
            using components::expressions::expression_ptr;
            using components::expressions::key_t;

            if (std::holds_alternative<key_t>(param)) {
                const auto& key = std::get<key_t>(param);
                if (!key.path().empty()) {
                    append_unique_projected_col(projected_cols, key.path().front());
                }
                return;
            }
            if (std::holds_alternative<expression_ptr>(param)) {
                collect_delete_scan_projection_from_expression(std::get<expression_ptr>(param), projected_cols);
            }
        }

        void collect_delete_scan_projection_from_expression(const components::expressions::expression_ptr& expression,
                                                            std::vector<size_t>& projected_cols) {
            if (!expression || expression->group() != components::expressions::expression_group::compare) {
                return;
            }

            const auto* compare = static_cast<const components::expressions::compare_expression_t*>(expression.get());
            collect_delete_scan_projection_from_param(compare->left(), projected_cols);
            collect_delete_scan_projection_from_param(compare->right(), projected_cols);
            for (const auto& child : compare->children()) {
                collect_delete_scan_projection_from_expression(child, projected_cols);
            }
        }

        std::vector<size_t>
        delete_scan_projection(const context_storage_t& context,
                               const components::logical_plan::node_delete_t& node_delete,
                               const components::logical_plan::node_ptr& node_match) {
            if (!context.indexed_keys.empty() || !node_delete.referencing_fks().empty() || !node_match ||
                node_match->expressions().empty()) {
                return {};
            }
            // RETURNING needs every returned column materialised; a predicate-only
            // projected scan leaves non-predicate columns as unprojected placeholders
            // (null data), which RETURNING then reads as empty / cannot gather across
            // chunks. Fall back to a full scan (no projection) when RETURNING is present.
            if (!node_delete.returning().empty()) {
                return {};
            }

            std::vector<size_t> projected_cols;
            collect_delete_scan_projection_from_expression(node_match->expressions().front(), projected_cols);
            std::sort(projected_cols.begin(), projected_cols.end());
            projected_cols.erase(std::unique(projected_cols.begin(), projected_cols.end()), projected_cols.end());
            return projected_cols;
        }

    } // namespace

    components::operators::operator_ptr create_plan_delete(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node,
                                                           const components::logical_plan::storage_parameters* params) {
        const auto* node_delete = static_cast<const components::logical_plan::node_delete_t*>(node.get());
        auto returning = build_returning_columns(context.resource, node_delete->returning(), params);

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_raw_data = nullptr;
        for (auto child : node_delete->children()) {
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
        // Target the simple (no USING) path when neither a USING-side
        // table_oid nor raw data join input is present.
        if (node_delete->table_oid_from() == components::catalog::INVALID_OID && !node_raw_data) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        std::move(returning)));
            auto projected_cols = delete_scan_projection(context, *node_delete, node_match);
            const bool row_ids_only =
                context.indexed_keys.empty() && node_delete->referencing_fks().empty() && !projected_cols.empty();
            plan->set_children(create_plan_match(context, node_match, limit, projected_cols, row_ids_only));

            return plan;
        } else {
            auto expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node_match->expressions()[0]);

            auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        std::move(returning),
                                                                                        *expr));
            if (node_raw_data) {
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             table_oid,
                                                                                             nullptr,
                                                                                             limit)),
                                   create_plan_data(node_raw_data));
            } else {
                // Read the USING-side table_oid from the node
                // (enrich_logical_plan stamps it via the same plan-tree
                // resolve path as the primary table). INVALID_OID would
                // scan an empty storage and silently drop the join condition.
                const auto using_oid = node_delete->table_oid_from();
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             table_oid,
                                                                                             nullptr,
                                                                                             limit)),
                                   boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             using_oid,
                                                                                             nullptr,
                                                                                             limit)));
            }

            return plan;
        }
    }

} // namespace services::planner::impl
