#include "create_plan_update.hpp"
#include "create_plan_match.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>

#include "create_plan_data.hpp"
#include "create_plan_select.hpp"

#include <algorithm>
#include <limits>

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

        void collect_update_scan_projection_from_expression(const components::expressions::expression_ptr& expression,
                                                            std::vector<size_t>& projected_cols);

        void collect_update_scan_projection_from_param(const components::expressions::param_storage& param,
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
                collect_update_scan_projection_from_expression(std::get<expression_ptr>(param), projected_cols);
            }
        }

        void collect_update_scan_projection_from_expression(const components::expressions::expression_ptr& expression,
                                                            std::vector<size_t>& projected_cols) {
            if (!expression || expression->group() != components::expressions::expression_group::compare) {
                return;
            }

            const auto* compare = static_cast<const components::expressions::compare_expression_t*>(expression.get());
            collect_update_scan_projection_from_param(compare->left(), projected_cols);
            collect_update_scan_projection_from_param(compare->right(), projected_cols);
            for (const auto& child : compare->children()) {
                collect_update_scan_projection_from_expression(child, projected_cols);
            }
        }

        bool is_projectable_compare_expression(const components::expressions::expression_ptr& expression) {
            using namespace components::expressions;

            if (!expression || expression->group() != expression_group::compare) {
                return false;
            }
            const auto* compare = static_cast<const compare_expression_t*>(expression.get());
            if (compare->type() == compare_type::regex) {
                return false;
            }
            if (std::holds_alternative<expression_ptr>(compare->left()) ||
                std::holds_alternative<expression_ptr>(compare->right())) {
                return false;
            }
            for (const auto& child : compare->children()) {
                if (!is_projectable_compare_expression(child)) {
                    return false;
                }
            }
            return true;
        }

	        void collect_update_scan_projection_from_update_expr(const components::expressions::update_expr_ptr& expression,
	                                                             std::vector<size_t>& projected_cols) {
	            using namespace components::expressions;

            if (!expression) {
                return;
            }

	            if (expression->type() == update_expr_type::set) {
	                const auto* set_expr = static_cast<const update_expr_set_t*>(expression.get());
	                if (set_expr->key().path().size() > 1) {
	                    append_unique_projected_col(projected_cols, set_expr->key().path().front());
	                }
	            } else if (expression->type() == update_expr_type::get_value) {
                const auto* get_expr = static_cast<const update_expr_get_value_t*>(expression.get());
                if (!get_expr->key().path().empty()) {
                    append_unique_projected_col(projected_cols, get_expr->key().path().front());
                }
            }

            collect_update_scan_projection_from_update_expr(expression->left(), projected_cols);
            collect_update_scan_projection_from_update_expr(expression->right(), projected_cols);
        }

        std::vector<size_t>
        update_scan_projection(const components::logical_plan::node_update_t& node_update,
                               const components::logical_plan::node_ptr& node_match) {
            if (!node_match || node_match->expressions().empty() ||
                !is_projectable_compare_expression(node_match->expressions().front())) {
                return {};
            }
            // RETURNING needs every returned column materialised; a projected scan
            // leaves non-predicate/non-SET columns as placeholders that RETURNING
            // reads as empty. Full scan (no projection) when RETURNING is present.
            if (!node_update.returning().empty()) {
                return {};
            }

            std::vector<size_t> projected_cols;

            collect_update_scan_projection_from_expression(node_match->expressions().front(), projected_cols);
            for (const auto& update : node_update.updates()) {
                collect_update_scan_projection_from_update_expr(update, projected_cols);
            }

            std::sort(projected_cols.begin(), projected_cols.end());
            projected_cols.erase(std::unique(projected_cols.begin(), projected_cols.end()), projected_cols.end());
            return projected_cols;
        }

    } // namespace

    components::operators::operator_ptr create_plan_update(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node,
                                                           const components::logical_plan::storage_parameters* params) {
        const auto* node_update = static_cast<const components::logical_plan::node_update_t*>(node.get());
        auto returning = build_returning_columns(context.resource, node_update->returning(), params);

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
            auto projected_cols = update_scan_projection(*node_update, node_match);
            plan->set_children(create_plan_match(context, node_match, limit, projected_cols));

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
