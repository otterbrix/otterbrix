#include "create_plan_match.hpp"

#include "index_selection_helpers.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>

namespace services::planner::impl {

    namespace {

        namespace expr = components::expressions;

        // Check if this compare expression can use an index scan
        [[maybe_unused]] bool can_use_index(const context_storage_t& context,
                           const expr::compare_expression_t& comp,
                           bool& key_on_left) {
            // Skip union conditions
            if (expr::is_union_compare_condition(comp.type())) {
                return false;
            }
            // Only simple comparisons (not regex, any, all, etc.)
            switch (comp.type()) {
                case expr::compare_type::eq:
                case expr::compare_type::lt:
                case expr::compare_type::lte:
                case expr::compare_type::gt:
                case expr::compare_type::gte:
                    break;
                default:
                    return false;
            }
            // Need parameters to resolve the value
            if (!context.parameters) {
                return false;
            }

            // Check key_t on left, parameter_id_t on right
            if (std::holds_alternative<expr::key_t>(comp.left()) &&
                std::holds_alternative<core::parameter_id_t>(comp.right())) {
                const auto& key = std::get<expr::key_t>(comp.left());
                if (context.has_index_on(key)) {
                    key_on_left = true;
                    return true;
                }
            }
            // Check key_t on right, parameter_id_t on left (symmetric)
            if (std::holds_alternative<core::parameter_id_t>(comp.left()) &&
                std::holds_alternative<expr::key_t>(comp.right())) {
                const auto& key = std::get<expr::key_t>(comp.right());
                if (context.has_index_on(key)) {
                    key_on_left = false;
                    return true;
                }
            }
            return false;
        }

        bool is_pure_compare(const components::expressions::expression_ptr& expr) {
            using namespace components::expressions;
            if (expr->group() != expression_group::compare) {
                return false;
            }
            auto comp_expr = reinterpret_cast<const compare_expression_ptr&>(expr);
            for (const auto& child : comp_expr->children()) {
                if (!is_pure_compare(child)) {
                    return false;
                }
            }

            if (std::holds_alternative<expression_ptr>(comp_expr->left()) ||
                std::holds_alternative<expression_ptr>(comp_expr->right())) {
                return false;
            }
            return true;
        }

        components::operators::operator_ptr create_plan_match_(const context_storage_t& context,
                                                               const collection_full_name_t& coll_name,
                                                               const components::expressions::expression_ptr& expr,
                                                               components::logical_plan::limit_t limit) {
            if (context.has_collection(coll_name)) {
                // TODO: function_expr in scans
                if (is_pure_compare(expr)) {
                    auto comp_expr = reinterpret_cast<const expr::compare_expression_ptr&>(expr);

                    // Index selection: detect if an index is available for this predicate.
                    // TODO: Enable index_scan when index save/load deduplication is fixed.
                    // The can_use_index() check and mirror_compare() are ready; uncomment
                    // the block below to route through index_scan instead of full_scan.
                    //
                    // if (!comp_expr->is_union()) {
                    //     bool key_on_left = true;
                    //     if (can_use_index(context, *comp_expr, key_on_left)) {
                    //         auto& key = key_on_left
                    //             ? std::get<expr::key_t>(comp_expr->left())
                    //             : std::get<expr::key_t>(comp_expr->right());
                    //         auto param_id = key_on_left
                    //             ? std::get<core::parameter_id_t>(comp_expr->right())
                    //             : std::get<core::parameter_id_t>(comp_expr->left());
                    //         auto& value = logical_plan::get_parameter(context.parameters, param_id);
                    //         auto ctype = key_on_left ? comp_expr->type() : mirror_compare(comp_expr->type());
                    //         return boost::intrusive_ptr(
                    //             new operators::index_scan(context.resource, context.log.clone(),
                    //                                       coll_name, key, value, ctype, limit));
                    //     }
                    // }

                    return boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                     context.log.clone(),
                                                                                     coll_name,
                                                                                     comp_expr,
                                                                                     limit));
                } else {
                    // For now we do a full scan and apply function after
                    auto match_operator =
                        boost::intrusive_ptr(new components::operators::operator_match_t(context.resource,
                                                                                         context.log.clone(),
                                                                                         expr,
                                                                                         limit));
                    match_operator->set_children(
                        boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                  context.log.clone(),
                                                                                  coll_name,
                                                                                  nullptr,
                                                                                  limit)));
                    return match_operator;
                }
            } else {
                return boost::intrusive_ptr(new components::operators::operator_match_t(nullptr, log_t{}, expr, limit));
            }
        }
    } // namespace

    components::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit) {
        if (node->expressions().empty()) {
            if (context.has_collection(node->collection_full_name())) {
                return boost::intrusive_ptr(
                    new components::operators::transfer_scan(context.resource, node->collection_full_name(), limit));
            } else {
                return boost::intrusive_ptr(
                    new components::operators::transfer_scan(nullptr, node->collection_full_name(), limit));
            }
        } else {
            return create_plan_match_(context, node->collection_full_name(), node->expressions()[0], limit);
        }
    }

} // namespace services::planner::impl
