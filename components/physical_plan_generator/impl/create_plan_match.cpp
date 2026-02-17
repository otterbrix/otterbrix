#include "create_plan_match.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/index/index_engine.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>

#include <services/collection/collection.hpp>

namespace services::planner::impl {

    bool is_can_index_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq || compare == compare_type::ne || compare == compare_type::gt ||
               compare == compare_type::lt || compare == compare_type::gte || compare == compare_type::lte;
    }

    bool is_can_primary_key_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq;
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

    components::operators::operator_ptr create_plan_match_(collection::context_collection_t* context_,
                                                           const components::expressions::expression_ptr& expr,
                                                           components::logical_plan::limit_t limit) {
        //if (is_can_primary_key_find_by_predicate(expr->type()) && expr->key().as_string() == "_id") {
        //return boost::intrusive_ptr(new components::operators::primary_key_scan(context_));
        //}
        if (context_ && expr->group() == components::expressions::expression_group::compare) {
            auto comp_expr = reinterpret_cast<const components::expressions::compare_expression_ptr&>(expr);
            if (is_pure_compare(comp_expr)) {
                if (is_can_index_find_by_predicate(comp_expr->type()) &&
                    components::index::search_index(context_->index_engine(),
                                                    {std::get<components::expressions::key_t>(comp_expr->left())})) {
                    return boost::intrusive_ptr(new components::operators::index_scan(context_, comp_expr, limit));
                }
                return boost::intrusive_ptr(new components::operators::full_scan(context_, comp_expr, limit));
            }
        }

        // TODO: function_expr in scans
        // For now we do a full scan and apply function after
        auto match_operator = boost::intrusive_ptr(new components::operators::operator_match_t(context_, expr, limit));
        match_operator->set_children(
            boost::intrusive_ptr(new components::operators::full_scan(context_, nullptr, limit)));
        return match_operator;
    }

    components::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit) {
        if (node->expressions().empty()) {
            return boost::intrusive_ptr(
                new components::operators::transfer_scan(context.at(node->collection_full_name()), limit));
        } else { //todo: other kinds scan
            return create_plan_match_(context.at(node->collection_full_name()), node->expressions()[0], limit);
        }
    }

} // namespace services::planner::impl