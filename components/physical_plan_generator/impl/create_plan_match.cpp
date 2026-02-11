#include "create_plan_match.hpp"
#include <components/expressions/compare_expression.hpp>
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

    components::operators::operator_ptr
    create_plan_match_(collection::context_collection_t* context_,
                       const components::expressions::compare_expression_ptr& expr,
                       components::logical_plan::limit_t limit) {
        //if (is_can_primary_key_find_by_predicate(expr->type()) && expr->key().as_string() == "_id") {
        //return boost::intrusive_ptr(new components::operators::primary_key_scan(context_));
        //}
        if (context_) {
            if (is_can_index_find_by_predicate(expr->type()) &&
                components::index::search_index(context_->index_engine(), {expr->primary_key()})) {
                return boost::intrusive_ptr(new components::operators::index_scan(context_, expr, limit));
            }
            return boost::intrusive_ptr(new components::operators::full_scan(context_, expr, limit));
        } else {
            return boost::intrusive_ptr(new components::operators::operator_match_t(context_, expr, limit));
        }
    }

    components::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                                const components::logical_plan::node_ptr& node,
                                                                components::logical_plan::limit_t limit) {
        if (node->expressions().empty()) {
            return boost::intrusive_ptr(
                new components::operators::transfer_scan(context.at(node->collection_full_name()), limit));
        } else { //todo: other kinds scan
            auto expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node->expressions()[0]);
            return create_plan_match_(context.at(node->collection_full_name()), *expr, limit);
        }
    }

} // namespace services::planner::impl