#include "create_plan_match.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/index/index_engine.hpp>
#include <components/physical_plan/collection/operators/merge/operator_merge.hpp>
#include <components/physical_plan/collection/operators/operator_match.hpp>
#include <components/physical_plan/collection/operators/scan/full_scan.hpp>
#include <components/physical_plan/collection/operators/scan/index_scan.hpp>
#include <components/physical_plan/collection/operators/scan/transfer_scan.hpp>
#include <components/physical_plan/table/operators/operator_match.hpp>
#include <components/physical_plan/table/operators/scan/full_scan.hpp>
#include <components/physical_plan/table/operators/scan/index_scan.hpp>
#include <components/physical_plan/table/operators/scan/transfer_scan.hpp>

#include <services/collection/collection.hpp>

namespace services::collection::planner::impl {

    bool is_can_index_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq || compare == compare_type::ne || compare == compare_type::gt ||
               compare == compare_type::lt || compare == compare_type::gte || compare == compare_type::lte;
    }

    bool is_can_primary_key_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq;
    }

    components::collection::operators::operator_ptr
    create_plan_match_(context_collection_t* context_,
                       const components::expressions::compare_expression_ptr& expr,
                       components::logical_plan::limit_t limit) {
        //if (is_can_primary_key_find_by_predicate(expr->type()) && expr->key().as_string() == "_id") {
        //return boost::intrusive_ptr(new components::collection::operators::primary_key_scan(context_));
        //}
        if (context_) {
            if (is_can_index_find_by_predicate(expr->type())) {
                auto* found_index = components::index::search_index(context_->index_engine(), {expr->primary_key()});
                bool index_exist  = static_cast<bool>(found_index);
                trace(context_->log(), "create_plan_match: key={}, is_can_index={}, found_index={}",
                      expr->primary_key().as_string(),
                      is_can_index_find_by_predicate(expr->type()),
                      index_exist);
                if (index_exist) {
                    return boost::intrusive_ptr(new components::collection::operators::index_scan(context_, expr, limit));
                }
            }
            auto predicate = components::collection::operators::predicates::create_predicate(expr);
            return boost::intrusive_ptr(
                new components::collection::operators::full_scan(context_, std::move(predicate), limit));
        } else {
            auto predicate = components::collection::operators::predicates::create_predicate(expr);
            return boost::intrusive_ptr(
                new components::collection::operators::operator_match_t(context_, std::move(predicate), limit));
        }
    }

    components::collection::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                                      const components::logical_plan::node_ptr& node,
                                                                      components::logical_plan::limit_t limit) {
        if (node->expressions().empty()) {
            return boost::intrusive_ptr(
                new components::collection::operators::transfer_scan(context.at(node->collection_full_name()), limit));
        } else { //todo: other kinds scan
            auto expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node->expressions()[0]);
            return create_plan_match_(context.at(node->collection_full_name()), *expr, limit);
        }
    }

} // namespace services::collection::planner::impl

namespace services::table::planner::impl {

    bool is_can_index_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq || compare == compare_type::ne || compare == compare_type::gt ||
               compare == compare_type::lt || compare == compare_type::gte || compare == compare_type::lte;
    }

    bool is_can_primary_key_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq;
    }

    components::base::operators::operator_ptr
    create_plan_match_(collection::context_collection_t* context_,
                       const components::expressions::compare_expression_ptr& expr,
                       components::logical_plan::limit_t limit) {
        //if (is_can_primary_key_find_by_predicate(expr->type()) && expr->key().as_string() == "_id") {
        //return boost::intrusive_ptr(new components::table::operators::primary_key_scan(context_));
        //}
        if (context_) {
            if (is_can_index_find_by_predicate(expr->type()) &&
                components::index::search_index(context_->index_engine(), {expr->primary_key()})) {
                return boost::intrusive_ptr(new components::table::operators::index_scan(context_, expr, limit));
            }
            return boost::intrusive_ptr(new components::table::operators::full_scan(context_, expr, limit));
        } else {
            return boost::intrusive_ptr(new components::table::operators::operator_match_t(context_, expr, limit));
        }
    }

    components::base::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                                const components::logical_plan::node_ptr& node,
                                                                components::logical_plan::limit_t limit) {
        if (node->expressions().empty()) {
            return boost::intrusive_ptr(
                new components::table::operators::transfer_scan(context.at(node->collection_full_name()), limit));
        } else { //todo: other kinds scan
            auto expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node->expressions()[0]);
            return create_plan_match_(context.at(node->collection_full_name()), *expr, limit);
        }
    }

} // namespace services::table::planner::impl
