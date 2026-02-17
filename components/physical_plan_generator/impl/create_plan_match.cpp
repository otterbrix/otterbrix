#include "create_plan_match.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>

namespace services::planner::impl {

    // Note: Index selection is handled at execution time by the executor
    // (via intercept_scan_/index_engine), not during plan generation.
    // Plan always creates full_scan/transfer_scan; executor may optimize
    // to index_scan based on available indexes.
    namespace {
        components::operators::operator_ptr
        create_plan_match_(const context_storage_t& context,
                           const collection_full_name_t& coll_name,
                           const components::expressions::compare_expression_ptr& expr,
                           components::logical_plan::limit_t limit) {
            if (context.has_collection(coll_name)) {
                return boost::intrusive_ptr(new components::operators::full_scan(
                    context.resource, context.log.clone(), coll_name, expr, limit));
            } else {
                return boost::intrusive_ptr(new components::operators::operator_match_t(
                    nullptr, log_t{}, expr, limit));
            }
        }
    } // namespace

    components::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                                const components::logical_plan::node_ptr& node,
                                                                components::logical_plan::limit_t limit) {
        if (node->expressions().empty()) {
            if (context.has_collection(node->collection_full_name())) {
                return boost::intrusive_ptr(
                    new components::operators::transfer_scan(
                        context.resource, node->collection_full_name(), limit));
            } else {
                return boost::intrusive_ptr(
                    new components::operators::transfer_scan(
                        nullptr, node->collection_full_name(), limit));
            }
        } else {
            return create_plan_match_(context, node->collection_full_name(), node->expressions()[0], limit);
        }
    }

} // namespace services::planner::impl
