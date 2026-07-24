#include "create_plan_match.hpp"

#include "index_selection_helpers.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/udf_references.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_having.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>

namespace services::planner::impl {

    namespace {

        namespace expr = components::expressions;

        bool is_range_compare(expr::compare_type type) {
            return type == expr::compare_type::lt || type == expr::compare_type::lte ||
                   type == expr::compare_type::gt || type == expr::compare_type::gte;
        }

        // Check if this compare expression can use an index scan
        [[maybe_unused]] bool
        can_use_index(const context_storage_t& context, const expr::compare_expression_t& comp, bool& key_on_left) {
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
                const bool range = is_range_compare(comp.type());
                if (context.has_index_on(key) &&
                    (!range ||
                     context.has_index_on_with_other_type(key, components::logical_plan::index_type::hashed))) {
                    key_on_left = true;
                    return true;
                }
            }
            // Check key_t on right, parameter_id_t on left (symmetric)
            if (std::holds_alternative<core::parameter_id_t>(comp.left()) &&
                std::holds_alternative<expr::key_t>(comp.right())) {
                const auto& key = std::get<expr::key_t>(comp.right());
                const bool range = is_range_compare(comp.type());
                if (context.has_index_on(key) &&
                    (!range ||
                     context.has_index_on_with_other_type(key, components::logical_plan::index_type::hashed))) {
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
            // A regex leaf (LIKE / ILIKE, pattern pre-converted by like_to_regex) is pushable to disk:
            // constant_filter_t compares it via RE2 (case-insensitively when regex_icase). It still needs the
            // `column OP constant` shape enforced below; an exotic non-key/expression operand falls back to
            // operator_match. do_not_fold() (correlated / sub-query-array compares) stays in-memory.
            if (comp_expr->do_not_fold()) {
                return false;
            }
            for (const auto& child : comp_expr->children()) {
                if (!is_pure_compare(child)) {
                    return false;
                }
            }

            // A LEAF compare (not a union AND/OR of sub-compares) is pushable into a disk
            // table_filter_t only in the shapes (A)/(B)/(C) checked below; any other shape
            // MUST be evaluated by operator_match instead. (union compare expressions carry
            // nullptr in the left/right slots — handled above.)
            if (!is_union_compare_condition(comp_expr->type())) {
                // param_storage is variant<parameter_id_t, key_t, expression_ptr>: a bound
                // parameter is the alternative that is neither a key nor a nested expression. Uses the
                // is_key/is_expr/is_parameter accessors, not std::holds_alternative.
                const bool no_expr = !is_expr(comp_expr->left()) && !is_expr(comp_expr->right());
                // (A) column OP constant: exactly one operand a key, the other a bound parameter.
                const bool col_op_const = no_expr && (is_key(comp_expr->left()) != is_key(comp_expr->right()));
                // (B) column-vs-column: both operands columns, a plain comparison -> a column_column_filter_t
                // (fetch both values, compare per row). regex/any/all with two keys are NOT this shape.
                const auto t = comp_expr->type();
                const bool plain_cmp = t == compare_type::eq || t == compare_type::ne || t == compare_type::lt ||
                                       t == compare_type::lte || t == compare_type::gt || t == compare_type::gte;
                const bool col_op_col = no_expr && plain_cmp && is_key(comp_expr->left()) && is_key(comp_expr->right());
                // (C) f(column...) OP constant: one operand a function/arithmetic expression over column(s),
                // the other a bound parameter -> an expression_filter_t evaluated per row on the agent. Only
                // when UDF-free (the disk agent cannot resolve a UDF, see
                // components/expressions/udf_references.hpp).
                const bool expr_op_const = ((is_expr(comp_expr->left()) && is_parameter(comp_expr->right())) ||
                                            (is_expr(comp_expr->right()) && is_parameter(comp_expr->left()))) &&
                                           !expr::param_references_udf(comp_expr->left()) &&
                                           !expr::param_references_udf(comp_expr->right());
                if (!col_op_const && !col_op_col && !expr_op_const) {
                    return false;
                }
            }
            return true;
        }

        components::operators::operator_ptr create_plan_match_(const context_storage_t& context,
                                                               components::catalog::oid_t table_oid,
                                                               const components::expressions::expression_ptr& expr,
                                                               components::logical_plan::limit_t limit,
                                                               const std::vector<size_t>& projected_cols) {
            if (context.has_table_oid(table_oid)) {
                // TODO: function_expr in scans
                if (is_pure_compare(expr)) {
                    auto comp_expr = reinterpret_cast<const expr::compare_expression_ptr&>(expr);
                    // Index selection: detect if an index is available for this predicate.
                    if (!comp_expr->is_union()) {
                        bool key_on_left = true;
                        if (can_use_index(context, *comp_expr, key_on_left)) {
                            auto& key = key_on_left ? std::get<expr::key_t>(comp_expr->left())
                                                    : std::get<expr::key_t>(comp_expr->right());
                            auto param_id = key_on_left ? std::get<core::parameter_id_t>(comp_expr->right())
                                                        : std::get<core::parameter_id_t>(comp_expr->left());
                            auto& value = get_parameter(context.parameters, param_id);
                            auto ctype = key_on_left ? comp_expr->type() : mirror_compare(comp_expr->type());
                            auto preferred_index_type = context.preferred_index_type_for_compare(key, ctype);
                            return boost::intrusive_ptr(new components::operators::index_scan(context.resource,
                                                                                              context.log.clone(),
                                                                                              table_oid,
                                                                                              key,
                                                                                              value,
                                                                                              ctype,
                                                                                              preferred_index_type,
                                                                                              limit));
                        }
                    }

                    return boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                     context.log.clone(),
                                                                                     table_oid,
                                                                                     comp_expr,
                                                                                     limit,
                                                                                     projected_cols));
                } else {
                    // Non-pushable predicate (column-vs-column, a function, a non-representable
                    // shape): the inner full_scan reads ALL raw rows (unlimit) because the filter
                    // runs ABOVE it in operator_match; capping the inner scan at the read-cap
                    // could starve the filter of matching rows. operator_match carries the
                    // read-cap and bounds the FILTERED (matched) stream — for SELECT it is an
                    // advisory hint under operator_limit, for DML …WHERE f(x) LIMIT n it is the
                    // AUTHORITATIVE affected-row bound (no operator_limit over a DML root).
                    auto match_operator =
                        boost::intrusive_ptr(new components::operators::operator_match_t(context.resource,
                                                                                         context.log.clone(),
                                                                                         expr,
                                                                                         limit));
                    match_operator->set_children(boost::intrusive_ptr(
                        new components::operators::full_scan(context.resource,
                                                             context.log.clone(),
                                                             table_oid,
                                                             nullptr,
                                                             components::logical_plan::limit_t::unlimit(),
                                                             projected_cols)));
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
        static const std::vector<size_t> empty_cols;
        return create_plan_match(context, node, limit, empty_cols);
    }

    components::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit,
                                                          const std::vector<size_t>& projected_cols) {
        if (node->expressions().empty()) {
            // Build projected_cols (storage chunk indices). For relkind='g' read
            // live columns by their chunk_position (resolved at resolve-table time).
            // For relkind='r' use caller's projected_cols (column_pruning output).
            std::vector<size_t> effective_cols;
            if (const auto* md = context.table_metadata_for(node->table_oid())) {
                if (md->relkind == components::catalog::relkind::computed) {
                    effective_cols.reserve(md->columns.size());
                    for (const auto& col : md->columns) {
                        if (col.chunk_position >= 0) {
                            effective_cols.push_back(static_cast<size_t>(col.chunk_position));
                        }
                    }
                } else {
                    effective_cols = projected_cols;
                }
            }
            if (context.has_table_oid(node->table_oid())) {
                return boost::intrusive_ptr(new components::operators::transfer_scan(context.resource,
                                                                                     node->table_oid(),
                                                                                     limit,
                                                                                     std::move(effective_cols)));
            } else {
                // No-table sentinel scan (INVALID_OID, e.g. a no-FROM SELECT). It is a
                // SOURCE that emits one synthetic 1-row placeholder batch (see
                // transfer_scan::source_next), so it needs a VALID resource to allocate
                // that batch on — use the logical node's own resource (mirrors
                // create_plan_aggregate's no-table fallback), not nullptr.
                return boost::intrusive_ptr(new components::operators::transfer_scan(node->resource(),
                                                                                     node->table_oid(),
                                                                                     limit,
                                                                                     std::move(effective_cols)));
            }
        } else {
            const auto* match_node = static_cast<const components::logical_plan::node_match_t*>(node.get());
            return create_plan_match_(context,
                                      match_node->table_oid(),
                                      match_node->expressions()[0],
                                      limit,
                                      projected_cols);
        }
    }

    // Lower a node_having_t to a dedicated operator_having_t filtering the group's output. HAVING
    // has no window of its own (the outer operator_limit is the sole window), so no limit parameter.
    // context.resource is always non-null and outlives the operator, so no null-resource sentinel.
    components::operators::operator_ptr create_plan_having(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node) {
        if (node->expressions().empty()) {
            return nullptr;
        }
        return boost::intrusive_ptr(new components::operators::operator_having_t(context.resource,
                                                                                 context.log.clone(),
                                                                                 node->expressions()[0]));
    }

} // namespace services::planner::impl
