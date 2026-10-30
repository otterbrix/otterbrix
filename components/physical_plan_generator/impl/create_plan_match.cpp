#include "create_plan_match.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>

#include <components/catalog/catalog_codes.hpp>
#include <components/expressions/compare_expression.hpp>
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

        bool is_pure_compare(const components::expressions::expression_ptr& expr) {
            using namespace components::expressions;
            if (expr->group() != expression_group::compare) {
                return false;
            }
            const auto* comp_expr = static_cast<const compare_expression_t*>(expr.get());
            // do_not_fold() (correlated / sub-query-array compares) must stay in-memory, never pushed to disk.
            if (comp_expr->do_not_fold()) {
                return false;
            }
            for (const auto& child : comp_expr->children()) {
                if (!is_pure_compare(child)) {
                    return false;
                }
            }

            // Union compare expressions carry nullptr in left/right (handled above).
            if (!is_union_compare_condition(comp_expr->type())) {
                // param_storage is variant<key_t, expression_ptr, parameter_id_t>; no_expr leaves the
                // operand as a key or a bound parameter.
                const bool no_expr = !is_expr(comp_expr->left()) && !is_expr(comp_expr->right());
                const bool col_op_const = no_expr && (is_key(comp_expr->left()) != is_key(comp_expr->right()));
                const auto t = comp_expr->type();
                const bool plain_cmp = t == compare_type::eq || t == compare_type::ne || t == compare_type::lt ||
                                       t == compare_type::lte || t == compare_type::gt || t == compare_type::gte;
                const bool col_op_col = no_expr && plain_cmp && is_key(comp_expr->left()) && is_key(comp_expr->right());
                // The non-expression operand may be a column too: this filter resolves paths on both
                // sides, not just the expression side.
                const bool expr_op_other = (is_expr(comp_expr->left()) && !is_expr(comp_expr->right())) ||
                                           (is_expr(comp_expr->right()) && !is_expr(comp_expr->left()));
                if (!col_op_const && !col_op_col && !expr_op_other) {
                    return false;
                }
            }
            return true;
        }

        // Where the indexed column lands in a fetched chunk of `projected_cols` (empty: every column), so
        // index_scan can re-read it off the row and re-apply the comparison the index answered. -1 when
        // the table's columns were not resolved into this context, or the key names none of them (a path
        // into a nested value): the scan then runs without the recheck rather than refusing every answer.
        int64_t indexed_chunk_position(const context_storage_t& context,
                                       components::catalog::oid_t table_oid,
                                       const components::expressions::key_t& key,
                                       const std::vector<size_t>& projected_cols) {
            const auto* metadata = context.table_metadata_for(table_oid);
            if (metadata == nullptr) {
                return -1;
            }
            const auto name = key.as_string();
            for (const auto& column : metadata->columns) {
                if (column.chunk_position < 0 || column.attname != name) {
                    continue;
                }
                if (projected_cols.empty()) {
                    return column.chunk_position;
                }
                auto position = static_cast<size_t>(column.chunk_position);
                auto found = std::find(projected_cols.begin(), projected_cols.end(), position);
                // Pruning keeps every column the WHERE reads, the indexed one included.
                assert(found != projected_cols.end() && "index_scan: the indexed column was pruned from its scan");
                return static_cast<int64_t>(found - projected_cols.begin());
            }
            return -1;
        }

        plan_result_t create_plan_match_(const context_storage_t& context,
                                         const components::logical_plan::node_match_t& match,
                                         components::logical_plan::row_cap_t cap,
                                         const std::vector<size_t>& requested_cols) {
            const auto table_oid = match.table_oid();
            const auto& expr = match.expressions().front();
            if (context.has_table_oid(table_oid)) {
                const auto projected_cols = scan_columns_of(context, table_oid, requested_cols);
                if (const auto& index = match.index(); index.has_value()) {
                    const auto& compare = static_cast<const expr::compare_expression_t&>(*expr);
                    const auto& key = expr::as_key(index->key_on_left ? compare.left() : compare.right());
                    const auto param_id = expr::as_parameter(index->key_on_left ? compare.right() : compare.left());
                    return boost::intrusive_ptr(new components::operators::index_scan(
                        context.resource,
                        context.log.clone(),
                        table_oid,
                        key,
                        param_id,
                        index->compare,
                        index->type,
                        cap,
                        projected_cols,
                        indexed_chunk_position(context, table_oid, key, projected_cols)));
                }
                VALUE_OR_RETURN(auto address, storage_address_of(context, table_oid));
                // Only the disk evaluates a predicate while it reads; any other storage is read whole, filtered above.
                if (is_pure_compare(expr) && !address.is_remote()) {
                    auto comp_expr = boost::static_pointer_cast<expr::compare_expression_t>(expr);
                    auto scan =
                        boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                  context.log.clone(),
                                                                                  table_oid,
                                                                                  std::move(address),
                                                                                  full_name_of(context, table_oid),
                                                                                  comp_expr,
                                                                                  cap,
                                                                                  projected_cols));
                    VALUE_OR_RETURN(auto types, page_types_of(context, table_oid, projected_cols));
                    scan->set_page_types(std::move(types));
                    return scan;
                } else {
                    // The inner full_scan is unlimited because operator_match filters above it; capping
                    // here could starve the filter of matching rows. operator_match's own limit is an
                    // advisory hint under operator_limit for SELECT, but the authoritative affected-row
                    // bound for DML (no operator_limit over a DML root).
                    auto match_operator = boost::intrusive_ptr(
                        new components::operators::operator_match_t(context.resource, context.log.clone(), expr, cap));
                    auto scan = boost::intrusive_ptr(
                        new components::operators::full_scan(context.resource,
                                                             context.log.clone(),
                                                             table_oid,
                                                             std::move(address),
                                                             full_name_of(context, table_oid),
                                                             nullptr,
                                                             components::logical_plan::row_cap_t::uncapped(),
                                                             projected_cols));
                    VALUE_OR_RETURN(auto types, page_types_of(context, table_oid, projected_cols));
                    scan->set_page_types(std::move(types));
                    match_operator->set_children(std::move(scan));
                    return match_operator;
                }
            } else {
                return boost::intrusive_ptr(new components::operators::operator_match_t(nullptr, log_t{}, expr, cap));
            }
        }
    } // namespace

    plan_result_t create_plan_match(const context_storage_t& context,
                                    const components::logical_plan::node_ptr& node,
                                    components::logical_plan::row_cap_t cap) {
        static const std::vector<size_t> empty_cols;
        return create_plan_match(context, node, cap, empty_cols);
    }

    plan_result_t create_plan_match(const context_storage_t& context,
                                    const components::logical_plan::node_ptr& node,
                                    components::logical_plan::row_cap_t cap,
                                    const std::vector<size_t>& projected_cols) {
        if (node->expressions().empty()) {
            auto effective_cols = scan_columns_of(context, node->table_oid(), projected_cols);
            if (context.has_table_oid(node->table_oid())) {
                VALUE_OR_RETURN(auto types, page_types_of(context, node->table_oid(), effective_cols));
                VALUE_OR_RETURN(auto address, storage_address_of(context, node->table_oid()));
                auto scan = boost::intrusive_ptr(
                    new components::operators::transfer_scan(context.resource,
                                                             node->table_oid(),
                                                             std::move(address),
                                                             full_name_of(context, node->table_oid()),
                                                             cap,
                                                             std::move(effective_cols)));
                scan->set_page_types(std::move(types));
                return scan;
            }
            // node->source() — not the oid, which is INVALID_OID either way — says which absent-table
            // case this is. No default arm: a new match_source value must get a case here or the build stops.
            switch (static_cast<const components::logical_plan::node_match_t*>(node.get())->source()) {
                case components::logical_plan::match_source::none:
                    // Emits a synthetic 1-row placeholder batch (transfer_scan::source_next).
                    return boost::intrusive_ptr(
                        new components::operators::transfer_scan(context.resource,
                                                                 node->table_oid(),
                                                                 components::table::storage_address_t{},
                                                                 std::pmr::string{context.resource},
                                                                 cap,
                                                                 std::move(effective_cols)));
                case components::logical_plan::match_source::table: {
                    // Validation should refuse a named table with an unresolved oid before plan generation;
                    // refusing here (not the `none` sentinel) stops a regression from silently answering a
                    // synthetic row for a nonexistent table.
                    const auto* match_node = static_cast<const components::logical_plan::node_match_t*>(node.get());
                    return unresolved_table_refusal(context.resource, match_node->target());
                }
            }
            assert(false && "unknown source of a WHERE scan");
            std::abort();
        } else {
            return create_plan_match_(context,
                                      static_cast<const components::logical_plan::node_match_t&>(*node),
                                      cap,
                                      projected_cols);
        }
    }

    // HAVING has no window of its own (the outer operator_limit is the sole window), so
    // create_plan_having takes no limit parameter. context.resource is always non-null and
    // outlives the operator, so there's no null-resource sentinel here.
    plan_result_t create_plan_having(const context_storage_t& context, const components::logical_plan::node_ptr& node) {
        assert(!node->expressions().empty() && "HAVING carries a predicate");
        return boost::intrusive_ptr(new components::operators::operator_having_t(context.resource,
                                                                                 context.log.clone(),
                                                                                 node->expressions()[0]));
    }

} // namespace services::planner::impl
