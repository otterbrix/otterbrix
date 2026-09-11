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

        // Index info is scoped per table_oid; another table's indexes from the same statement never apply.
        [[maybe_unused]] bool can_use_index(const context_storage_t& context,
                                            components::catalog::oid_t table_oid,
                                            const expr::compare_expression_t& comp,
                                            bool& key_on_left) {
            if (expr::is_union_compare_condition(comp.type())) {
                return false;
            }
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
            if (!context.parameters) {
                return false;
            }

            if (std::holds_alternative<expr::key_t>(comp.left()) &&
                std::holds_alternative<core::parameter_id_t>(comp.right())) {
                const auto& key = std::get<expr::key_t>(comp.left());
                const bool range = is_range_compare(comp.type());
                if (context.has_index_on(table_oid, key) &&
                    (!range || context.has_index_on_with_other_type(table_oid,
                                                                   key,
                                                                   components::logical_plan::index_type::hashed))) {
                    key_on_left = true;
                    return true;
                }
            }
            if (std::holds_alternative<core::parameter_id_t>(comp.left()) &&
                std::holds_alternative<expr::key_t>(comp.right())) {
                const auto& key = std::get<expr::key_t>(comp.right());
                const bool range = is_range_compare(comp.type());
                if (context.has_index_on(table_oid, key) &&
                    (!range || context.has_index_on_with_other_type(table_oid,
                                                                    key,
                                                                    components::logical_plan::index_type::hashed))) {
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
                // sides, not just the expression side. UDF-free only — the disk agent can't resolve a
                // UDF (components/expressions/udf_references.hpp).
                const bool expr_op_other = ((is_expr(comp_expr->left()) && !is_expr(comp_expr->right())) ||
                                            (is_expr(comp_expr->right()) && !is_expr(comp_expr->left()))) &&
                                           !expr::param_references_udf(comp_expr->left()) &&
                                           !expr::param_references_udf(comp_expr->right());
                if (!col_op_const && !col_op_col && !expr_op_other) {
                    return false;
                }
            }
            return true;
        }

        // Where the indexed column lands in a fetched chunk, so index_scan can re-read it off the row
        // and re-apply the comparison the index answered. -1 when the table's columns were not
        // resolved into this context, or the key names none of them (a path into a nested value):
        // the scan then runs without the recheck rather than refusing every answer.
        int64_t indexed_chunk_position(const context_storage_t& context,
                                       components::catalog::oid_t table_oid,
                                       const components::expressions::key_t& key) {
            const auto* metadata = context.table_metadata_for(table_oid);
            if (metadata == nullptr) {
                return -1;
            }
            const auto name = key.as_string();
            for (const auto& column : metadata->columns) {
                if (column.chunk_position >= 0 && column.attname == name) {
                    return column.chunk_position;
                }
            }
            return -1;
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
                    if (!comp_expr->is_union()) {
                        bool key_on_left = true;
                        if (can_use_index(context, table_oid, *comp_expr, key_on_left)) {
                            auto& key = key_on_left ? std::get<expr::key_t>(comp_expr->left())
                                                    : std::get<expr::key_t>(comp_expr->right());
                            auto param_id = key_on_left ? std::get<core::parameter_id_t>(comp_expr->right())
                                                        : std::get<core::parameter_id_t>(comp_expr->left());
                            auto& value = get_parameter(context.parameters, param_id);
                            auto ctype = key_on_left ? comp_expr->type() : mirror_compare(comp_expr->type());
                            auto preferred_index_type = context.preferred_index_type_for_compare(table_oid, key, ctype);
                            return boost::intrusive_ptr(
                                new components::operators::index_scan(context.resource,
                                                                      context.log.clone(),
                                                                      table_oid,
                                                                      key,
                                                                      value,
                                                                      ctype,
                                                                      preferred_index_type,
                                                                      limit,
                                                                      projected_cols,
                                                                      indexed_chunk_position(context, table_oid, key)));
                        }
                    }

                    return boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                     context.log.clone(),
                                                                                     table_oid,
                                                                                     comp_expr,
                                                                                     limit,
                                                                                     projected_cols));
                } else {
                    // The inner full_scan is unlimited because operator_match filters above it; capping
                    // here could starve the filter of matching rows. operator_match's own limit is an
                    // advisory hint under operator_limit for SELECT, but the authoritative affected-row
                    // bound for DML (no operator_limit over a DML root).
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
            // relkind::computed ('g') columns are read live by chunk_position, resolved at resolve-table
            // time; relkind::regular ('r') tables use the caller's projected_cols (column_pruning output).
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
            }
            // node->source() — not the oid, which is INVALID_OID either way — says which absent-table
            // case this is. No default arm: a new match_source value must get a case here or the build stops.
            switch (static_cast<const components::logical_plan::node_match_t*>(node.get())->source()) {
                case components::logical_plan::match_source::none:
                    // Emits a synthetic 1-row placeholder batch (transfer_scan::source_next), so it needs a
                    // valid resource — the node's own, as create_plan_aggregate's no-table fallback also
                    // does — not nullptr.
                    return boost::intrusive_ptr(new components::operators::transfer_scan(node->resource(),
                                                                                         node->table_oid(),
                                                                                         limit,
                                                                                         std::move(effective_cols)));
                case components::logical_plan::match_source::table:
                    // Validation should refuse a named table with an unresolved oid before plan generation;
                    // returning nullptr here (not the `none` sentinel) stops a regression from silently
                    // answering a synthetic row for a nonexistent table. A null root surfaces as
                    // create_physical_plan_error downstream.
                    return nullptr;
            }
            return nullptr; // unreachable: the switch above covers every match_source
        } else {
            const auto* match_node = static_cast<const components::logical_plan::node_match_t*>(node.get());
            return create_plan_match_(context,
                                      match_node->table_oid(),
                                      match_node->expressions()[0],
                                      limit,
                                      projected_cols);
        }
    }

    // HAVING has no window of its own (the outer operator_limit is the sole window), so
    // create_plan_having takes no limit parameter. context.resource is always non-null and
    // outlives the operator, so there's no null-resource sentinel here.
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
