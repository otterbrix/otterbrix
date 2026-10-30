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
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan/operators/scan/transfer_scan.hpp>

namespace services::planner::impl {

    namespace {

        namespace expr = components::expressions;

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
                // The scan is unlimited because operator_match filters above it; capping it could starve the
                // filter of matching rows. operator_match's own limit is an advisory hint under operator_limit
                // for SELECT, but the authoritative affected-row bound for DML (no operator_limit over a DML root).
                auto match_operator = boost::intrusive_ptr(
                    new components::operators::operator_match_t(context.resource, context.log.clone(), expr, cap));
                VALUE_OR_RETURN(auto types, page_types_of(context, table_oid, projected_cols));
                auto scan = boost::intrusive_ptr(
                    new components::operators::transfer_scan(context.resource,
                                                             table_oid,
                                                             std::move(address),
                                                             full_name_of(context, table_oid),
                                                             components::logical_plan::row_cap_t::uncapped(),
                                                             projected_cols));
                scan->set_page_types(std::move(types));
                match_operator->set_children(std::move(scan));
                return match_operator;
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
