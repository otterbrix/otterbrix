#include "create_plan_sequence.hpp"

#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/physical_plan/operators/operator_create_collection.hpp>
#include <components/physical_plan/operators/operator_create_index_backfill.hpp>
#include <components/physical_plan/operators/operator_create_index_metadata.hpp>
#include <components/physical_plan/operators/operator_drop_index.hpp>
#include <components/physical_plan/operators/operator_sequence.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    namespace {
        // Assumes insert_child is a node_insert_t into pg_catalog with a single node_data_t child
        // holding the row (unchecked here).
        components::vector::data_chunk_t& catalog_write_row(const components::logical_plan::node_ptr& insert_child) {
            using namespace components::logical_plan;
            auto* ins = static_cast<node_insert_t*>(insert_child.get());
            auto* data = static_cast<node_data_t*>(ins->children().front().get());
            return data->data_chunk();
        }
    } // namespace

    components::operators::operator_ptr
    create_plan_sequence(const context_storage_t& context,
                         const components::compute::function_registry_t& function_registry,
                         const components::logical_plan::node_ptr& node,
                         const components::logical_plan::storage_parameters* params) {
        using namespace components::logical_plan;

        // DDL create-table sequence: sequence_t(create_collection_t, catalog-write node_insert_t×N).
        if (!node->children().empty() && node->children().front()->type() == node_type::create_collection_t) {
            auto* cc = static_cast<node_create_collection_t*>(node->children().front().get());
            std::vector<components::operators::operator_create_collection_t::catalog_write_t> writes;
            writes.reserve(node->children().size() - 1);
            for (std::size_t i = 1; i < node->children().size(); ++i) {
                writes.emplace_back(node->children()[i]->table_oid(),
                                    std::move(catalog_write_row(node->children()[i])));
            }
            return boost::intrusive_ptr(
                new components::operators::operator_create_collection_t(context.resource,
                                                                        context.log.clone(),
                                                                        cc->table_oid(),
                                                                        cc->namespace_oid(),
                                                                        cc->column_definitions(),
                                                                        std::move(writes)));
        }

        // DDL create-index sequence: sequence_t(catalog-write node_insert_t × N, create_index_t) lowers to two
        // chained operators — metadata_op (pg_class/pg_index(indisvalid=false)/pg_depend writes) wired as
        // backfill_op's left child (registers the engine, scans + insert_rows, flips indisvalid) — so the
        // executor walks metadata first via the same left_/right_ traversal the ALTER TABLE chain uses below.
        if (!node->children().empty() && node->children().back()->type() == node_type::create_index_t) {
            auto* ci = static_cast<node_create_index_t*>(node->children().back().get());
            std::vector<components::operators::operator_create_index_metadata_t::catalog_write_t> writes;
            writes.reserve(node->children().size() - 1);
            for (std::size_t i = 0; i + 1 < node->children().size(); ++i) {
                writes.emplace_back(node->children()[i]->table_oid(),
                                    std::move(catalog_write_row(node->children()[i])));
            }
            auto metadata_op =
                boost::intrusive_ptr(new components::operators::operator_create_index_metadata_t(context.resource,
                                                                                                 context.log.clone(),
                                                                                                 std::move(writes)));
            auto backfill_op =
                boost::intrusive_ptr(new components::operators::operator_create_index_backfill_t(context.resource,
                                                                                                 context.log.clone(),
                                                                                                 ci->type(),
                                                                                                 ci->keys(),
                                                                                                 ci->table_oid(),
                                                                                                 ci->index_oid(),
                                                                                                 ci->indkey()));
            backfill_op->set_children(metadata_op, nullptr);
            return backfill_op;
        }

        // DDL drop-index sequence: sequence_t(catalog-delete node_delete_t × N, drop_index_t).
        // Lower to a single operator_drop_index_t that owns both the catalog scrub
        // (collected from the catalog-delete leaves) and the index-actor
        // teardown. DROP INDEX has no useful intermediate state to expose — the
        // metadata/runtime split that motivates the CREATE INDEX two-operator
        // design has no analogue here.
        if (!node->children().empty() && node->children().back()->type() == node_type::drop_t &&
            static_cast<node_drop_t*>(node->children().back().get())->kind() == drop_target_kind::index) {
            auto* di = static_cast<node_drop_t*>(node->children().back().get());
            std::vector<components::operators::operator_drop_index_t::catalog_delete_t> deletes;
            deletes.reserve(node->children().size() - 1);
            for (std::size_t i = 0; i + 1 < node->children().size(); ++i) {
                auto* pd = static_cast<node_delete_t*>(node->children()[i].get());
                deletes.push_back({pd->table_oid(), pd->oid_col_idx(), pd->target_oid()});
            }
            return boost::intrusive_ptr(new components::operators::operator_drop_index_t(context.resource,
                                                                                         context.log.clone(),
                                                                                         di->table_oid(),
                                                                                         di->index_oid(),
                                                                                         std::move(deletes)));
        }

        // ALTER TABLE: chain rewrite_alter_table's sequence_t(alter_column_t × N) as left children (head =
        // innermost step) so the executor walks it via left_/right_ traversal — a sequence wrapper carries no
        // steps of its own and would strand a multi-clause ALTER after the first async step. Computed
        // (relkind='g') and op=drop clauses are excluded; they ride the generic path below.
        auto is_chainable_alter = [](const node_ptr& child) {
            if (child->type() != node_type::alter_column_t) {
                return false;
            }
            const auto* ac = static_cast<const node_alter_column_t*>(child.get());
            return !ac->computed() && (ac->op() == alter_column_op::add || ac->op() == alter_column_op::rename);
        };
        if (!node->children().empty()) {
            if (is_chainable_alter(node->children().front())) {
                bool all_alter = true;
                for (const auto& child : node->children()) {
                    if (!is_chainable_alter(child)) {
                        all_alter = false;
                        break;
                    }
                }
                if (all_alter) {
                    // children[0] (first user-written clause) must end up at the deepest nesting level — the
                    // executor runs left children first, so walk forward, wrapping the chain built so far as
                    // the new operator's left child. A reverse walk would put children[0] at the root instead,
                    // running a multi-clause ALTER's clauses back to front — user-visible, since two ADD
                    // COLUMNs decide their attnum order by who runs first.
                    components::operators::operator_ptr head;
                    for (const auto& child : node->children()) {
                        auto op = create_plan(context, function_registry, child, {}, params);
                        if (!op) {
                            // A child that fails to lower refuses the whole statement:
                            // a null root maps to create_physical_plan_error in the
                            // executor. Chaining past it would run a truncated ALTER.
                            return {};
                        }
                        if (head) {
                            // op wraps `head` (op runs after head's chain executes).
                            op->set_children(head, nullptr);
                        }
                        head = op;
                    }
                    return head;
                }
            }
        }

        // Generic case (CREATE DATABASE/SEQUENCE/VIEW/MACRO/TYPE, INSERT-into-relkind='g', etc.): chain
        // children as left children so the executor walks each via left_/right_ traversal —
        // operator_sequence_t carries no steps of its own. Iterate FORWARD (first child becomes deepest left,
        // last becomes outer root) to preserve declared order: critical for INSERT-then-register, where
        // register depends on insert having processed the chunk first.
        if (!node->children().empty()) {
            components::operators::operator_ptr head;
            for (const auto& child : node->children()) {
                auto op = create_plan(context, function_registry, child, {}, params);
                if (!op) {
                    // Unchecked, the loop would silently drop a null first child (statement runs with a step
                    // missing) or dereference a null later child via op->left() outright.
                    return {};
                }
                if (head) {
                    // op consumes left_ as its data source for catalog-write chains (e.g. operator_insert reads
                    // left_->output()), so clobbering left_ with the chain predecessor would drop the row
                    // chunk — attach the predecessor to the free right_ slot instead (executor still runs
                    // left → right → self). When left_ is already free (a childless leaf), keep the left-chain shape.
                    if (op->left()) {
                        op->set_children(op->left(), head);
                    } else {
                        op->set_children(head, nullptr);
                    }
                }
                head = op;
            }
            return head;
        }

        // Reached only when node has no children (all child-bearing shapes return
        // above): emit the childless no-op sequence fallback.
        return boost::intrusive_ptr(
            new components::operators::operator_sequence_t(context.resource, context.log.clone()));
    }

} // namespace services::planner::impl