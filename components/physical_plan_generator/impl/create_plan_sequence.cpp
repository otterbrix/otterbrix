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
        // A catalog-write child is a node_insert_t targeting a pg_catalog table; its
        // single node_data_t child carries the ready-made row. Harvest (table_oid,
        // row) for folding into the specialized operator's catalog_write_t vector.
        // Replaces the former node_primitive_write_t::catalog_table_oid()/row().
        components::vector::data_chunk_t&
        catalog_write_row(const components::logical_plan::node_ptr& insert_child) {
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
        // Produce a single operator_create_collection_t that does storage creation,
        // index registration, and all pg_catalog writes in one await_async_and_resume.
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
                                                                        cc->is_disk_storage(),
                                                                        std::move(writes)));
        }

        // DDL create-index sequence: sequence_t(catalog-write node_insert_t × N, create_index_t).
        // Lower to two chained operators:
        //   metadata_op  — writes pg_class/pg_index(indisvalid=false)/pg_depend rows
        //   backfill_op  — registers/creates the engine entry, scans + insert_rows,
        //                  flips pg_index.indisvalid → true
        // metadata_op is wired as backfill_op's left child so the executor's
        // find_waiting_operator walks it first via the same left_/right_
        // traversal used by the multi-clause ALTER TABLE chain below.
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
                                                                                                 ci->name(),
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
                                                                                         di->runtime_index_name(),
                                                                                         std::move(deletes)));
        }

        // ALTER TABLE: rewrite_alter_table emits sequence_t(alter_column_t × N)
        // (op=add | rename, computed=false). Build the operators and chain them
        // as left children (head = innermost step) so the executor's
        // find_waiting_operator walks the chain via left_/right_ traversal.
        // operator_sequence_t doesn't surface its internal steps_ to find_waiting,
        // which would strand multi-clause ALTER TABLE statements after the first
        // async-wait. Computed (relkind='g') alter_columns and op=drop are
        // excluded — they ride other sequence shapes handled by the generic path.
        auto is_chainable_alter = [](const node_ptr& child) {
            if (child->type() != node_type::alter_column_t) {
                return false;
            }
            const auto* ac = static_cast<const node_alter_column_t*>(child.get());
            return !ac->computed() &&
                   (ac->op() == alter_column_op::add || ac->op() == alter_column_op::rename);
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
                    // Build the chain so children[0] (the first user-written clause)
                    // ends up at the DEEPEST nesting level — executor runs left children
                    // first (depth-first via on_execute), so the deepest leaf executes
                    // first. Walk children in reverse, set each as the left child of the
                    // previous; the last-built operator (head) wraps everything and
                    // becomes the root returned to the caller.
                    components::operators::operator_ptr head;
                    for (auto it = node->children().rbegin(); it != node->children().rend(); ++it) {
                        auto op = create_plan(context, function_registry, *it, {}, params);
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

        // Generic case (e.g. CREATE DATABASE/SEQUENCE/VIEW/MACRO/TYPE → sequence_t(catalog-write node_insert_t × N),
        // INSERT-into-relkind='g' → sequence_t(insert, computed_field_register)):
        // build a left-child chain so executor's find_waiting_operator can walk
        // each child via left_/right_ traversal. operator_sequence_t.steps_ is
        // invisible to find_waiting_operator, which would strand async children.
        //
        // Order: depth-first traversal runs LEFT first, then own on_execute_impl,
        // so the DEEPEST-left operator executes first. To preserve children-in-
        // declared-order semantics (child[0] runs before child[1], etc.), iterate
        // FORWARD: first child becomes deepest left, last child becomes outer root.
        // This is critical for INSERT-then-register semantics where register depends
        // on insert having processed the chunk first.
        if (!node->children().empty()) {
            // Skip catalog_resolve_*_t children when a non-resolve consumer
            // (DML/SELECT) is present: they already ran upstream and stamped OIDs
            // on the logical nodes. Chaining a resolve as the DML consumer's left_
            // would make operator_insert read left_->output() and get the resolve
            // metadata chunk instead of the VALUES chunk — wrong shape, 0 rows.
            // EXCEPTION: a resolve-only sub-plan must NOT skip, or the chain is
            // empty; the caller explicitly wants those resolves executed.
            auto is_catalog_resolve = [](node_type t) { return t == node_type::catalog_resolve_t; };
            bool has_non_resolve_child = false;
            for (const auto& child : node->children()) {
                if (child && !is_catalog_resolve(child->type())) {
                    has_non_resolve_child = true;
                    break;
                }
            }
            components::operators::operator_ptr head;
            for (const auto& child : node->children()) {
                if (has_non_resolve_child && child && is_catalog_resolve(child->type())) {
                    continue;
                }
                auto op = create_plan(context, function_registry, child, {}, params);
                if (head) {
                    // op consumes left_ as its DATA source (e.g. operator_insert
                    // reads left_->output() — the case for catalog-write node_insert_t
                    // chains: CREATE TYPE/SEQUENCE/VIEW/MACRO/DATABASE/CONSTRAINT).
                    // Clobbering left_ with the chain predecessor would drop the row
                    // chunk. Attach the predecessor to the free right_ slot instead:
                    // on_execute still runs left (data leaf) → right (head) → self,
                    // preserving child-declared order, and find_waiting_operator walks
                    // both slots. When left_ is free (childless leaf, e.g. a
                    // catalog-delete operator_delete or computed_field_register),
                    // keep the left-chain shape.
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

        std::vector<components::operators::operator_ptr> steps;
        steps.reserve(node->children().size());
        for (const auto& child : node->children()) {
            steps.push_back(create_plan(context, function_registry, child, {}, params));
        }
        return boost::intrusive_ptr(
            new components::operators::operator_sequence_t(context.resource, context.log.clone(), std::move(steps)));
    }

} // namespace services::planner::impl