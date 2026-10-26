#include "create_plan_sequence.hpp"

#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_schema.hpp>
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

    namespace {
        // A table or schema of a connection's database: the connector's CREATE first, then its catalog rows, one step
        // after another; no storage. The oids the planner gave it are the ones its rows carry.
        plan_result_t create_remote(const context_storage_t& context,
                                    const components::compute::function_registry_t& function_registry,
                                    const components::logical_plan::node_ptr& node,
                                    const components::logical_plan::storage_parameters* params) {
            using namespace components::logical_plan;
            const auto& created = node->children().front();
            VALUE_OR_RETURN(auto* connector, connector_for(context, remote_name_of(created->target())));
            plan_result_t backend = components::operators::operator_ptr{};
            if (created->type() == node_type::create_collection_t) {
                const auto* cc = static_cast<const node_create_collection_t*>(created.get());
                std::pmr::vector<components::planner::remote_column_t> columns(context.resource);
                columns.reserve(cc->column_definitions().size());
                for (const auto& column : cc->column_definitions()) {
                    columns.push_back({column.attoid(), column.name(), column.type(), column.is_not_null()});
                }
                const components::planner::remote_table_t table{cc->table_oid(), remote_name_of(cc->target()), columns};
                backend = storage_operator(context.resource,
                                           cc->target().collection.t,
                                           connector->make_create_table(context, table));
            } else {
                const auto* cs = static_cast<const node_create_schema_t*>(created.get());
                backend = storage_operator(
                    context.resource,
                    cs->target().schema.t,
                    connector->make_create_schema(context, {cs->namespace_oid(), remote_name_of(cs->target())}));
            }
            VALUE_OR_RETURN(auto first, std::move(backend));
            auto steps = boost::intrusive_ptr(
                new components::operators::operator_sequence_t(context.resource, context.log.clone()));
            steps->append_step(std::move(first));
            for (std::size_t i = 1; i < node->children().size(); ++i) {
                VALUE_OR_RETURN(auto write, create_plan(context, function_registry, node->children()[i], {}, params));
                steps->append_step(std::move(write));
            }
            return steps;
        }
    } // namespace

    plan_result_t create_plan_sequence(const context_storage_t& context,
                                       const components::compute::function_registry_t& function_registry,
                                       const components::logical_plan::node_ptr& node,
                                       const components::logical_plan::storage_parameters* params) {
        using namespace components::logical_plan;

        // DDL create-table sequence: sequence_t(create_collection_t, catalog-write node_insert_t×N).
        if (!node->children().empty() && node->children().front()->type() == node_type::create_collection_t) {
            auto* cc = static_cast<node_create_collection_t*>(node->children().front().get());
            // A table of a connection's database: the target, completed with the place, names the connection.
            if (!cc->target().connection.t.empty()) {
                return create_remote(context, function_registry, node, params);
            }
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

        if (!node->children().empty() && node->children().front()->type() == node_type::create_schema_t) {
            return create_remote(context, function_registry, node, params);
        }

        // DDL create-index sequence: sequence_t(catalog-write node_insert_t × N, create_index_t) lowers to two
        // chained operators — metadata_op (pg_class/pg_index(indisvalid=false)/pg_depend writes) wired as
        // backfill_op's left child (registers the engine, scans + insert_rows, flips indisvalid) — so the
        // executor walks metadata first.
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

        // Every other sequence (ALTER TABLE's clauses, CREATE DATABASE / SEQUENCE / VIEW / MACRO / TYPE, INSERT into a
        // computed table): its steps in the declared order, each a plan of its own. A computed-field register reads
        // the INSERT before it (left_->written(), left_->output()), so that pair stays one step.
        auto steps =
            boost::intrusive_ptr(new components::operators::operator_sequence_t(context.resource, context.log.clone()));
        for (const auto& child : node->children()) {
            // A child that fails to lower refuses the whole statement: running past it would run a truncated DDL.
            VALUE_OR_RETURN(auto op, create_plan(context, function_registry, child, {}, params));
            if (op->type() == components::operators::operator_type::computed_field_register &&
                !steps->steps().empty()) {
                auto insert = steps->steps().back();
                steps->drop_last_step();
                op->set_children(std::move(insert), nullptr);
            }
            steps->append_step(std::move(op));
        }
        if (steps->steps().size() == 1) {
            return steps->steps().front();
        }
        return steps;
    }

} // namespace services::planner::impl