#include "create_plan.hpp"

#include <cassert>
#include <cstdlib>

#include "impl/create_plan_abort_transaction.hpp"
#include "impl/create_plan_aggregate.hpp"
#include "impl/create_plan_allocate_oids.hpp"
#include "impl/create_plan_alter_column_add.hpp"
#include "impl/create_plan_alter_column_drop.hpp"
#include "impl/create_plan_alter_column_rename.hpp"
#include "impl/create_plan_begin_transaction.hpp"
#include "impl/create_plan_check_constraint.hpp"
#include "impl/create_plan_checkpoint.hpp"
#include "impl/create_plan_commit_transaction.hpp"
#include "impl/create_plan_computed_field_register.hpp"
#include "impl/create_plan_computed_field_unregister.hpp"
#include "impl/create_plan_create_matview.hpp"
#include "impl/create_plan_cte_scan.hpp"
#include "impl/create_plan_data.hpp"
#include "impl/create_plan_delete.hpp"
#include "impl/create_plan_dynamic_cascade_delete.hpp"
#include "impl/create_plan_fk_cascade.hpp"
#include "impl/create_plan_fk_check.hpp"
#include "impl/create_plan_function.hpp"
#include "impl/create_plan_group.hpp"
#include "impl/create_plan_insert.hpp"
#include "impl/create_plan_insert_on_conflict.hpp"
#include "impl/create_plan_join.hpp"
#include "impl/create_plan_match.hpp"
#include "impl/create_plan_recursive_cte.hpp"
#include "impl/create_plan_register_cast.hpp"
#include "impl/create_plan_register_udf.hpp"
#include "impl/create_plan_remote.hpp"
#include "impl/create_plan_resolve_constraint.hpp"
#include "impl/create_plan_resolve_database.hpp"
#include "impl/create_plan_resolve_namespace.hpp"
#include "impl/create_plan_resolve_table.hpp"
#include "impl/create_plan_resolve_type.hpp"
#include "impl/create_plan_savepoint.hpp"
#include "impl/create_plan_select.hpp"
#include "impl/create_plan_sequence.hpp"
#include "impl/create_plan_set_setting.hpp"
#include "impl/create_plan_sort.hpp"
#include "impl/create_plan_union.hpp"
#include "impl/create_plan_unregister_udf.hpp"
#include "impl/create_plan_update.hpp"
#include "impl/create_plan_vacuum.hpp"
#include <components/physical_plan/operators/operator_alter_column_set_not_null.hpp>

#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_transaction.hpp>

namespace services::planner {

    using components::logical_plan::node_type;

    core::error_t plan_refusal(std::pmr::memory_resource* resource, std::string_view what) {
        return core::error_t{core::error_code_t::internal_error, std::pmr::string{what, resource}};
    }

    core::error_t unresolved_table_refusal(std::pmr::memory_resource* resource, const qualified_name_t& name) {
        std::pmr::string what{"relation \"", resource};
        what += name.to_string();
        what += "\" has no resolved table in this plan";
        return plan_refusal(resource, what);
    }

    core::result_wrapper_t<components::table::storage_address_t>
    storage_address_of(const context_storage_t& context, components::catalog::oid_t table_oid) {
        // The no-table sentinel (a SELECT without FROM) reads no storage.
        if (table_oid == components::catalog::INVALID_OID) {
            return components::table::storage_address_t{};
        }
        if (const auto* table = context.table_metadata_for(table_oid); table != nullptr) {
            return table->address;
        }
        if (components::catalog::is_catalog_table(table_oid)) {
            return components::table::storage_address_t{context.disk_address, {}};
        }
        std::pmr::string what{"no table metadata for oid ", context.resource};
        what += std::to_string(static_cast<unsigned>(table_oid));
        return core::error_t{core::error_code_t::internal_error, std::move(what)};
    }

    qualified_name_t qualified_name_of(const components::logical_plan::resolved_table_metadata_t& table) {
        return qualified_name_t{core::connection_name_t{table.connection},
                                core::dbname_t{table.database},
                                core::schema_t{table.schema},
                                core::relname_t{table.name}};
    }

    std::vector<size_t> scan_columns_of(const context_storage_t& context,
                                        components::catalog::oid_t table_oid,
                                        const std::vector<size_t>& projected) {
        const auto* table = context.table_metadata_for(table_oid);
        if (table == nullptr || table->relkind != components::catalog::relkind::computed) {
            return projected;
        }
        std::vector<size_t> live;
        live.reserve(table->columns.size());
        for (const auto& column : table->columns) {
            if (column.chunk_position >= 0) {
                live.push_back(static_cast<size_t>(column.chunk_position));
            }
        }
        return live;
    }

    core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>
    page_types_of(const context_storage_t& context,
                  components::catalog::oid_t table_oid,
                  const std::vector<size_t>& projected) {
        std::pmr::vector<components::types::complex_logical_type> types{context.resource};
        // The no-table sentinel (a SELECT without FROM) reads no column.
        if (table_oid == components::catalog::INVALID_OID) {
            return types;
        }
        const auto* table = context.table_metadata_for(table_oid);
        if (table == nullptr) {
            std::pmr::string what{"table oid ", context.resource};
            what += std::to_string(static_cast<unsigned>(table_oid));
            what += " has no columns in this plan";
            return plan_refusal(context.resource, what);
        }
        if (table->relkind == components::catalog::relkind::computed) {
            const auto at = [&table](size_t position) -> const components::logical_plan::resolved_column_metadata_t* {
                for (const auto& column : table->columns) {
                    if (column.chunk_position == static_cast<std::int32_t>(position)) {
                        return &column;
                    }
                }
                return nullptr;
            };
            const auto read = projected.empty() ? scan_columns_of(context, table_oid, projected) : projected;
            for (const auto position : read) {
                const auto* column = at(position);
                if (column == nullptr) {
                    return plan_refusal(context.resource, "a computed table's scan reads a column it does not have");
                }
                types.push_back(column->type);
            }
            return types;
        }
        if (projected.empty()) {
            for (const auto& column : table->columns) {
                types.push_back(column.type);
            }
            return types;
        }
        for (const auto position : projected) {
            if (position >= table->columns.size()) {
                return plan_refusal(context.resource, "a scan reads a column its table does not have");
            }
            types.push_back(table->columns[position].type);
        }
        return types;
    }

    std::pmr::string full_name_of(const context_storage_t& context, components::catalog::oid_t table_oid) {
        const auto* table = context.table_metadata_for(table_oid);
        if (table == nullptr) {
            return std::pmr::string{context.resource};
        }
        return std::pmr::string{qualified_name_of(*table).to_string(), context.resource};
    }

    plan_result_t create_plan(const context_storage_t& context,
                              const components::compute::function_registry_t& function_registry,
                              const components::logical_plan::node_ptr& node,
                              components::logical_plan::limit_t limit,
                              const components::logical_plan::storage_parameters* params) {
        switch (node->type()) {
            case node_type::aggregate_t:
                // Aggregate-pushdown is lowered INSIDE create_plan_aggregate: when the group
                // child is stamped pushdown() and the shape is POD-representable it returns a
                // group_merge over a pushed_reduce_scan carrying the reduce spec (the owning agent
                // reduces its slice); otherwise it builds the normal coordinator aggregate chain.
                // No separate routing here.
                return impl::create_plan_aggregate(context, function_registry, node, std::move(limit), params);
            case node_type::data_t:
                return impl::create_plan_data(node);
            case node_type::union_t:
                return impl::create_plan_union(context, function_registry, node, params);
            case node_type::recursive_cte_t:
                return impl::create_plan_recursive_cte(context, function_registry, node, params);
            case node_type::cte_scan_t:
                return impl::create_plan_cte_scan(context, function_registry, node, params);
            case node_type::delete_t:
                return impl::create_plan_delete(context, function_registry, node, params);
            case node_type::insert_t:
                return impl::create_plan_insert(context, function_registry, node, params);
            case node_type::insert_on_conflict_t:
                return impl::create_plan_insert_on_conflict(context, function_registry, node, params);
            case node_type::match_t:
                // The window of an aggregate is lowered by create_plan_aggregate, that of a DML node by
                // create_plan_delete/update; none reaches a bare WHERE scan.
                assert(!limit.is_effective());
                return impl::create_plan_match(context, node, components::logical_plan::row_cap_t::uncapped());
            // No having_t case: a HAVING node exists only as an aggregate child and is lowered
            // inline by create_plan_aggregate, so it never reaches this generic dispatch.
            case node_type::group_t:
                return impl::create_plan_group(context, function_registry, node, params);
            case node_type::select_t:
                return impl::create_plan_select(context, node);
            case node_type::sort_t:
                return impl::create_plan_sort(context, node);
            case node_type::update_t:
                return impl::create_plan_update(context, function_registry, node, params);
            case node_type::join_t:
                return impl::create_plan_join(context, function_registry, node, params);
            case node_type::check_constraint_t:
                return impl::create_plan_check_constraint(context, function_registry, node, params);
            case node_type::fk_check_t:
                return impl::create_plan_fk_check(context, function_registry, node, params);
            case node_type::fk_cascade_t:
                return impl::create_plan_fk_cascade(context, function_registry, node, params);
            case node_type::sequence_t:
                return impl::create_plan_sequence(context, function_registry, node, params);
            case node_type::alter_column_t: {
                const auto* ac = static_cast<const components::logical_plan::node_alter_column_t*>(node.get());
                // computed()==true: route to the operator_computed_field_*
                // pipeline (relkind='g' pg_computed_column upkeep, dependency-free).
                if (ac->computed()) {
                    switch (ac->op()) {
                        case components::logical_plan::alter_column_op::add:
                            return impl::create_plan_computed_field_register(context, node);
                        case components::logical_plan::alter_column_op::drop:
                            return impl::create_plan_computed_field_unregister(context, node);
                        case components::logical_plan::alter_column_op::rename:
                        case components::logical_plan::alter_column_op::set_not_null:
                            // rewrite_alter_table never sets computed_ on a rename clause: a document table's
                            // RENAME is answered by operator_alter_column_rename_t itself; a document table has
                            // no PRIMARY KEY.
                            break;
                    }
                    assert(false && "a computed column is never renamed by ALTER");
                    std::abort();
                }
                switch (ac->op()) {
                    case components::logical_plan::alter_column_op::add:
                        return impl::create_plan_alter_column_add(context, node);
                    case components::logical_plan::alter_column_op::rename:
                        return impl::create_plan_alter_column_rename(context, node);
                    case components::logical_plan::alter_column_op::drop:
                        return impl::create_plan_alter_column_drop(context, node);
                    case components::logical_plan::alter_column_op::set_not_null:
                        return components::operators::operator_ptr{
                            new components::operators::operator_alter_column_set_not_null_t(context.resource,
                                                                                            context.log.clone(),
                                                                                            ac->table_oid(),
                                                                                            ac->attoid())};
                }
                assert(false && "unknown ALTER COLUMN operation");
                std::abort();
            }
            case node_type::dynamic_cascade_delete_t:
                return impl::create_plan_dynamic_cascade_delete(context, node);
            case node_type::checkpoint_t:
                return impl::create_plan_checkpoint(context, node);
            case node_type::set_setting_t:
                return impl::create_plan_set_setting(context, node);
            case node_type::vacuum_t:
                return impl::create_plan_vacuum(context, node);
            case node_type::create_matview_t:
                return impl::create_plan_create_matview(context, node);
            case node_type::register_udf_t:
                return impl::create_plan_register_udf(context, node);
            case node_type::unregister_udf_t:
                return impl::create_plan_unregister_udf(context, node);
            case node_type::register_cast_t:
                return impl::create_plan_register_cast(context, node);
            case node_type::unregister_cast_t:
                return impl::create_plan_unregister_cast(context, node);
            case node_type::transaction_t: {
                const auto* txn = static_cast<const components::logical_plan::node_transaction_t*>(node.get());
                switch (txn->op()) {
                    case components::logical_plan::transaction_op::begin:
                        return impl::create_plan_begin_transaction(context, node);
                    case components::logical_plan::transaction_op::commit:
                        return impl::create_plan_commit_transaction(context, node);
                    case components::logical_plan::transaction_op::abort:
                        return impl::create_plan_abort_transaction(context, node);
                    case components::logical_plan::transaction_op::savepoint:
                        return impl::create_plan_savepoint(context, node);
                    case components::logical_plan::transaction_op::release_savepoint:
                        return impl::create_plan_release_savepoint(context, node);
                    case components::logical_plan::transaction_op::rollback_to_savepoint:
                        return impl::create_plan_rollback_to_savepoint(context, node);
                }
                assert(false && "unknown transaction operation");
                std::abort();
            }
            case node_type::catalog_resolve_t: {
                const auto* rn = static_cast<const components::logical_plan::node_catalog_resolve_t*>(node.get());
                switch (rn->kind()) {
                    case components::logical_plan::resolve_kind::namespace_:
                        return impl::create_plan_resolve_namespace(context, node);
                    case components::logical_plan::resolve_kind::database:
                        return impl::create_plan_resolve_database(context, node);
                    case components::logical_plan::resolve_kind::table:
                        return impl::create_plan_resolve_table(context, node);
                    case components::logical_plan::resolve_kind::type:
                        return impl::create_plan_resolve_type(context, node);
                    case components::logical_plan::resolve_kind::constraint:
                        return impl::create_plan_resolve_constraint(context, node);
                }
                assert(false && "unknown catalog resolve kind");
                std::abort();
            }
            case node_type::allocate_oids_t:
                return impl::create_plan_allocate_oids(context, node);
            case node_type::function_t:
                return impl::create_plan_function(context, function_registry, node);
            case node_type::remote_t:
                return impl::create_plan_remote(context, node);
            default:
                break;
        }
        std::pmr::string what{"no physical plan for a logical node of this kind: ", context.resource};
        what += node->to_string();
        return plan_refusal(context.resource, what);
    }

} // namespace services::planner