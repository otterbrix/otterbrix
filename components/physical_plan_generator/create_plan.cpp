#include "create_plan.hpp"

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
#include "impl/create_plan_group.hpp"
#include "impl/create_plan_hash_join.hpp"
#include "impl/create_plan_insert.hpp"
#include "impl/create_plan_join.hpp"
#include "impl/create_plan_match.hpp"
#include "impl/create_plan_primitive_delete.hpp"
#include "impl/create_plan_primitive_write.hpp"
#include "impl/create_plan_recursive_cte.hpp"
#include "impl/create_plan_resolve_constraint.hpp"
#include "impl/create_plan_resolve_database.hpp"
#include "impl/create_plan_resolve_function.hpp"
#include "impl/create_plan_resolve_namespace.hpp"
#include "impl/create_plan_resolve_table.hpp"
#include "impl/create_plan_resolve_type.hpp"
#include "impl/create_plan_select.hpp"
#include "impl/create_plan_sequence.hpp"
#include "impl/create_plan_set_timezone.hpp"
#include "impl/create_plan_sort.hpp"
#include "impl/create_plan_union.hpp"
#include "impl/create_plan_unregister_udf.hpp"
#include "impl/create_plan_update.hpp"
#include "impl/create_plan_vacuum.hpp"

namespace services::planner {

    using components::logical_plan::node_type;

    components::operators::operator_ptr create_plan(const context_storage_t& context,
                                                    const components::compute::function_registry_t& function_registry,
                                                    const components::logical_plan::node_ptr& node,
                                                    components::logical_plan::limit_t limit,
                                                    const components::logical_plan::storage_parameters* params) {
        switch (node->type()) {
            case node_type::aggregate_t:
                return impl::create_plan_aggregate(context, function_registry, node, std::move(limit), params);
            case node_type::data_t:
                return impl::create_plan_data(node);
            case node_type::union_t:
                return impl::create_plan_union(context, function_registry, node, std::move(limit), params);
            case node_type::recursive_cte_t:
                return impl::create_plan_recursive_cte(context, function_registry, node, std::move(limit), params);
            case node_type::cte_scan_t:
                return impl::create_plan_cte_scan(context, function_registry, node, std::move(limit), params);
            case node_type::delete_t:
                return impl::create_plan_delete(context, node, params);
            case node_type::insert_t:
                return impl::create_plan_insert(context, function_registry, node, std::move(limit), params);
            case node_type::match_t:
                return impl::create_plan_match(context, node, std::move(limit));
            case node_type::having_t:
                return impl::create_plan_having(context, node, std::move(limit));
            case node_type::group_t:
                return impl::create_plan_group(context, function_registry, node, params);
            case node_type::select_t:
                return impl::create_plan_select(context, node, params);
            case node_type::sort_t:
                return impl::create_plan_sort(context, node);
            case node_type::update_t:
                return impl::create_plan_update(context, node, params);
            case node_type::join_t:
                return impl::create_plan_join(context, function_registry, node, std::move(limit), params);
            case node_type::hash_join_t:
                return impl::create_plan_hash_join(context, function_registry, node, std::move(limit), params);
            case node_type::check_constraint_t:
                return impl::create_plan_check_constraint(context, function_registry, node, params);
            case node_type::fk_check_t:
                return impl::create_plan_fk_check(context, function_registry, node, params);
            case node_type::fk_cascade_t:
                return impl::create_plan_fk_cascade(context, function_registry, node, params);
            case node_type::sequence_t:
                return impl::create_plan_sequence(context, function_registry, node, params);
            case node_type::primitive_write_t:
                return impl::create_plan_primitive_write(context, node);
            case node_type::primitive_delete_t:
                return impl::create_plan_primitive_delete(context, node);
            case node_type::alter_column_add_t:
                return impl::create_plan_alter_column_add(context, node);
            case node_type::alter_column_rename_t:
                return impl::create_plan_alter_column_rename(context, node);
            case node_type::alter_column_drop_t:
                return impl::create_plan_alter_column_drop(context, node);
            case node_type::dynamic_cascade_delete_t:
                return impl::create_plan_dynamic_cascade_delete(context, node);
            case node_type::checkpoint_t:
                return impl::create_plan_checkpoint(context, node);
            case node_type::set_timezone_t:
                return impl::create_plan_set_timezone(context, node);
            case node_type::vacuum_t:
                return impl::create_plan_vacuum(context, node);
            case node_type::create_matview_t:
                return impl::create_plan_create_matview(context, function_registry, node, params);
            case node_type::unregister_udf_t:
                return impl::create_plan_unregister_udf(context, node);
            case node_type::commit_transaction_t:
                return impl::create_plan_commit_transaction(context, node);
            case node_type::abort_transaction_t:
                return impl::create_plan_abort_transaction(context, node);
            case node_type::begin_transaction_t:
                return impl::create_plan_begin_transaction(context, node);
            case node_type::computed_field_register_t:
                return impl::create_plan_computed_field_register(context, node);
            case node_type::computed_field_unregister_t:
                return impl::create_plan_computed_field_unregister(context, node);
            case node_type::catalog_resolve_namespace_t:
                return impl::create_plan_resolve_namespace(context, node);
            case node_type::catalog_resolve_database_t:
                return impl::create_plan_resolve_database(context, node);
            case node_type::catalog_resolve_function_t:
                return impl::create_plan_resolve_function(context, node);
            case node_type::catalog_resolve_table_t:
                return impl::create_plan_resolve_table(context, node);
            case node_type::catalog_resolve_type_t:
                return impl::create_plan_resolve_type(context, node);
            case node_type::catalog_resolve_constraint_t:
                return impl::create_plan_resolve_constraint(context, node);
            case node_type::allocate_oids_t:
                return impl::create_plan_allocate_oids(context, node);
            default:
                break;
        }
        return nullptr;
    }

} // namespace services::planner