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
#include "impl/create_plan_function.hpp"
#include "impl/create_plan_group.hpp"
#include "impl/create_plan_insert.hpp"
#include "impl/create_plan_join.hpp"
#include "impl/create_plan_match.hpp"
#include "impl/create_plan_recursive_cte.hpp"
#include "impl/create_plan_resolve_constraint.hpp"
#include "impl/create_plan_resolve_database.hpp"
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

#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_transaction.hpp>

namespace services::planner {

    using components::logical_plan::node_type;

    // Null-Object default for context_storage_t::create_plan_rule (see
    // context_storage.hpp): no host lowering registered -> a node_extension leaf
    // has no host operator, so its plan errors downstream. Defined here (operator_t
    // complete) so the returned intrusive_ptr<operator_t> temporary is destructible.
    components::operators::operator_ptr no_custom_lowering(const context_storage_t&,
                                                           const components::compute::function_registry_t&,
                                                           const components::logical_plan::node_ptr&) {
        return {};
    }

    components::operators::operator_ptr create_plan(const context_storage_t& context,
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
            case node_type::match_t:
                return impl::create_plan_match(context, node, std::move(limit));
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
                            return nullptr; // computed rename is not emitted
                    }
                    return nullptr;
                }
                switch (ac->op()) {
                    case components::logical_plan::alter_column_op::add:
                        return impl::create_plan_alter_column_add(context, node);
                    case components::logical_plan::alter_column_op::rename:
                        return impl::create_plan_alter_column_rename(context, node);
                    case components::logical_plan::alter_column_op::drop:
                        return impl::create_plan_alter_column_drop(context, node);
                }
                return nullptr;
            }
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
            case node_type::transaction_t: {
                const auto* txn = static_cast<const components::logical_plan::node_transaction_t*>(node.get());
                switch (txn->op()) {
                    case components::logical_plan::transaction_op::begin:
                        return impl::create_plan_begin_transaction(context, node);
                    case components::logical_plan::transaction_op::commit:
                        return impl::create_plan_commit_transaction(context, node);
                    case components::logical_plan::transaction_op::abort:
                        return impl::create_plan_abort_transaction(context, node);
                }
                return nullptr;
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
                return nullptr;
            }
            case node_type::allocate_oids_t:
                return impl::create_plan_allocate_oids(context, node);
            case node_type::function_t:
                return impl::create_plan_function(context, node);
            case node_type::extension_t: {
                // Host-custom node (node_extension): the engine cannot lower it — the
                // host-injected create_plan rule (stamped on context_storage, see
                // context_storage.hpp) builds the host operator. A SOURCE is a childless
                // leaf; a SINK is a node WITH a child (e.g. INSERT..SELECT into a
                // backend) whose child sub-plan the engine wires. The rule is never null
                // (Null Object default); the operator it RETURNS may be null ("no host
                // lowering"), and the plan then errors downstream (create_physical_plan_error).
                auto op = context.create_plan_rule(context, function_registry, node);
                if (op && !node->children().empty()) {
                    op->set_children(create_plan(context,
                                                 function_registry,
                                                 node->children().front(),
                                                 components::logical_plan::limit_t::unlimit(),
                                                 params));
                }
                return op;
            }
            default:
                break;
        }
        return nullptr;
    }

} // namespace services::planner