#include "create_plan_hash_join.hpp"

#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator_hash_join.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_hash_join(const context_storage_t& context,
                          const components::compute::function_registry_t& function_registry,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit,
                          const components::logical_plan::storage_parameters* params) {
        const auto* hash_node = static_cast<const components::logical_plan::node_hash_join_t*>(node.get());
        // Assign left table as actor for join: try left child context first, fall
        // back to right (one side may be raw data with a nullptr context).
        auto left_oid = node->children().front()->table_oid();
        auto right_oid = node->children().back()->table_oid();
        bool known = context.has_table_oid(left_oid) || context.has_table_oid(right_oid);
        auto* resource = known ? context.resource : nullptr;
        auto log = known ? context.log.clone() : log_t{};

        components::operators::operator_ptr join =
            boost::intrusive_ptr(new components::operators::operator_hash_join_t(resource,
                                                                                 std::move(log),
                                                                                 hash_node->type(),
                                                                                 hash_node->left_col(),
                                                                                 hash_node->right_col()));

        // Push the LIMIT down to whichever side an outer join preserves (mirrors
        // create_plan_join). The hash path covers inner/left/right/full only.
        using join_type = components::logical_plan::join_type;
        auto limit_left = components::logical_plan::limit_t::unlimit();
        auto limit_right = components::logical_plan::limit_t::unlimit();
        switch (hash_node->type()) {
            case join_type::left:
                limit_left = limit;
                break;
            case join_type::right:
                limit_right = limit;
                break;
            case join_type::inner:
            case join_type::full:
                break;
            case join_type::cross:
            case join_type::invalid:
                throw std::logic_error("create_plan_hash_join: non-equi join type");
        }
        components::operators::operator_ptr left;
        components::operators::operator_ptr right;
        if (node->children().front()) {
            left = create_plan(context, function_registry, node->children().front(), limit_left, params);
        }
        if (node->children().back()) {
            right = create_plan(context, function_registry, node->children().back(), limit_right, params);
        }
        join->set_children(std::move(left), std::move(right));
        return join;
    }

} // namespace services::planner::impl
