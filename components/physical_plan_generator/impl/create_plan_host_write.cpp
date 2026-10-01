#include "create_plan_host_write.hpp"

#include <components/logical_plan/host_write_target.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator_assign_cast.hpp>

namespace services::planner::impl {

    plan_result_t create_plan_host_write(const context_storage_t& context,
                                         const components::compute::function_registry_t& function_registry,
                                         const components::logical_plan::node_ptr& node,
                                         const components::logical_plan::storage_parameters* params) {
        using namespace components::logical_plan;
        const auto& target = *host_write_target(*node);
        VALUE_OR_RETURN(auto write, target.write_fn()(context, function_registry, target.relation(), *node));
        if (!write) {
            std::pmr::string what{"the host built no operator for a write into \"", context.resource};
            what += target.relation().name();
            what += '"';
            return plan_refusal(context.resource, what);
        }
        if (node->type() != node_type::insert_t) {
            return write;
        }
        const auto& insert = static_cast<const node_insert_t&>(*node);
        insert_column_bindings_t bindings(context.resource);
        bindings.reserve(insert.column_bindings().size());
        for (const auto& binding : insert.column_bindings()) {
            bindings.emplace_back(
                insert_column_binding_t{.target_index = binding.target_index,
                                        .target_name = std::pmr::string{binding.target_name.c_str(), context.resource},
                                        .target_type = binding.target_type,
                                        .cast = binding.cast});
        }
        VALUE_OR_RETURN(auto source,
                        create_plan(context, function_registry, node->children().front(), limit_t::unlimit(), params));
        auto cast = boost::intrusive_ptr(new components::operators::operator_assign_cast_t(context.resource,
                                                                                            context.log.clone(),
                                                                                            std::move(bindings)));
        cast->set_children(std::move(source));
        write->set_children(std::move(cast));
        return write;
    }

} // namespace services::planner::impl
