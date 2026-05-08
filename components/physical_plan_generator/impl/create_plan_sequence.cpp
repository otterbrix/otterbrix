#include "create_plan_sequence.hpp"

#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_primitive_write.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/physical_plan/operators/operator_create_collection.hpp>
#include <components/physical_plan/operators/operator_sequence.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_sequence(const context_storage_t& context,
                         const components::compute::function_registry_t& function_registry,
                         const components::logical_plan::node_ptr& node,
                         const components::logical_plan::storage_parameters* params) {
        using namespace components::logical_plan;

        // DDL create-table sequence: sequence_t(create_collection_t, primitive_write×N).
        // Produce a single operator_create_collection_t that does storage creation,
        // index registration, and all pg_catalog writes in one await_async_and_resume.
        if (!node->children().empty() &&
            node->children().front()->type() == node_type::create_collection_t) {
            auto* cc = static_cast<node_create_collection_t*>(node->children().front().get());
            std::vector<components::operators::operator_create_collection_t::catalog_write_t> writes;
            writes.reserve(node->children().size() - 1);
            for (std::size_t i = 1; i < node->children().size(); ++i) {
                auto* pw = static_cast<node_primitive_write_t*>(node->children()[i].get());
                writes.emplace_back(pw->catalog_table(), std::move(pw->row()));
            }
            return boost::intrusive_ptr(new components::operators::operator_create_collection_t(
                context.resource,
                context.log.clone(),
                cc->collection_full_name(),
                cc->column_definitions(),
                cc->is_disk_storage(),
                std::move(writes)));
        }

        std::vector<components::operators::operator_ptr> steps;
        steps.reserve(node->children().size());
        for (const auto& child : node->children()) {
            steps.push_back(create_plan(context, function_registry, child, {}, params));
        }
        return boost::intrusive_ptr(new components::operators::operator_sequence_t(
            context.resource,
            context.log.clone(),
            std::move(steps)));
    }

} // namespace services::planner::impl