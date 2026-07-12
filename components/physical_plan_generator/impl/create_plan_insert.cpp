#include "create_plan_insert.hpp"

#include "create_plan_select.hpp"
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

#include <memory>

namespace services::planner::impl {

    namespace {

        // Pack the catalog-decoded (name, value) DEFAULTs of the target table into
        // the shape the disk contract already speaks: a one-row data_chunk_t whose
        // column aliases carry the column names and whose row 0 holds the decoded
        // values. The values stay UN-cast — the disk agent casts them to the live
        // storage type (with the session timezone), which is the only place the
        // physical column types are known. Null/undecodable defaults are dropped
        // here; a table with no usable default yields nullptr.
        std::unique_ptr<components::vector::data_chunk_t> build_column_defaults_chunk(
            std::pmr::memory_resource* resource,
            const std::vector<std::pair<std::string, components::types::logical_value_t>>& defaults) {
            std::pmr::vector<components::types::complex_logical_type> types{resource};
            std::pmr::vector<const components::types::logical_value_t*> values{resource};
            types.reserve(defaults.size());
            values.reserve(defaults.size());
            for (const auto& [name, value] : defaults) {
                if (value.is_null()) {
                    continue;
                }
                // The column type is the value's own type: vector_t::set_value gates
                // on type equality (and ignores the alias), so this always passes.
                auto type = value.type();
                type.set_alias(name);
                types.emplace_back(std::move(type));
                values.emplace_back(&value);
            }
            if (types.empty()) {
                return nullptr;
            }
            auto chunk = std::make_unique<components::vector::data_chunk_t>(resource, types, /*capacity=*/1);
            chunk->set_cardinality(1);
            for (size_t i = 0; i < values.size(); ++i) {
                chunk->set_value(i, 0, *values[i]);
            }
            return chunk;
        }

    } // namespace

    components::operators::operator_ptr
    create_plan_insert(const context_storage_t& context,
                       const components::compute::function_registry_t& function_registry,
                       const components::logical_plan::node_ptr& node,
                       components::logical_plan::limit_t limit,
                       const components::logical_plan::storage_parameters* params) {
        const auto* node_insert = static_cast<const components::logical_plan::node_insert_t*>(node.get());
        auto returning = build_returning_columns(context.resource, node_insert->returning(), params);
        // Forward the plan-resolved RETURNING output types (stamped on the insert node by
        // validate_schema) onto the projection columns, in projection order, so a
        // CASE/COALESCE/deep-field RETURNING column over the appended rows stays correctly
        // typed instead of being dropped as an untyped (NA) placeholder. evaluate_projection
        // reads col.result_type authoritatively. Mirrors create_plan_aggregate's select path.
        // No RETURNING -> output_types() is empty -> guard skips (no-op). No data-derived
        // fallback (rule 6): a column without a resolved type stays unset.
        if (node->has_output_types()) {
            const auto& out_types = node->output_types();
            for (size_t i = 0; i < returning.size() && i < out_types.size(); ++i) {
                returning[i].result_type = out_types[i];
            }
        }
        auto plan = boost::intrusive_ptr(new components::operators::operator_insert(context.resource,
                                                                                    context.log.clone(),
                                                                                    node->table_oid(),
                                                                                    std::move(returning),
                                                                                    build_column_defaults_chunk(
                                                                                        context.resource,
                                                                                        node_insert->column_defaults())));
        plan->set_children(create_plan(context, function_registry, node->children().front(), std::move(limit), params));

        return plan;
    }

} // namespace services::planner::impl
