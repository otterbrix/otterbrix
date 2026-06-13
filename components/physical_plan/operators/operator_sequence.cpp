#include "operator_sequence.hpp"

#include <components/physical_plan/operators/operator_resolve_table.hpp>
#include <components/physical_plan/operators/resolved_table_metadata.hpp>

namespace components::operators {

    operator_sequence_t::operator_sequence_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             std::vector<operator_ptr> steps)
        : read_write_operator_t(resource, log, operator_type::sequence)
        , steps_(std::move(steps)) {}

    void operator_sequence_t::on_execute_impl(pipeline::context_t* ctx) {
        for (std::size_t i = 0; i < steps_.size(); ++i) {
            const auto& step = steps_[i];
            step->on_execute(ctx);
            if (step->has_error()) {
                set_error(step->get_error());
                return;
            }

            // When a resolve_table step finishes, forward its parsed
            // metadata to the next step that asks for it. Callers
            // (planner / create_plan_sequence) ensure the consumer follows
            // the resolver directly when this pattern is in use; we still
            // scan forward defensively so a resolve_namespace stepping in
            // between doesn't break the hand-off.
            if (step->type() == operator_type::resolve_table && step->is_executed()) {
                auto* resolver = static_cast<operator_resolve_table_t*>(step.get());
                auto metadata = parse_resolved_table_metadata(resolver->resolved_table_oid(), step->output());
                if (metadata.has_value()) {
                    for (std::size_t j = i + 1; j < steps_.size(); ++j) {
                        if (steps_[j]->wants_resolved_metadata()) {
                            steps_[j]->accept_resolved_metadata(*metadata);
                            break;
                        }
                    }
                }
            }
        }
    }

} // namespace components::operators