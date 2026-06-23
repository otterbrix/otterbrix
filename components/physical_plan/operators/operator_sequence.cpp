#include "operator_sequence.hpp"

namespace components::operators {

    operator_sequence_t::operator_sequence_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             std::vector<operator_ptr> steps)
        : read_write_operator_t(resource, log, operator_type::sequence)
        , steps_(std::move(steps)) {}

    // No-op: real DDL/DML sequences are flattened into left_/right_ operator chains by
    // create_plan_sequence (its generic and ALTER-TABLE paths). The only
    // operator_sequence_t ever constructed comes from the childless fallback at the
    // tail of create_plan_sequence, where node->children() is empty, so steps_ is
    // always empty and there is nothing to drive here. The legacy step->on_execute()
    // internal drive was therefore dead and has been removed.
    void operator_sequence_t::on_execute_impl(pipeline::context_t* /*ctx*/) {}

} // namespace components::operators