#include "operator_check_constraint.hpp"

#include <components/catalog/constraint_evaluator.hpp>

namespace components::operators {

    operator_check_constraint_t::operator_check_constraint_t(std::pmr::memory_resource* resource,
                                                              log_t log,
                                                              catalog::row_predicate_fn predicate,
                                                              std::string conexpr)
        : read_write_operator_t(resource, log, operator_type::check_constraint)
        , predicate_(std::move(predicate))
        , conexpr_(std::move(conexpr)) {}

    void operator_check_constraint_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (!left_ || !left_->output()) return;
        output_ = left_->output();
        if (!predicate_) return;
        const auto& chunk = output_->data_chunk();
        auto fail_row = catalog::evaluate_check(chunk, predicate_);
        if (fail_row >= 0) {
            set_error("CHECK constraint violation: " + conexpr_);
        }
    }

} // namespace components::operators
