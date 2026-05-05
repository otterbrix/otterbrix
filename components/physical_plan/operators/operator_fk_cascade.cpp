#include "operator_fk_cascade.hpp"

namespace components::operators {

    operator_fk_cascade_t::operator_fk_cascade_t(std::pmr::memory_resource* resource,
                                                   log_t log,
                                                   catalog::resolved_fk_t fk)
        : read_write_operator_t(resource, log, operator_type::fk_cascade)
        , fk_(std::move(fk)) {}

    void operator_fk_cascade_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        // Pass-through: cascade actions via disk.scan_by_key added in Etap 5.
        if (left_ && left_->output()) output_ = left_->output();
    }

} // namespace components::operators
