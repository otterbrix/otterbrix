#include "operator_fk_check.hpp"

namespace components::operators {

    operator_fk_check_t::operator_fk_check_t(std::pmr::memory_resource* resource,
                                              log_t log,
                                              catalog::resolved_fk_t fk)
        : read_write_operator_t(resource, log, operator_type::fk_check)
        , fk_(std::move(fk)) {}

    void operator_fk_check_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        // Pass-through: FK enforcement via disk.point_lookup_by_index added in Etap 5.
        if (left_ && left_->output()) output_ = left_->output();
    }

} // namespace components::operators
