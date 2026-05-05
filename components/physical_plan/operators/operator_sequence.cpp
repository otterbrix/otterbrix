#include "operator_sequence.hpp"

namespace components::operators {

    operator_sequence_t::operator_sequence_t(std::pmr::memory_resource* resource,
                                              log_t                      log,
                                              std::vector<operator_ptr>  steps)
        : read_write_operator_t(resource, log, operator_type::sequence)
        , steps_(std::move(steps)) {}

    void operator_sequence_t::on_execute_impl(pipeline::context_t* ctx) {
        for (const auto& step : steps_) {
            step->on_execute(ctx);
            if (!step->error_message().empty()) {
                set_error(step->error_message());
                return;
            }
        }
    }

} // namespace components::operators