#include "operator_sequence.hpp"

namespace components::operators {

    operator_sequence_t::operator_sequence_t(std::pmr::memory_resource* resource,
                                              log_t log,
                                              std::vector<operator_ptr> ops)
        : read_write_operator_t(resource, log, operator_type::sequence)
        , ops_(std::move(ops)) {}

    void operator_sequence_t::prepare_ops() {
        for (auto& op : ops_) {
            op->prepare();
        }
    }

    void operator_sequence_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        for (auto& op : ops_) {
            op->on_execute(pipeline_context);
            if (op->has_error()) {
                set_error(op->error_message());
                return;
            }
        }
    }

} // namespace components::operators
