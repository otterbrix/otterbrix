#include "operator_insert.hpp"

namespace components::operators {

    operator_insert::operator_insert(std::pmr::memory_resource* resource, log_t log, collection_full_name_t name)
        : read_write_operator_t(resource, log, operator_type::insert)
        , name_(std::move(name)) {}

    void operator_insert::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (left_ && left_->output()) {
            output_ = left_->output();
            modified_ = operators::make_operator_write_data(resource());
        }
        if (output_ && output_->size() > 0 && !name_.empty()) {
            async_wait();
        }
    }

} // namespace components::operators
