#include "operator_empty.hpp"

namespace components::operators {

    operator_empty_t::operator_empty_t(std::pmr::memory_resource* resource, operator_data_ptr&& data)
        : read_only_operator_t(resource, log_t{}, operator_type::empty) {
        output_ = std::move(data);
    }

    void operator_empty_t::on_execute_impl(pipeline::context_t*) {}

} // namespace components::operators