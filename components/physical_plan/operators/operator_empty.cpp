#include "operator_empty.hpp"

namespace components::operators {

    operator_empty_t::operator_empty_t(services::collection::context_collection_t* context, operator_data_ptr&& data)
        : read_only_operator_t(context, operator_type::empty) {
        output_ = std::move(data);
    }

    void operator_empty_t::on_execute_impl(pipeline::context_t*) {}

} // namespace components::operators