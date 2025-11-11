#include "operator_aggregate.hpp"
#include <services/collection/collection.hpp>

namespace components::collection::operators::aggregate {

    operator_aggregate_t::operator_aggregate_t(services::collection::context_collection_t* context)
        : read_only_operator_t(context, operator_type::aggregate) {}

    void operator_aggregate_t::on_execute_impl(pipeline::context_t*) {
        auto resource = left_ && left_->output() ? left_->output()->resource() : context_->resource();
        output_ = base::operators::make_operator_data(resource);
        output_->append(aggregate_impl());
    }

    void operator_aggregate_t::set_value(document::document_ptr& doc, std::string_view key) const {
        doc->set(key, output_->documents().at(0), key_impl());
    }

    document::value_t operator_aggregate_t::value() const { return output_->documents().at(0)->get_value(key_impl()); }
} // namespace components::collection::operators::aggregate
