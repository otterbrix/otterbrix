#include "simple_value.hpp"

namespace components::collection::operators::get {

    operator_get_ptr simple_value_t::create(const expressions::key_t& key) {
        return operator_get_ptr(new simple_value_t(key));
    }

    simple_value_t::simple_value_t(const expressions::key_t& key)
        : operator_get_t()
        , key_(key) {}

    document::value_t simple_value_t::get_value_impl(const document::document_ptr& document) {
        return document->get_value(key_.as_string());
    }

} // namespace components::collection::operators::get
