#include "simple_value.hpp"

namespace components::table::operators::get {

    operator_get_ptr simple_value_t::create(const expressions::key_t& key) {
        return operator_get_ptr(new simple_value_t(key));
    }

    simple_value_t::simple_value_t(const expressions::key_t& key)
        : operator_get_t()
        , key_(key) {}

    std::vector<types::logical_value_t>
    simple_value_t::get_values_impl(const std::pmr::vector<types::logical_value_t>& row) {
        // pointer + size to avoid std::vector and std::pmr::vector clashing
        auto* local_values = row.data();
        size_t size = row.size();
        for (size_t i = 0; i < key_.storage().size(); i++) {
            if (key_.storage()[i] == "*") {
                return {local_values, local_values + size};
            }
            auto it = std::find_if(local_values, local_values + size, [&](const types::logical_value_t& value) {
                return core::pmr::operator==(value.type().alias(), key_.storage()[i]);
            });
            if (it == local_values + size) {
                return {};
            }
            // if it isn't the last one
            if (i + 1 != key_.storage().size()) {
                local_values = it->children().data();
                size = it->children().size();
            } else {
                return {*it};
            }
        }
        return {};
    }

} // namespace components::table::operators::get
