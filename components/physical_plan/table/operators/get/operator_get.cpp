#include "operator_get.hpp"

namespace components::table::operators::get {

    std::vector<types::logical_value_t> operator_get_t::values(const std::pmr::vector<types::logical_value_t>& row) {
        return get_values_impl(row);
    }

} // namespace components::table::operators::get
