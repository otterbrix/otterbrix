#include "simple_value.hpp"

#include <stdexcept>

namespace components::operators::get {

    operator_get_ptr simple_value_t::create(const expressions::key_t& key) {
        return operator_get_ptr(new simple_value_t(key));
    }

    simple_value_t::simple_value_t(const expressions::key_t& key)
        : operator_get_t()
        , key_(key) {}

    std::vector<types::logical_value_t>
    simple_value_t::get_values_impl(const std::pmr::vector<types::logical_value_t>& row) {
        const auto& path = key_.path();
        if (path.empty()) {
            throw std::logic_error("simple_value_t: key path is empty");
        }
        if (path[0] >= row.size()) {
            throw std::logic_error("simple_value_t: path[0] out of range");
        }

        // Wildcard: navigate to parent, return all children
        bool is_wildcard = !key_.storage().empty() && key_.storage().back() == "*";
        if (is_wildcard && path.size() >= 2) {
            auto* val = &row[path[0]];
            for (size_t i = 1; i < path.size() - 1; i++) {
                if (path[i] >= val->children().size()) {
                    throw std::logic_error("simple_value_t: child path out of range");
                }
                val = &val->children()[path[i]];
            }
            auto& children = val->children();
            return {children.data(), children.data() + children.size()};
        }

        auto* val = &row[path[0]];
        for (size_t i = 1; i < path.size(); i++) {
            if (path[i] >= val->children().size()) {
                throw std::logic_error("simple_value_t: child path out of range");
            }
            val = &val->children()[path[i]];
        }
        return {*val};
    }

} // namespace components::operators::get
