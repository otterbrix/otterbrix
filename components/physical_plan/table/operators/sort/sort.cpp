#include "sort.hpp"

namespace components::table::sort {

    sorter_t::sorter_t(size_t index, order order_) { add(index, order_); }
    sorter_t::sorter_t(const std::string& key, order order_) { add(key, order_); }

    void sorter_t::add(size_t index, order order_) {
        functions_.emplace_back([index, order_](const std::pmr::vector<types::logical_value_t>& vec1,
                                                const std::pmr::vector<types::logical_value_t>& vec2) {
            auto k_order = static_cast<int>(order_ == order::ascending ? compare_t::more : compare_t::less);
            const auto& v1 = vec1.at(index);
            const auto& v2 = vec2.at(index);
            if (v1.is_null() && v2.is_null()) {
                return compare_t::equals;
            }
            if (v1.is_null()) {
                return static_cast<compare_t>(k_order * static_cast<int>(compare_t::more));
            }
            if (v2.is_null()) {
                return static_cast<compare_t>(k_order * static_cast<int>(compare_t::less));
            }
            return static_cast<compare_t>(k_order * static_cast<int>(v1.compare(v2)));
        });
    }

    void sorter_t::add(const std::string& key, order order_) {
        functions_.emplace_back([key, order_](const std::pmr::vector<types::logical_value_t>& vec1,
                                              const std::pmr::vector<types::logical_value_t>& vec2) {
            auto pos_1 = static_cast<size_t>(
                std::find_if(vec1.begin(), vec1.end(), [&key](const auto& val) { return val.type().has_alias() && val.type().alias() == key; }) -
                vec1.begin());
            auto pos_2 = static_cast<size_t>(
                std::find_if(vec2.begin(), vec2.end(), [&key](const auto& val) { return val.type().has_alias() && val.type().alias() == key; }) -
                vec2.begin());
            auto k_order = static_cast<int>(order_ == order::ascending ? compare_t::more : compare_t::less);
            if (pos_1 >= vec1.size() && pos_2 >= vec2.size()) {
                return compare_t::equals;
            } else if (pos_1 >= vec1.size()) {
                return static_cast<compare_t>(k_order * static_cast<int>(compare_t::more));
            } else if (pos_2 >= vec2.size()) {
                return static_cast<compare_t>(k_order * static_cast<int>(compare_t::less));
            }
            const auto& v1 = vec1.at(pos_1);
            const auto& v2 = vec2.at(pos_2);
            if (v1.is_null() && v2.is_null()) {
                return compare_t::equals;
            }
            if (v1.is_null()) {
                return static_cast<compare_t>(k_order * static_cast<int>(compare_t::more));
            }
            if (v2.is_null()) {
                return static_cast<compare_t>(k_order * static_cast<int>(compare_t::less));
            }
            return static_cast<compare_t>(k_order * static_cast<int>(v1.compare(v2)));
        });
    }

} // namespace components::table::sort