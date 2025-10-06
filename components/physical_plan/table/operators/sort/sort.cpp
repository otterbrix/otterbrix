#include "sort.hpp"

namespace components::table::sort {

    sorter_t::sorter_t(size_t index, order order_) { add(index, order_); }
    sorter_t::sorter_t(const std::string& key, order order_) { add(key, order_); }

    void sorter_t::add(size_t index, order order_) {
        functions_.emplace_back([index, order_](const std::pmr::vector<types::logical_value_t>& vec1,
                                                const std::pmr::vector<types::logical_value_t>& vec2) {
            auto k_order = static_cast<int>(order_ == order::ascending ? compare_t::more : compare_t::less);
            return static_cast<compare_t>(k_order * static_cast<int>(vec1.at(index).compare(vec2.at(index))));
        });
    }

    void sorter_t::add(const std::string& key, order order_) {
        functions_.emplace_back([&key, order_](const std::pmr::vector<types::logical_value_t>& vec1,
                                               const std::pmr::vector<types::logical_value_t>& vec2) {
            auto pos_1 =
                std::find_if(vec1.begin(), vec1.end(), [&key](const auto& val) { return val.type().alias() == key; }) -
                vec1.begin();
            auto pos_2 =
                std::find_if(vec2.begin(), vec2.end(), [&key](const auto& val) { return val.type().alias() == key; }) -
                vec2.begin();
            auto k_order = static_cast<int>(order_ == order::ascending ? compare_t::less : compare_t::more);
            if (pos_1 >= vec1.size() && pos_2 >= vec2.size()) {
                return compare_t::equals;
            } else if (pos_1 >= vec1.size()) {
                return static_cast<compare_t>(
                    k_order * static_cast<int>(types::logical_value_t{nullptr}.compare(vec2.at(pos_2))));
            } else if (pos_2 >= vec2.size()) {
                return static_cast<compare_t>(
                    k_order * static_cast<int>(vec1.at(pos_1).compare(types::logical_value_t{nullptr})));
            } else {
                return static_cast<compare_t>(k_order * static_cast<int>(vec1.at(pos_1).compare(vec2.at(pos_2))));
            }
        });
    }

} // namespace components::table::sort