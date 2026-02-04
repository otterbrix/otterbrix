#include "operator_write_data.hpp"

namespace components::base::operators {

    operator_write_data_t::ptr operator_write_data_t::copy() const {
        auto copy_data = make_operator_write_data(resource_);
        copy_data->ids_.reserve(ids_.size());
        for (const auto& id : ids_) {
            copy_data->ids_.push_back(id);
        }
        return copy_data;
    }

    std::size_t operator_write_data_t::size() const { return ids_.size(); }

    operator_write_data_t::ids_t& operator_write_data_t::ids() { return ids_; }

    operator_write_data_t::updated_types_map_t& operator_write_data_t::updated_types_map() { return updated_; }

    void operator_write_data_t::append(size_t id) { ids_.emplace_back(id); }

    bool operator_write_data_t::pair_comparator::operator()(
        const std::pair<std::pmr::string, components::types::complex_logical_type>& lhs,
        const std::pair<std::pmr::string, components::types::complex_logical_type>& rhs) const {
        if (lhs.first != rhs.first) {
            return lhs.first < rhs.first;
        }

        const auto& lhs_type = lhs.second;
        const auto& rhs_type = rhs.second;

        if (lhs_type.type() != rhs_type.type()) {
            return lhs_type.type() < rhs_type.type();
        }

        auto lhs_ext = lhs_type.extension();
        auto rhs_ext = rhs_type.extension();

        if (lhs_ext == nullptr && rhs_ext == nullptr) {
            return false;
        }
        if (lhs_ext == nullptr) {
            return true;
        }
        if (rhs_ext == nullptr) {
            return false;
        }

        if (lhs_ext->type() != rhs_ext->type()) {
            return lhs_ext->type() < rhs_ext->type();
        }

        return lhs_ext->alias() < rhs_ext->alias();
    }

} // namespace components::base::operators
