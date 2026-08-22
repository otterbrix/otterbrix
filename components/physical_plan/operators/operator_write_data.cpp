#include "operator_write_data.hpp"

namespace components::operators {

    std::size_t operator_write_data_t::size() const { return ids_.size(); }

    operator_write_data_t::ids_t& operator_write_data_t::ids() { return ids_; }

    void operator_write_data_t::append(size_t id) { ids_.emplace_back(id); }

} // namespace components::operators
