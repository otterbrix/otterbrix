#include "operator_data.hpp"

namespace components::operators {

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<types::complex_logical_type>& types,
                                     uint64_t capacity)
        : resource_(resource)
        , data_(resource, types, capacity) {}

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : resource_(resource)
        , data_(std::move(chunk)) {}

    operator_data_t::ptr operator_data_t::copy() const {
        auto copy_data = make_operator_data(resource_, data_.types(), data_.size());
        data_.copy(copy_data->data_, 0);
        return copy_data;
    }

    std::size_t operator_data_t::size() const { return data_.size(); }

    vector::data_chunk_t& operator_data_t::data_chunk() { return data_; }

    const vector::data_chunk_t& operator_data_t::data_chunk() const { return data_; }

    std::pmr::memory_resource* operator_data_t::resource() const { return resource_; }

    void operator_data_t::append(vector::vector_t row) {
        size_t index = data_.size();
        for (size_t i = 0; i < row.size(); i++) {
            data_.set_value(0, index, row.value(i));
        }
    }

} // namespace components::operators
