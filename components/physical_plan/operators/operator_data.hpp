#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <memory_resource>
#include <vector/data_chunk.hpp>

namespace components::operators {

    using data_t = vector::data_chunk_t;

    class operator_data_t : public boost::intrusive_ref_counter<operator_data_t> {
    public:
        using ptr = boost::intrusive_ptr<operator_data_t>;

        operator_data_t(std::pmr::memory_resource* resource,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        uint64_t capacity = vector::DEFAULT_VECTOR_CAPACITY);
        operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);

        ptr copy() const;

        std::size_t size() const;
        vector::data_chunk_t& data_chunk();
        const vector::data_chunk_t& data_chunk() const;
        std::pmr::memory_resource* resource() const;
        void append(vector::vector_t row);

    private:
        std::pmr::memory_resource* resource_;
        data_t data_;
    };

    using operator_data_ptr = operator_data_t::ptr;

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource,
                                                const std::pmr::vector<types::complex_logical_type>& types,
                                                uint64_t capacity = vector::DEFAULT_VECTOR_CAPACITY) {
        return {new operator_data_t(resource, types, capacity)};
    }

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        return {new operator_data_t(resource, std::move(chunk))};
    }

} // namespace components::operators
