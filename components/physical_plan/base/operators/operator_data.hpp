#pragma once

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/document.hpp>
#include <memory_resource>
#include <vector/data_chunk.hpp>

namespace components::base::operators {

    using data_t = std::variant<std::pmr::vector<document::document_ptr>, vector::data_chunk_t>;

    class operator_data_t : public boost::intrusive_ref_counter<operator_data_t> {
    public:
        using ptr = boost::intrusive_ptr<operator_data_t>;

        explicit operator_data_t(std::pmr::memory_resource* resource);
        operator_data_t(std::pmr::memory_resource* resource,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        uint64_t capacity);
        operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);

        ptr copy() const;

        std::size_t size() const;
        std::pmr::vector<document::document_ptr>& documents();
        const std::pmr::vector<document::document_ptr>& documents() const;
        vector::data_chunk_t& data_chunk();
        const vector::data_chunk_t& data_chunk() const;
        bool uses_data_chunk() const;
        bool uses_documents() const;
        std::pmr::memory_resource* resource();
        void append(document::document_ptr document);
        void append(vector::vector_t row);

    private:
        std::pmr::memory_resource* resource_;
        data_t data_;
    };

    using operator_data_ptr = operator_data_t::ptr;

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource) {
        return {new operator_data_t(resource)};
    }

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource,
                                                const std::pmr::vector<types::complex_logical_type>& types,
                                                uint64_t capacity = vector::DEFAULT_VECTOR_CAPACITY) {
        return {new operator_data_t(resource, types, capacity)};
    }

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        return {new operator_data_t(resource, std::move(chunk))};
    }

} // namespace components::base::operators
