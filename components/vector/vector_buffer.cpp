#include "vector_buffer.hpp"

#include <cassert>

#include "vector.hpp"
#include "vector_operations.hpp"

namespace components::vector {

    vector_buffer_t::vector_buffer_t(std::pmr::memory_resource* resource, size_t data_size)
        : type_(vector_buffer_type::STANDARD)
        , data_(new (resource->allocate(data_size, alignof(std::byte))) std::byte[data_size],
                core::pmr::array_deleter_t(resource, data_size, alignof(std::byte))) {}

    vector_buffer_t::vector_buffer_t(std::pmr::memory_resource* resource, vector_buffer_type type, size_t capacity)
        : type_(type)
        , data_(new (resource->allocate(capacity, alignof(std::byte))) std::byte[capacity],
                core::pmr::array_deleter_t(resource, capacity, alignof(std::byte))) {}

    vector_buffer_t::vector_buffer_t(std::pmr::memory_resource* resource,
                                     types::complex_logical_type type,
                                     size_t capacity)
        : type_(vector_buffer_type::STANDARD)
        , data_(type.size() == 0 ? nullptr
                                 : new (resource->allocate(capacity * type.size(), type.align()))
                                       std::byte[capacity * type.size()],
                core::pmr::array_deleter_t(resource, capacity * type.size(), type.align())) {}

    string_vector_buffer_t::string_vector_buffer_t(std::pmr::memory_resource* resource)
        : vector_buffer_t(resource, vector_buffer_type::STRING)
        , string_heap_(resource)
        , refs_(resource) {}

    void* string_vector_buffer_t::insert(void* data, size_t size) { return string_heap_.insert(data, size); }

    void* string_vector_buffer_t::empty_string(size_t size) { return string_heap_.empty_string(size); }

    void string_vector_buffer_t::add_heap_reference(std::unique_ptr<vector_buffer_t> heap) {
        refs_.push_back(std::move(heap));
    }

    dictionary_vector_buffer_t::dictionary_vector_buffer_t(const indexing_vector_t& select)
        : vector_buffer_t(select.resource(), vector_buffer_type::DICTIONARY)
        , indexing_vector_(select) {}

    dictionary_vector_buffer_t::dictionary_vector_buffer_t(std::shared_ptr<indexing_data> data)
        : vector_buffer_t(data->data.get_deleter().resource(), vector_buffer_type::DICTIONARY)
        , indexing_vector_(data) {}

    dictionary_vector_buffer_t::dictionary_vector_buffer_t(std::pmr::memory_resource* resource, uint64_t count)
        : vector_buffer_t(resource, vector_buffer_type::DICTIONARY)
        , indexing_vector_(resource, count) {}

    const indexing_vector_t& dictionary_vector_buffer_t::get_indexing_vector() const noexcept {
        return indexing_vector_;
    }

    indexing_vector_t& dictionary_vector_buffer_t::get_indexing_vector() { return indexing_vector_; }

    void dictionary_vector_buffer_t::set_indexing_vector(const indexing_vector_t& indexing_vector) {
        indexing_vector_ = indexing_vector;
    }

    struct_vector_buffer_t::struct_vector_buffer_t(std::pmr::memory_resource* resource)
        : vector_buffer_t(resource, vector_buffer_type::STRUCT)
        , nested_data_(resource) {}

    struct_vector_buffer_t::struct_vector_buffer_t(std::pmr::memory_resource* resource,
                                                   types::complex_logical_type struct_type,
                                                   uint64_t capacity)
        : vector_buffer_t(resource, vector_buffer_type::STRUCT)
        , nested_data_(resource) {
        auto& child_types = struct_type.child_types();
        for (auto& child_type : child_types) {
            auto vector = std::make_unique<vector_t>(resource, child_type, capacity);
            nested_data_.push_back(std::move(vector));
        }
    }

    struct_vector_buffer_t::struct_vector_buffer_t(vector_t& other, const indexing_vector_t& indexing, uint64_t count)
        : vector_buffer_t(other.resource(), vector_buffer_type::STRUCT)
        , nested_data_(other.resource()) {
        for (auto& child_vector : other.entries()) {
            auto vector = std::make_unique<vector_t>(*child_vector, indexing, count);
            nested_data_.push_back(std::move(vector));
        }
    }

    list_vector_buffer_t::list_vector_buffer_t(std::unique_ptr<vector_t> vector, uint64_t capacity)
        : vector_buffer_t(vector->resource(), vector_buffer_type::LIST)
        , nested_data_(std::move(vector))
        , capacity_(capacity) {}

    list_vector_buffer_t::list_vector_buffer_t(std::pmr::memory_resource* resource,
                                               const types::complex_logical_type& list_type,
                                               uint64_t capacity)
        : vector_buffer_t(resource, vector_buffer_type::LIST)
        , nested_data_(std::make_unique<vector_t>(resource, list_type.child_type(), capacity))
        , capacity_(capacity) {}

    void list_vector_buffer_t::reserve(uint64_t capacity) {
        if (capacity <= capacity_) {
            return;
        }
        capacity = next_power_of_two(capacity);
        nested_data_->resize(capacity_, capacity);
        capacity_ = capacity;
    }

    void list_vector_buffer_t::append(const vector_t& node, uint64_t size, uint64_t offset) {
        reserve(size_ + size - offset);
        vector_ops::copy(node, *nested_data_, size, offset, size_);
        size_ += size - offset;
    }

    void list_vector_buffer_t::append(const vector_t& node,
                                      const indexing_vector_t& indexing,
                                      uint64_t size,
                                      uint64_t offset) {
        reserve(size_ + size - offset);
        vector_ops::copy(node, *nested_data_, indexing, size, offset, size_);
        size_ += size - offset;
    }

    void list_vector_buffer_t::push_back(types::logical_value_t&& node) {
        while (size_ + 1 > capacity_) {
            nested_data_->resize(capacity_, capacity_ * 2);
            capacity_ *= 2;
        }
        nested_data_->set_value(size_++, node);
    }

    array_vector_buffer_t::array_vector_buffer_t(std::unique_ptr<vector_t> vector, uint64_t size, uint64_t capacity)
        : vector_buffer_t(vector->resource(), vector_buffer_type::ARRAY, capacity * size)
        , nested_data_(std::move(vector))
        , underlying_size_(size)
        , size_(capacity) {}

    array_vector_buffer_t::array_vector_buffer_t(std::pmr::memory_resource* resource,
                                                 const types::complex_logical_type& array_type,
                                                 uint64_t capacity)
        : vector_buffer_t(resource,
                          vector_buffer_type::ARRAY,
                          capacity *
                              static_cast<const types::array_logical_type_extension*>(array_type.extension())->size())
        , nested_data_(std::make_unique<vector_t>(
              resource,
              array_type.child_type(),
              capacity * static_cast<const types::array_logical_type_extension*>(array_type.extension())->size()))
        , underlying_size_(static_cast<const types::array_logical_type_extension*>(array_type.extension())->size())
        , size_(capacity) {}

    child_vector_buffer_t::child_vector_buffer_t(vector_t vector)
        : vector_buffer_t(vector.resource(), vector_buffer_type::VECTOR_CHILD)
        , data_(std::move(vector)) {}

    vector_t& child_vector_buffer_t::nested_data() noexcept { return data_; }

    const vector_t& child_vector_buffer_t::nested_data() const noexcept { return data_; }

} // namespace components::vector