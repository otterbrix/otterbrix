#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>

#include "indexing_vector.hpp"
#include <components/types/logical_value.hpp>
#include <core/string_heap/string_heap.hpp>

namespace components::vector {

    class vector_t;

    constexpr uint64_t INVALID_ID = uint64_t(-1);

    static constexpr bool is_power_of_two(uint64_t v) { return (v & (v - 1)) == 0; }

    static constexpr uint64_t next_power_of_two(uint64_t value) {
        assert(value != std::numeric_limits<uint64_t>::max());
        if (value == 0) {
            return 1;
        }
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        value |= value >> 32;
        value++;
        return value;
    }

    enum class vector_buffer_type : uint8_t
    {
        STANDARD,
        STRING,
        DICTIONARY,
        VECTOR_CHILD,
        STRUCT,
        LIST,
        MANAGED,
        OPAQUE,
        ARRAY
    };

    enum class vector_auxiliary_type : uint8_t
    {
        ARROW
    };

    struct vector_auxiliary_data_t {
        explicit vector_auxiliary_data_t(vector_auxiliary_type type)
            : type_(type) {}
        virtual ~vector_auxiliary_data_t() = default;

        vector_auxiliary_type type_;

    public:
        template<class TARGET>
        TARGET& cast() {
            if (type_ != TARGET::TYPE) {
                throw std::logic_error("Failed to cast vector auxiliary data to type - type mismatch");
            }
            return reinterpret_cast<TARGET&>(*this);
        }

        template<class TARGET>
        const TARGET& cast() const {
            if (type_ != TARGET::TYPE) {
                throw std::logic_error("Failed to cast vector auxiliary data to type - type mismatch");
            }
            return reinterpret_cast<const TARGET&>(*this);
        }
    };

    class vector_buffer_t {
    public:
        explicit vector_buffer_t(std::pmr::memory_resource* resource, size_t data_size);
        explicit vector_buffer_t(std::pmr::memory_resource* resource,
                                 vector_buffer_type type,
                                 size_t capacity = DEFAULT_VECTOR_CAPACITY);
        explicit vector_buffer_t(std::pmr::memory_resource* resource,
                                 types::complex_logical_type type,
                                 size_t capacity = DEFAULT_VECTOR_CAPACITY);
        vector_buffer_t(const vector_buffer_t& other) = delete;
        vector_buffer_t operator=(const vector_buffer_t& other) = delete;
        vector_buffer_t(vector_buffer_t&& other) = default;
        vector_buffer_t& operator=(vector_buffer_t&& other) = default;
        virtual ~vector_buffer_t() = default;

        std::byte* data() noexcept { return data_.get(); }
        const std::byte* data() const noexcept { return data_.get(); }
        void set_data(std::unique_ptr<std::byte[], core::pmr::array_deleter_t> data) noexcept {
            data_ = std::move(data);
        }
        vector_buffer_type type() const noexcept { return type_; }

        void set_auxiliary(std::unique_ptr<vector_auxiliary_data_t> data) { aux_data = std::move(data); }
        vector_auxiliary_data_t* get_auxiliary() noexcept { return aux_data.get(); }
        const vector_auxiliary_data_t* get_auxiliary() const noexcept { return aux_data.get(); }
        std::unique_ptr<vector_auxiliary_data_t>&& take_auxiliary() noexcept { return std::move(aux_data); }

    protected:
        vector_buffer_type type_;
        std::unique_ptr<vector_auxiliary_data_t> aux_data;
        std::unique_ptr<std::byte[], core::pmr::array_deleter_t> data_;
    };

    class string_vector_buffer_t : public vector_buffer_t {
    public:
        explicit string_vector_buffer_t(std::pmr::memory_resource* resource);
        void* insert(void* data, size_t size);
        template<typename T>
        void* insert(T&& str_like);
        void* empty_string(size_t size);
        void add_heap_reference(std::unique_ptr<vector_buffer_t> heap);

    private:
        core::string_heap_t string_heap_;
        // used for overflow strings
        std::pmr::vector<std::shared_ptr<vector_buffer_t>> refs_; // TODO: turn into intrusive_ptr
    };

    template<typename T>
    void* string_vector_buffer_t::insert(T&& str_like) {
        return string_heap_.insert(std::forward<T>(str_like));
    }

    class dictionary_vector_buffer_t : public vector_buffer_t {
    public:
        explicit dictionary_vector_buffer_t(const indexing_vector_t& select);
        explicit dictionary_vector_buffer_t(std::shared_ptr<indexing_data> data);
        explicit dictionary_vector_buffer_t(std::pmr::memory_resource* resource,
                                            uint64_t count = DEFAULT_VECTOR_CAPACITY);

        const indexing_vector_t& get_indexing_vector() const noexcept;
        indexing_vector_t& get_indexing_vector();
        void set_indexing_vector(const indexing_vector_t& indexing_vector);

    private:
        indexing_vector_t indexing_vector_;
    };

    class struct_vector_buffer_t : public vector_buffer_t {
    public:
        explicit struct_vector_buffer_t(std::pmr::memory_resource* resource);
        explicit struct_vector_buffer_t(std::pmr::memory_resource* resource,
                                        types::complex_logical_type struct_type,
                                        uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        struct_vector_buffer_t(vector_t& other, const indexing_vector_t& indexing, uint64_t count);

        std::pmr::vector<std::unique_ptr<vector_t>>& entries() noexcept { return nested_data_; }
        const std::pmr::vector<std::unique_ptr<vector_t>>& entries() const noexcept { return nested_data_; }

    private:
        std::pmr::vector<std::unique_ptr<vector_t>> nested_data_;
    };

    class list_vector_buffer_t : public vector_buffer_t {
    public:
        explicit list_vector_buffer_t(std::unique_ptr<vector_t> vector, uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        explicit list_vector_buffer_t(std::pmr::memory_resource* resource,
                                      const types::complex_logical_type& list_type,
                                      uint64_t capacity = DEFAULT_VECTOR_CAPACITY);

        vector_t& nested_data() { return *nested_data_; }
        const vector_t& nested_data() const { return *nested_data_; }
        void reserve(uint64_t capacity);
        void append(const vector_t& node, uint64_t size, uint64_t offset = 0);
        void append(const vector_t& node, const indexing_vector_t& indexing, uint64_t size, uint64_t offset = 0);
        void push_back(types::logical_value_t&& node);
        uint64_t size() const noexcept { return size_; }
        uint64_t capacity() const noexcept { return capacity_; }
        void set_size(uint64_t size) noexcept { size_ = size; }
        void set_capacity(uint64_t capacity) noexcept { capacity_ = capacity; }

    private:
        std::unique_ptr<vector_t> nested_data_;
        uint64_t capacity_ = 0;
        uint64_t size_ = 0;
    };

    class array_vector_buffer_t : public vector_buffer_t {
    public:
        explicit array_vector_buffer_t(std::unique_ptr<vector_t> vector,
                                       uint64_t size,
                                       uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        explicit array_vector_buffer_t(std::pmr::memory_resource* resource,
                                       const types::complex_logical_type& array_type,
                                       uint64_t capacity = DEFAULT_VECTOR_CAPACITY);

        vector_t& nested_data() { return *nested_data_; }
        const vector_t& nested_data() const { return *nested_data_; }

        uint64_t size() const noexcept { return size_; }
        uint64_t array_size() const noexcept { return underlying_size_; }

    private:
        std::unique_ptr<vector_t> nested_data_;
        uint64_t underlying_size_ = 0;
        uint64_t size_ = 0; // underlying_size_* sizeof(item in array)
    };

} // namespace components::vector