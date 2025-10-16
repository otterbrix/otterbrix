#pragma once

#include "validation.hpp"
#include "vector_buffer.hpp"
#include <components/types/logical_value.hpp>

namespace components::vector {

    class vector_t;

    static const indexing_vector_t* incremental_indexing_vector(std::pmr::memory_resource* resource) {
        static const indexing_vector_t INCREMENTAL_INDEXING_VECTOR(resource, nullptr);
        return &INCREMENTAL_INDEXING_VECTOR;
    }

    static const indexing_vector_t* zero_indexing_vector(std::pmr::memory_resource* resource) {
        static const indexing_vector_t ZERO_INDEXING_VECTOR = indexing_vector_t(resource, ZERO_VECTOR);
        return &ZERO_INDEXING_VECTOR;
    }

    static const indexing_vector_t*
    zero_indexing_vector(std::pmr::memory_resource* resource, uint64_t count, indexing_vector_t& owned_indexing) {
        if (count <= DEFAULT_VECTOR_CAPACITY) {
            return zero_indexing_vector(resource);
        }
        owned_indexing.reset(count);
        for (uint64_t i = 0; i < count; i++) {
            owned_indexing.set_index(i, 0);
        }
        return &owned_indexing;
    }

    struct unified_vector_format {
        unified_vector_format(std::pmr::memory_resource* resource, uint64_t capacity);
        unified_vector_format(const unified_vector_format& other) = delete;
        unified_vector_format& operator=(const unified_vector_format& other) = delete;
        unified_vector_format(unified_vector_format&& other) noexcept;
        unified_vector_format& operator=(unified_vector_format&& other) noexcept;
        ~unified_vector_format() = default;

        template<typename T>
        const T* get_data() const {
            return reinterpret_cast<const T*>(data);
        }
        template<typename T>
        T* get_data() {
            return reinterpret_cast<T*>(data);
        }

        const indexing_vector_t* referenced_indexing = nullptr;
        std::byte* data = nullptr;
        validity_mask_t validity;
        indexing_vector_t owned_indexing;
    };

    struct recursive_unified_vector_format {
        recursive_unified_vector_format(std::pmr::memory_resource* resource, uint64_t capacity);
        recursive_unified_vector_format(const recursive_unified_vector_format& other) = delete;
        recursive_unified_vector_format& operator=(const recursive_unified_vector_format& other) = delete;
        recursive_unified_vector_format(recursive_unified_vector_format&& other) noexcept = default;
        recursive_unified_vector_format& operator=(recursive_unified_vector_format&& other) noexcept = default;
        ~recursive_unified_vector_format() = default;

        unified_vector_format parent;
        std::vector<recursive_unified_vector_format> children;
        types::complex_logical_type type;
    };

    struct resize_info_t {
        resize_info_t(vector_t& vec, std::byte* data, vector_buffer_t* buf, uint64_t multiplier)
            : vec(vec)
            , data(data)
            , buffer(buf)
            , multiplier(multiplier) {}

        vector_t& vec;
        std::byte* data;
        vector_buffer_t* buffer;
        uint64_t multiplier;
    };

    enum class vector_type : uint8_t
    {
        FLAT,
        CONSTANT,
        DICTIONARY,
        SEQUENCE
    };

    class vector_t {
    public:
        explicit vector_t(std::pmr::memory_resource* resource,
                          types::complex_logical_type type,
                          uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        explicit vector_t(std::pmr::memory_resource* resource,
                          const types::logical_value_t& value,
                          uint64_t capacity = DEFAULT_VECTOR_CAPACITY);
        explicit vector_t(const vector_t& other, const indexing_vector_t& indexing, uint64_t count);
        explicit vector_t(const vector_t& other, uint64_t offset, uint64_t count);
        explicit vector_t(std::pmr::memory_resource* resource,
                          types::complex_logical_type type,
                          bool create_data,
                          bool zero_data,
                          uint64_t capacity = DEFAULT_VECTOR_CAPACITY);

        vector_t(const vector_t& other);
        vector_t& operator=(const vector_t& other);
        vector_t(vector_t&& other) noexcept;
        vector_t& operator=(vector_t&& other) noexcept;

        vector_type get_vector_type() const noexcept { return vector_type_; }
        const types::complex_logical_type& type() const noexcept { return type_; }
        std::byte* data() noexcept { return data_; }
        const std::byte* data() const noexcept { return data_; }
        void set_data(std::byte* data) noexcept { data_ = data; }
        template<typename T>
        T* data() noexcept {
            return reinterpret_cast<T*>(data_);
        }
        template<typename T>
        const T* data() const noexcept {
            return reinterpret_cast<T*>(data_);
        }
        std::shared_ptr<vector_buffer_t> auxiliary() { return auxiliary_; }
        std::shared_ptr<vector_buffer_t> get_buffer() { return buffer_; }

        void reference(const types::logical_value_t& value);
        void reference(const vector_t& other);
        void reinterpret(const vector_t& other);

        void reference_and_set_type(const vector_t& other);

        void slice(const vector_t& other, uint64_t offset, uint64_t end);
        void slice(const vector_t& other, const indexing_vector_t& indexing, uint64_t count);
        void slice(const indexing_vector_t& indexing, uint64_t count);
        void slice(const indexing_vector_t& indexing, uint64_t count, indexing_cache_t& cache);

        void flatten(uint64_t count);
        void flatten(const indexing_vector_t& indexing, uint64_t count);
        void to_unified_format(uint64_t count, unified_vector_format& data);
        static void recursive_to_unified_format(vector_t& input, uint64_t count, recursive_unified_vector_format& data);

        void sequence(int64_t start, int64_t increment, uint64_t count);

        void push_back(types::logical_value_t logical_value);
        void set_value(uint64_t index, const types::logical_value_t& val);

        void set_auxiliary(std::shared_ptr<vector_buffer_t> new_buffer) { auxiliary_ = std::move(new_buffer); }

        void copy_buffer(vector_t& other) {
            buffer_ = other.buffer_;
            data_ = other.data_;
        }

        void resize(uint64_t cur_size, uint64_t new_size);
        void reserve(uint64_t required_capacity);

        void find_resize_infos(std::vector<resize_info_t>& resize_infos, uint64_t multiplier);

        uint64_t allocation_size(uint64_t cardinality) const;

        void set_vector_type(vector_type vector_type);

        void set_list_size(uint64_t size);
        void set_null(uint64_t position, bool value);
        void set_null(bool is_null);

        void append(const vector_t& source, uint64_t source_size, uint64_t source_offset = 0);
        void append(const vector_t& source,
                    const indexing_vector_t& indexing,
                    uint64_t source_size,
                    uint64_t source_offset = 0);

        std::pmr::memory_resource* resource() const noexcept { return validity_.resource(); }
        validity_mask_t& validity() noexcept { return validity_; }
        const validity_mask_t& validity() const noexcept { return validity_; }
        std::pmr::vector<std::unique_ptr<vector_t>>& entries();
        const std::pmr::vector<std::unique_ptr<vector_t>>& entries() const;
        vector_t& entry();
        const vector_t& entry() const;
        vector_t& child();
        const vector_t& child() const;
        indexing_vector_t& indexing();
        const indexing_vector_t& indexing() const;
        size_t size() const;
        bool is_null(uint64_t index = 0) const;

        void get_sequence(int64_t& start, int64_t& increment, int64_t& sequence_count) const;
        void get_sequence(int64_t& start, int64_t& increment) const;

        types::logical_value_t value(uint64_t index) const;

    private:
        types::logical_value_t value_internal(uint64_t index) const;

        vector_type vector_type_;
        types::complex_logical_type type_;
        std::byte* data_;
        validity_mask_t validity_;
        std::shared_ptr<vector_buffer_t> buffer_;
        std::shared_ptr<vector_buffer_t> auxiliary_;
    };

    class child_vector_buffer_t : public vector_buffer_t {
    public:
        explicit child_vector_buffer_t(vector_t vector);

        vector_t& nested_data() noexcept;
        const vector_t& nested_data() const noexcept;

    private:
        vector_t data_;
    };

} // namespace components::vector
