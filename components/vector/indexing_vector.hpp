#pragma once
#include <memory>
#include <memory_resource>
#include <unordered_map>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <core/pmr.hpp>

namespace components::vector {
    class vector_buffer_t;

    constexpr size_t DEFAULT_VECTOR_CAPACITY = 1024;

    // Intrusively counted, not shared_ptr: the index array is copied by value all over the
    // vector operations, so every copy used to touch a separate control block. std::shared_ptr
    // is also on the project's forbidden list.
    struct indexing_data : public boost::intrusive_ref_counter<indexing_data> {
        explicit indexing_data(std::pmr::memory_resource* resource, size_t count);

        std::unique_ptr<uint64_t[], core::pmr::array_deleter_t> data;
    };

    // pmr container: it is built per merge/slice on the caller's resource rather than on the
    // global one. The mapped type is still a shared_ptr — vector_buffer_t's ownership is a
    // separate change, flagged by a TODO in vector_buffer.hpp.
    using indexing_cache_t = std::pmr::unordered_map<uint64_t*, std::shared_ptr<vector_buffer_t>>;

    class indexing_vector_t {
    public:
        explicit indexing_vector_t(std::pmr::memory_resource* resource, uint64_t* indexing = nullptr) noexcept;
        explicit indexing_vector_t(std::pmr::memory_resource* resource, uint64_t count);
        explicit indexing_vector_t(std::pmr::memory_resource* resource, uint64_t start, uint64_t count);
        explicit indexing_vector_t(boost::intrusive_ptr<indexing_data> data) noexcept;

        indexing_vector_t(const indexing_vector_t& other);
        indexing_vector_t& operator=(const indexing_vector_t& other);
        indexing_vector_t(indexing_vector_t&& other) noexcept;
        indexing_vector_t& operator=(indexing_vector_t&& other) noexcept;

        void reset(uint64_t count);
        void reset(uint64_t* indexing);
        bool is_set() const noexcept { return indexing_; }
        void set_index(uint64_t index, uint64_t location);
        void swap(uint64_t i, uint64_t j) noexcept;
        uint64_t get_index(uint64_t index) const;
        uint64_t* data() noexcept;
        const uint64_t* data() const noexcept;
        boost::intrusive_ptr<indexing_data>
        slice(std::pmr::memory_resource* resource, const indexing_vector_t& indexing, uint64_t count) const;
        uint64_t& operator[](uint64_t index) const;
        bool is_valid() const noexcept;
        std::pmr::memory_resource* resource() const { return resource_; }
        uint64_t capacity() const;

    private:
        std::pmr::memory_resource* resource_;
        boost::intrusive_ptr<indexing_data> data_;
        uint64_t* indexing_{nullptr};
    };

    static uint64_t ZERO_VECTOR[DEFAULT_VECTOR_CAPACITY] = {0};
    // nullptr as memory_resource* is questionable here, but i didn't find a better alternative
    inline static indexing_vector_t ZERO_INDEXING_VECTOR = indexing_vector_t(nullptr, ZERO_VECTOR);

} // namespace components::vector