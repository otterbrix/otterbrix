#pragma once

#include <filesystem>

#include "block_handle.hpp"

namespace core::filesystem {
    class local_file_system_t;
}

namespace components::table::storage {
    class buffer_pool_t;

    constexpr size_t DEFAULT_BLOCK_ALLOC_SIZE = size_t{1} << 18; // 262144

    template<typename T>
    static T align_value(T n, T val = 8) {
        return ((n + (val - 1)) / val) * val;
    }

    template<typename T>
    static bool is_value_aligned(T n, T val = 8) {
        return (n % val) == 0;
    }

    struct memory_info_t {
        memory_tag tag;
        uint64_t size;
        uint64_t evicted_data;
    };

    class buffer_manager_t {
        friend class buffer_handle_t;
        friend class block_handle_t;
        friend class block_manager_t;

    public:
        buffer_manager_t() = default;
        virtual ~buffer_manager_t() = default;

        virtual buffer_handle_t allocate(memory_tag tag, uint64_t block_size, bool can_destroy = true) = 0;
        virtual void reallocate(std::shared_ptr<block_handle_t>& handle, uint64_t block_size) = 0;
        virtual buffer_handle_t pin(std::shared_ptr<block_handle_t>& handle) = 0;
        virtual void prefetch(std::vector<std::shared_ptr<block_handle_t>>& handles) = 0;
        virtual void unpin(std::shared_ptr<block_handle_t>& handle) = 0;

        virtual uint64_t block_allocation_size() const = 0;
        virtual uint64_t block_size() const = 0;

        virtual std::shared_ptr<block_handle_t> register_transient_memory(uint64_t size, uint64_t block_size);
        virtual std::shared_ptr<block_handle_t> register_small_memory(uint64_t size);
        virtual std::shared_ptr<block_handle_t> register_small_memory(memory_tag tag, uint64_t size);

        virtual void reserve_memory(uint64_t size);
        virtual void free_reserved_memory(uint64_t size);
        virtual std::vector<memory_info_t> get_memory_usage_info() const = 0;
        virtual void set_memory_limit(uint64_t limit = std::numeric_limits<uint64_t>::max());

        virtual std::unique_ptr<file_buffer_t>
        construct_manager_buffer(uint64_t size,
                                 std::unique_ptr<file_buffer_t>&& source,
                                 file_buffer_type type = file_buffer_type::MANAGED_BUFFER);
        virtual buffer_pool_t& buffer_pool() const;

        static uint64_t allocation_size(uint64_t block_size) {
            return align_value<uint64_t>(block_size + DEFAULT_BLOCK_HEADER_SIZE, SECTOR_SIZE);
        }
        uint64_t query_max_memory() const;

        virtual std::pmr::memory_resource* resource() const noexcept = 0;

    protected:
        virtual void purge_queue(const block_handle_t& handle) = 0;
        virtual void add_to_eviction_queue(std::shared_ptr<block_handle_t>& handle);
    };

} // namespace components::table::storage