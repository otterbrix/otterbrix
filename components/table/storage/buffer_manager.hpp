#pragma once

#include <filesystem>

#include <core/result_wrapper.hpp>

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
    public:
        buffer_manager_t() = default;
        virtual ~buffer_manager_t() = default;

        [[nodiscard]] core::result_wrapper_t<buffer_handle_t>
        allocate(memory_tag tag, uint64_t block_size, bool can_destroy = true) {
            return allocate_impl(tag, block_size, can_destroy);
        }
        // Returns true on success; result_wrapper_t<bool> rather than <void> because result_wrapper_t forbids
        // void (`!is_same_v<T, void>` constraint).
        [[nodiscard]] core::result_wrapper_t<bool> reallocate(std::shared_ptr<block_handle_t>& handle,
                                                              uint64_t block_size) {
            return reallocate_impl(handle, block_size);
        }
        [[nodiscard]] core::result_wrapper_t<buffer_handle_t> pin(std::shared_ptr<block_handle_t>& handle) {
            return pin_impl(handle);
        }
        void prefetch(std::vector<std::shared_ptr<block_handle_t>>& handles) { prefetch_impl(handles); }
        void unpin(block_handle_t* handle) { unpin_impl(handle); }

        uint64_t block_allocation_size() const { return block_allocation_size_impl(); }
        uint64_t block_size() const { return block_size_impl(); }

        [[nodiscard]] core::result_wrapper_t<std::shared_ptr<block_handle_t>>
        register_transient_memory(uint64_t size, uint64_t block_size) {
            return register_transient_memory_impl(size, block_size);
        }
        [[nodiscard]] core::result_wrapper_t<std::shared_ptr<block_handle_t>> register_small_memory(uint64_t size) {
            return register_small_memory_impl(memory_tag::BASE_TABLE, size);
        }
        [[nodiscard]] core::result_wrapper_t<std::shared_ptr<block_handle_t>> register_small_memory(memory_tag tag,
                                                                                                    uint64_t size) {
            return register_small_memory_impl(tag, size);
        }

        // Returns true when the reservation was made, out_of_memory when it could not be: a
        // `void` here would make "granted" and "eviction freed nothing" the same observation, so
        // the caller would spend memory it was never given.
        [[nodiscard]] core::result_wrapper_t<bool> reserve_memory(uint64_t size) { return reserve_memory_impl(size); }
        // Stays void: releases a reservation this manager already granted, and a counter
        // decrement has nothing to fail at. Call only after a reserve_memory that answered true.
        void free_reserved_memory(uint64_t size) { free_reserved_memory_impl(size); }
        std::vector<memory_info_t> get_memory_usage_info() const { return get_memory_usage_info_impl(); }
        // Returns out_of_memory when eviction can't shrink to the new limit; forwards
        // buffer_pool_t::set_limit's result.
        [[nodiscard]] core::result_wrapper_t<bool>
        set_memory_limit(uint64_t limit = std::numeric_limits<uint64_t>::max()) {
            return set_memory_limit_impl(limit);
        }

        std::unique_ptr<file_buffer_t>
        construct_manager_buffer(uint64_t size,
                                 std::unique_ptr<file_buffer_t>&& source,
                                 file_buffer_type type = file_buffer_type::MANAGED_BUFFER) {
            return construct_manager_buffer_impl(size, std::move(source), type);
        }
        buffer_pool_t& buffer_pool() const { return buffer_pool_impl(); }

        static uint64_t allocation_size(uint64_t block_size) {
            return align_value<uint64_t>(block_size + DEFAULT_BLOCK_HEADER_SIZE, SECTOR_SIZE);
        }
        static uint64_t memory_footprint(uint64_t block_size) {
            return allocation_size(block_size) + file_buffer_t::lead_for(DEFAULT_BLOCK_HEADER_SIZE);
        }
        uint64_t query_max_memory() const;

        std::pmr::memory_resource* resource() const noexcept { return resource_impl(); }

    private:
        [[nodiscard]] virtual core::result_wrapper_t<buffer_handle_t>
        allocate_impl(memory_tag tag, uint64_t block_size, bool can_destroy) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool> reallocate_impl(std::shared_ptr<block_handle_t>& handle,
                                                                           uint64_t block_size) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<buffer_handle_t>
        pin_impl(std::shared_ptr<block_handle_t>& handle) = 0;
        virtual void prefetch_impl(std::vector<std::shared_ptr<block_handle_t>>& handles) = 0;
        virtual void unpin_impl(block_handle_t* handle) = 0;

        virtual uint64_t block_allocation_size_impl() const = 0;
        virtual uint64_t block_size_impl() const = 0;

        [[nodiscard]] virtual core::result_wrapper_t<std::shared_ptr<block_handle_t>>
        register_transient_memory_impl(uint64_t size, uint64_t block_size) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<std::shared_ptr<block_handle_t>>
        register_small_memory_impl(memory_tag tag, uint64_t size) = 0;

        [[nodiscard]] virtual core::result_wrapper_t<bool> reserve_memory_impl(uint64_t size) = 0;
        virtual void free_reserved_memory_impl(uint64_t size) = 0;
        virtual std::vector<memory_info_t> get_memory_usage_info_impl() const = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool> set_memory_limit_impl(uint64_t limit) = 0;

        virtual std::unique_ptr<file_buffer_t> construct_manager_buffer_impl(uint64_t size,
                                                                             std::unique_ptr<file_buffer_t>&& source,
                                                                             file_buffer_type type) = 0;
        virtual buffer_pool_t& buffer_pool_impl() const = 0;

        virtual std::pmr::memory_resource* resource_impl() const noexcept = 0;
    };

} // namespace components::table::storage