#pragma once

#include "block_handle.hpp"
#include "buffer_manager.hpp"
#include <core/file/local_file_system.hpp>
#include <filesystem>
#include <map>

namespace components::table::storage {

#ifdef DEV_MODE
    // Test-observable count of buffer pins. A pin is a lock on the block handle plus atomics, and on
    // release it may allocate an eviction-queue node from the shared pool — so it belongs once per
    // SEGMENT scanned, never once per row.
    uint64_t buffer_pins() noexcept;
    void reset_buffer_pins() noexcept;
#endif

    class block_manager_t;
    struct eviction_queue_t;

    class standard_buffer_manager_t : public buffer_manager_t {
    public:
        standard_buffer_manager_t(std::pmr::memory_resource* resource,
                                  core::filesystem::local_file_system_t& fs,
                                  buffer_pool_t& buffer_pool);

        core::filesystem::local_file_system_t& filesystem() const noexcept { return fs_; }

    protected:
        [[nodiscard]] core::result_wrapper_t<temp_buffer_pool_reservation_t>
        evict_blocks_or_error(memory_tag tag, uint64_t memory_delta, std::unique_ptr<file_buffer_t>* buffer);

        [[nodiscard]] core::result_wrapper_t<std::shared_ptr<block_handle_t>>
        register_memory(memory_tag tag, uint64_t block_size, bool can_destroy);

        [[nodiscard]] core::result_wrapper_t<bool> batch_read(std::vector<std::shared_ptr<block_handle_t>>& handles,
                                                              const std::map<uint64_t, uint64_t>& load_map,
                                                              uint64_t first_block,
                                                              uint64_t last_block);

        std::pmr::memory_resource* resource_;
        core::filesystem::local_file_system_t& fs_;
        buffer_pool_t& buffer_pool_;
        std::atomic<uint64_t> temp_id_;
        std::atomic<uint64_t> evicted_data_per_tag_[static_cast<uint64_t>(memory_tag::MEMORY_TAG_COUNT)];

    private:
        [[nodiscard]] core::result_wrapper_t<buffer_handle_t>
        allocate_impl(memory_tag tag, uint64_t block_size, bool can_destroy) final;
        [[nodiscard]] core::result_wrapper_t<bool> reallocate_impl(std::shared_ptr<block_handle_t>& handle,
                                                                   uint64_t block_size) final;
        [[nodiscard]] core::result_wrapper_t<buffer_handle_t> pin_impl(std::shared_ptr<block_handle_t>& handle) final;
        void prefetch_impl(std::vector<std::shared_ptr<block_handle_t>>& handles) final;
        void unpin_impl(block_handle_t* handle) final;

        uint64_t block_allocation_size_impl() const final;
        uint64_t block_size_impl() const final;

        [[nodiscard]] core::result_wrapper_t<std::shared_ptr<block_handle_t>>
        register_transient_memory_impl(uint64_t size, uint64_t block_size) final;
        [[nodiscard]] core::result_wrapper_t<std::shared_ptr<block_handle_t>>
        register_small_memory_impl(memory_tag tag, uint64_t size) final;

        [[nodiscard]] core::result_wrapper_t<bool> reserve_memory_impl(uint64_t size) final;
        void free_reserved_memory_impl(uint64_t size) final;
        std::vector<memory_info_t> get_memory_usage_info_impl() const final;
        [[nodiscard]] core::result_wrapper_t<bool> set_memory_limit_impl(uint64_t limit) final;

        std::unique_ptr<file_buffer_t> construct_manager_buffer_impl(uint64_t size,
                                                                     std::unique_ptr<file_buffer_t>&& source,
                                                                     file_buffer_type type) final;
        buffer_pool_t& buffer_pool_impl() const final;

        std::pmr::memory_resource* resource_impl() const noexcept final { return resource_; }
    };

} // namespace components::table::storage