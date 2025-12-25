#pragma once

#include "block_handle.hpp"
#include "buffer_manager.hpp"
#include <core/file/local_file_system.hpp>
#include <filesystem>
#include <map>

namespace components::table::storage {

    class block_manager_t;
    struct eviction_queue_t;

    class standard_buffer_manager_t : public buffer_manager_t {
        friend class buffer_handle_t;
        friend class block_handle_t;
        friend class block_manager_t;

    public:
        standard_buffer_manager_t(std::pmr::memory_resource* resource,
                                  core::filesystem::local_file_system_t& fs,
                                  buffer_pool_t& buffer_pool);

        std::shared_ptr<block_handle_t> register_transient_memory(uint64_t size, uint64_t block_size) final;
        std::shared_ptr<block_handle_t> register_small_memory(uint64_t size) final;
        std::shared_ptr<block_handle_t> register_small_memory(memory_tag tag, uint64_t size) final;

        uint64_t block_allocation_size() const final;
        uint64_t block_size() const final;

        buffer_handle_t allocate(memory_tag tag, uint64_t block_size, bool can_destroy = true) final;

        void reallocate(std::shared_ptr<block_handle_t>& handle, uint64_t block_size) final;

        buffer_handle_t pin(std::shared_ptr<block_handle_t>& handle) final;
        void prefetch(std::vector<std::shared_ptr<block_handle_t>>& handles) final;
        void unpin(std::shared_ptr<block_handle_t>& handle) final;

        void set_memory_limit(uint64_t limit = std::numeric_limits<uint64_t>::max()) final;

        std::vector<memory_info_t> get_memory_usage_info() const override;

        std::unique_ptr<file_buffer_t>
        construct_manager_buffer(uint64_t size,
                                 std::unique_ptr<file_buffer_t>&& source,
                                 file_buffer_type type = file_buffer_type::MANAGED_BUFFER) override;

        void reserve_memory(uint64_t size) final;
        void free_reserved_memory(uint64_t size) final;

        std::pmr::memory_resource* resource() const noexcept final { return resource_; }
        core::filesystem::local_file_system_t& filesystem() const noexcept { return fs_; }

    protected:
        temp_buffer_pool_reservation_t
        evict_blocks_or_throw(memory_tag tag, uint64_t memory_delta, std::unique_ptr<file_buffer_t>* buffer);

        std::shared_ptr<block_handle_t> register_memory(memory_tag tag, uint64_t block_size, bool can_destroy);

        void purge_queue(const block_handle_t& handle) final;

        buffer_pool_t& buffer_pool() const final;

        void add_to_eviction_queue(std::shared_ptr<block_handle_t>& handle) final;

        void batch_read(std::vector<std::shared_ptr<block_handle_t>>& handles,
                        const std::map<uint64_t, uint64_t>& load_map,
                        uint64_t first_block,
                        uint64_t last_block);

        std::pmr::memory_resource* resource_;
        core::filesystem::local_file_system_t& fs_;
        buffer_pool_t& buffer_pool_;
        std::atomic<uint64_t> temp_id_;
        std::unique_ptr<block_manager_t> temp_block_manager_;
        std::atomic<uint64_t> evicted_data_per_tag_[static_cast<uint64_t>(memory_tag::MEMORY_TAG_COUNT)];
    };

} // namespace components::table::storage