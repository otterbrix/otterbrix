#include "block_manager.hpp"

#include <cstring>
#include <vector/indexing_vector.hpp>

#include "block_handle.hpp"
#include "buffer_handle.hpp"
#include "buffer_manager.hpp"
#include "buffer_pool.hpp"

namespace components::table::storage {

    block_manager_t::block_manager_t(buffer_manager_t& buffer_manager, uint64_t block_alloc_size)
        : buffer_manager(buffer_manager)
        , block_alloc_size_(block_alloc_size) {}

    std::shared_ptr<block_handle_t> block_manager_t::register_block(uint64_t block_id) {
        std::lock_guard lock(blocks_lock_);
        auto entry = blocks_.find(block_id);
        if (entry != blocks_.end()) {
            auto existing_ptr = entry->second.lock();
            if (existing_ptr) {
                return existing_ptr;
            }
        }
        auto result = std::make_shared<block_handle_t>(*this, block_id, memory_tag::BASE_TABLE);
        blocks_[block_id] = std::weak_ptr(result);
        return result;
    }

    void block_manager_t::unregister_block(uint64_t id) {
        assert(id < MAXIMUM_BLOCK);
        std::lock_guard lock(blocks_lock_);
        blocks_.erase(id);
    }

    void block_manager_t::unregister_block(block_handle_t& block) {
        auto id = block.block_id();
        std::lock_guard lock(blocks_lock_);
        blocks_.erase(id);
    }

    void block_manager_t::truncate() {}

} // namespace components::table::storage