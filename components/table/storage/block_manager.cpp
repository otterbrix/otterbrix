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
        // The registry is a pmr container on the resource this manager already owns a
        // reference to. The class has no resource member of its own, but buffer_manager is a
        // public reference member declared ABOVE blocks_, so it is fully initialized here.
        , blocks_(buffer_manager.resource())
        , block_alloc_size_(block_alloc_size) {}

    core::result_wrapper_t<bool> block_manager_t::set_block_allocation_size(uint64_t block_alloc_size) {
        // Must be a sector multiple: block_location() computes BLOCK_START + id * alloc over a
        // sector-aligned BLOCK_START, so anything else misaligns every block after the first.
        // This also keeps the size above DEFAULT_BLOCK_HEADER_SIZE, or block_size() (an unsigned
        // subtraction) would wrap to ~1.8e19.
        const bool sector_aligned = block_alloc_size >= SECTOR_SIZE && (block_alloc_size % SECTOR_SIZE) == 0;
        if (!sector_aligned) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Unusable block allocation size " + std::to_string(block_alloc_size) +
                                                      ": it must be a non-zero "
                                                      "multiple of the " +
                                                      std::to_string(SECTOR_SIZE) + "-byte sector size",
                                                  buffer_manager.resource()});
        }
        block_alloc_size_ = block_alloc_size;
        return true;
    }

    std::shared_ptr<block_handle_t> block_manager_t::register_block(uint64_t block_id) {
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
        // An assert, not a runtime check, because every caller already screens the transient
        // domain before calling (reclaim_superseded_root, data_table_t::compact). A new caller
        // must screen too, or this needs a real check instead.
        assert(id < MAXIMUM_BLOCK);
        blocks_.erase(id);
    }

    // The registry is keyed by block ID, but this call means "THIS HANDLE is going away" — the two
    // differ once shadow paging makes id reuse the normal case: an outgoing collection's blocks
    // sit in pending_free_, get promoted to reusable_, and a later round's register_block(id)
    // installs a FRESH handle H2 BEFORE the stale H1's destructor (a lingering counted holder)
    // runs. Erasing by id there would remove H2's slot instead, telling reclaim_superseded_root a
    // live block is free. The check is on OWNERSHIP, not the raw pointer, because the registry's
    // weak_ptr to `block` is already expired by the time ~block_handle_t runs; weak_from_this()
    // still names the right control block, and owner_before compares correctly on expired
    // weak_ptrs. A handle that was never registered erases nothing, which is also correct.
    void block_manager_t::unregister_block(block_handle_t& block) {
        auto entry = blocks_.find(block.block_id());
        if (entry == blocks_.end()) {
            return;
        }
        auto self = block.weak_from_this();
        const bool same_handle = !entry->second.owner_before(self) && !self.owner_before(entry->second);
        if (!same_handle) {
            return; // the slot has already been re-registered to a different, live handle
        }
        blocks_.erase(entry);
    }

    core::result_wrapper_t<bool> block_manager_t::truncate() { return true; }

    bool block_manager_t::registry_alive(uint64_t id) {
        auto entry = blocks_.find(id);
        return entry != blocks_.end() && !entry->second.expired();
    }

    std::pmr::vector<uint64_t> block_manager_t::live_registry_ids() {
        std::pmr::vector<uint64_t> ids(buffer_manager.resource());
        ids.reserve(blocks_.size());
        for (auto& [id, weak] : blocks_) {
            if (!weak.expired()) {
                ids.push_back(id);
            }
        }
        return ids;
    }

} // namespace components::table::storage
