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
        // Rule 8: the registry is a pmr container on the resource this manager already owns a
        // reference to. The class has no resource member of its own, but buffer_manager is a
        // public reference member declared ABOVE blocks_, so it is fully initialized here.
        , blocks_(buffer_manager.resource())
        , block_alloc_size_(block_alloc_size) {}

    core::result_wrapper_t<bool> block_manager_t::set_block_allocation_size(uint64_t block_alloc_size) {
        // Two things must hold: the size must be a sector multiple (block_location() computes
        // BLOCK_START + id * alloc over a sector-aligned BLOCK_START, so anything else puts every
        // block after the first at an unaligned offset), and it must exceed
        // DEFAULT_BLOCK_HEADER_SIZE, or block_size() — an unsigned subtraction — wraps to ~1.8e19
        // and every bound derived from it becomes nonsense. The sector-multiple rule implies the
        // second, and leaves metadata_manager_t's 64 sub-blocks above their 12-byte chain header
        // even at the smallest legal size.
        const bool sector_aligned = block_alloc_size >= SECTOR_SIZE && (block_alloc_size % SECTOR_SIZE) == 0;
        if (!sector_aligned) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Unusable block allocation size " +
                                                      std::to_string(block_alloc_size) + ": it must be a non-zero "
                                                      "multiple of the " + std::to_string(SECTOR_SIZE) +
                                                      "-byte sector size",
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
        // Every caller screens the transient domain BEFORE calling — reclaim_superseded_root
        // and data_table_t::compact both `continue` on an out-of-domain id rather than relying
        // on mark_as_free's own early return, which stops nothing in the caller. That screening
        // is what keeps this an assert: it guards this code's callers, not the file's contents.
        // If a new caller appears, screen there too; an assert on a path fed by file bytes
        // vanishes under NDEBUG and becomes a silent wrong answer.
        assert(id < MAXIMUM_BLOCK);
        blocks_.erase(id);
    }

    // The registry is keyed by block ID, but this call means "THIS HANDLE is going away", and the
    // two differ once shadow paging makes id reuse the normal case:
    //
    //   data_table_t::compact mark_as_free's + unregister_block(id)'s the outgoing collection's
    //   blocks while its segments still own handles for them -> the ids sit in pending_free_ -> a
    //   committed header promotes them to reusable_ -> a later round's free_block_id hands one back
    //   out and register_block(id) installs a FRESH handle H2 -> only THEN does the stale holder let
    //   go (row_group() returns COUNTED collection copies BY VALUE, so the replaced collection lives
    //   until its last holder drops) and H1's destructor lands here.
    //
    // Erasing by id there removes H2's slot: registry_alive(id) goes false while a live segment is
    // still reading the block — and that is the subtraction stopping reclaim_superseded_root from
    // freeing live table state — and register_block's dedup is defeated, so two handles with their
    // own buffers back one id and one of the writes is silently lost.
    //
    // The check is on OWNERSHIP, not on the raw pointer: this runs from ~block_handle_t, where the
    // registry's weak_ptr to this handle is already expired and lock() would return null for the very
    // entry we do want to erase. weak_from_this() still names this handle's control block during the
    // destructor, and owner_before both ways works on expired weak_ptrs. A handle that was never
    // registered compares unequal to every entry and erases nothing, which is also correct.
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
