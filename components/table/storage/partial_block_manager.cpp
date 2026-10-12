#include "partial_block_manager.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>

#include <absl/crc/crc32c.h>

#include "block_handle.hpp"
#include "buffer_handle.hpp"
#include "buffer_manager.hpp"

namespace components::table::storage {

    partial_block_manager_t::partial_block_manager_t(block_manager_t& block_manager, tails_t tails)
        : block_manager_(block_manager)
        , tails_(tails)
        , ready_(block_manager.buffer_manager.resource()) {}

    partial_block_manager_t::~partial_block_manager_t() {
        for (auto& pb : partial_blocks_) {
            if (pb.flushed_bytes == 0) {
                block_manager_.mark_as_free(pb.block_id);
            }
        }
    }

    partial_block_manager_t partial_block_manager_t::for_appends(block_manager_t& block_manager) {
        return partial_block_manager_t{block_manager, tails_t::kept};
    }

    partial_block_manager_t partial_block_manager_t::for_checkpoint(block_manager_t& block_manager) {
        return partial_block_manager_t{block_manager, tails_t::dropped};
    }

    // Place every segment at an 8-byte-aligned offset: the offset is dereferenced after reload as
    // uint64_t*/int32_t*/T*, and byte-granular packing made those reads misaligned UB (caught by
    // -fsanitize=alignment). Padding bytes stay zeroed (fresh_image memsets the buffer).
    uint64_t partial_block_manager_t::partial_block_t::next_offset() const { return align_value<uint64_t>(used_bytes); }

    uint64_t partial_block_manager_t::partial_block_t::free_space() const {
        const uint64_t offset = next_offset();
        return offset < block_capacity ? block_capacity - offset : 0;
    }

    std::unique_ptr<block_t> partial_block_manager_t::fresh_image(uint64_t block_id) {
        auto block = std::make_unique<block_t>(block_manager_.buffer_manager.resource(),
                                               block_id,
                                               static_cast<uint64_t>(block_manager_.block_size()));
        std::memset(block->buffer(), 0, static_cast<size_t>(block_manager_.block_size()));
        return block;
    }

    void partial_block_manager_t::ready(std::pmr::vector<placed_t>& placements) {
        for (auto& placed : placements) {
            ready_.push_back(std::move(placed));
        }
        placements.clear();
    }

    bool partial_block_manager_t::dead_since_flush(const partial_block_t& pb) {
        return pb.used_bytes > pb.flushed_bytes && pb.plain_since_flush == 0 && !pb.placements.empty() &&
               std::none_of(pb.placements.begin(), pb.placements.end(), [](const placed_t& placed) {
                   return placed.placement->alive();
               });
    }

    core::result_wrapper_t<partial_block_allocation_t>
    partial_block_manager_t::place(const void* data, uint64_t size, std::unique_ptr<placement_t> placement) {
        assert(size > 0 && data != nullptr && "a placement carries bytes");
        const uint64_t block_size = block_manager_.block_size();

        // Dedicated: nothing else will land in it, so it is written now and the image dies here.
        if (size > static_cast<uint64_t>(static_cast<double>(block_size) * FULL_THRESHOLD)) {
            const uint64_t block_id = block_manager_.free_block_id();
            auto image = fresh_image(block_id);
            std::memcpy(image->buffer(), data, size);
            if (auto written = block_manager_.write(*image, block_id); written.has_error()) {
                return written.error(); // the placement dies here: its segment stays transient
            }
            const partial_block_allocation_t dedicated{block_id, 0, size};
            if (placement) {
                ready_.push_back(placed_t{dedicated, std::move(placement)});
            }
            return dedicated;
        }

        // Packed: the first open tail with room, else a fresh tail.
        auto pb = std::find_if(partial_blocks_.begin(), partial_blocks_.end(), [size](const partial_block_t& p) {
            return size <= p.free_space();
        });
        if (pb == partial_blocks_.end()) {
            const uint64_t block_id = block_manager_.free_block_id();
            std::pmr::vector<placed_t> placements(block_manager_.buffer_manager.resource());
            partial_blocks_.push_back(
                partial_block_t{block_id, 0, block_size, 0, fresh_image(block_id), 0, std::move(placements), 0});
            pb = std::prev(partial_blocks_.end());
        }
        const uint64_t offset = pb->next_offset();
        pb->used_bytes = static_cast<uint32_t>(offset + size);
        std::memcpy(pb->image->buffer() + offset, data, size);
        if (placement) {
            pb->placements.push_back(
                placed_t{partial_block_allocation_t{pb->block_id, static_cast<uint32_t>(offset), size},
                         std::move(placement)});
        } else {
            pb->plain_since_flush++;
        }

        // A grown tail's block may already be resident (a reader pinned an earlier segment of it):
        // that copy would otherwise keep serving zeros where this segment now lives. The range is
        // past every byte a reader can hold, so the patch races nothing.
        if (tails_ == tails_t::kept && block_manager_.registry_alive(pb->block_id)) {
            auto handle = block_manager_.register_block(pb->block_id);
            if (handle->state() == block_state::LOADED) {
                auto pinned = block_manager_.buffer_manager.pin(handle);
                if (pinned.has_error()) {
                    return pinned.error();
                }
                std::memcpy(pinned.value().ptr() + offset, data, size);
            }
        }

        const partial_block_allocation_t allocation{pb->block_id, static_cast<uint32_t>(offset), size};
        if (pb->free_space() >= MIN_REUSABLE_TAIL) {
            return allocation; // still open: buffered until the flush
        }
        // Full: written and dropped now, by the same write the flush would have done. Its
        // placements wait for the next flush to adopt them; a refused write destroys them, and
        // their segments stay transient.
        core::error_t written = core::error_t::no_error();
        if (tails_ == tails_t::kept) {
            written = flush_tail(*pb);
        } else if (auto whole = block_manager_.write(*pb->image, pb->block_id); whole.has_error()) {
            written = whole.error();
        }
        if (!written.contains_error()) {
            ready(pb->placements);
        }
        partial_blocks_.erase(pb);
        if (written.contains_error()) {
            return written;
        }
        return allocation;
    }

    // Every byte of a tail reaches the disk once: the first flush writes the used prefix, each
    // later one the range appended since (plus the checksum slot both times).
    core::error_t partial_block_manager_t::flush_tail(partial_block_t& pb) {
        const uint64_t from = align_value<uint64_t>(pb.flushed_bytes);
        // The tail's CRC grows with its bytes (the padding between flushed_bytes and
        // `from` is zero in the image and on disk), so a placement pays a CRC of its own bytes, not
        // of the whole block. Adopted only once the write landed: a refused placement is forgotten.
        const auto* payload = reinterpret_cast<const char*>(pb.image->buffer());
        const auto crc = static_cast<uint32_t>(
            absl::ExtendCrc32c(absl::crc32c_t{pb.crc}, {payload + pb.flushed_bytes, pb.used_bytes - pb.flushed_bytes}));
        auto written = pb.flushed_bytes == 0
                           ? block_manager_.write_prefix(*pb.image, pb.block_id, pb.used_bytes)
                           : block_manager_.write_range(*pb.image, pb.block_id, from, pb.used_bytes - from, crc);
        if (written.contains_error()) {
            return written;
        }
        pb.flushed_bytes = pb.used_bytes;
        pb.crc = crc;
        return core::error_t::no_error();
    }

    // Over the limit, the tails with the least room go first. Every tail here still has room for a
    // segment: place() wrote and dropped the ones below MIN_REUSABLE_TAIL.
    void partial_block_manager_t::keep_reusable_tails() {
        if (partial_blocks_.size() <= MAX_OPEN_TAILS) {
            return;
        }
        std::stable_sort(
            partial_blocks_.begin(),
            partial_blocks_.end(),
            [](const partial_block_t& a, const partial_block_t& b) { return a.free_space() > b.free_space(); });
        partial_blocks_.erase(partial_blocks_.begin() + static_cast<int64_t>(MAX_OPEN_TAILS), partial_blocks_.end());
    }

    core::result_wrapper_t<bool> partial_block_manager_t::flush_partial_blocks() {
        // Only the open tails are buffered here: place() wrote and dropped every other image. The
        // first failure ends the flush and is returned.
        core::result_wrapper_t<bool> result = true;
        // A tail whose every byte since its last write belongs to segments a refused
        // append unwound is rolled back to what is on the file; a fresh one is given back whole.
        for (auto it = partial_blocks_.begin(); it != partial_blocks_.end();) {
            if (!dead_since_flush(*it)) {
                ++it;
                continue;
            }
            it->placements.clear();
            it->used_bytes = it->flushed_bytes;
            if (it->flushed_bytes == 0) {
                block_manager_.mark_as_free(it->block_id);
                it = partial_blocks_.erase(it);
            } else {
                ++it;
            }
        }
        if (tails_ == tails_t::dropped) {
            for (auto& pb : partial_blocks_) {
                auto written = block_manager_.write(*pb.image, pb.block_id);
                if (written.has_error()) {
                    result = written.error();
                    break;
                }
                ready(pb.placements);
            }
            partial_blocks_.clear();
        } else {
            for (auto& pb : partial_blocks_) {
                if (pb.flushed_bytes == pb.used_bytes) {
                    continue; // nothing appended since the last flush
                }
                if (auto flushed = flush_tail(pb); flushed.contains_error()) {
                    result = flushed;
                    break;
                }
                ready(pb.placements);
                pb.plain_since_flush = 0;
            }
            if (result.has_error()) {
                partial_blocks_.clear(); // the unwritten tails' placements die here
            } else {
                keep_reusable_tails();
            }
        }
        // Every block these placements name is on the file now (the tails written
        // above and by place()); the ones written before a refused tail are adopted too, their
        // segments read bytes that are there. A block none of whose placements adopted is unnamed
        // and given back (a handle still holding it parks the free in the manager).
        auto adopted = std::move(ready_);
        ready_.clear();
        std::pmr::vector<uint64_t> unadopted(block_manager_.buffer_manager.resource());
        for (auto& placed : adopted) {
            if (!placed.placement->adopt(placed.at)) {
                unadopted.push_back(placed.at.block_id);
            }
        }
        std::sort(unadopted.begin(), unadopted.end());
        unadopted.erase(std::unique(unadopted.begin(), unadopted.end()), unadopted.end());
        for (uint64_t block_id : unadopted) {
            block_manager_.mark_as_free(block_id);
        }
        return result;
    }

#ifdef DEV_MODE
    namespace {
        std::atomic<bool> dev_refuse_next_seal_{false};
    }

    void partial_block_manager_t::dev_refuse_next_seal() { dev_refuse_next_seal_.store(true); }
#endif

    core::error_t partial_block_manager_t::seal() {
#ifdef DEV_MODE
        if (dev_refuse_next_seal_.exchange(false)) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string("seal of the append packer refused by the DEV fault seam",
                                                  block_manager_.buffer_manager.resource()));
        }
#endif
        auto flushed = flush_partial_blocks();
        partial_blocks_.clear(); // written and adopted by the flush, or refused and dropped there
        if (flushed.has_error()) {
            return flushed.error();
        }
        return core::error_t::no_error();
    }

} // namespace components::table::storage
