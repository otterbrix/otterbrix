#include "partial_block_manager.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>

#include "block_handle.hpp"
#include "buffer_handle.hpp"
#include "buffer_manager.hpp"

namespace components::table::storage {

    partial_block_manager_t::partial_block_manager_t(block_manager_t& block_manager,
                                                     double full_threshold,
                                                     bool reuse_tails)
        : block_manager_(block_manager)
        , full_threshold_(full_threshold)
        , reuse_tails_(reuse_tails) {}

    partial_block_allocation_t partial_block_manager_t::get_block_allocation(uint64_t segment_size) {
        auto block_alloc_size = block_manager_.block_size();

        // if segment is large enough (> threshold of block), give it a dedicated block
        if (segment_size > static_cast<uint64_t>(static_cast<double>(block_alloc_size) * full_threshold_)) {
            uint64_t block_id = block_manager_.free_block_id();
            return {block_id, 0, segment_size};
        }

        // try to fit into an existing partial block
        for (auto& pb : partial_blocks_) {
            // Place every segment at an 8-byte-aligned offset: the offset is dereferenced after
            // reload as uint64_t*/int32_t*/T*, and byte-granular packing made those reads
            // misaligned UB (caught by -fsanitize=alignment). Padding bytes stay zeroed
            // (write_to_block memsets fresh buffers).
            uint64_t aligned_offset = align_value<uint64_t>(pb.used_bytes);
            if (aligned_offset + segment_size <= pb.block_capacity) {
                pb.used_bytes = static_cast<uint32_t>(aligned_offset + segment_size);
                return {pb.block_id, static_cast<uint32_t>(aligned_offset), segment_size};
            }
        }

        // allocate new partial block
        uint64_t block_id = block_manager_.free_block_id();
        partial_block_t pb;
        pb.block_id = block_id;
        pb.used_bytes = static_cast<uint32_t>(segment_size);
        pb.block_capacity = block_alloc_size;
        pb.flushed_bytes = 0;
        partial_blocks_.push_back(pb);

        return {block_id, 0, segment_size};
    }

    void partial_block_manager_t::register_partial_block(uint64_t block_id, uint32_t used_size) {
        // used_size may be reconstructed from a persisted value and is NOT rounded here:
        // get_block_allocation aligns the offset at placement time, so the 8-byte-aligned-offset
        // invariant holds no matter what fill level this block is re-adopted with.
        auto block_alloc_size = block_manager_.block_size();
        partial_block_t pb;
        pb.block_id = block_id;
        pb.used_bytes = used_size;
        pb.block_capacity = block_alloc_size;
        pb.flushed_bytes = 0;
        partial_blocks_.push_back(pb);
    }

    void partial_block_manager_t::write_to_block(uint64_t block_id, uint32_t offset, const void* data, uint64_t size) {
        auto it = block_buffers_.find(block_id);
        if (it == block_buffers_.end()) {
            auto block = std::make_unique<block_t>(block_manager_.buffer_manager.resource(),
                                                   block_id,
                                                   static_cast<uint64_t>(block_manager_.block_size()));
            std::memset(block->buffer(), 0, static_cast<size_t>(block_manager_.block_size()));
            it = block_buffers_.emplace(block_id, std::move(block)).first;
        }
        std::memcpy(it->second->buffer() + offset, data, size);

        // A grown tail's block may already be resident (a reader pinned an earlier segment of it):
        // that copy would otherwise keep serving zeros where this segment now lives. The range is
        // past every byte a reader can hold, so the patch races nothing.
        if (reuse_tails_ && block_manager_.registry_alive(block_id)) {
            auto handle = block_manager_.register_block(block_id);
            if (handle->state() == block_state::LOADED) {
                auto pinned = block_manager_.buffer_manager.pin(handle);
                if (!pinned.has_error()) {
                    std::memcpy(pinned.value().ptr() + offset, data, size);
                }
            }
        }
    }

    // Every byte of a tail reaches the disk once: the first flush writes the used prefix, each
    // later one the range appended since (plus the checksum slot both times).
    core::result_wrapper_t<bool> partial_block_manager_t::flush_tail(partial_block_t& pb, block_t& block) {
        const uint64_t from = align_value<uint64_t>(pb.flushed_bytes);
        auto written = pb.flushed_bytes == 0
                           ? block_manager_.write_prefix(block, pb.block_id, pb.used_bytes)
                           : block_manager_.write_range(block, pb.block_id, from, pb.used_bytes - from);
        if (written.has_error()) {
            return written;
        }
        pb.flushed_bytes = pb.used_bytes;
        return true;
    }

    void partial_block_manager_t::keep_reusable_tails() {
        std::vector<partial_block_t> kept;
        for (const auto& pb : partial_blocks_) {
            const uint64_t free_space = pb.block_capacity - align_value<uint64_t>(pb.used_bytes);
            if (free_space >= MIN_REUSABLE_TAIL && block_buffers_.count(pb.block_id) != 0) {
                kept.push_back(pb);
            }
        }
        // Over the limit, the tails with the least room go first.
        if (kept.size() > MAX_OPEN_TAILS) {
            std::stable_sort(kept.begin(), kept.end(), [](const partial_block_t& a, const partial_block_t& b) {
                return (a.block_capacity - a.used_bytes) > (b.block_capacity - b.used_bytes);
            });
            kept.resize(MAX_OPEN_TAILS);
        }
        for (auto it = block_buffers_.begin(); it != block_buffers_.end();) {
            const uint64_t id = it->first;
            const bool keep = std::any_of(kept.begin(), kept.end(), [id](const partial_block_t& pb) {
                return pb.block_id == id;
            });
            it = keep ? std::next(it) : block_buffers_.erase(it);
        }
        partial_blocks_ = std::move(kept);
    }

    core::result_wrapper_t<bool> partial_block_manager_t::flush_partial_blocks() {
        // First failure ends the flush and is returned; buffers are still cleared regardless,
        // since their segments are already re-pointed at these block ids (the round is over
        // either way).
        core::result_wrapper_t<bool> result = true;
        if (!reuse_tails_) {
            for (auto& [block_id, block] : block_buffers_) {
                auto written = block_manager_.write(*block, block_id);
                if (written.has_error()) {
                    result = written.error();
                    break;
                }
            }
            block_buffers_.clear();
            partial_blocks_.clear();
            return result;
        }
        for (auto& pb : partial_blocks_) {
            auto it = block_buffers_.find(pb.block_id);
            if (it == block_buffers_.end() || pb.flushed_bytes == pb.used_bytes) {
                continue; // nothing appended since the last flush
            }
            auto flushed = flush_tail(pb, *it->second);
            if (flushed.has_error()) {
                result = flushed.error();
                break;
            }
        }
        // Dedicated blocks never enter partial_blocks_; they are written here, once.
        for (auto& [block_id, block] : block_buffers_) {
            if (result.has_error()) {
                break;
            }
            const bool is_tail = std::any_of(partial_blocks_.begin(),
                                             partial_blocks_.end(),
                                             [block_id](const partial_block_t& pb) { return pb.block_id == block_id; });
            if (is_tail) {
                continue;
            }
            auto written = block_manager_.write(*block, block_id);
            if (written.has_error()) {
                result = written.error();
                break;
            }
        }
        if (result.has_error()) {
            block_buffers_.clear();
            partial_blocks_.clear();
            return result;
        }
        keep_reusable_tails();
        return result;
    }

#ifdef DEV_MODE
    namespace {
        std::atomic<bool> dev_refuse_next_seal_{false};
    }

    void partial_block_manager_t::dev_refuse_next_seal() { dev_refuse_next_seal_.store(true); }
#endif

    core::result_wrapper_t<bool> partial_block_manager_t::seal() {
#ifdef DEV_MODE
        if (dev_refuse_next_seal_.exchange(false)) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string("seal of the append packer refused by the DEV fault seam",
                                                  block_manager_.buffer_manager.resource()));
        }
#endif
        auto flushed = flush_partial_blocks();
        block_buffers_.clear();
        partial_blocks_.clear();
        return flushed;
    }

} // namespace components::table::storage
