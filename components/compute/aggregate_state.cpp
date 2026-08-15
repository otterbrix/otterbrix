#include "aggregate_state.hpp"

#include <cassert>

namespace components::compute {

    namespace {
        size_t round_up(size_t size, size_t alignment) noexcept {
            return alignment == 0 ? size : (size + alignment - 1) / alignment * alignment;
        }
    } // namespace

    aggregate_state_arena_t::aggregate_state_arena_t(std::pmr::memory_resource* resource,
                                                     aggregate_state_layout_t layout)
        : resource_(resource)
        , layout_(layout)
        , stride_(round_up(layout.size, layout.alignment))
        , blocks_(resource) {}

    aggregate_state_arena_t::~aggregate_state_arena_t() { release(); }

    aggregate_state_arena_t::aggregate_state_arena_t(aggregate_state_arena_t&& other) noexcept
        : resource_(other.resource_)
        , layout_(other.layout_)
        , stride_(other.stride_)
        , blocks_(std::move(other.blocks_))
        , count_(other.count_) {
        other.blocks_.clear();
        other.count_ = 0;
    }

    aggregate_state_arena_t& aggregate_state_arena_t::operator=(aggregate_state_arena_t&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        release();
        resource_ = other.resource_;
        layout_ = other.layout_;
        stride_ = other.stride_;
        blocks_ = std::move(other.blocks_);
        count_ = other.count_;
        other.blocks_.clear();
        other.count_ = 0;
        return *this;
    }

    void aggregate_state_arena_t::reserve(uint64_t group_count) {
        assert(stride_ > 0 && "an aggregate with no accumulator layout must be rejected at bind time");
        while (count_ < group_count) {
            const uint64_t block = count_ / states_per_block;
            if (block == blocks_.size()) {
                blocks_.push_back(
                    static_cast<std::byte*>(resource_->allocate(stride_ * states_per_block, layout_.alignment)));
            }
            layout_.construct(blocks_[block] + (count_ % states_per_block) * stride_, resource_);
            count_++;
        }
    }

    void aggregate_state_arena_t::clear() {
        for (uint64_t group = 0; group < count_; group++) {
            layout_.destroy(blocks_[group / states_per_block] + (group % states_per_block) * stride_);
        }
        count_ = 0;
    }

    void aggregate_state_arena_t::release() {
        clear();
        for (auto* block : blocks_) {
            resource_->deallocate(block, stride_ * states_per_block, layout_.alignment);
        }
        blocks_.clear();
    }

} // namespace components::compute