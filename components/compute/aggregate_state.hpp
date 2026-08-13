#pragma once

#include <components/vector/indexing_vector.hpp>
#include <cstddef>
#include <memory_resource>
#include <new>
#include <type_traits>
#include <vector>

namespace components::compute {

    // How one aggregate lays its accumulator out in memory. Resolved once per kernel binding,
    // because the accumulator follows the RESOLVED argument type: sum over BIGINT accumulates an
    // int64, sum over DOUBLE a double. A zero size means the kernel cannot accumulate the types it
    // was handed — the executor reports that before any row is folded.
    struct aggregate_state_layout_t {
        size_t size{0};
        size_t alignment{alignof(std::max_align_t)};
        void (*construct)(void* slot, std::pmr::memory_resource* resource){nullptr};
        void (*destroy)(void* slot){nullptr};
    };

    // Layout of a plain accumulator struct. A state that needs an allocator declares a
    // constructor taking one, everything else is default-constructed.
    template<typename state_t>
    aggregate_state_layout_t aggregate_state_of() {
        return {sizeof(state_t),
                alignof(state_t),
                [](void* slot, std::pmr::memory_resource* resource) {
                    if constexpr (std::is_constructible_v<state_t, std::pmr::memory_resource*>) {
                        new (slot) state_t(resource);
                    } else {
                        static_cast<void>(resource);
                        new (slot) state_t();
                    }
                },
                [](void* slot) { static_cast<state_t*>(slot)->~state_t(); }};
    }

    // Accumulators are allocated in blocks of this many, so a group id splits into a block index
    // and a slot within it. A power of two, so the division folds to a shift.
    inline constexpr uint64_t states_per_block = vector::DEFAULT_VECTOR_CAPACITY;

    // The accumulators of ONE aggregate, addressed by group id: a kernel folds row i into
    // at<state_t>(groups[i]). Non-owning — the arena behind it keeps the blocks alive.
    class aggregate_states_t {
    public:
        aggregate_states_t() = default;
        aggregate_states_t(std::byte* const* blocks, size_t stride) noexcept
            : blocks_(blocks)
            , stride_(stride) {}

        template<typename state_t>
        state_t& at(uint64_t group) const noexcept {
            return *reinterpret_cast<state_t*>(blocks_[group / states_per_block] +
                                               (group % states_per_block) * stride_);
        }

    private:
        std::byte* const* blocks_{nullptr};
        size_t stride_{0};
    };

    // Owns the accumulators of one aggregate. Blocks are fixed size and never reallocated, so a
    // group's accumulator keeps its address for the whole aggregation however many groups follow.
    class aggregate_state_arena_t {
    public:
        aggregate_state_arena_t(std::pmr::memory_resource* resource, aggregate_state_layout_t layout);
        ~aggregate_state_arena_t();

        aggregate_state_arena_t(const aggregate_state_arena_t&) = delete;
        aggregate_state_arena_t& operator=(const aggregate_state_arena_t&) = delete;
        aggregate_state_arena_t(aggregate_state_arena_t&& other) noexcept;
        aggregate_state_arena_t& operator=(aggregate_state_arena_t&& other) noexcept;

        // Constructs accumulators up to `group_count`. Groups already present keep their value
        // and their address.
        void reserve(uint64_t group_count);
        // Destroys every accumulator; the blocks are kept for the next run.
        void clear();

        [[nodiscard]] uint64_t size() const noexcept { return count_; }
        [[nodiscard]] aggregate_states_t states() const noexcept { return {blocks_.data(), stride_}; }

    private:
        void release();

        std::pmr::memory_resource* resource_;
        aggregate_state_layout_t layout_;
        size_t stride_;
        std::pmr::vector<std::byte*> blocks_;
        uint64_t count_{0};
    };

} // namespace components::compute