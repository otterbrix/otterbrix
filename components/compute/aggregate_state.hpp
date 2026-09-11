#pragma once

#include <components/types/types.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector.hpp>
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
        // The resolved argument type, for an accumulator whose STORAGE follows it
        types::complex_logical_type argument_type{};
        void (*construct)(void* slot,
                          std::pmr::memory_resource* resource,
                          const types::complex_logical_type& argument_type){nullptr};
        void (*destroy)(void* slot){nullptr};
    };

    template<typename state_t>
    inline constexpr bool takes_argument_type =
        !std::is_aggregate_v<state_t> &&
        std::is_constructible_v<state_t, std::pmr::memory_resource*, const types::complex_logical_type&>;

    template<typename state_t>
    inline constexpr bool takes_resource =
        !std::is_aggregate_v<state_t> && std::is_constructible_v<state_t, std::pmr::memory_resource*>;

    template<typename state_t>
    aggregate_state_layout_t aggregate_state_of() {
        return {sizeof(state_t),
                alignof(state_t),
                types::complex_logical_type{},
                [](void* slot, std::pmr::memory_resource* resource, const types::complex_logical_type& type) {
                    if constexpr (takes_argument_type<state_t>) {
                        new (slot) state_t(resource, type);
                    } else if constexpr (takes_resource<state_t>) {
                        static_cast<void>(type);
                        new (slot) state_t(resource);
                    } else {
                        static_cast<void>(resource);
                        static_cast<void>(type);
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
        aggregate_states_t(std::byte* const* blocks, size_t stride, vector::vector_t* values) noexcept
            : blocks_(blocks)
            , stride_(stride)
            , values_(values) {}

        template<typename state_t>
        state_t& at(uint64_t group) const noexcept {
            return *reinterpret_cast<state_t*>(blocks_[group / states_per_block] + slot_of(group) * stride_);
        }

        // The block's vector of accumulated VALUES, one row per group
        [[nodiscard]] vector::vector_t& values(uint64_t group) const noexcept {
            return values_[group / states_per_block];
        }
        [[nodiscard]] uint64_t slot_of(uint64_t group) const noexcept { return group % states_per_block; }

    private:
        std::byte* const* blocks_{nullptr};
        size_t stride_{0};
        vector::vector_t* values_{nullptr};
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
        // Not const: the view it hands out writes into the accumulators.
        [[nodiscard]] aggregate_states_t states() noexcept { return {blocks_.data(), stride_, value_blocks_.data()}; }

    private:
        void release();
        [[nodiscard]] bool keeps_values() const noexcept {
            return layout_.argument_type.type() != types::logical_type::NA;
        }

        std::pmr::memory_resource* resource_;
        aggregate_state_layout_t layout_;
        size_t stride_;
        std::pmr::vector<std::byte*> blocks_;
        // One per block, parallel to blocks_, only when the layout named an argument_type.
        std::pmr::vector<vector::vector_t> value_blocks_;
        uint64_t count_{0};
    };

} // namespace components::compute