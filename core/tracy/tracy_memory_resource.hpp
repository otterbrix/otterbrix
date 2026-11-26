#pragma once

#include "tracy.hpp"

#include <cassert>
#include <cstddef>
#include <memory_resource>

namespace core::profiling {

    // PMR interposer that reports every upstream allocation to Tracy under a named pool, giving a per-component
    // breakdown in the Memory -> Pools view. Without TRACY_ENABLE it forwards to the upstream and nothing else.
    //
    // It must outlive every container that uses it as a resource. `name` must be a string literal: Tracy keeps the
    // pointer and never copies it.
    class tracy_memory_resource final : public std::pmr::memory_resource {
    public:
        tracy_memory_resource(std::pmr::memory_resource* upstream, const char* name) noexcept
            : upstream_(upstream)
            , name_(name) {
            assert(upstream_ != nullptr);
            assert(name_ != nullptr);
        }

        // Containers hold this object's address, so it can neither be copied nor moved.
        tracy_memory_resource(const tracy_memory_resource&) = delete;
        tracy_memory_resource& operator=(const tracy_memory_resource&) = delete;

    private:
        void* do_allocate(std::size_t bytes, std::size_t align) override {
            void* ptr = upstream_->allocate(bytes, align);
#ifdef TRACY_ENABLE
            TracyAllocN(ptr, bytes, name_);
#endif
            return ptr;
        }

        void do_deallocate(void* ptr, std::size_t bytes, std::size_t align) override {
            // Report before releasing: the address may be handed out again the moment it is freed.
#ifdef TRACY_ENABLE
            TracyFreeN(ptr, name_);
#endif
            upstream_->deallocate(ptr, bytes, align);
        }

        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
        const char* name_;
    };

} // namespace core::profiling
