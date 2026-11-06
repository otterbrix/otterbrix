#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory_resource>

namespace core::pmr {

    // Counts what it hands out against a limit; the upstream serves every byte.
    // try_allocate(): nullptr over the LIMIT only (MCXT_ALLOC_NO_OOM for the budget); a real upstream OOM terminates.
    // allocate(): aborts over the limit.
    class budget_resource_t final : public std::pmr::memory_resource {
    public:
        budget_resource_t(std::pmr::memory_resource* upstream, std::uint64_t limit) noexcept
            : upstream_(upstream)
            , limit_(limit) {}

        budget_resource_t(const budget_resource_t&) = delete;
        budget_resource_t& operator=(const budget_resource_t&) = delete;

        [[nodiscard]] void* try_allocate(std::size_t bytes,
                                         std::size_t alignment = alignof(std::max_align_t)) noexcept {
            std::uint64_t used = used_.load(std::memory_order_relaxed);
            do {
                const std::uint64_t limit = limit_.load(std::memory_order_relaxed);
                if (used > limit || bytes > limit - used) {
                    return nullptr;
                }
            } while (!used_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
            return upstream_->allocate(bytes, alignment);
        }

        void deallocate(void* p, std::size_t bytes, std::size_t alignment = alignof(std::max_align_t)) noexcept {
            upstream_->deallocate(p, bytes, alignment);
            used_.fetch_sub(bytes, std::memory_order_relaxed);
        }

        // Below used() refuses every request until enough is given back; nothing already handed out is taken.
        void set_limit(std::uint64_t limit) noexcept { limit_.store(limit, std::memory_order_relaxed); }

        [[nodiscard]] std::uint64_t used() const noexcept { return used_.load(std::memory_order_relaxed); }

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            void* p = try_allocate(bytes, alignment);
            if (p == nullptr) {
                std::fputs("otterbrix: memory budget exhausted\n", stderr);
                std::abort();
            }
            return p;
        }

        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            deallocate(p, bytes, alignment);
        }

        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
        std::atomic<std::uint64_t> limit_;
        std::atomic<std::uint64_t> used_{0};
    };

} // namespace core::pmr
