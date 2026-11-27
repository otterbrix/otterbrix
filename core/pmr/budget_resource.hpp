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
        // Room asked for before an operation allocates; given back when destroyed
        class reservation_t {
        public:
            reservation_t() = default;
            reservation_t(reservation_t&& other) noexcept;
            reservation_t& operator=(reservation_t&& other) noexcept;
            reservation_t(const reservation_t&) = delete;
            reservation_t& operator=(const reservation_t&) = delete;
            ~reservation_t();

            explicit operator bool() const noexcept { return budget_ != nullptr; }

        private:
            friend class budget_resource_t;
            reservation_t(budget_resource_t* budget, std::uint64_t bytes) noexcept;

            budget_resource_t* budget_ = nullptr;
            std::uint64_t bytes_ = 0;
        };

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
                if (bytes > limit - used) {
                    return nullptr;
                }
            } while (!used_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
            return upstream_->allocate(bytes, alignment);
        }

        void deallocate(void* p, std::size_t bytes, std::size_t alignment = alignof(std::max_align_t)) noexcept {
            upstream_->deallocate(p, bytes, alignment);
            used_.fetch_sub(bytes, std::memory_order_relaxed);
        }

        // Refuses a limit below used() + reserved()
        [[nodiscard]] bool set_limit(std::uint64_t limit) noexcept;

        [[nodiscard]] std::uint64_t limit() const noexcept { return limit_.load(std::memory_order_relaxed); }
        [[nodiscard]] std::uint64_t used() const noexcept { return used_.load(std::memory_order_relaxed); }
        [[nodiscard]] std::uint64_t reserved() const noexcept { return reserved_.load(std::memory_order_relaxed); }

        // What a reservation may still take: the limit less what is used and what is reserved.
        [[nodiscard]] std::uint64_t available() const noexcept;

        // Empty when used + reserved + bytes would pass the limit.
        [[nodiscard]] reservation_t try_reserve(std::uint64_t bytes) noexcept;

        // For a caller out of room: take releases(), fail to reserve, then wait_for_release() with it.
        [[nodiscard]] std::uint64_t releases() const noexcept { return releases_.load(std::memory_order_acquire); }
        void wait_for_release(std::uint64_t seen) const noexcept;

    private:
        void release_(std::uint64_t bytes) noexcept;

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
        std::atomic<std::uint64_t> reserved_{0};
        std::atomic<std::uint64_t> releases_{0};
    };

} // namespace core::pmr
