#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory_resource>
#include <mutex>
#include <unordered_map>

class resource_tracer_t final : public std::pmr::memory_resource {
public:
    explicit resource_tracer_t(std::pmr::memory_resource* upstream = std::pmr::new_delete_resource())
        : upstream_(upstream) {}

    resource_tracer_t(const resource_tracer_t&) = delete;
    resource_tracer_t& operator=(const resource_tracer_t&) = delete;

    // Still-live blocks are left to LeakSanitizer, which reports each one with the stack that
    // allocated it; freeing them here would hide the leak.
    ~resource_tracer_t() override { live_.clear(); }

    size_t total_allocated() const noexcept { return allocated_.load(std::memory_order_relaxed); }
    size_t total_deallocated() const noexcept { return deallocated_.load(std::memory_order_relaxed); }
    size_t leaked_bytes() const noexcept { return total_allocated() - total_deallocated(); }
    size_t live_allocations() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return live_.size();
    }

private:
    struct allocation_info_t {
        size_t bytes;
        size_t alignment;
    };

    void* do_allocate(size_t bytes, size_t alignment) override {
        void* ptr = upstream_->allocate(bytes, alignment);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            live_.emplace(reinterpret_cast<uintptr_t>(ptr), allocation_info_t{bytes, alignment});
        }
        allocated_.fetch_add(bytes, std::memory_order_relaxed);
        return ptr;
    }

    // The tracer's only message; there is no print-and-continue.
    [[noreturn]] static void
    report_and_abort(const char* what, void* ptr, allocation_info_t allocated, allocation_info_t freed) noexcept {
        std::fprintf(stderr,
                     "[resource_tracer] %s at %p (allocated %zu/%zu, freed %zu/%zu)\n",
                     what,
                     ptr,
                     allocated.bytes,
                     allocated.alignment,
                     freed.bytes,
                     freed.alignment);
        std::abort();
    }

    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
        const allocation_info_t freed{bytes, alignment};
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = live_.find(reinterpret_cast<uintptr_t>(ptr));
            if (it == live_.end()) {
                report_and_abort("deallocate of a block this tracer does not hold", ptr, {0, 0}, freed);
            }
            if (it->second.bytes != bytes || it->second.alignment != alignment) {
                report_and_abort("deallocate with a size or alignment other than allocated", ptr, it->second, freed);
            }
            live_.erase(it);
        }
        deallocated_.fetch_add(bytes, std::memory_order_relaxed);
        upstream_->deallocate(ptr, bytes, alignment);
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

    std::pmr::memory_resource* upstream_;
    std::atomic<size_t> allocated_{0};
    std::atomic<size_t> deallocated_{0};
    mutable std::mutex mutex_;
    std::unordered_map<uintptr_t, allocation_info_t> live_;
};
