#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>
#include <memory_resource>
#include <mutex>
#include <unordered_map>

static size_t align_to(size_t size, size_t align) { return (size + (align - 1)) / align * align; }

class resource_tracer_t final : public std::pmr::memory_resource {
public:
    explicit resource_tracer_t(std::pmr::memory_resource* upstream = std::pmr::new_delete_resource())
        : upstream_(upstream) {}

    resource_tracer_t(const resource_tracer_t&) = delete;
    resource_tracer_t& operator=(const resource_tracer_t&) = delete;

    ~resource_tracer_t() override {
        const size_t live = live_allocations();
        const size_t leaked = leaked_bytes();
        if (live != 0 || leaked != 0) {
            std::cerr << "[resource_tracer] LEAK: " << live << " allocation(s), " << leaked
                      << " byte(s) not deallocated" << std::endl;
            std::lock_guard<std::mutex> lock(mutex_);
            for (const auto& [address, info] : live_) {
                std::cerr << "[resource_tracer]   leaked " << info.bytes << " byte(s) at "
                          << reinterpret_cast<void*>(address) << " (alignment " << info.alignment << ")" << std::endl;
            }
        }
    }

    std::pmr::memory_resource* upstream_resource() const noexcept { return upstream_; }

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

    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = live_.find(reinterpret_cast<uintptr_t>(ptr));
            if (it == live_.end()) {
                std::cerr << "[resource_tracer] WARNING: deallocate of untracked pointer " << ptr << std::endl;
            } else {
                if (it->second.bytes != bytes || it->second.alignment != alignment) {
                    std::cerr << "[resource_tracer] WARNING: size/alignment mismatch at " << ptr << " (allocated "
                              << it->second.bytes << "/" << it->second.alignment << ", freed " << bytes << "/"
                              << alignment << ")" << std::endl;
                }
                live_.erase(it);
            }
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

template<class memory_pool_t>
class memory_tracer_t : public memory_pool_t {
public:
    memory_tracer_t()
        : memory_pool_t() {}

    virtual ~memory_tracer_t() {
        std::cout << "total allocated bytes: " << allocated_ << std::endl;
        std::cout << "total deallocated bytes: " << deallocated_ << std::endl;
        std::cout << "missed pointers count: " << addresses_.size() << std::endl;
    }

    void release() { memory_pool_t::release(); }

    std::pmr::memory_resource* upstream_resource() const noexcept { return memory_pool_t::upstream_resource(); }

protected:
    size_t allocated_{0};
    size_t deallocated_{0};
    std::unordered_map<uint64_t, uint64_t> addresses_;

    void* do_allocate(size_t bytes, size_t alignment) override {
        std::byte* ptr = memory_pool_t::do_allocate(bytes, alignment);
        size_t with_alignment = align_to(bytes, alignment);
        auto it = addresses_.find(reinterpret_cast<uint64_t>(ptr));
        if (it == addresses_.end()) {
            addresses_.emplace(reinterpret_cast<uint64_t>(ptr), with_alignment);
        } else {
            std::cout << "do_allocate: ptr exists: " << ptr << std::endl;
        }
        allocated_ += with_alignment;
        std::cout << "do_allocate: region: " << ptr << " - " << ptr + bytes << std::endl;
        std::cout << "do_allocate: bytes: " << bytes << std::endl;
        std::cout << "do_allocate: alignment: " << alignment << std::endl;
        if (with_alignment != bytes) {
            std::cout << "do_allocate: extra bytes due to alignment: " << with_alignment - bytes << std::endl;
        }
        return ptr;
    }

    void do_deallocate(void* p, size_t bytes, size_t alignment) override {
        size_t with_alignment = align_to(bytes, alignment);
        deallocated_ += with_alignment;
        auto it = addresses_.find(reinterpret_cast<uint64_t>(p));
        if (it != addresses_.end()) {
            if (it->second != with_alignment) {
                std::cout << "do_deallocate: region size error: allocated: " << it->second
                          << "; requested to deallocate: " << with_alignment << std::endl;
            }
            addresses_.erase(it);
        } else {
            std::cout << "do_deallocate: ptr does not exists: " << p << std::endl;
        }
        std::cout << "do_deallocate: region: " << p << " - " << p + bytes << std::endl;
        std::cout << "do_deallocate: bytes: " << bytes << std::endl;
        std::cout << "do_deallocate: alignment: " << alignment << std::endl;
        if (with_alignment != bytes) {
            std::cout << "do_deallocate: extra bytes due to alignment: " << with_alignment - bytes << std::endl;
        }
        memory_pool_t::do_deallocate(p, bytes, alignment);
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
};