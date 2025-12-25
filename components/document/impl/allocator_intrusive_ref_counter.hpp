#pragma once

#include <atomic>
#include <core/pmr.hpp>
#include <memory_resource>

template<typename T>
class allocator_intrusive_ref_counter {
public:
    explicit allocator_intrusive_ref_counter();

    virtual ~allocator_intrusive_ref_counter() = default;

    allocator_intrusive_ref_counter(const allocator_intrusive_ref_counter&) = delete;

    allocator_intrusive_ref_counter(allocator_intrusive_ref_counter&&) noexcept = delete;

    allocator_intrusive_ref_counter& operator=(const allocator_intrusive_ref_counter&) = delete;

    allocator_intrusive_ref_counter& operator=(allocator_intrusive_ref_counter&&) noexcept = delete;

    friend void intrusive_ptr_add_ref(allocator_intrusive_ref_counter<T>* p) {
        p->ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    friend void intrusive_ptr_release(allocator_intrusive_ref_counter<T>* p) {
        if (p->ref_count_.fetch_sub(1, std::memory_order_release) == 1) {
            std::atomic_thread_fence(std::memory_order_acquire);
            core::pmr::deallocate_ptr(p->get_allocator(), static_cast<T*>(p));
        }
    }

protected:
    virtual std::pmr::memory_resource* get_allocator() = 0;

private:
    std::atomic<std::size_t> ref_count_;
};

template<typename T>
inline allocator_intrusive_ref_counter<T>::allocator_intrusive_ref_counter()
    : ref_count_(0) {}
