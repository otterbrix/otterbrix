#pragma once

#include <memory>
#include <memory_resource>
#include <type_traits>

namespace core::pmr {
    template<class Target>
    void deallocate_ptr(std::pmr::memory_resource* ptr, Target* target);

    class deleter_t final {
    public:
        explicit deleter_t(std::pmr::memory_resource* ptr)
            : ptr_(ptr) {}

        template<class T>
        void operator()(T* target) {
            deallocate_ptr(ptr_, target);
        }

    private:
        std::pmr::memory_resource* ptr_;
    };

    class array_deleter_t final {
    public:
        explicit array_deleter_t(std::pmr::memory_resource* ptr, size_t size, size_t align)
            : ptr_(ptr)
            , size_(size)
            , align_(align) {}

        template<typename T>
        void operator()(T* target) {
            if (!ptr_) {
                return;
            }
            for (size_t i = 0; i < size_; i++) {
                target[i].~T();
            }
            ptr_->deallocate(target, size_ * sizeof(T), align_);
        }
        std::pmr::memory_resource* resource() const noexcept { return ptr_; }
        size_t size() const noexcept { return size_; }
        size_t align() const noexcept { return align_; }

    private:
        std::pmr::memory_resource* ptr_;
        size_t size_;
        size_t align_;
    };
    template<class T>
    using unique_ptr = std::unique_ptr<T, deleter_t>;

    template<class Target, class... Args>
    unique_ptr<Target> make_unique(std::pmr::memory_resource* ptr, Args&&... args) {
        auto size = sizeof(Target);
        auto align = alignof(Target);
        auto* buffer = ptr->allocate(size, align);
        auto* target_ptr = new (buffer) Target(ptr, std::forward<Args>(args)...);
        return {target_ptr, deleter_t(ptr)};
    }

    template<class Target, class... Args>
    Target* allocate_ptr(std::pmr::memory_resource* ptr, Args&&... args) {
        auto size = sizeof(Target);
        auto align = alignof(Target);
        auto* buffer = ptr->allocate(size, align);
        auto* target_ptr = new (buffer) Target(ptr, std::forward<Args>(args)...);
        return target_ptr;
    }

    template<class Target>
    void deallocate_ptr(std::pmr::memory_resource* ptr, Target* target) {
        auto align = alignof(Target);
        target->~Target();
        ptr->deallocate(target, sizeof(Target), align);
    }

} // namespace core::pmr