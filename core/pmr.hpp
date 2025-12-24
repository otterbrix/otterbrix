#pragma once

#include <memory>
#include <memory_resource>
#include <string>

namespace core::pmr {

    using pmr_string_stream =
        std::basic_stringstream<char, std::char_traits<char>, std::pmr::polymorphic_allocator<char>>;

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

    template<typename T>
    std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, const char* format, T value) {
        // somewhat inconvenient way to convert a number into std::pmr::string without calling default allocator
        // TODO: use std::format_to() after C++20
        auto size = static_cast<size_t>(std::snprintf(nullptr, 0, format, value));
        std::pmr::string result(resource);
        result.resize(size + 1); // +1 for null terminator
        std::sprintf(result.data(), format, value);
        // remove null terminator
        result.resize(size);
        return result;
    }

    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, int value) {
        return to_pmr_string(resource, "%d", value);
    }
    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, long value) {
        return to_pmr_string(resource, "%ld", value);
    }
    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, long long value) {
        return to_pmr_string(resource, "%lld", value);
    }
    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, unsigned value) {
        return to_pmr_string(resource, "%u", value);
    }
    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, unsigned long value) {
        return to_pmr_string(resource, "%lu", value);
    }
    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, unsigned long long value) {
        return to_pmr_string(resource, "%llu", value);
    }
    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, float value) {
        return to_pmr_string(resource, "%f", value);
    }
    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, double value) {
        return to_pmr_string(resource, "%f", value);
    }
    inline std::pmr::string to_pmr_string(std::pmr::memory_resource* resource, long double value) {
        return to_pmr_string(resource, "%Lf", value);
    }

    // are useful while we have not converted all strings to pmr::string
    inline bool operator==(const std::string& str1, const std::pmr::string& str2) { return str1.compare(str2) == 0; }
    inline bool operator==(const std::pmr::string& str1, const std::string& str2) { return str1.compare(str2) == 0; }
    inline bool operator!=(const std::string& str1, const std::pmr::string& str2) { return str1.compare(str2) != 0; }
    inline bool operator!=(const std::pmr::string& str1, const std::string& str2) { return str1.compare(str2) != 0; }
    inline bool operator<(const std::string& str1, const std::pmr::string& str2) { return str1.compare(str2) < 0; }
    inline bool operator<(const std::pmr::string& str1, const std::string& str2) { return str1.compare(str2) < 0; }
    inline bool operator>(const std::string& str1, const std::pmr::string& str2) { return str1.compare(str2) > 0; }
    inline bool operator>(const std::pmr::string& str1, const std::string& str2) { return str1.compare(str2) > 0; }
    inline bool operator<=(const std::string& str1, const std::pmr::string& str2) { return str1.compare(str2) <= 0; }
    inline bool operator<=(const std::pmr::string& str1, const std::string& str2) { return str1.compare(str2) <= 0; }
    inline bool operator>=(const std::string& str1, const std::pmr::string& str2) { return str1.compare(str2) >= 0; }
    inline bool operator>=(const std::pmr::string& str1, const std::string& str2) { return str1.compare(str2) >= 0; }

} // namespace core::pmr
