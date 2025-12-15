#pragma once

#include "buffer.hpp"
#include "operations_helper.hpp"

#include <cstddef>
#include <type_traits>
#include <vector>

#include "core/assert/assert.hpp"

namespace core {

    template<typename T>
    class uvector {
        static_assert(std::is_trivially_copyable_v<T>, "only supports trivially copyable");

    public:
        using value_type = T;
        using size_type = std::size_t;
        using reference = value_type&;
        using const_reference = value_type const&;
        using pointer = value_type*;
        using const_pointer = value_type const*;
        using iterator = pointer;
        using const_iterator = const_pointer;

        ~uvector() = default;
        uvector(uvector&&) noexcept = default;
        uvector& operator=(uvector&&) noexcept = default;
        uvector(uvector const&) = delete;
        uvector& operator=(uvector const&) = delete;

        uvector() = delete;

        uvector(std::pmr::memory_resource* resource, std::size_t size)
            : storage_{resource, elements_to_bytes(size)} {}

        uvector(std::pmr::memory_resource* mr, uvector const& other)
            : storage_{mr, other.storage_} {}

        [[nodiscard]] pointer element_ptr(std::size_t element_index) noexcept {
            assertion_exception(element_index < size());
            return data() + element_index;
        }

        [[nodiscard]] const_pointer element_ptr(std::size_t element_index) const noexcept {
            assertion_exception(element_index < size());
            return data() + element_index;
        }

        void set_element_to_zero(std::size_t element_index) {
            assertion_exception(element_index < size());
            assertion_exception(memset(element_ptr(element_index), 0, sizeof(value_type)));
        }

        void set_element(std::size_t element_index, T const& value) {
            assertion_exception(element_index < size());

            if constexpr (std::is_same<value_type, bool>::value) {
                memset(element_ptr(element_index), value, sizeof(value));
                return;
            }

            if constexpr (std::is_fundamental<value_type>::value) {
                if (core::is_equals<value_type>(value, {0})) {
                    set_element_to_zero(element_index);
                    return;
                }
            }

            std::memcpy(element_ptr(element_index), &value, sizeof(value));
        }

        [[nodiscard]] value_type element(std::size_t element_index) const {
            assertion_exception(element_index < size());
            value_type value;
            std::memcpy(&value, element_ptr(element_index), sizeof(value));
            return value;
        }
        [[nodiscard]] value_type front_element() const { return element(0); }
        [[nodiscard]] value_type back_element() const { return element(size() - 1); }
        void reserve(std::size_t new_capacity) { storage_.reserve(elements_to_bytes(new_capacity)); }
        void resize(std::size_t new_size) { storage_.resize(elements_to_bytes(new_size)); }
        void shrink_to_fit() { storage_.shrink_to_fit(); }
        buffer release() noexcept { return std::move(storage_); }
        [[nodiscard]] std::size_t capacity() const noexcept { return bytes_to_elements(storage_.capacity()); }
        [[nodiscard]] pointer data() noexcept { return static_cast<pointer>(storage_.data()); }
        [[nodiscard]] const_pointer data() const noexcept { return static_cast<const_pointer>(storage_.data()); }
        [[nodiscard]] iterator begin() noexcept { return data(); }
        [[nodiscard]] const_iterator cbegin() const noexcept { return data(); }
        [[nodiscard]] const_iterator begin() const noexcept { return cbegin(); }
        [[nodiscard]] iterator end() noexcept { return data() + size(); }
        [[nodiscard]] const_iterator cend() const noexcept { return data() + size(); }
        [[nodiscard]] const_iterator end() const noexcept { return cend(); }
        [[nodiscard]] std::size_t size() const noexcept { return bytes_to_elements(storage_.size()); }

        [[nodiscard]] std::int64_t ssize() const noexcept {
            assertion_exception(size() < static_cast<std::size_t>(std::numeric_limits<int64_t>::max()));
            return static_cast<int64_t>(size());
        }

        [[nodiscard]] bool is_empty() const noexcept { return size() == 0; }

        [[nodiscard]] std::pmr::memory_resource* memory_resource() const noexcept { return storage_.memory_resource(); }

    private:
        buffer storage_;

        [[nodiscard]] std::size_t constexpr elements_to_bytes(std::size_t num_elements) const noexcept {
            return num_elements * sizeof(value_type);
        }

        [[nodiscard]] std::size_t constexpr bytes_to_elements(std::size_t num_bytes) const noexcept {
            return num_bytes / sizeof(value_type);
        }

        template<class stream_t>
        friend stream_t& operator<<(stream_t& stream, const uvector& v) {
            stream << v.storage_;
            return stream;
        }
    };

    template<typename T, typename It>
    uvector<T> make_uvector(std::pmr::memory_resource* resource, It begin, It end) {
        const auto size = static_cast<std::size_t>(std::distance(begin, end));
        core::uvector<T> ret(resource, size);
        std::memcpy(ret.data(), &*begin, size * sizeof(T));
        return ret;
    }

    template<typename T>
    uvector<T> make_uvector(std::pmr::memory_resource* resource, const std::vector<T>& src) {
        return make_uvector<T>(resource, src.begin(), src.end());
    }

    template<typename T>
    std::pmr::vector<T> make_vector(std::pmr::memory_resource* resource, uvector<T>& src) {
        std::pmr::vector<T> result(src.size(), resource);
        std::memcpy(result.data(), src.data(), src.size() * sizeof(T));
        return result;
    }

    template<>
    inline std::pmr::vector<bool> make_vector(std::pmr::memory_resource* resource, uvector<bool>& src) {
        std::pmr::vector<bool> result(src.size(), resource);
        std::copy(src.begin(), src.end(), result.begin());
        return result;
    }

    template<typename T>
    uvector<T> make_empty_uvector(std::pmr::memory_resource* resource, std::size_t size) {
        uvector<T> ret(resource, size);
        std::memset(ret.data(), 0, size * sizeof(T));
        return ret;
    }

} // namespace core
