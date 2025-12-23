#pragma once

#include <type_traits>

#include "uvector.hpp"

namespace core {

    template<typename T>
    class scalar {
    public:
        static_assert(std::is_trivially_copyable<T>::value, "Scalar type must be trivially copyable");

        using value_type = typename uvector<T>::value_type;
        using reference = typename uvector<T>::reference;
        using const_reference = typename uvector<T>::const_reference;
        using pointer = typename uvector<T>::pointer;
        using const_pointer = typename uvector<T>::const_pointer;

        ~scalar() = default;
        scalar(scalar&&) noexcept = default;
        scalar& operator=(scalar&&) noexcept = default;
        scalar(scalar const&) = delete;
        scalar& operator=(scalar const&) = delete;
        scalar() = delete;

        explicit scalar(std::pmr::memory_resource* mr)
            : _storage{mr, 1} {}

        explicit scalar(std::pmr::memory_resource* mr, value_type const& initial_value)
            : _storage{mr, 1} {
            set_value(initial_value);
        }

        scalar(std::pmr::memory_resource* mr, scalar const& other)
            : _storage{mr, other._storage} {}

        [[nodiscard]] value_type value() const { return _storage.front_element(); }

        void set_value(value_type const& value) { _storage.set_element(0, value); }

        void set_value(value_type&&) = delete;

        void set_value_to_zero() { _storage.set_element_to_zero(static_cast<size_t>(value_type{0})); }

        [[nodiscard]] pointer data() noexcept { return static_cast<pointer>(_storage.data()); }

        [[nodiscard]] const_pointer data() const noexcept { return static_cast<const_pointer>(_storage.data()); }

    private:
        uvector<T> _storage;
    };
} // namespace core
