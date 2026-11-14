#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <ostream>

namespace components::types {

    class oid_t {
    public:
        constexpr oid_t() noexcept = default;
        constexpr explicit oid_t(std::uint32_t raw) noexcept
            : value_(raw) {}

        constexpr explicit operator std::uint32_t() const noexcept { return value_; }

        friend constexpr bool operator==(oid_t, oid_t) noexcept = default;
        friend constexpr std::strong_ordering operator<=>(oid_t, oid_t) noexcept = default;

    private:
        std::uint32_t value_{0};
    };

    static_assert(sizeof(oid_t) == sizeof(std::uint32_t), "an oid is stored as uint32");

    // fmt (and so the log) prints an oid as its number.
    constexpr std::uint32_t format_as(oid_t oid) noexcept { return static_cast<std::uint32_t>(oid); }

    inline std::ostream& operator<<(std::ostream& out, oid_t oid) { return out << static_cast<std::uint32_t>(oid); }

} // namespace components::types

template<>
struct std::hash<components::types::oid_t> {
    std::size_t operator()(components::types::oid_t oid) const noexcept {
        return std::hash<std::uint32_t>{}(static_cast<std::uint32_t>(oid));
    }
};
