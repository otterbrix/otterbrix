#pragma once

#include <components/casts/cast_function.hpp>

#include <cassert>
#include <cstdint>

namespace components::casts {

    enum class cast_type : uint8_t
    {
        implicit = 0,
        assignment = 1,
        explicit_only = 2
    };

    [[nodiscard]] constexpr bool allowed_in(cast_type type, cast_type allowed_type) noexcept {
        return static_cast<uint8_t>(type) <= static_cast<uint8_t>(allowed_type);
    }

    [[nodiscard]] constexpr cast_type least_permissive(cast_type left, cast_type right) noexcept {
        return static_cast<uint8_t>(left) >= static_cast<uint8_t>(right) ? left : right;
    }

    // Used for common type deduction. only implicit casts take part in that search
    struct cast_cost {
        // Arbitrary cast 'cost', lowest wins
        uint32_t precision_loss;
        // Size of target type, used as a tie-breaker for the same costs
        uint32_t footprint;

        [[nodiscard]] constexpr bool operator<(const cast_cost& other) const noexcept {
            if (precision_loss != other.precision_loss) {
                return precision_loss < other.precision_loss;
            }
            return footprint < other.footprint;
        }
    };

    // Computes cost for run-time dependent types
    using cost_rule_fn = cast_cost (*)(const cast_signature& signature);

    struct cast_entry {
        // Implicit with fixed cost
        constexpr cast_entry(cast_function_t fn, cast_cost fixed_cost, bool convertable_inplace)
            : fn(fn)
            , fixed_cost(fixed_cost)
            , level(cast_type::implicit)
            , has_fixed_cost(true)
            , convertable_inplace(convertable_inplace) {}

        // Implicit with function-based cost
        constexpr cast_entry(cast_function_t fn, cost_rule_fn cost_rule, bool convertable_inplace)
            : fn(fn)
            , cost_rule(cost_rule)
            , level(cast_type::implicit)
            , has_fixed_cost(false)
            , convertable_inplace(convertable_inplace) {}

        // Assignment/explicit casts
        constexpr cast_entry(cast_function_t fn, cast_type level, bool convertable_inplace)
            : fn(fn)
            , fixed_cost{.precision_loss = 0, .footprint = 0}
            , level(level)
            , has_fixed_cost(true)
            , convertable_inplace(convertable_inplace) {
            assert(level != cast_type::implicit && "an implicit cast must be given a cost");
        }

        [[nodiscard]] constexpr bool promotes() const noexcept { return level == cast_type::implicit; }

        //! Only valid on an implicit entry -- see cast_cost.
        [[nodiscard]] constexpr cast_cost resolve_cost(const cast_signature& signature) const {
            assert(promotes() && "cost is meaningful only for implicit casts");
            return has_fixed_cost ? fixed_cost : cost_rule(signature);
        }

        cast_function_t fn{};
        union {
            cast_cost fixed_cost;
            cost_rule_fn cost_rule;
        };
        cast_type level;
        bool has_fixed_cost : 1;
        // not used for now
        bool convertable_inplace : 1;
    };

    inline constexpr cast_entry null_entry{cast_function_t{},
                                           cast_cost{.precision_loss = 0, .footprint = 0},
                                           /*convertable_inplace*/ true};

} // namespace components::casts