#pragma once

#include <components/catalog/catalog_codes.hpp>

#include <cstddef>

namespace components::catalog {

    // Result of evaluating null-presence against an FK matchtype.
    enum class fk_null_result : std::uint8_t {
        pass,           // row is exempt from parent check (null allows skip)
        reject_partial, // partial NULL violates MATCH FULL semantics — return error
        check_parent,   // proceed with parent row lookup
    };

    // Classify how to handle the FK tuple for this row given matchtype and null counts.
    // Implements SQL standard MATCH SIMPLE / MATCH FULL / MATCH PARTIAL null semantics.
    //
    //   MATCH SIMPLE ('s'): any NULL component → pass (no parent check).
    //   MATCH FULL   ('f'): all-NULL → pass; partial-NULL → reject_partial; all-non-null → check_parent.
    //   MATCH PARTIAL('p'): all-NULL → pass; otherwise → check_parent (NULLs act as wildcards).
    inline fk_null_result classify_fk_null(char matchtype,
                                            std::size_t null_count,
                                            std::size_t total_cols) noexcept {
        const bool all_null = (null_count == total_cols);
        if (all_null) {
            return fk_null_result::pass; // all matchtypes: all-NULL → pass
        }
        if (matchtype == fk_match::simple) {
            return null_count > 0 ? fk_null_result::pass : fk_null_result::check_parent;
        }
        if (matchtype == fk_match::full) {
            return null_count > 0 ? fk_null_result::reject_partial : fk_null_result::check_parent;
        }
        // MATCH PARTIAL: proceed even with partial null (nulls are wildcards on parent side)
        return fk_null_result::check_parent;
    }

    // For MATCH PARTIAL: returns true if this FK column should be treated as a wildcard
    // (i.e. the parent column predicate is skipped for this position).
    inline bool is_fk_wildcard_col(char matchtype, bool fk_col_is_null) noexcept {
        return matchtype == fk_match::partial && fk_col_is_null;
    }

    // Classified referential action — a typed alternative to the raw fk_action char codes.
    enum class cascade_action_t : std::uint8_t {
        no_action,   // 'a' — check only at end of statement (behaves like RESTRICT in otterbrix)
        restrict_,   // 'r' — reject immediately on first matching child row
        cascade,     // 'c' — delete child rows that reference deleted parent
        set_null,    // 'n' — null out FK columns in child rows
        set_default, // 'd' — set FK columns to their defaults
    };

    // Map fk_action char code to cascade_action_t.
    inline cascade_action_t classify_action(char fk_action_char) noexcept {
        switch (fk_action_char) {
            case fk_action::restrict_:    return cascade_action_t::restrict_;
            case fk_action::cascade:      return cascade_action_t::cascade;
            case fk_action::set_null:     return cascade_action_t::set_null;
            case fk_action::set_default:  return cascade_action_t::set_default;
            default:                      return cascade_action_t::no_action;
        }
    }

    // Returns true if the action requires collecting all matching child rows before acting
    // (CASCADE or SET NULL). RESTRICT/NO_ACTION stop at first match.
    inline bool action_requires_collection(cascade_action_t action) noexcept {
        return action == cascade_action_t::cascade || action == cascade_action_t::set_null;
    }

} // namespace components::catalog
