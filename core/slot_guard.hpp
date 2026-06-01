#pragma once

namespace core {

    /// @brief RAII scope guard that saves a value at a pointer-targeted location,
    ///        replaces it with a new value, and restores the saved value on scope exit.
    ///
    /// Designed for the manager-actor pump-loop: each manager has a private
    /// `current_slot_` member (pointer to the in-flight entry whose handler is
    /// currently executing). Pump-loop sets `current_slot_` before invoking
    /// `behavior(msg)` or `cont.resume()` and restores it afterward. Recursive
    /// re-entry from within a resumed handler must preserve the outer slot.
    ///
    /// Used inside the manager's own member function (where private members are
    /// visible by standard C++ access control) — no friend declaration needed.
    ///
    /// Non-copyable and non-movable: the guard must not outlive the frame it
    /// guards. All operations are noexcept (no exceptions in this project per
    /// project rules).
    template<typename T>
    struct slot_guard {
        T* target_;
        T saved_;

        slot_guard(T* target, T new_value) noexcept
            : target_(target)
            , saved_(*target) {
            *target = new_value;
        }

        ~slot_guard() noexcept { *target_ = saved_; }

        slot_guard(const slot_guard&) = delete;
        slot_guard& operator=(const slot_guard&) = delete;
        slot_guard(slot_guard&&) = delete;
        slot_guard& operator=(slot_guard&&) = delete;
    };

} // namespace core
