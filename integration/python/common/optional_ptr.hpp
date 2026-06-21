#pragma once

#include <memory>
#include <stdexcept>

namespace otterbrix {

    template<bool IS_ENABLED>
    struct memory_safety_t {
#ifdef DEBUG
        // In DEBUG mode safety is always on
        static constexpr bool ENABLED = true;
#else
        static constexpr bool ENABLED = IS_ENABLED;
#endif
    };

    template<class T, bool SAFE = true>
    class optional_ptr { // NOLINT: mimic std casing
    public:
        optional_ptr() noexcept
            : ptr(nullptr) {}
        optional_ptr(T* ptr_p)
            : ptr(ptr_p) { // NOLINT: allow implicit creation from pointer
        }
        optional_ptr(T& ref)
            : ptr(&ref) { // NOLINT: allow implicit creation from reference
        }
        optional_ptr(const std::unique_ptr<T>& ptr_p)
            : ptr(ptr_p.get()) { // NOLINT: allow implicit creation from unique pointer
        }
        optional_ptr(const std::shared_ptr<T>& ptr_p)
            : ptr(ptr_p.get()) { // NOLINT: allow implicit creation from shared pointer
        }

        void check_valid() const {
            if (memory_safety_t<SAFE>::ENABLED) {
                if (!ptr) {
                    throw std::runtime_error("Attempting to dereference an optional pointer that is not set");
                }
            }
        }

        operator bool() const { // NOLINT: allow implicit conversion to bool
            return ptr;
        }
        T& operator*() {
            check_valid();
            return *ptr;
        }
        const T& operator*() const {
            check_valid();
            return *ptr;
        }
        T* operator->() {
            check_valid();
            return ptr;
        }
        const T* operator->() const {
            check_valid();
            return ptr;
        }
        T* get() { // NOLINT: mimic std casing
            // check_valid();
            return ptr;
        }
        const T* get() const { // NOLINT: mimic std casing
            // check_valid();
            return ptr;
        }
        // this looks dirty - but this is the default behavior of raw pointers
        T* get_mutable() const { // NOLINT: mimic std casing
            // check_valid();
            return ptr;
        }

        bool operator==(const optional_ptr<T>& rhs) const { return ptr == rhs.ptr; }

        bool operator!=(const optional_ptr<T>& rhs) const { return ptr != rhs.ptr; }

    private:
        T* ptr;
    };

    template<typename T>
    using unsafe_optional_ptr = optional_ptr<T, false>;

} // namespace otterbrix
