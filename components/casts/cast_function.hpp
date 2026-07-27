#pragma once

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include <functional>

namespace components::casts {

    enum class cast_kind : uint8_t
    {
        cast,
        try_cast
    };

    struct cast_signature {
        types::complex_logical_type source;
        types::complex_logical_type target;
    };

    // Every external parameter casts may need
    struct cast_context {
        core::date::timezone_offset_t timezone_offset;
        // Cast may fill values with that, instead of simply leaving a null
        const types::logical_value_t* fill_value = nullptr;
    };

    // CAST: fails hard on a failed cast -> returns error_t
    //! noexcept by design -> every error should be processed and returned explicitly
    using cast_fn = core::error_t (*)(const vector::vector_t& source,
                                      vector::vector_t* result,
                                      const cast_context& params,
                                      uint64_t count) noexcept;

    // TRY_CAST: never fails -> writes NULL for failed cast
    //! noexcept by design -> every error should be processed and turned into NULL result
    using try_cast_fn = void (*)(const vector::vector_t& source,
                                 vector::vector_t* result,
                                 const cast_context& params,
                                 uint64_t count) noexcept;

    class cast_function_t {
    public:
        cast_function_t() noexcept = default;
        explicit cast_function_t(cast_fn cast, try_cast_fn try_cast = nullptr) noexcept
            : cast_(cast)
            , try_cast_(try_cast) {}

        [[nodiscard]] bool has_try_cast() const noexcept { return try_cast_ != nullptr; }

        [[nodiscard]] explicit operator bool() const noexcept { return cast_ != nullptr; }

        [[nodiscard]] bool operator==(const cast_function_t& other) const noexcept {
            return cast_ == other.cast_ && try_cast_ == other.try_cast_;
        }

        [[nodiscard]] core::error_t invoke(cast_kind kind,
                                           const vector::vector_t& source,
                                           vector::vector_t* result,
                                           const cast_context& params,
                                           uint64_t count) const {
            // If there is no try_cast, assumes that regular one is safe
            if (kind == cast_kind::try_cast && has_try_cast()) {
                try_cast_(source, result, params, count);
                return core::error_t::no_error();
            } else {
                return cast_(source, result, params, count);
            }
        }

    private:
        //! Mandatory
        cast_fn cast_ = nullptr;
        //! Optional
        try_cast_fn try_cast_ = nullptr;
    };

    // Composite types require children's casts to be captured
    // And plain function pointers can't handle that
    using cast_t = std::function<
        core::error_t(cast_kind, const vector::vector_t&, vector::vector_t*, const cast_context&, uint64_t)>;

} // namespace components::casts