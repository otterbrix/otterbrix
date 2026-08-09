#pragma once

#include <components/casts/cast_function.hpp>
#include <components/types/logical_value.hpp>

namespace components::casts::kernels {

    namespace detail {

        // An ENUM is stored as its INT32, but is accessable only via label, so we have to decode it here
        [[nodiscard]] inline bool
        enum_value_of(const types::complex_logical_type& enum_type, std::string_view label, int32_t* value) noexcept {
            const auto* extension = static_cast<const types::enum_logical_type_extension*>(enum_type.extension());
            if (extension == nullptr) {
                return false;
            }
            for (const auto& entry : extension->entries()) {
                if (entry.type().alias() == label) {
                    *value = entry.value<int32_t>();
                    return true;
                }
            }
            return false;
        }

    } // namespace detail

    inline core::error_t string_to_enum_cast(const vector::vector_t& source,
                                             vector::vector_t* result,
                                             const graph_execution_context&,
                                             uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            if (source.is_null(row)) {
                result->set_null(row, true);
                continue;
            }
            const auto label = source.get_value<std::string_view>(row);
            int32_t value = 0;
            if (!detail::enum_value_of(result->type(), label, &value)) {
                std::pmr::string message{"invalid input value for enum: '", result->resource()};
                message.append(label.data(), label.size());
                message += "'";
                return core::error_t(core::error_code_t::conversion_failure, std::move(message));
            }
            result->set_null(row, false);
            result->set_value(row, value);
        }
        return core::error_t::no_error();
    }

    inline void string_to_enum_try_cast(const vector::vector_t& source,
                                        vector::vector_t* result,
                                        const graph_execution_context&,
                                        uint64_t count) noexcept {
        for (uint64_t row = 0; row < count; ++row) {
            int32_t value = 0;
            if (source.is_null(row) || !detail::enum_value_of(result->type(), source.get_value<std::string_view>(row), &value)) {
                result->set_null(row, true);
                continue;
            }
            result->set_null(row, false);
            result->set_value(row, value);
        }
    }

} // namespace components::casts::kernels