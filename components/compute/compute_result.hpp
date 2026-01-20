#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

namespace components::compute {
    enum class compute_status_code_t : uint8_t
    {
        OK,
        INVALID,
        TYPE_ERROR,
        NOT_IMPLEMENTED,
        EXECUTION_ERROR,
    };

    class compute_status {
    public:
        static compute_status ok();
        static compute_status invalid(std::string msg);
        static compute_status type_error(std::string msg);
        static compute_status not_implemented(std::string msg);
        static compute_status execution_error(std::string msg);

        explicit operator bool() const;
        bool operator==(const compute_status& lhs) const;
        bool operator!=(const compute_status& lhs) const;

        [[nodiscard]] compute_status_code_t code() const;
        [[nodiscard]] const std::string& message() const;

    private:
        compute_status(compute_status_code_t code, std::string message = "");

        compute_status_code_t code_;
        std::string message_;
    };

    template<typename T>
    class compute_result {
    public:
        compute_result(T value)
            : data_(std::move(value))
            , status_(compute_status::ok()) {}

        compute_result(compute_status status)
            : status_(std::move(status)) {
            if (status_) {
                throw std::logic_error("Constructed compute_result with non-error status: " +
                                       std::to_string(static_cast<int>(status_.code())));
            }
        }

        explicit operator bool() const { return data_.has_value(); }

        const T& value() const& { return data_.value(); }
        T& value() & { return data_.value(); }
        T&& value() && { return data_.value(); }

        [[nodiscard]] const compute_status& status() const { return status_; }

    private:
        std::optional<T> data_;
        compute_status status_;
    };
} // namespace components::compute
