#pragma once

#include <cassert>
#include <memory_resource>
#include <optional>
#include <source_location>
#include <string>

namespace core {

    // TODO: define specific value for each error to make documentation easier
    // Fill free to add your error type to it
    // It is advised against using 'other_error'
    enum class error_code_t : int32_t
    {
        other_error = -1,
        none = 0,
        already_exists,
        do_not_exists,
        unimplemented_yet,

        duplicate_field,
        missing_field,
        missing_primary_key_id,
        missing_namespace,
        transaction_inactive,
        transaction_finalized,
        missing_savepoint,
        commit_failed,
        missing_table,
        database_already_exists,
        database_not_exists,
        table_already_exists,
        table_not_exists,
        table_dropped,
        type_already_exists,
        type_not_exists,
        ambiguous_name,
        field_not_exists,
        invalid_parameter,

        physical_plan_error,
        create_physical_plan_error,

        arithmetics_failure,
        comparison_failure,
        conversion_failure,

        index_create_fail,
        index_not_exists,
        sql_parse_error,
        schema_error,
        kernel_error,
        function_registry_error,
        unrecognized_function,
        incorrect_function_argument,
        incorrect_function_return_type,
    };

    struct error_t {
        error_code_t type;
        std::pmr::string what;
#if not defined(NDEBUG)
        std::source_location error_origin{};

        explicit error_t(error_code_t type,
                         const std::pmr::string& what,
                         std::source_location location = std::source_location::current())
            : type(type)
            , what(what)
            , error_origin(location) {
            assert(type != error_code_t::none &&
                   "no error state of error_t can only be created using no_error() constructor");
        }
        explicit error_t(error_code_t type,
                         std::pmr::string&& what,
                         std::source_location location = std::source_location::current())
            : type(type)
            , what(std::move(what))
            , error_origin(location) {
            assert(type != error_code_t::none &&
                   "no error state of error_t can only be created using no_error() constructor");
        }

        error_t& operator=(const error_t& other) {
            type = other.type;
            reconstruct_string(other.what);
            error_origin = other.error_origin;
            return *this;
        }
        error_t& operator=(error_t&& other) noexcept {
            type = other.type;
            reconstruct_string(std::move(other.what));
            error_origin = std::move(other.error_origin);
            return *this;
        }
#else

        explicit error_t(error_code_t type, const std::pmr::string& what)
            : type(type)
            , what(what) {}
        explicit error_t(error_code_t type, std::pmr::string&& what)
            : type(type)
            , what(std::move(what)) {}

        error_t& operator=(const error_t& other) {
            type = other.type;
            reconstruct_string(other.what);
            return *this;
        }
        error_t& operator=(error_t&& other) noexcept {
            type = other.type;
            reconstruct_string(std::move(other.what));
            return *this;
        }
#endif

        error_t(const error_t&) = default;
        error_t(error_t&&) noexcept = default;

        static error_t no_error() { return error_t(); }

        bool contains_error() const noexcept { return type != error_code_t::none; }

    private:
        explicit error_t()
            : type(error_code_t::none)
            // since we are using null_memory_resource, we have to explicitly change allocator on assignments
            , what(std::pmr::null_memory_resource()) {}

        template<typename... Args>
        void reconstruct_string(Args&&... args) {
            what.~basic_string();
            std::construct_at(&what, std::forward<Args>(args)...);
        }
    };

    // has implicit constructors to simplify usage
    template<typename T>
    requires(!std::is_same_v<std::decay<T>, error_t> && !std::is_same_v<T, void>) class result_wrapper_t {
    private:
        static constexpr bool trivial_store = std::is_default_constructible_v<T>;
        using Store_T = std::conditional_t<trivial_store, T, std::optional<T>>;

    public:
        template<typename... Args>
        result_wrapper_t(Args&&... args) requires(std::constructible_from<T, Args...>)
            : value_(std::forward<Args>(args)...)
            , error_(error_t::no_error()) {}

        result_wrapper_t(const error_t& error)
            : error_(error) {}
        result_wrapper_t(error_t&& error)
            : error_(std::move(error)) {}

#if not defined(NDEBUG)
        result_wrapper_t(const result_wrapper_t& other) requires(std::is_copy_constructible_v<T>)
            : value_(other.value_)
            , error_(other.error_) {}

        result_wrapper_t(result_wrapper_t&& other) noexcept requires(std::is_move_constructible_v<T>)
            : value_(std::move(other.value_))
            , error_(std::move(other.error_)) {}

        result_wrapper_t& operator=(const result_wrapper_t& other) requires(std::is_copy_assignable_v<T>) {
            value_ = other.value_;
            error_ = other.error_;
            error_checked_ = false;
            return *this;
        }

        result_wrapper_t& operator=(result_wrapper_t&& other) noexcept requires(std::is_move_assignable_v<T>) {
            value_ = std::move(other.value_);
            error_ = other.error_;
            error_checked_ = false;
            other.error_checked_ = true;
            return *this;
        }
#else
        result_wrapper_t(const result_wrapper_t& other) requires(std::is_copy_constructible_v<T>) = default;

        result_wrapper_t(result_wrapper_t&&) noexcept requires(std::is_move_constructible_v<T>) = default;

        result_wrapper_t& operator=(const result_wrapper_t& other) requires(std::is_copy_assignable_v<T>) = default;

        result_wrapper_t& operator=(result_wrapper_t&&) noexcept requires(std::is_move_assignable_v<T>) = default;
#endif

        bool has_error() const noexcept {
#if not defined(NDEBUG)
            error_checked_ = true;
#endif
            return error_.type != error_code_t::none;
        }
        const error_t& error() const noexcept {
#if not defined(NDEBUG)
            error_checked_ = true;
#endif
            return error_;
        }
        const T& value() const noexcept {
            assert(error_checked_ && "result_wrapper_t::value() called without checking for errors");
            assert(!has_error() && "result_wrapper_t::value() called with error present");
            if constexpr (trivial_store) {
                return value_;
            } else {
                return *value_;
            }
        }
        T& value() noexcept {
            assert(error_checked_ && "result_wrapper_t::value() called without checking for errors");
            assert(!has_error() && "result_wrapper_t::value() called with error present");
            if constexpr (trivial_store) {
                return value_;
            } else {
                return *value_;
            }
        }
        bool operator()() const noexcept { return !has_error(); }

        template<typename U>
        requires(!std::is_same_v<T, U>) [[nodiscard]] result_wrapper_t<U> convert_error() {
            assert(error_.contains_error());
            return result_wrapper_t<U>(std::move(error_));
        }

    private:
        Store_T value_;
#if not defined(NDEBUG)
        mutable bool error_checked_{false};
#endif
        error_t error_;
    };

    // TODO: assert for unchecked errors in the destructor of result_wrapper_t

} // namespace core