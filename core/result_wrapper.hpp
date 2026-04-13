#pragma once

#include <cassert>
#include <memory>
#include <memory_resource>
#include <source_location>

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
#if defined(DEV_MODE)
        std::source_location error_origin{};

        explicit error_t(std::pmr::memory_resource* resource,
                         error_code_t type,
                         std::source_location location = std::source_location::current())
            : type(type)
            , what(resource)
            , error_origin(location) {}
        explicit error_t(error_code_t type,
                         const std::pmr::string& what,
                         std::source_location location = std::source_location::current())
            : type(type)
            , what(what)
            , error_origin(location) {}
        explicit error_t(error_code_t type,
                         std::pmr::string&& what,
                         std::source_location location = std::source_location::current())
            : type(type)
            , what(std::move(what))
            , error_origin(location) {}

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

        explicit error_t(std::pmr::memory_resource* resource, error_code_t type)
            : type(type)
            , what(resource) {}
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
    requires(!std::is_same_v<std::decay<T>, error_t>) class result_wrapper_t {
    private:
        static constexpr bool trivial_store = std::is_default_constructible_v<T>;
        using Store_T = std::conditional_t<trivial_store, T, std::unique_ptr<T>>;

    public:
        template<typename... Args>
        result_wrapper_t(Args&&... args) requires(trivial_store&& std::constructible_from<T, Args...>)
            : value_(std::forward<Args>(args)...)
            , error_(error_t::no_error()) {}
        template<typename... Args>
        result_wrapper_t(Args&&... args) requires(!trivial_store && std::constructible_from<T, Args...>)
            : value_(std::make_unique<T>(std::forward<Args>(args)...))
            , error_(error_t::no_error()) {}
        template<typename... Args>
        result_wrapper_t(Args&&... args) requires(!trivial_store &&
                                                  std::constructible_from<std::unique_ptr<T>, Args...>)
            : value_(std::forward<Args>(args)...)
            , error_(error_t::no_error()) {}

        result_wrapper_t(const error_t& error)
            : error_(error) {}
        result_wrapper_t(error_t&& error)
            : error_(std::move(error)) {}

        result_wrapper_t(const result_wrapper_t& other) requires(trivial_store&& std::is_copy_constructible_v<T>) =
            default;
        result_wrapper_t(const result_wrapper_t& other) requires(!trivial_store && std::is_copy_constructible_v<T>)
            : value_(std::make_unique<T>(other.value()))
            , error_(other.error_) {}
        result_wrapper_t&
        operator=(const result_wrapper_t& other) requires(trivial_store&& std::is_copy_constructible_v<T>) = default;
        result_wrapper_t& operator=(const result_wrapper_t& other) requires(!trivial_store &&
                                                                            std::is_copy_constructible_v<T>) {
            value_ = std::make_unique<T>(other.value_);
            error_ = other.error_;
            return *this;
        }

        result_wrapper_t(result_wrapper_t&&) noexcept = default;
        result_wrapper_t& operator=(result_wrapper_t&&) noexcept = default;

        bool has_error() const noexcept { return error_.type != error_code_t::none; }
        const error_t& error() const noexcept { return error_; }
        const T& value() const noexcept {
            assert(!has_error());
            if constexpr (trivial_store) {
                return value_;
            } else {
                return *value_;
            }
        }
        T& value() noexcept {
            assert(!has_error());
            if constexpr (trivial_store) {
                return value_;
            } else {
                return *value_;
            }
        }
        bool operator()() const noexcept { return !has_error(); }

        template<typename U>
        requires(!std::is_same_v<T, U>) [[nodiscard]] result_wrapper_t<U> convert_error() const {
            assert(error_.contains_error());
            return result_wrapper_t<U>(error_);
        }

    private:
        Store_T value_;
        error_t error_;
    };

} // namespace core