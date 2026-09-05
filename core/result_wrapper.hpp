#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <memory_resource>
#include <optional>
#include <source_location>
#include <string>
#include <type_traits>
#include <utility>

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
        invalid_constraint,

        // Buffer/storage-layer runtime errors. Surfaced via result_wrapper_t/error_t instead of throwing on
        // the agent_disk thread.
        out_of_memory,   // buffer pool exhausted; evict_blocks could not free enough memory
        data_corruption, // block checksum mismatch on read (disk reload / spill read)
        io_error,        // file create/open/header/read/write failure
        write_conflict,  // MVCC write-write conflict
    };

    struct [[nodiscard]] error_t {
        error_code_t type;
        std::pmr::string what;
#if not defined(NDEBUG)
        std::source_location error_origin{};
#endif

        explicit error_t(error_code_t type,
                         const std::pmr::string& what,
                         [[maybe_unused]] std::source_location location = std::source_location::current())
            : type(type)
            , what(what)
#if not defined(NDEBUG)
            , error_origin(location)
#endif
        {
            assert(type != error_code_t::none &&
                   "no error state of error_t can only be created using no_error() constructor");
        }
        explicit error_t(error_code_t type,
                         std::pmr::string&& what,
                         [[maybe_unused]] std::source_location location = std::source_location::current())
            : type(type)
            , what(std::move(what))
#if not defined(NDEBUG)
            , error_origin(location)
#endif
        {
            assert(type != error_code_t::none &&
                   "no error state of error_t can only be created using no_error() constructor");
        }

        // NB (allocator residency): neither assignment decides WHERE the message lives, and
        // neither can — an error_t carries no resource beyond its string's own allocator, and
        // the destination may still be a no_error() anchored on null_memory_resource, which is
        // why both reconstruct instead of assigning. The consequences are therefore fixed:
        //   - copy  -> std::pmr::string's copy constructor, which does NOT propagate the
        //              allocator, so the new buffer lands on the DEFAULT resource;
        //   - move  -> std::pmr::string's move constructor, which keeps the SOURCE allocator,
        //              so this object starts pointing into an arena it does not own.
        // An owner that HAS a resource (cursor_t, operator_t) must therefore never assign a
        // foreign error_t directly: it rebuilds through error_on() below. See its comment.
        error_t& operator=(const error_t& other) {
            type = other.type;
            reconstruct_string(other.what);
#if not defined(NDEBUG)
            error_origin = other.error_origin;
#endif
            return *this;
        }
        error_t& operator=(error_t&& other) noexcept {
            type = other.type;
            reconstruct_string(std::move(other.what));
#if not defined(NDEBUG)
            error_origin = std::move(other.error_origin);
#endif
            return *this;
        }

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

    // THE one place that decides where an error message lives.
    //
    // Every owner that has a resource of its own — a cursor, an operator — funnels a foreign
    // error_t through here instead of copying or moving it, because neither of error_t's own
    // paths puts the message on the owner's arena: a copy lands on the default resource, a
    // move keeps the producer's (see the note on error_t's assignments). Both are a lie in the
    // same contract, in opposite directions; this rebuilds the string on `resource` so the
    // answer is simply "the owner's".
    [[nodiscard]] inline error_t error_on(std::pmr::memory_resource* resource, const error_t& error) {
        assert(resource != nullptr && "an error message needs a resource to live on");
        if (!error.contains_error()) {
            return error_t::no_error();
        }
        error_t rebuilt{error.type, std::pmr::string{error.what, resource}};
#if not defined(NDEBUG)
        rebuilt.error_origin = error.error_origin;
#endif
        return rebuilt;
    }

    // has implicit constructors to simplify usage
    template<typename T>
    requires(!std::is_same_v<std::decay<T>, error_t> && !std::is_same_v<T, void>) class [[nodiscard]] result_wrapper_t {
    private:
        static constexpr bool trivial_store = std::is_default_constructible_v<T>;
        using Store_T = std::conditional_t<trivial_store, T, std::optional<T>>;

    public:
        template<typename... Args>
        result_wrapper_t(Args&&... args) requires(std::constructible_from<T, Args...>)
            : value_(std::forward<Args>(args)...)
            , error_(error_t::no_error()) {}

        // DEBT (allocator residency): these two cannot call error_on() — a result_wrapper_t
        // has no resource of its own, only a value and an error, so there is no arena here to
        // name. The first therefore leaves the message on the DEFAULT resource and the second
        // keeps the producer's. Both are corrected at the first owner that does have a
        // resource: cursor_t's error constructors and operator_t::set_error rebuild through
        // core::error_on. Giving result_wrapper_t a resource of its own is the real fix and is
        // a separate change — it touches every construction site of every result_wrapper_t.
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
            // MOVE the error, do not copy it: `other` is being consumed, and a copy here
            // reallocates the message onto the default resource, which is neither wrapper's
            // arena. The NDEBUG branch below is `= default` and therefore moves, so reading
            // `other.error_` by name here would make Debug and Release disagree on where the
            // message of a moved-from result lives.
            error_ = std::move(other.error_);
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
        // Value-initialized because the error-carrying constructors below leave it alone: without
        // this, copying or moving a wrapper that holds an error reads an indeterminate value, which
        // gcc reports as -Wmaybe-uninitialized and which is undefined behaviour regardless.
        // Store_T is either a trivially-copyable T or std::optional<T>, so {} is always valid here.
        Store_T value_{};
#if not defined(NDEBUG)
        mutable bool error_checked_{false};
#endif
        error_t error_;
    };
    // TODO: assert for unchecked errors in the destructor of result_wrapper_t

    namespace detail {
        template<typename... Ts>
        std::integral_constant<std::size_t, sizeof...(Ts)> arity(Ts&&...);

        // Must sit after error_t: the global scope has a glibc `typedef int error_t`
        template<typename T>
        concept result_like = requires(T& t) {
            { t.has_error() } -> std::same_as<bool>;
            { t.error() } -> std::convertible_to<const core::error_t&>;
            t.value();
        };
    } // namespace detail
} // namespace core

#define CORE_DETAIL_CONCAT_IMPL(x, y) x##y
#define CORE_DETAIL_CONCAT(x, y) CORE_DETAIL_CONCAT_IMPL(x, y)

#define CORE_DETAIL_VALUE_OR_RETURN(tmp, decl, ...)                                                                    \
    static_assert(decltype(core::detail::arity(__VA_ARGS__))::value == 1,                                              \
                  "VALUE_OR_RETURN takes a declaration and ONE expression");                                           \
    auto tmp = (__VA_ARGS__);                                                                                          \
    static_assert(core::detail::result_like<decltype(tmp)>,                                                            \
                  "VALUE_OR_RETURN expects a core::result_wrapper_t; "                                                 \
                  "for a plain core::error_t use RETURN_IF_ERROR");                                                    \
    if (tmp.has_error()) {                                                                                             \
        return tmp.error();                                                                                            \
    }                                                                                                                  \
    decl = std::move(tmp.value())

#define CORE_DETAIL_RETURN_IF_ERROR(tmp, ...)                                                                          \
    static_assert(decltype(core::detail::arity(__VA_ARGS__))::value == 1,                                              \
                  "RETURN_IF_ERROR takes ONE expression");                                                             \
    if (auto tmp = (__VA_ARGS__); tmp.contains_error()) {                                                              \
        return tmp;                                                                                                    \
    }

// The first argument is a whole declaration, so auto&/explicit type/assignment all work
#define VALUE_OR_RETURN(decl, ...)                                                                                     \
    CORE_DETAIL_VALUE_OR_RETURN(CORE_DETAIL_CONCAT(value_or_return_, __COUNTER__), decl, __VA_ARGS__)

// RETURN_IF_ERROR is the same for a plain error_t, which carries no value
#define RETURN_IF_ERROR(...) CORE_DETAIL_RETURN_IF_ERROR(CORE_DETAIL_CONCAT(return_if_error_, __COUNTER__), __VA_ARGS__)
