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
    // Every member carries its number: the C ABI and the C# enum hand these out as is. A removed code leaves
    // its number unused; a new code takes the next free one.
    enum class error_code_t : int32_t
    {
        none = 0,
        unimplemented_yet = 3,

        transaction_finalized = 9,
        missing_savepoint = 10,
        database_already_exists = 13,
        database_not_exists = 14,
        table_already_exists = 15,
        table_not_exists = 16,
        invalid_parameter = 22, // PostgreSQL 22023

        sql_parse_error = 30,
        unrecognized_function = 34,

        // Buffer/storage-layer runtime errors. Surfaced via result_wrapper_t/error_t instead of throwing on
        // the agent_disk thread.
        out_of_memory = 38,   // buffer pool exhausted; evict_blocks could not free enough memory
        data_corruption = 39, // block checksum mismatch on read (disk reload / spill read)
        io_error = 40,        // file create/open/header/read/write failure
        write_conflict = 41,  // MVCC write-write conflict

        connection_closed = 44,
        active_sql_transaction = 46,            // PostgreSQL 25001: the statement cannot run inside a transaction block
        dependent_objects_still_exist = 47,     // PostgreSQL 2BP01: other objects or the database system need it
        schema_already_exists = 48,             // PostgreSQL 42P06
        schema_not_exists = 49,                 // PostgreSQL 3F000
        indeterminate_datatype = 50,            // PostgreSQL 42P18: no clause gives a $n a type
        ambiguous_parameter = 51,               // PostgreSQL 42P08: two clauses give a $n two types
        duplicate_object = 52,                  // PostgreSQL 42710
        duplicate_column = 53,                  // PostgreSQL 42701
        undefined_object = 54,                  // PostgreSQL 42704
        invalid_function_definition = 55,       // PostgreSQL 42P13
        lock_file_exists = 56,                  // PostgreSQL F0001
        duplicate_function = 57,                // PostgreSQL 42723
        wrong_object_type = 58,                 // PostgreSQL 42809
        internal_error = 59,                    // PostgreSQL XX000: an invariant of the engine itself broke
        undefined_column = 60,                  // PostgreSQL 42703
        datatype_mismatch = 61,                 // PostgreSQL 42804
        ambiguous_column = 62,                  // PostgreSQL 42702
        invalid_table_definition = 63,          // PostgreSQL 42P16
        invalid_object_definition = 64,         // PostgreSQL 42P17
        protocol_violation = 65,                // PostgreSQL 08P01: the host broke the engine's contract
        numeric_value_out_of_range = 66,        // PostgreSQL 22003
        division_by_zero = 67,                  // PostgreSQL 22012
        cannot_coerce = 68,                     // PostgreSQL 42846
        cardinality_violation = 69,             // PostgreSQL 21000
        invalid_row_count_in_limit_clause = 70, // PostgreSQL 2201W
        invalid_row_count_in_result_offset_clause = 71, // PostgreSQL 2201X
        program_limit_exceeded = 72,                    // PostgreSQL 54000
        invalid_text_representation = 73,               // PostgreSQL 22P02
        invalid_datetime_format = 74,                   // PostgreSQL 22007
        null_value_no_indicator_parameter = 75,         // PostgreSQL 22002
        null_value_not_allowed = 76,                    // PostgreSQL 22004
        invalid_regular_expression = 77,                // PostgreSQL 2201B
        ambiguous_function = 78,                        // PostgreSQL 42725
        grouping_error = 79,                            // PostgreSQL 42803
        statement_too_complex = 80,                     // PostgreSQL 54001
        check_violation = 81,                           // PostgreSQL 23514
        not_null_violation = 82,                        // PostgreSQL 23502
        unique_violation = 83,                          // PostgreSQL 23505
        foreign_key_violation = 84,                     // PostgreSQL 23503
        invalid_foreign_key = 85,                       // PostgreSQL 42830
        invalid_column_reference = 86,                  // PostgreSQL 42P10
        ambiguous_alias = 87,                           // PostgreSQL 42P09
        duplicate_alias = 88,                           // PostgreSQL 42712
        index_corrupted = 89,                           // PostgreSQL XX002
        no_active_sql_transaction = 91,                 // PostgreSQL 25P01
        object_not_in_prerequisite_state = 92,          // PostgreSQL 55000
        insufficient_resources = 93,                    // PostgreSQL 53000
    };

    struct [[nodiscard]] error_t {
        error_code_t type;
        // 1-based character offset into the statement text (PostgreSQL's cursor position), -1 when none
        int32_t position{-1};
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
        explicit error_t(error_code_t type,
                         std::pmr::string&& what,
                         int32_t position,
                         [[maybe_unused]] std::source_location location = std::source_location::current())
            : type(type)
            , position(position)
            , what(std::move(what))
#if not defined(NDEBUG)
            , error_origin(location)
#endif
        {
            assert(type != error_code_t::none &&
                   "no error state of error_t can only be created using no_error() constructor");
        }

        // Reconstructs rather than assigns: the destination's allocator may be null_memory_resource
        // (no_error()), so copy must land on the default resource and move must keep the source's —
        // an owner with its own resource rebuilds through error_on() below instead.
        error_t& operator=(const error_t& other) {
            type = other.type;
            position = other.position;
            reconstruct_string(other.what);
#if not defined(NDEBUG)
            error_origin = other.error_origin;
#endif
            return *this;
        }
        error_t& operator=(error_t&& other) noexcept {
            type = other.type;
            position = other.position;
            reconstruct_string(std::move(other.what));
#if not defined(NDEBUG)
            error_origin = std::move(other.error_origin);
#endif
            return *this;
        }

        // Defaulted, so member-wise: copy lands on the default resource, move keeps the source's
        // allocator — a moved error_t outlives its arena only if the arena outlives it. Safe today:
        // the short-lived arenas are base_otterbrix_t::host_t::resource (integration/cpp/base_spaces.cpp) and
        // four core::pmr::arena_resource_t arenas (components/planner/view_expansion.cpp,
        // components/sql/transformer/impl/transfrom_common.cpp, integration/cpp/wrapper_dispatcher.cpp x2), which
        // hold raw_parser's error until the caller rebuilds it on its own resource, and one
        // monotonic_buffer_resource (integration/python/arrow/arrow_scan_function.cpp); none let an error_t
        // escape scope — recheck this list before adding a shorter-lived one.
        error_t(const error_t&) = default;
        error_t(error_t&&) noexcept = default;

        // Allocator-extended copy: std::pmr::string's copy ctor can't inherit a resource
        // (select_on_container_copy_construction defaults it for polymorphic_allocator), so this
        // is the only way to name where the message lands; error_on() below calls it.
        //
        // The null check runs from the initializer, not the body: a message long enough to
        // allocate does so during member-init, before the body would run, so a body-level check
        // would miss exactly the input it exists for.
        error_t(const error_t& other, std::pmr::memory_resource* resource)
            : type(other.type)
            , position(other.position)
            , what(other.what, message_resource(resource))
#if not defined(NDEBUG)
            , error_origin(other.error_origin)
#endif
        {
        }

        static error_t no_error() { return error_t(); }

        bool contains_error() const noexcept { return type != error_code_t::none; }

    private:
        explicit error_t()
            : type(error_code_t::none)
            // since we are using null_memory_resource, we have to explicitly change allocator on assignments
            , what(std::pmr::null_memory_resource()) {}

        // Assert, not a refusal: error_t is the bottom of the error channel, with nothing to report
        // a bad argument to. Gone under NDEBUG, where a null resource null-derefs inside std::pmr::string.
        static std::pmr::memory_resource* message_resource(std::pmr::memory_resource* resource) noexcept {
            assert(resource != nullptr && "an error message needs a resource to live on");
            return resource;
        }

        template<typename... Args>
        void reconstruct_string(Args&&... args) {
            what.~basic_string();
            std::construct_at(&what, std::forward<Args>(args)...);
        }
    };

    // The one place that puts a foreign error_t's message on the owner's own arena: a plain
    // copy lands on the default resource, a plain move keeps the producer's (see error_t's
    // assignments). Wraps the allocator-extended copy constructor.
    [[nodiscard]] inline error_t error_on(std::pmr::memory_resource* resource, const error_t& error) {
        assert(resource != nullptr && "an error message needs a resource to live on");
        if (!error.contains_error()) {
            return error_t::no_error();
        }
        return error_t{error, resource};
    }

    template<typename T, typename Error = error_t>
    requires(!std::is_same_v<std::decay<T>, Error> && !std::is_same_v<T, void>) class [[nodiscard]] result_wrapper_t {
    private:
        static constexpr bool trivial_store = std::is_default_constructible_v<T>;
        using Store_T = std::conditional_t<trivial_store, T, std::optional<T>>;

    public:
        template<typename... Args>
        result_wrapper_t(Args&&... args) requires(std::constructible_from<T, Args...>)
            : value_(std::forward<Args>(args)...)
            , error_(Error::no_error()) {}

        // Can't call error_on() here: result_wrapper_t has no resource of its own, so the copy
        // lands on the default resource and the move keeps the producer's — corrected at the first
        // owner that does have one (cursor_t's error constructors, operator_t::set_error) through
        // core::error_on.
        result_wrapper_t(const Error& error)
            : error_(error) {}
        result_wrapper_t(Error&& error)
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
            // Move, not copy: the `= default` NDEBUG branch below moves, and copying here
            // instead would make debug and release disagree on where the message lives.
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
            return error_.contains_error();
        }
        const Error& error() const noexcept {
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
        requires(!std::is_same_v<T, U>) [[nodiscard]] result_wrapper_t<U, Error> convert_error() {
            assert(error_.contains_error());
            return result_wrapper_t<U, Error>(std::move(error_));
        }

    private:
        // Value-initialized because the error-carrying constructors leave it alone: otherwise
        // copying/moving a wrapper that holds an error reads an indeterminate value (gcc's
        // -Wmaybe-uninitialized, and UB regardless). Store_T is T or std::optional<T>, so {} is always valid.
        Store_T value_{};
#if not defined(NDEBUG)
        mutable bool error_checked_{false};
#endif
        Error error_;
    };
    // TODO: assert for unchecked errors in the destructor of result_wrapper_t

    namespace detail {
        template<typename... Ts>
        std::integral_constant<std::size_t, sizeof...(Ts)> arity(Ts&&...);

        // Must sit after error_t: the global scope has a glibc `typedef int error_t`
        template<typename T>
        concept result_like = requires(T& t) {
            { t.has_error() }
            ->std::same_as<bool>;
            { t.error() }
            ->std::convertible_to<const core::error_t&>;
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
    static_assert(decltype(core::detail::arity(__VA_ARGS__))::value == 1, "RETURN_IF_ERROR takes ONE expression");     \
    if (auto tmp = (__VA_ARGS__); tmp.contains_error()) {                                                              \
        return tmp;                                                                                                    \
    }

// The first argument is a whole declaration, so auto&/explicit type/assignment all work
#define VALUE_OR_RETURN(decl, ...)                                                                                     \
    CORE_DETAIL_VALUE_OR_RETURN(CORE_DETAIL_CONCAT(value_or_return_, __COUNTER__), decl, __VA_ARGS__)

// RETURN_IF_ERROR is the same for a plain error_t, which carries no value
#define RETURN_IF_ERROR(...) CORE_DETAIL_RETURN_IF_ERROR(CORE_DETAIL_CONCAT(return_if_error_, __COUNTER__), __VA_ARGS__)
