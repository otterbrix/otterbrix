#pragma once

#include <core/result_wrapper.hpp>

#include <components/types/types.hpp>
#include <memory_resource>
#include <type_traits>
#include <vector>

namespace components::compute {

    // have to be power of 2 for masking
    enum class function_type_t : uint8_t
    {
        invalid = 0,
        vector = 1,
        aggregate = 2,
        expand = 4
    };

    using function_types_mask = std::underlying_type_t<function_type_t>;

    template<typename T, typename... Args>
    requires(std::is_same_v<T, function_type_t>) constexpr function_types_mask create_mask(T first, Args... args) {
        if constexpr (sizeof...(args) == 0) {
            return static_cast<function_types_mask>(first);
        } else {
            return static_cast<function_types_mask>(first) | create_mask(args...);
        }
    }

    constexpr bool check_mask(function_types_mask mask, function_type_t type) {
        return (mask & static_cast<function_types_mask>(type)) != 0;
    }

    // One parameter of a kernel signature
    struct parameter_type {
        using variable_id = uint8_t;

        static parameter_type exact(types::complex_logical_type type);
        // `admissible` empty == the variable accepts any type
        static parameter_type variable(variable_id id, std::pmr::vector<types::complex_logical_type> admissible);
        static parameter_type variable(variable_id id);

        [[nodiscard]] bool is_variable() const noexcept { return is_variable_; }
        [[nodiscard]] variable_id id() const noexcept { return id_; }
        [[nodiscard]] const types::complex_logical_type& type() const noexcept { return type_; }
        // Variables only; empty means unconstrained.
        [[nodiscard]] const std::pmr::vector<types::complex_logical_type>& admissible() const noexcept {
            return admissible_;
        }
        [[nodiscard]] bool admits(const types::complex_logical_type& candidate) const;

    private:
        parameter_type() = default;
        // MOVE-CONSTRUCTS admissible_, and that is the whole point: assigning a vector over a
        // default-constructed member is move-ASSIGNMENT, which for a pmr vector with a
        // DIFFERENT allocator relocates element by element and allocates in the TARGET's
        // resource. Constructing takes the source's buffer AND its resource, so nothing is
        // allocated and the caller's arena is preserved.
        parameter_type(variable_id id, std::pmr::vector<types::complex_logical_type> admissible)
            : is_variable_(true)
            , id_(id)
            , admissible_(std::move(admissible)) {}

        bool is_variable_{false};
        variable_id id_{0};
        types::complex_logical_type type_{types::logical_type::ANY};
        // null, NOT get_default_resource() and NOT new_delete_resource(): both of those are
        // process-global arenas, and a default member initializer runs on every
        // default-construction, so either one would silently decide this vector's arena.
        // NOTHING is allowed to allocate through this initializer: exact() and variable(id)
        // leave the vector empty forever, and variable(id, admissible) MOVES a caller-owned
        // vector -- with the caller's own resource -- in over it. So an allocation here means
        // a new path filled admissible_ without bringing a resource, and null makes that
        // refuse loudly instead of quietly borrowing an arena nobody chose (rule 6).
        std::pmr::vector<types::complex_logical_type> admissible_{std::pmr::null_memory_resource()};
    };

    using fixed_t = types::complex_logical_type;

    // The callable a signature carries for an output whose type is only known once the input
    // types are. NOT std::function: rule 14 forbids it, and nothing here needs what it buys.
    // Type erasure exists to store arbitrary captured state; the whole tree has exactly TWO
    // resolver shapes, and only one of them captures anything at all:
    //   stateless -- a capture-less lambda or free function, passed in verbatim (today:
    //                test_execution.cpp and test_prorettype_channel.cpp);
    //   indexed   -- what same_type_resolver(i) produces, whose entire state IS the index i
    //                (kernels/aggregate.cpp x4, integration test_udfs.cpp).
    // Keeping the two apart as two pointers plus one size_t means no vtable, no allocation,
    // and a trivially copyable object -- pinned by
    // components::compute::output_type::the_resolver_carries_no_erased_state.
    //
    // An EMPTY resolver reports on the error channel (rule 6). std::function answered the same
    // call by throwing std::bad_function_call, which this engine has no way to catch.
    struct type_resolver_fn {
        using stateless_fn_t = core::result_wrapper_t<fixed_t> (*)(std::pmr::memory_resource* resource,
                                                                   const std::pmr::vector<fixed_t>& input_types);
        using indexed_fn_t = core::result_wrapper_t<fixed_t> (*)(size_t input_index,
                                                                 std::pmr::memory_resource* resource,
                                                                 const std::pmr::vector<fixed_t>& input_types);

        type_resolver_fn() = default;

        // IMPLICIT, and a TEMPLATE, and both on purpose. Implicit so `computed(lambda)` keeps
        // reading as it does today; a template because a non-template ctor taking
        // stateless_fn_t would need lambda -> stateless_fn_t -> type_resolver_fn, two
        // user-defined conversions in one implicit sequence, which is ill-formed. Deducing F
        // and casting in the initializer spends only one.
        template<typename F>
        requires(!std::is_same_v<std::remove_cvref_t<F>, type_resolver_fn> &&
                 std::is_convertible_v<F, stateless_fn_t>) type_resolver_fn(F&& fn) noexcept
            : stateless_(static_cast<stateless_fn_t>(fn)) {}

        type_resolver_fn(indexed_fn_t fn, size_t input_index) noexcept
            : indexed_(fn)
            , input_index_(input_index) {}

        [[nodiscard]] bool empty() const noexcept { return stateless_ == nullptr && indexed_ == nullptr; }

        core::result_wrapper_t<fixed_t> operator()(std::pmr::memory_resource* resource,
                                                   const std::pmr::vector<fixed_t>& input_types) const;

    private:
        stateless_fn_t stateless_{nullptr};
        indexed_fn_t indexed_{nullptr};
        size_t input_index_{0};
    };

    // Output-type for a kernel signature. Same hybrid pattern as input_type:
    // typed factories `fixed(t)` / `same_type_at(idx)` are introspectable for
    // pg_proc.prorettype persistence; `computed(resolver)` keeps an arbitrary
    // closure for runtime-only callsites (kind_=custom, not persistable).
    struct output_type {
        enum class kind_t : uint8_t
        {
            custom,
            fixed_value,
            same_type_at_index
        };

        static output_type fixed(fixed_t type);
        static output_type same_type_at(size_t input_index);
        static output_type computed(type_resolver_fn resolver); // kind_=custom

        [[nodiscard]] core::result_wrapper_t<fixed_t> resolve(std::pmr::memory_resource* resource,
                                                              const std::pmr::vector<fixed_t>& input_types) const;

        kind_t kind() const noexcept { return kind_; }
        fixed_t fixed_value() const noexcept { return fixed_value_; }
        size_t input_index() const noexcept { return input_index_; }

    private:
        output_type() = default;

        // kind_ IS the discriminator -- there is no second copy of the answer to fall out of
        // step with it. The two introspectable kinds resolve straight out of fixed_value_ /
        // input_index_, and resolver_ is read only for kind_t::custom. (std::variant, which
        // used to hold a duplicate of the fixed value alongside these, is banned by rule 14.)
        kind_t kind_{kind_t::custom};
        fixed_t fixed_value_{types::logical_type::ANY};
        size_t input_index_{0};
        type_resolver_fn resolver_;
    };

    struct kernel_signature_t {
        kernel_signature_t() = delete;
        kernel_signature_t(function_type_t function_type,
                           std::pmr::vector<parameter_type> input_types,
                           std::pmr::vector<struct output_type> output_types);

        function_type_t function_type;
        std::pmr::vector<parameter_type> input_types;
        std::pmr::vector<output_type> output_types;

        [[nodiscard]] bool matches_inputs(const std::pmr::vector<types::complex_logical_type>& types) const;
    };

    type_resolver_fn same_type_resolver(size_t input_index);

    bool check_signature_conflicts(const kernel_signature_t& lhs, const kernel_signature_t& rhs);

    bool check_signature_conflicts(const std::vector<kernel_signature_t>& lhs,
                                   const std::vector<kernel_signature_t>& rhs);

} // namespace components::compute
