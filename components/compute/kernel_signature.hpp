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
        // Constructs admissible_ rather than assigning over the default member: assignment onto
        // a pmr vector with a different allocator relocates elements into the TARGET's resource.
        parameter_type(variable_id id, std::pmr::vector<types::complex_logical_type> admissible)
            : is_variable_(true)
            , id_(id)
            , admissible_(std::move(admissible)) {}

        bool is_variable_{false};
        variable_id id_{0};
        types::complex_logical_type type_{types::logical_type::ANY};
        // null_memory_resource() on purpose: this member is never allocated through
        // directly, so an allocation here is a bug that should fail loudly, not quietly borrow
        // a process-global arena.
        std::pmr::vector<types::complex_logical_type> admissible_{std::pmr::null_memory_resource()};
    };

    using fixed_t = types::complex_logical_type;

    // Not std::function: only two resolver shapes exist -- stateless (capture-less
    // callable) and indexed (same_type_resolver(i), state = i) -- so two pointers + a size_t
    // erase nothing, staying trivially copyable; an empty one reports via the error channel
    // instead of throwing bad_function_call.
    struct type_resolver_fn {
        using stateless_fn_t = core::result_wrapper_t<fixed_t> (*)(std::pmr::memory_resource* resource,
                                                                   const std::pmr::vector<fixed_t>& input_types);
        using indexed_fn_t = core::result_wrapper_t<fixed_t> (*)(size_t input_index,
                                                                 std::pmr::memory_resource* resource,
                                                                 const std::pmr::vector<fixed_t>& input_types);

        type_resolver_fn() = default;

        // Implicit template, not a non-template ctor taking stateless_fn_t directly: that would
        // need lambda -> stateless_fn_t -> type_resolver_fn, two user-defined conversions in one
        // implicit sequence (ill-formed). Deducing F and casting inline spends only one.
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

        // kind_ is the sole discriminator: the introspectable kinds read straight from
        // fixed_value_/input_index_, resolver_ only for kind_t::custom. (std::variant, which
        // duplicated the fixed value here before, is rule-14 banned.)
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
