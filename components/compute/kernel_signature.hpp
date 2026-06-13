#pragma once

#include <core/result_wrapper.hpp>

#include <components/types/types.hpp>
#include <functional>
#include <memory_resource>
#include <variant>
#include <vector>

namespace components::compute {

    // have to be power of 2 for masking
    enum class function_type_t : uint8_t
    {
        invalid = 0,
        row = 1,
        vector = 2,
        aggregate = 4
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

    using type_matcher_fn = std::function<bool(const types::complex_logical_type&)>;

    // Input-type matcher used by kernel_signature_t for runtime dispatch and
    // pg_proc.proargmatchers persistence. Two construction paths:
    //   * Typed factories (make_exact / make_numeric / ...) — set `kind_` so
    //     encode_proargmatchers can introspect and re-build the matcher on
    //     restart.
    //   * Raw closure ctor input_type(type_matcher_fn) — kind_ stays at
    //     `custom`; the closure is opaque and cannot be persisted (used by
    //     tests or runtime-only callsites).
    struct input_type {
        enum class kind_t : uint8_t
        {
            custom,
            exact,
            numeric,
            integer,
            floating,
            string,
            any_of,
            always_true
        };

        static input_type make_exact(types::logical_type t);
        static input_type make_numeric();
        static input_type make_integer();
        static input_type make_floating();
        static input_type make_string();
        static input_type make_any_of(std::pmr::vector<types::logical_type> types);
        static input_type make_always_true();

        input_type(type_matcher_fn m); // kind_=custom — not introspectable; allows {closure} brace-init

        bool matches(const types::complex_logical_type& type) const;

        // Introspection for persistence.
        kind_t kind() const noexcept { return kind_; }
        types::logical_type exact_type() const noexcept { return exact_type_; }
        const std::pmr::vector<types::logical_type>& any_of_list() const noexcept { return any_of_list_; }

    private:
        kind_t kind_{kind_t::custom};
        types::logical_type exact_type_{types::logical_type::ANY};
        std::pmr::vector<types::logical_type> any_of_list_{std::pmr::get_default_resource()};
        type_matcher_fn matcher_;
    };

    using fixed_t = types::complex_logical_type;
    using type_resolver_fn = std::function<core::result_wrapper_t<fixed_t>(std::pmr::memory_resource* resource,
                                                                           const std::pmr::vector<fixed_t>&)>;

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

        kind_t kind_{kind_t::custom};
        fixed_t fixed_value_{types::logical_type::ANY};
        size_t input_index_{0};
        std::variant<fixed_t, type_resolver_fn> value_;
    };

    struct kernel_signature_t {
        kernel_signature_t() = delete;
        kernel_signature_t(function_type_t function_type,
                           std::pmr::vector<input_type> input_types,
                           std::pmr::vector<struct output_type> output_types);

        function_type_t function_type;
        std::pmr::vector<input_type> input_types;
        std::pmr::vector<output_type> output_types;

        [[nodiscard]] bool matches_inputs(const std::pmr::vector<types::complex_logical_type>& types) const;
    };

    type_matcher_fn exact_type_matcher(types::logical_type type);
    type_matcher_fn numeric_types_matcher();
    type_matcher_fn integer_types_matcher();
    type_matcher_fn floating_types_matcher();
    type_matcher_fn string_types_matcher();
    type_matcher_fn any_type_matcher(std::pmr::vector<types::logical_type> type_list);
    type_matcher_fn always_true_type_matcher();

    type_resolver_fn same_type_resolver(size_t input_index);

    // Returns true if there are no conflicts
    bool check_signature_conflicts(
        const kernel_signature_t& lhs,
        const kernel_signature_t& rhs,
        const std::pmr::unordered_map<std::string, types::complex_logical_type>& registered_types);

    // Returns true if none of signature permutation results in conflict
    bool check_signature_conflicts(
        const std::vector<kernel_signature_t>& lhs,
        const std::vector<kernel_signature_t>& rhs,
        const std::pmr::unordered_map<std::string, types::complex_logical_type>& registered_types);

} // namespace components::compute
