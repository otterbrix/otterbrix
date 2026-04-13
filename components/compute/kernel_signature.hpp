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

    struct input_type {
        input_type(type_matcher_fn m);
        bool matches(const types::complex_logical_type& type) const;

    private:
        type_matcher_fn matcher_;
    };

    using fixed_t = types::complex_logical_type;
    using type_resolver_fn = std::function<core::result_wrapper_t<fixed_t>(std::pmr::memory_resource* resource,
                                                                           const std::pmr::vector<fixed_t>&)>;

    struct output_type {
        static output_type fixed(fixed_t type);
        static output_type computed(type_resolver_fn resolver);

        [[nodiscard]] core::result_wrapper_t<fixed_t> resolve(std::pmr::memory_resource* resource,
                                                              const std::pmr::vector<fixed_t>& input_types) const;

    private:
        output_type() = default;

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
