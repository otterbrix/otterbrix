#pragma once

#include "compute_result.hpp"

#include <components/types/types.hpp>
#include <functional>
#include <memory_resource>
#include <variant>
#include <vector>

namespace components::compute {
    using type_matcher_fn = std::function<bool(const types::complex_logical_type&)>;

    struct input_type {
        input_type(type_matcher_fn m);
        bool matches(const types::complex_logical_type& type) const;

    private:
        type_matcher_fn matcher_;
    };

    using fixed_t = types::complex_logical_type;
    using type_resolver_fn = std::function<compute_result<fixed_t>(const std::pmr::vector<fixed_t>&)>;

    struct output_type {
        static output_type fixed(fixed_t type);
        static output_type computed(type_resolver_fn resolver);

        compute_result<fixed_t> resolve(const std::pmr::vector<fixed_t>& input_types) const;

    private:
        output_type() = default;

        std::variant<fixed_t, type_resolver_fn> value_;
    };

    struct kernel_signature_t {
        kernel_signature_t() = delete;
        kernel_signature_t(std::pmr::vector<input_type> input_types, output_type output_type);

        std::pmr::vector<input_type> input_types;
        struct output_type output_type;

        bool matches_inputs(const std::pmr::vector<types::complex_logical_type>& types) const;
    };

    type_matcher_fn exact_type_matcher(types::logical_type type);
    type_matcher_fn numeric_types_matcher();
    type_matcher_fn integer_types_matcher();
    type_matcher_fn floating_types_matcher();
    type_matcher_fn any_type_matcher(std::pmr::vector<types::logical_type> type_list);
    type_matcher_fn always_true_type_matcher();

    type_resolver_fn same_type_resolver();
} // namespace components::compute
