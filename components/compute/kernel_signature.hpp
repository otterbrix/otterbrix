#pragma once

#include "compute_result.hpp"

#include <components/types/types.hpp>
#include <cstdint>
#include <functional>
#include <memory_resource>
#include <variant>
#include <vector>

namespace components::compute {
    // Tagged kind for input_type. Replaces std::function-based type_matcher_fn so signatures
    // are serializable (#152 — UDF persistence). Each factory function returns an input_type
    // with the appropriate kind + parameters.
    enum class type_matcher_kind : std::uint8_t
    {
        exact = 0,    // matches exactly types::logical_type::<exact_type>
        numeric = 1,  // matches any types::is_numeric(t.type())
        integer = 2,  // matches signed/unsigned ints (TINYINT..UHUGEINT)
        floating = 3, // matches FLOAT or DOUBLE
        any_of = 4,   // matches any type in any_of_list
        always_true = 5,
    };

    struct input_type {
        type_matcher_kind kind{type_matcher_kind::always_true};
        types::logical_type exact_type{types::logical_type::INVALID};
        std::vector<types::logical_type> any_of_list;

        input_type() = default;
        input_type(type_matcher_kind k, types::logical_type ex, std::vector<types::logical_type> list = {})
            : kind(k)
            , exact_type(ex)
            , any_of_list(std::move(list)) {}

        [[nodiscard]] bool matches(const types::complex_logical_type& type) const;
    };

    using fixed_t = types::complex_logical_type;
    using type_resolver_fn = std::function<compute_result<fixed_t>(const std::pmr::vector<fixed_t>&)>;

    // Tagged kind for output_type so the spec is persistable. `fixed` and
    // `same_type_at_index` cover the common cases (statically known type / output equals
    // an input). `computed_fn` keeps the legacy std::function path for resolvers that
    // aren't representable in either of the above; persistence falls back to
    // `same_type_at_index{0}` for those (lossy, but rare in practice).
    enum class output_kind : std::uint8_t
    {
        fixed = 0,
        same_type_at_index = 1,
        computed_fn = 2,
    };

    struct output_type {
        output_kind kind{output_kind::computed_fn};
        fixed_t fixed_value{};
        std::size_t index{0};
        type_resolver_fn resolver{};

        static output_type fixed(fixed_t type);
        static output_type same_at(std::size_t input_index);
        static output_type computed(type_resolver_fn resolver);

        [[nodiscard]] compute_result<fixed_t> resolve(const std::pmr::vector<fixed_t>& input_types) const;
    };

    struct kernel_signature_t {
        kernel_signature_t() = delete;
        kernel_signature_t(std::pmr::vector<input_type> input_types, std::pmr::vector<struct output_type> output_types);

        std::pmr::vector<input_type> input_types;
        std::pmr::vector<output_type> output_types;

        [[nodiscard]] bool matches_inputs(const std::pmr::vector<types::complex_logical_type>& types) const;
    };

    input_type exact_type_matcher(types::logical_type type);
    input_type numeric_types_matcher();
    input_type integer_types_matcher();
    input_type floating_types_matcher();
    input_type any_type_matcher(std::pmr::vector<types::logical_type> type_list);
    input_type always_true_type_matcher();

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
