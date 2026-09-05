#include "kernel_signature.hpp"

#include "types/logical_value.hpp"

#include <algorithm>

namespace components::compute {

    namespace {

        bool is_null_argument(const types::complex_logical_type& type) noexcept {
            return type.type() == types::logical_type::NA;
        }

        // The one indexed resolver in the tree, as a plain function: same_type_resolver() hands
        // its address to type_resolver_fn together with the index, and output_type::resolve
        // calls it directly for kind_t::same_type_at_index. No closure, so nothing to erase.
        core::result_wrapper_t<fixed_t> resolve_same_type_at(size_t input_index,
                                                             std::pmr::memory_resource* resource,
                                                             const std::pmr::vector<fixed_t>& in) {
            if (in.size() <= input_index) {
                return core::error_t(core::error_code_t::incorrect_function_argument,
                                     std::pmr::string{"less inputs than expected", resource});
            }
            return in[input_index];
        }

        bool same_parameter(const parameter_type& lhs, const parameter_type& rhs) {
            if (lhs.is_variable() != rhs.is_variable()) {
                return false;
            }
            if (lhs.is_variable()) {
                return lhs.id() == rhs.id() && lhs.admissible() == rhs.admissible();
            }
            return lhs.type() == rhs.type();
        }

    } // namespace

    parameter_type parameter_type::exact(types::complex_logical_type type) {
        parameter_type result;
        result.type_ = std::move(type);
        return result;
    }

    parameter_type parameter_type::variable(variable_id id, std::pmr::vector<types::complex_logical_type> admissible) {
        // Through the constructor, NOT by assigning over a default-constructed member: see the
        // note at that constructor. Assignment would allocate in admissible_'s own resource.
        return parameter_type{id, std::move(admissible)};
    }

    parameter_type parameter_type::variable(variable_id id) {
        parameter_type result;
        result.is_variable_ = true;
        result.id_ = id;
        return result;
    }

    bool parameter_type::admits(const types::complex_logical_type& candidate) const {
        // An entry with no extension stands for its whole family
        auto admitted_by = [&candidate](const types::complex_logical_type& entry) {
            return entry.extension() == nullptr ? entry.type() == candidate.type() : entry == candidate;
        };
        if (!is_variable_) {
            return admitted_by(type_);
        }
        if (admissible_.empty()) {
            return true;
        }
        return std::any_of(admissible_.begin(), admissible_.end(), admitted_by);
    }

    core::result_wrapper_t<fixed_t> type_resolver_fn::operator()(std::pmr::memory_resource* resource,
                                                                 const std::pmr::vector<fixed_t>& input_types) const {
        if (empty()) {
            // Rule 6: a signature that reached execution with no way to name its output type is
            // a caller mistake, and the caller is holding an error channel. Refuse on it.
            return core::error_t(core::error_code_t::kernel_error,
                                 std::pmr::string{"output type resolver is empty", resource});
        }
        if (indexed_ != nullptr) {
            return indexed_(input_index_, resource, input_types);
        }
        return stateless_(resource, input_types);
    }

    output_type output_type::fixed(fixed_t type) {
        output_type out;
        out.kind_ = kind_t::fixed_value;
        out.fixed_value_ = std::move(type);
        return out;
    }

    output_type output_type::same_type_at(size_t input_index) {
        output_type out;
        out.kind_ = kind_t::same_type_at_index;
        out.input_index_ = input_index;
        return out;
    }

    output_type output_type::computed(type_resolver_fn resolver) {
        output_type out;
        out.kind_ = kind_t::custom;
        out.resolver_ = resolver;
        return out;
    }

    core::result_wrapper_t<fixed_t> output_type::resolve(std::pmr::memory_resource* resource,
                                                         const std::pmr::vector<fixed_t>& input_types) const {
        switch (kind_) {
            case kind_t::fixed_value:
                return fixed_value_;
            case kind_t::same_type_at_index:
                return resolve_same_type_at(input_index_, resource, input_types);
            case kind_t::custom:
                break;
        }
        return resolver_(resource, input_types);
    }

    kernel_signature_t::kernel_signature_t(function_type_t function_type,
                                           std::pmr::vector<parameter_type> input_types,
                                           std::pmr::vector<struct output_type> output_types)
        : function_type(function_type)
        , input_types(std::move(input_types))
        , output_types(std::move(output_types)) {}

    bool kernel_signature_t::matches_inputs(const std::pmr::vector<types::complex_logical_type>& types) const {
        if (types.size() != input_types.size()) {
            return false;
        }
        for (size_t i = 0; i < types.size(); ++i) {
            if (is_null_argument(types[i])) {
                continue;
            }
            const auto& parameter = input_types[i];
            if (!parameter.admits(types[i])) {
                return false;
            }
            if (!parameter.is_variable()) {
                continue;
            }
            // One variable, one type: every other position naming it must agree.
            for (size_t other = i + 1; other < types.size(); ++other) {
                if (input_types[other].is_variable() && input_types[other].id() == parameter.id() &&
                    !is_null_argument(types[other]) && types[other] != types[i]) {
                    return false;
                }
            }
        }
        return true;
    }

    type_resolver_fn same_type_resolver(size_t input_index) {
        // The index travels as DATA next to a plain function pointer, not as a lambda capture
        // that would then need erasing. Callers keep writing computed(same_type_resolver(0)).
        return type_resolver_fn{&resolve_same_type_at, input_index};
    }

    bool check_signature_conflicts(const kernel_signature_t& lhs, const kernel_signature_t& rhs) {
        if (lhs.input_types.size() != rhs.input_types.size()) {
            return true;
        }
        for (size_t i = 0; i < lhs.input_types.size(); i++) {
            if (!same_parameter(lhs.input_types[i], rhs.input_types[i])) {
                return true;
            }
        }
        return false;
    }

    bool check_signature_conflicts(const std::vector<kernel_signature_t>& lhs,
                                   const std::vector<kernel_signature_t>& rhs) {
        for (const auto& left : lhs) {
            for (const auto& right : rhs) {
                if (!check_signature_conflicts(left, right)) {
                    return false;
                }
            }
        }
        return true;
    }

} // namespace components::compute