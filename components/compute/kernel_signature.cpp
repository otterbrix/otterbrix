#include "kernel_signature.hpp"

#include "types/logical_value.hpp"

#include <algorithm>

namespace components::compute {
    bool input_type::matches(const types::complex_logical_type& type) const {
        using lt = types::logical_type;
        const auto id = type.type();
        switch (kind) {
            case type_matcher_kind::exact:
                return id == exact_type;
            case type_matcher_kind::numeric:
                return types::is_numeric(id);
            case type_matcher_kind::integer:
                return id == lt::TINYINT || id == lt::SMALLINT || id == lt::INTEGER || id == lt::BIGINT ||
                       id == lt::HUGEINT || id == lt::UTINYINT || id == lt::USMALLINT || id == lt::UINTEGER ||
                       id == lt::UBIGINT || id == lt::UHUGEINT;
            case type_matcher_kind::floating:
                return id == lt::FLOAT || id == lt::DOUBLE;
            case type_matcher_kind::any_of:
                return std::find(any_of_list.begin(), any_of_list.end(), id) != any_of_list.end();
            case type_matcher_kind::always_true:
                return true;
        }
        return false;
    }

    output_type output_type::fixed(fixed_t type) {
        output_type out;
        out.kind = output_kind::fixed;
        out.fixed_value = std::move(type);
        return out;
    }

    output_type output_type::same_at(std::size_t input_index) {
        output_type out;
        out.kind = output_kind::same_type_at_index;
        out.index = input_index;
        return out;
    }

    output_type output_type::computed(type_resolver_fn resolver) {
        output_type out;
        out.kind = output_kind::computed_fn;
        out.resolver = std::move(resolver);
        return out;
    }

    compute_result<fixed_t> output_type::resolve(const std::pmr::vector<fixed_t>& input_types) const {
        switch (kind) {
            case output_kind::fixed:
                return fixed_value;
            case output_kind::same_type_at_index:
                if (input_types.size() <= index)
                    return compute_status::invalid("No inputs");
                return input_types[index];
            case output_kind::computed_fn:
                if (resolver) return resolver(input_types);
                return compute_status::invalid("output_type has no resolver");
        }
        return compute_status::invalid("output_type unhandled kind");
    }

    kernel_signature_t::kernel_signature_t(std::pmr::vector<input_type> input_types,
                                           std::pmr::vector<struct output_type> output_types)
        : input_types(std::move(input_types))
        , output_types(std::move(output_types)) {}

    bool kernel_signature_t::matches_inputs(const std::pmr::vector<types::complex_logical_type>& types) const {
        if (types.size() != input_types.size()) {
            return false;
        }
        for (size_t i = 0; i < types.size(); ++i) {
            if (!input_types[i].matches(types[i])) {
                return false;
            }
        }
        return true;
    }

    input_type exact_type_matcher(types::logical_type type) {
        return input_type{type_matcher_kind::exact, type};
    }

    input_type numeric_types_matcher() {
        return input_type{type_matcher_kind::numeric, types::logical_type::INVALID};
    }

    input_type integer_types_matcher() {
        return input_type{type_matcher_kind::integer, types::logical_type::INVALID};
    }

    input_type floating_types_matcher() {
        return input_type{type_matcher_kind::floating, types::logical_type::INVALID};
    }

    input_type any_type_matcher(std::pmr::vector<types::logical_type> type_list) {
        std::vector<types::logical_type> list(type_list.begin(), type_list.end());
        return input_type{type_matcher_kind::any_of, types::logical_type::INVALID, std::move(list)};
    }

    input_type always_true_type_matcher() {
        return input_type{type_matcher_kind::always_true, types::logical_type::INVALID};
    }

    type_resolver_fn same_type_resolver(size_t input_index) {
        return [input_index](const std::pmr::vector<fixed_t>& in) -> compute_result<fixed_t> {
            if (in.size() <= input_index)
                return compute_status::invalid("No inputs");
            return in[input_index];
        };
    }

    /*
    * Deducing conflicts and ambiguity
    * In case we have a conflict, we move to the next check, which can resolve it
    * Only explicit type matters, ignoring any possible implicit casts
    * 1) if number of arguments is different - no conflicts
    * 2) loop over corresponding arguments
    *   2.1) if there is conflict over types, move to the next
    *   2.2) if we have any pair of arguments that does not have any overlaps, than we can call signatures distinct
    *   2.3) if all pairs have conflicts, than signatures have a conflict
      3) include outputs?
    */

    bool check_signature_conflicts(
        const std::pmr::vector<input_type>& lhs,
        const std::pmr::vector<input_type>& rhs,
        const std::pmr::unordered_map<std::string, types::complex_logical_type>& registered_types) {
        if (lhs.size() != rhs.size()) {
            return true;
        }

        bool result = true;

        for (size_t i = 0; i < lhs.size(); i++) {
            result = true;
            // check default types
            for (size_t j = 0; j < types::DEFAULT_LOGICAL_TYPES.size(); j++) {
                if (lhs[i].matches(types::DEFAULT_LOGICAL_TYPES[j]) &&
                    rhs[i].matches(types::DEFAULT_LOGICAL_TYPES[j])) {
                    result = false;
                    break;
                }
            }

            // If there are any overlaps, then we have a conflict, and we have to check next set of arguments
            if (!result) {
                continue;
            }

            // check registered udt`s
            for (const auto& pair : registered_types) {
                if (lhs[i].matches(pair.second) && rhs[i].matches(pair.second)) {
                    result = false;
                    break;
                }
            }
            if (result) {
                break;
            }
        }

        return result;
    }

    bool check_signature_conflicts(
        const kernel_signature_t& lhs,
        const kernel_signature_t& rhs,
        const std::pmr::unordered_map<std::string, types::complex_logical_type>& registered_types) {
        return check_signature_conflicts(lhs.input_types, rhs.input_types, registered_types);
    }

    bool check_signature_conflicts(
        const std::vector<kernel_signature_t>& lhs,
        const std::vector<kernel_signature_t>& rhs,
        const std::pmr::unordered_map<std::string, types::complex_logical_type>& registered_types) {
        for (size_t i = 0; i < lhs.size(); i++) {
            for (size_t j = 0; j < lhs.size(); j++) {
                if (!check_signature_conflicts(lhs[i], rhs[i], registered_types)) {
                    return false;
                }
            }
        }
        return true;
    }

} // namespace components::compute
