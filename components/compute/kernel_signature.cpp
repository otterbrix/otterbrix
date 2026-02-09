#include "kernel_signature.hpp"

#include "types/logical_value.hpp"

#include <algorithm>

namespace components::compute {
    input_type::input_type(type_matcher_fn m)
        : matcher_(std::move(m)) {}

    bool input_type::matches(const types::complex_logical_type& type) const { return matcher_(type); }

    output_type output_type::fixed(fixed_t type) {
        output_type out;
        out.value_ = std::move(type);
        return out;
    }

    output_type output_type::computed(type_resolver_fn resolver) {
        output_type out;
        out.value_ = std::move(resolver);
        return out;
    }

    compute_result<fixed_t> output_type::resolve(const std::pmr::vector<fixed_t>& input_types) const {
        if (std::holds_alternative<fixed_t>(value_)) {
            return std::get<fixed_t>(value_);
        }

        const auto& resolver = std::get<type_resolver_fn>(value_);
        return resolver(input_types);
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

    type_matcher_fn exact_type_matcher(types::logical_type type) {
        return [type](const types::complex_logical_type& t) { return t.type() == type; };
    }

    type_matcher_fn numeric_types_matcher() {
        return [](const types::complex_logical_type& t) { return types::is_numeric(t.type()); };
    }

    type_matcher_fn integer_types_matcher() {
        return [](const types::complex_logical_type& t) {
            using lt = types::logical_type;
            auto id = t.type();
            return id == lt::TINYINT || id == lt::SMALLINT || id == lt::INTEGER || id == lt::BIGINT ||
                   id == lt::HUGEINT || id == lt::UTINYINT || id == lt::USMALLINT || id == lt::UINTEGER ||
                   id == lt::UBIGINT || id == lt::UHUGEINT;
        };
    }

    type_matcher_fn floating_types_matcher() {
        return [](const types::complex_logical_type& t) {
            using lt = types::logical_type;
            auto id = t.type();
            return id == lt::FLOAT || id == lt::DOUBLE;
        };
    }

    type_matcher_fn any_type_matcher(std::pmr::vector<types::logical_type> type_list) {
        return [list = std::move(type_list)](const types::complex_logical_type& t) {
            return std::find(list.begin(), list.end(), t.type()) != list.end();
        };
    }

    type_matcher_fn always_true_type_matcher() {
        return [](const types::complex_logical_type&) { return true; };
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
