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

    kernel_signature_t::kernel_signature_t(std::pmr::vector<input_type> input_types, struct output_type output_type)
        : input_types(std::move(input_types))
        , output_type(std::move(output_type)) {}

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

    type_resolver_fn same_type_resolver() {
        return [](const std::pmr::vector<fixed_t>& in) -> compute_result<fixed_t> {
            if (in.empty())
                return compute_status::invalid("No inputs");
            return in[0];
        };
    }

} // namespace components::compute
