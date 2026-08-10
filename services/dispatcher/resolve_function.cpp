#include "resolve_function.hpp"

#include <components/catalog/system_table_schemas.hpp>

#include <algorithm>
#include <optional>
#include <utility>

namespace services::dispatcher {

    using components::graph_execution_context;
    using components::casts::allowed_in;
    using components::casts::cast_cost;
    using components::casts::cast_registry_t;
    using components::casts::cast_type;
    using components::compute::function_registry_t;
    using components::compute::kernel_signature_t;
    using components::compute::parameter_type;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    namespace {

        bool is_null_argument(const complex_logical_type& type) noexcept { return type.type() == logical_type::NA; }

        struct total_cost_t {
            uint64_t precision_loss{0};
            uint64_t footprint{0};

            void add(cast_cost cost) {
                precision_loss += cost.precision_loss;
                footprint += cost.footprint;
            }

            bool operator<(const total_cost_t& other) const {
                return precision_loss != other.precision_loss ? precision_loss < other.precision_loss
                                                              : footprint < other.footprint;
            }
            bool operator==(const total_cost_t& other) const {
                return precision_loss == other.precision_loss && footprint == other.footprint;
            }
        };

        struct candidate_t {
            explicit candidate_t(std::pmr::memory_resource* resource)
                : arguments(resource) {}

            std::pmr::vector<resolved_argument_t> arguments;
            complex_logical_type result;
            total_cost_t cost;
        };

        std::optional<complex_logical_type> unify_variable(const cast_registry_t& cast_registry,
                                                           const kernel_signature_t& signature,
                                                           const std::pmr::vector<complex_logical_type>& arguments,
                                                           parameter_type::variable_id id) {
            std::optional<complex_logical_type> unified;
            for (size_t i = 0; i < arguments.size(); ++i) {
                const auto& parameter = signature.input_types[i];
                if (!parameter.is_variable() || parameter.id() != id || is_null_argument(arguments[i])) {
                    continue;
                }
                if (!unified.has_value() || *unified == arguments[i]) {
                    unified = arguments[i];
                    continue;
                }
                auto common = cast_registry.find_best_common_type(*unified, arguments[i]);
                if (!common.has_value()) {
                    return std::nullopt;
                }
                unified = common->type;
            }
            if (!unified.has_value()) {
                return complex_logical_type{logical_type::NA};
            }
            return unified;
        }

        bool is_family_entry(const complex_logical_type& type) noexcept {
            if (type.extension() != nullptr) {
                return false;
            }
            switch (type.type()) {
                case logical_type::DECIMAL:
                case logical_type::ENUM:
                case logical_type::STRUCT:
                case logical_type::LIST:
                case logical_type::MAP:
                case logical_type::ARRAY:
                    return true;
                default:
                    return false;
            }
        }

        std::optional<complex_logical_type> materialize_family(const graph_execution_context& context,
                                                               const complex_logical_type& family,
                                                               const complex_logical_type& argument) {
            if (family.type() == argument.type()) {
                return argument;
            }
            if (family.type() != logical_type::DECIMAL) {
                return std::nullopt;
            }
            return complex_logical_type::create_decimal(context.decimal_width, context.decimal_scale);
        }

        std::optional<complex_logical_type> constrain_to_domain(const cast_registry_t& cast_registry,
                                                                const graph_execution_context& context,
                                                                const parameter_type& parameter,
                                                                const complex_logical_type& unified) {
            if (parameter.admissible().empty() || is_null_argument(unified) || parameter.admits(unified)) {
                return unified;
            }
            std::optional<complex_logical_type> best;
            std::optional<cast_cost> best_cost;
            for (const auto& entry : parameter.admissible()) {
                auto materialized = is_family_entry(entry) ? materialize_family(context, entry, unified) : entry;
                if (!materialized.has_value()) {
                    continue;
                }
                const complex_logical_type& candidate = *materialized;
                auto info = cast_registry.lookup(unified, candidate);
                if (!info.has_value() || !allowed_in(info->level, cast_type::implicit)) {
                    continue;
                }
                if (!best_cost.has_value() || info->cost < *best_cost) {
                    best = candidate;
                    best_cost = info->cost;
                }
            }
            return best;
        }

        std::optional<candidate_t> try_signature(std::pmr::memory_resource* resource,
                                                 const cast_registry_t& cast_registry,
                                                 const graph_execution_context& context,
                                                 const kernel_signature_t& signature,
                                                 const std::pmr::vector<complex_logical_type>& arguments) {
            if (signature.input_types.size() != arguments.size() || signature.output_types.empty()) {
                return std::nullopt;
            }

            candidate_t candidate{resource};
            candidate.arguments.reserve(arguments.size());
            std::pmr::vector<complex_logical_type> targets(resource);
            targets.reserve(arguments.size());
            std::pmr::vector<std::pair<parameter_type::variable_id, complex_logical_type>> variables(resource);

            for (size_t i = 0; i < arguments.size(); ++i) {
                const auto& parameter = signature.input_types[i];
                complex_logical_type target;
                if (!parameter.is_variable()) {
                    if (is_family_entry(parameter.type())) {
                        auto materialized = materialize_family(context, parameter.type(), arguments[i]);
                        if (!materialized.has_value()) {
                            return std::nullopt;
                        }
                        target = *materialized;
                    } else {
                        target = parameter.type();
                    }
                } else {
                    auto known = std::find_if(variables.begin(), variables.end(), [&](const auto& entry) {
                        return entry.first == parameter.id();
                    });
                    if (known != variables.end()) {
                        target = known->second;
                    } else {
                        auto unified = unify_variable(cast_registry, signature, arguments, parameter.id());
                        if (!unified.has_value()) {
                            return std::nullopt;
                        }
                        auto constrained = constrain_to_domain(cast_registry, context, parameter, *unified);
                        if (!constrained.has_value()) {
                            return std::nullopt;
                        }
                        target = *constrained;
                        variables.emplace_back(parameter.id(), target);
                    }
                }

                resolved_argument_t argument;
                argument.target = target;
                if (!is_null_argument(arguments[i]) && arguments[i] != target) {
                    auto info = cast_registry.lookup(arguments[i], target);
                    if (!info.has_value() || !allowed_in(info->level, cast_type::implicit)) {
                        return std::nullopt;
                    }
                    auto cast = cast_registry.resolve(arguments[i], target, cast_type::implicit);
                    if (!cast.has_value()) {
                        return std::nullopt;
                    }
                    argument.cast = std::move(cast.value());
                    candidate.cost.add(info->cost);
                }
                targets.emplace_back(target);
                candidate.arguments.emplace_back(std::move(argument));
            }

            // The function decides its own return type
            auto result = signature.output_types.front().resolve(resource, targets);
            if (result.has_error()) {
                return std::nullopt;
            }
            candidate.result = std::move(result.value());
            return candidate;
        }

        std::string describe_arguments(const std::pmr::vector<complex_logical_type>& arguments) {
            std::string out;
            for (size_t i = 0; i < arguments.size(); ++i) {
                if (i > 0) {
                    out += ", ";
                }
                out += std::string(components::catalog::logical_type_to_pg_name(arguments[i].type()));
            }
            return out;
        }

    } // namespace

    core::result_wrapper_t<resolved_function_t>
    resolve_function(std::pmr::memory_resource* resource,
                     const cast_registry_t& cast_registry,
                     const graph_execution_context& context,
                     const function_registry_t& function_registry,
                     std::string_view name,
                     const std::pmr::vector<complex_logical_type>& arguments,
                     components::compute::function_types_mask allowed_function_types) {
        bool name_exists = false;
        bool rejected_by_context = false;
        bool ambiguous = false;
        std::optional<resolved_function_t> best;
        std::optional<total_cost_t> best_cost;

        for (const auto& [registered_name, uid] : function_registry.get_functions()) {
            if (registered_name != name) {
                continue;
            }
            name_exists = true;
            auto* function = function_registry.get_function(uid);
            if (!function) {
                continue;
            }
            for (const auto& signature : function->get_signatures()) {
                if (!components::compute::check_mask(allowed_function_types, signature.function_type)) {
                    rejected_by_context = true;
                    continue;
                }
                auto candidate = try_signature(resource, cast_registry, context, signature, arguments);
                if (!candidate.has_value()) {
                    continue;
                }
                if (best_cost.has_value() && candidate->cost == *best_cost) {
                    ambiguous = true;
                    continue;
                }
                if (best_cost.has_value() && !(candidate->cost < *best_cost)) {
                    continue;
                }
                resolved_function_t resolved{resource};
                resolved.uid = uid;
                resolved.arguments = std::move(candidate->arguments);
                resolved.result = std::move(candidate->result);
                resolved.function_type = signature.function_type;
                resolved.mergeable = function->is_mergeable();
                best = std::move(resolved);
                best_cost = candidate->cost;
                ambiguous = false;
            }
        }

        if (!name_exists) {
            return core::error_t(core::error_code_t::unrecognized_function,
                                 std::pmr::string{"unrecognized function '" + std::string(name) + "'", resource});
        }
        if (!best.has_value() && rejected_by_context) {
            return core::error_t(
                core::error_code_t::incorrect_function_argument,
                std::pmr::string{"function '" + std::string(name) + "' is not allowed in this part of the statement",
                                 resource});
        }
        if (!best.has_value()) {
            return core::error_t(core::error_code_t::incorrect_function_argument,
                                 std::pmr::string{"function '" + std::string(name) + "' does not accept (" +
                                                      describe_arguments(arguments) + ")",
                                                  resource});
        }
        if (ambiguous) {
            return core::error_t(core::error_code_t::incorrect_function_argument,
                                 std::pmr::string{"call to function '" + std::string(name) + "' with (" +
                                                      describe_arguments(arguments) +
                                                      ") is ambiguous between equally costly overloads",
                                                  resource});
        }
        return std::move(best.value());
    }

} // namespace services::dispatcher