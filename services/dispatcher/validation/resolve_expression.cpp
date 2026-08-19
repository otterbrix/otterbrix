#include "resolve_expression.hpp"

#include <services/dispatcher/resolve_arithmetic.hpp>
#include <services/dispatcher/resolve_function.hpp>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>

#include <algorithm>
#include <source_location>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace services::dispatcher::validation {

    using namespace components::types;
    using namespace components::expressions;
    using components::logical_plan::storage_parameters;

    namespace {

        const complex_logical_type invalid_type{logical_type::INVALID};
        const complex_logical_type boolean_type{logical_type::BOOLEAN};

        bool is_stamped(const complex_logical_type& type) noexcept { return type.type() != logical_type::INVALID; }

        template<typename... Args>
        std::pmr::string message(std::pmr::memory_resource* resource, const Args&... args) {
            std::pmr::string composed{resource};
            (composed.append(args), ...);
            return composed;
        }

        void splice_cast(std::pmr::memory_resource* resource,
                         param_storage& slot,
                         const complex_logical_type& target,
                         const components::casts::cast_t& cast) {
            if (!cast) {
                return;
            }
            slot = param_storage{
                expression_ptr{make_cast_expression(resource, slot, target, cast, components::casts::cast_kind::cast)}};
        }

        [[nodiscard]] complex_logical_type unify_operands(std::pmr::memory_resource* resource,
                                                          const components::casts::cast_registry_t& cast_registry,
                                                          std::span<param_storage* const> operands,
                                                          std::span<complex_logical_type> types,
                                                          core::error_t* error,
                                                          std::string_view what) {
            auto common = cast_registry.find_best_common_type(types);
            if (!common.has_value()) {
                *error = core::error_t(core::error_code_t::invalid_parameter,
                                       message(resource, "no type is common to every ", what));
                return invalid_type;
            }
            for (size_t index = 0; index < operands.size(); index++) {
                splice_cast(resource, *operands[index], common->type, common->casts[index]);
            }
            return common->type;
        }

        class resolver_t {
        public:
            resolver_t(const expression_context_t& context, bool* saw_reduction)
                : context_(context)
                , saw_reduction_(saw_reduction) {}

            core::error_t take_error() const { return error_; }

            // Resolves the 'slot' (expr/parameter/key) and returns its type
            // May add a cast
            complex_logical_type slot(param_storage& operand, bool inside_aggregate) {
                if (error_.contains_error()) {
                    return invalid_type;
                }
                if (is_parameter(operand)) {
                    auto bound = context_.parameters.parameters.find(as_parameter(operand));
                    if (bound == context_.parameters.parameters.end()) {
                        return fail(core::error_code_t::invalid_parameter, "unbound parameter in expression");
                    }
                    last_cardinality_ = cardinality_t::constant;
                    return bound->second.type();
                }
                if (std::holds_alternative<components::expressions::key_t>(operand)) {
                    return column(std::get<components::expressions::key_t>(operand), inside_aggregate);
                }
                auto& nested = std::get<expression_ptr>(operand);
                if (!nested) {
                    last_cardinality_ = cardinality_t::constant;
                    return invalid_type;
                }
                if (auto error = resolve(nested, inside_aggregate); error.contains_error()) {
                    return invalid_type;
                }
                return nested->result_type();
            }

            core::error_t resolve(expression_ptr& expression, bool inside_aggregate) {
                if (error_.contains_error()) {
                    return error_;
                }
                if (!expression) {
                    return core::error_t::no_error();
                }
                switch (expression->group()) {
                    case expression_group::compare:
                        resolve_compare(static_cast<compare_expression_t*>(expression.get()), inside_aggregate);
                        break;
                    case expression_group::scalar:
                        resolve_scalar(static_cast<scalar_expression_t*>(expression.get()), inside_aggregate);
                        break;
                    case expression_group::function:
                        resolve_function_call(expression, inside_aggregate);
                        break;
                    case expression_group::cast:
                        resolve_cast(static_cast<cast_expression_t*>(expression.get()), inside_aggregate);
                        break;
                    case expression_group::aggregate:
                        resolve_aggregate(static_cast<aggregate_expression_t*>(expression.get()), inside_aggregate);
                        break;
                    case expression_group::sort:
                        last_cardinality_ = cardinality_t::row;
                        break;
                    case expression_group::invalid:
                        fail(core::error_code_t::invalid_parameter, "invalid expression");
                        break;
                }
                if (error_.contains_error()) {
                    return error_;
                }
                expression->set_cardinality(last_cardinality_);
                return core::error_t::no_error();
            }

        private:
            complex_logical_type
            fail(core::error_code_t code,
                 std::pmr::string text,
                 [[maybe_unused]] std::source_location location = std::source_location::current()) {
#if not defined(NDEBUG)
                error_ = core::error_t(code, std::move(text), location);
#else
                error_ = core::error_t(code, std::move(text));
#endif
                return invalid_type;
            }

            complex_logical_type
            fail(core::error_code_t code,
                 const char* text,
                 [[maybe_unused]] std::source_location location = std::source_location::current()) {
#if not defined(NDEBUG)
                error_ = core::error_t(code, std::pmr::string{text, context_.resource}, location);
#else
                error_ = core::error_t(code, std::pmr::string{text, context_.resource});
#endif
                return invalid_type;
            }

            complex_logical_type column(components::expressions::key_t& key, bool inside_aggregate) {
                auto resolved = validate_key(context_.resource, key, &context_.schema, context_.schema_right);
                if (resolved.has_error()) {
                    error_ = resolved.error();
                    return invalid_type;
                }
                last_cardinality_ = cardinality_of_column(key, inside_aggregate);
                return resolved.value().front().type;
            }

            // Each result column should have the same number of rows + some additional rules
            cardinality_t cardinality_of_column(const components::expressions::key_t& key,
                                                bool inside_aggregate) const {
                if (inside_aggregate) {
                    return cardinality_t::group;
                }
                if (context_.group_key_paths == nullptr) {
                    return cardinality_t::row;
                }
                const auto& path = key.path();
                if (path.empty()) {
                    return cardinality_t::row;
                }
                for (const auto& candidate : *context_.group_key_paths) {
                    if (candidate.size() == path.size() &&
                        std::equal(candidate.begin(), candidate.end(), path.begin())) {
                        return cardinality_t::group;
                    }
                }
                return cardinality_t::row;
            }

            cardinality_t combine(cardinality_t left, cardinality_t right) const {
                if (left == cardinality_t::constant || left == cardinality_t::unknown) {
                    return right;
                }
                if (right == cardinality_t::constant || right == cardinality_t::unknown) {
                    return left;
                }
                return left == right ? left : cardinality_t::row;
            }

            // SQL LIKE / ILIKE / regexp all match through regexp_like(subject, pattern, flags).
            // TODO: should resolve like any other call -- there is nothing special about regex.
            void resolve_regex_uid(compare_expression_t* comparison,
                                   const complex_logical_type& subject,
                                   const complex_logical_type& pattern) {
                std::pmr::vector<complex_logical_type> arguments(context_.resource);
                arguments.push_back(subject);
                arguments.push_back(pattern);
                arguments.emplace_back(logical_type::STRING_LITERAL);
                auto resolved = resolve_function(context_.resource,
                                                 context_.cast_registry,
                                                 context_.execution_context,
                                                 context_.function_registry,
                                                 "regexp_like",
                                                 arguments,
                                                 context_.allowed_functions);
                if (resolved.has_error()) {
                    error_ = resolved.error();
                    return;
                }
                comparison->add_function_uid(resolved.value().uid);
            }

            void resolve_compare(compare_expression_t* comparison, bool inside_aggregate) {
                switch (comparison->type()) {
                    case compare_type::invalid:
                        fail(core::error_code_t::invalid_parameter, "invalid comparison");
                        return;
                    case compare_type::union_and:
                    case compare_type::union_or:
                    case compare_type::union_not:
                    case compare_type::eq:
                    case compare_type::ne:
                    case compare_type::gt:
                    case compare_type::lt:
                    case compare_type::gte:
                    case compare_type::lte:
                    case compare_type::all_true:
                    case compare_type::all_false:
                        break;
                    case compare_type::regex: {
                        const auto subject = slot(comparison->left(), inside_aggregate);
                        if (error_.contains_error()) {
                            return;
                        }
                        auto combined = last_cardinality_;
                        const auto pattern = slot(comparison->right(), inside_aggregate);
                        if (error_.contains_error()) {
                            return;
                        }
                        combined = combine(combined, last_cardinality_);
                        resolve_regex_uid(comparison, subject, pattern);
                        if (error_.contains_error()) {
                            return;
                        }
                        comparison->set_result_type(boolean_type);
                        last_cardinality_ = combined;
                        return;
                    }
                    case compare_type::any:
                    case compare_type::all:
                    case compare_type::is_null:
                    case compare_type::is_not_null: {
                        // Unary in shape: the right slot is a sentinel. ANY/ALL carry the element
                        // match in inner_op(); a LIKE ANY matches each element with the same call
                        // the scalar form uses, over strings whatever set they came from.
                        const auto subject = slot(comparison->left(), inside_aggregate);
                        if (error_.contains_error()) {
                            return;
                        }
                        if (comparison->inner_op() == compare_type::regex) {
                            resolve_regex_uid(comparison, subject, complex_logical_type{logical_type::STRING_LITERAL});
                            if (error_.contains_error()) {
                                return;
                            }
                        }
                        comparison->set_result_type(boolean_type);
                        return;
                    }
                }

                cardinality_t combined = cardinality_t::constant;
                if (comparison->is_union()) {
                    // AND / OR / NOT carry every operand as a child. Each has to answer BOOLEAN,
                    // and anything convertible to BOOLEAN is converted rather than rejected.
                    for (auto& child : comparison->children()) {
                        param_storage nested{child};
                        const auto operand_type = slot(nested, inside_aggregate);
                        if (error_.contains_error()) {
                            return;
                        }
                        if (operand_type.type() != logical_type::BOOLEAN) {
                            auto cast = context_.cast_registry.resolve(operand_type,
                                                                       boolean_type,
                                                                       components::casts::cast_type::implicit);
                            if (!cast.has_value()) {
                                fail(core::error_code_t::schema_error,
                                     message(context_.resource,
                                             "operand of ",
                                             to_string(comparison->type()),
                                             " must be BOOLEAN"));
                                return;
                            }
                            splice_cast(context_.resource, nested, boolean_type, *cast);
                        }
                        combined = combine(combined, last_cardinality_);
                        child = std::get<expression_ptr>(nested);
                    }
                    comparison->set_result_type(boolean_type);
                    last_cardinality_ = combined;
                    return;
                }

                const auto code = to_operator_code(comparison->type());
                const bool binary =
                    code != components::operators::operator_code::invalid &&
                    components::operators::arity_of(code) == components::operators::operator_arity::binary;

                const auto left_type = slot(comparison->left(), inside_aggregate);
                if (error_.contains_error()) {
                    return;
                }
                combined = combine(combined, last_cardinality_);
                complex_logical_type right_type = invalid_type;
                if (binary) {
                    right_type = slot(comparison->right(), inside_aggregate);
                    if (error_.contains_error()) {
                        return;
                    }
                    combined = combine(combined, last_cardinality_);
                }

                if (binary && left_type != right_type) {
                    std::pmr::vector<param_storage*> sides(context_.resource);
                    sides.push_back(&comparison->left());
                    sides.push_back(&comparison->right());
                    std::pmr::vector<complex_logical_type> side_types(context_.resource);
                    side_types.push_back(left_type);
                    side_types.push_back(right_type);
                    if (!is_stamped(
                            unify_operands(context_.resource,
                                           context_.cast_registry,
                                           std::span<param_storage* const>{sides},
                                           std::span<complex_logical_type>{side_types},
                                           &error_,
                                           message(context_.resource, "side of ", to_string(comparison->type()))))) {
                        return;
                    }
                }
                comparison->set_result_type(boolean_type);
                last_cardinality_ = combined;
            }

            void resolve_cast(cast_expression_t* conversion, bool inside_aggregate) {
                const auto source = slot(conversion->child(), inside_aggregate);
                if (error_.contains_error()) {
                    return;
                }
                if (conversion->cast()) {
                    return;
                }
                auto resolved = context_.cast_registry.resolve(source,
                                                               conversion->result_type(),
                                                               components::casts::cast_type::explicit_only);
                if (!resolved.has_value()) {
                    fail(core::error_code_t::invalid_parameter,
                         message(context_.resource,
                                 "no cast from ",
                                 describe_type(source),
                                 " to ",
                                 describe_type(conversion->result_type())));
                    return;
                }
                conversion->set_cast(std::move(*resolved));
            }

            void resolve_aggregate(aggregate_expression_t* aggregate, bool inside_aggregate) {
                if (inside_aggregate) {
                    fail(core::error_code_t::sql_parse_error, "aggregate function calls cannot be nested");
                    return;
                }
                std::pmr::vector<complex_logical_type> argument_types(context_.resource);
                argument_types.reserve(aggregate->params().size());
                for (auto& argument : aggregate->params()) {
                    argument_types.push_back(slot(argument, true));
                    if (error_.contains_error()) {
                        return;
                    }
                }
                last_cardinality_ = cardinality_t::group;
                // Resolved unconditionally: a stamped result_type does NOT mean the marker is
                // finished. make_aggregate_over stamps the result type alone, so short-circuiting
                // on it leaves function_uid and mergeable unset -- and an aggregate that does not
                // report itself mergeable is one the eager-aggregation rule refuses to push down.
                auto resolved =
                    resolve_function(context_.resource,
                                     context_.cast_registry,
                                     context_.execution_context,
                                     context_.function_registry,
                                     aggregate->function_name(),
                                     argument_types,
                                     components::compute::create_mask(components::compute::function_type_t::aggregate));
                if (resolved.has_error()) {
                    error_ = resolved.error();
                    return;
                }
                for (size_t index = 0; index < aggregate->params().size() && index < resolved.value().arguments.size();
                     index++) {
                    splice_cast(context_.resource,
                                aggregate->params()[index],
                                resolved.value().arguments[index].target,
                                resolved.value().arguments[index].cast);
                }
                aggregate->add_function_uid(resolved.value().uid);
                aggregate->set_mergeable(resolved.value().mergeable);
                aggregate->set_result_type(resolved.value().result);
            }

            void resolve_function_call(expression_ptr& expression, bool inside_aggregate) {
                auto* call = static_cast<function_expression_t*>(expression.get());
                std::pmr::vector<complex_logical_type> argument_types(context_.resource);
                argument_types.reserve(call->args().size());
                cardinality_t combined = cardinality_t::constant;
                for (auto& argument : call->args()) {
                    argument_types.push_back(slot(argument, inside_aggregate));
                    if (error_.contains_error()) {
                        return;
                    }
                    combined = combine(combined, last_cardinality_);
                }
                if (error_.contains_error()) {
                    return;
                }

                auto resolved = resolve_function(context_.resource,
                                                 context_.cast_registry,
                                                 context_.execution_context,
                                                 context_.function_registry,
                                                 call->name(),
                                                 argument_types,
                                                 context_.allowed_functions);
                if (resolved.has_error()) {
                    error_ = resolved.error();
                    return;
                }
                const bool reduces = resolved.value().function_type == components::compute::function_type_t::aggregate;
                if (call->args().empty() && !call->has_star_argument() && reduces) {
                    fail(core::error_code_t::invalid_parameter,
                         message(context_.resource,
                                 "(*) must be used to call the parameterless aggregate '",
                                 call->name(),
                                 "'"));
                    return;
                }
                if (reduces && inside_aggregate) {
                    fail(core::error_code_t::sql_parse_error, "aggregate function calls cannot be nested");
                    return;
                }

                // The signature was chosen on the strength of these conversions, so the tree has to
                // carry them: without this the runtime re-dispatches on the ORIGINAL argument types.
                for (size_t index = 0; index < call->args().size() && index < resolved.value().arguments.size();
                     index++) {
                    splice_cast(context_.resource,
                                call->args()[index],
                                resolved.value().arguments[index].target,
                                resolved.value().arguments[index].cast);
                }

                call->add_function_uid(resolved.value().uid);
                call->set_result_type(resolved.value().result);
                if (!reduces) {
                    last_cardinality_ = combined;
                    return;
                }
                if (saw_reduction_ != nullptr) {
                    *saw_reduction_ = true;
                }
                auto marker = make_aggregate_over(expression, call->key());
                marker->set_result_type(call->result_type());
                marker->set_cardinality(cardinality_t::group);
                marker->set_mergeable(resolved.value().mergeable);
                expression = marker;
                last_cardinality_ = cardinality_t::group;
            }

            void resolve_scalar(scalar_expression_t* scalar, bool inside_aggregate) {
                switch (scalar->type()) {
                    case scalar_type::get_field:
                    case scalar_type::group_field: {
                        // A plain column reference carries the key as its only param -- or, for a
                        // group field promoted out of a GROUP BY, on the expression itself.
                        if (scalar->params().empty()) {
                            auto& key = scalar->key();
                            scalar->set_result_type(column(key, inside_aggregate));
                            return;
                        }
                        scalar->set_result_type(slot(scalar->params()[0], inside_aggregate));
                        return;
                    }
                    case scalar_type::constant: {
                        if (scalar->params().empty()) {
                            fail(core::error_code_t::invalid_parameter, "constant expression with no value");
                            return;
                        }
                        if (is_stamped(scalar->result_type())) {
                            last_cardinality_ = cardinality_t::constant;
                            return;
                        }
                        scalar->set_result_type(slot(scalar->params()[0], inside_aggregate));
                        last_cardinality_ = cardinality_t::constant;
                        return;
                    }
                    case scalar_type::case_expr:
                    case scalar_type::coalesce:
                        resolve_blend(scalar, inside_aggregate);
                        return;
                    case scalar_type::unary_minus:
                    case scalar_type::bit_not:
                        resolve_unary_operator(scalar, inside_aggregate);
                        return;
                    case scalar_type::add:
                    case scalar_type::subtract:
                    case scalar_type::multiply:
                    case scalar_type::divide:
                    case scalar_type::mod:
                    case scalar_type::bit_and:
                    case scalar_type::bit_or:
                    case scalar_type::bit_xor:
                    case scalar_type::shift_left:
                    case scalar_type::shift_right:
                        resolve_binary_operator(scalar, inside_aggregate);
                        return;
                    case scalar_type::case_when:
                    case scalar_type::star_expand:
                    case scalar_type::jsonb_expand:
                    case scalar_type::jsonb_delete:
                        // Structural markers: the node-level pass expands them against the schema
                        // before any expression reaches here.
                        fail(
                            core::error_code_t::invalid_parameter,
                            message(context_.resource, to_string(scalar->type()), " is not valid in a value position"));
                        return;
                    case scalar_type::round:
                    case scalar_type::ceil:
                    case scalar_type::floor:
                    case scalar_type::abs:
                    case scalar_type::pow:
                    case scalar_type::sqrt:
                        // Legacy python-integration spellings. Every one of them is a function, so
                        // it resolves through the function arm; none is a scalar operator.
                        fail(core::error_code_t::invalid_parameter,
                             message(context_.resource,
                                     to_string(scalar->type()),
                                     " is a function, not a scalar operator"));
                        return;
                    case scalar_type::invalid:
                        fail(core::error_code_t::invalid_parameter, "invalid scalar expression");
                        return;
                }
                fail(core::error_code_t::invalid_parameter,
                     message(context_.resource, "unsupported scalar expression ", to_string(scalar->type())));
            }

            // CASE / COALESCE: every arm has to reach ONE type
            void resolve_blend(scalar_expression_t* scalar, bool inside_aggregate) {
                const bool is_case = scalar->type() == scalar_type::case_expr;
                const size_t minimum = is_case ? 2 : 1;
                if (scalar->params().size() < minimum) {
                    fail(core::error_code_t::invalid_parameter,
                         is_case ? "CASE expression with no THEN branch" : "COALESCE with no operands");
                    return;
                }
                std::pmr::vector<param_storage*> arms(context_.resource);
                std::pmr::vector<complex_logical_type> arm_types(context_.resource);
                cardinality_t combined = cardinality_t::constant;
                for (size_t position = 0; position < scalar->params().size(); position++) {
                    auto param_type = slot(scalar->params()[position], inside_aggregate);
                    if (error_.contains_error()) {
                        return;
                    }
                    combined = combine(combined, last_cardinality_);
                    const bool trailing_else =
                        is_case && scalar->params().size() % 2 == 1 && position == scalar->params().size() - 1;
                    if (!is_case || position % 2 == 1 || trailing_else) {
                        arms.push_back(&scalar->params()[position]);
                        arm_types.push_back(std::move(param_type));
                        continue;
                    }
                    // An even position that is not the trailing ELSE is a WHEN condition. It takes
                    // no part in the common type below; it only has to answer BOOLEAN.
                    if (param_type.type() != logical_type::BOOLEAN) {
                        fail(core::error_code_t::schema_error, "CASE WHEN condition must be BOOLEAN");
                        return;
                    }
                }
                const auto blend = unify_operands(context_.resource,
                                                  context_.cast_registry,
                                                  std::span<param_storage* const>{arms},
                                                  std::span<complex_logical_type>{arm_types},
                                                  &error_,
                                                  message(context_.resource, "branch of ", to_string(scalar->type())));
                if (!is_stamped(blend)) {
                    return;
                }
                scalar->set_result_type(blend);
                last_cardinality_ = combined;
            }

            void resolve_unary_operator(scalar_expression_t* scalar, bool inside_aggregate) {
                if (scalar->params().empty()) {
                    fail(core::error_code_t::invalid_parameter,
                         message(context_.resource, to_string(scalar->type()), " with no operand"));
                    return;
                }
                const auto operand = slot(scalar->params()[0], inside_aggregate);
                if (error_.contains_error()) {
                    return;
                }
                const auto cardinality = last_cardinality_;
                const auto resolved =
                    resolve_arithmetic(context_.cast_registry, to_operator_code(scalar->type()), operand);
                if (!is_stamped(resolved.op.result)) {
                    fail(core::error_code_t::arithmetics_failure,
                         message(context_.resource,
                                 "operator ",
                                 to_string(scalar->type()),
                                 " is not defined for type ",
                                 describe_type(operand)));
                    return;
                }
                scalar->set_result_type(resolved.op.result);
                splice_cast(context_.resource, scalar->params()[0], resolved.lhs_target, resolved.lhs_cast);
                last_cardinality_ = cardinality;
            }

            void resolve_binary_operator(scalar_expression_t* scalar, bool inside_aggregate) {
                if (scalar->params().size() < 2) {
                    fail(core::error_code_t::invalid_parameter,
                         message(context_.resource, "operator ", to_string(scalar->type()), " needs two operands"));
                    return;
                }
                auto left = slot(scalar->params()[0], inside_aggregate);
                if (error_.contains_error()) {
                    return;
                }
                cardinality_t combined = last_cardinality_;
                auto right = slot(scalar->params()[1], inside_aggregate);
                if (error_.contains_error()) {
                    return;
                }
                combined = combine(combined, last_cardinality_);
                if (error_.contains_error()) {
                    return;
                }

                const auto resolved =
                    resolve_arithmetic(context_.cast_registry, to_operator_code(scalar->type()), left, right);
                if (!is_stamped(resolved.op.result)) {
                    fail(core::error_code_t::arithmetics_failure,
                         message(context_.resource,
                                 "operator ",
                                 to_string(scalar->type()),
                                 " is not defined for types ",
                                 describe_type(left),
                                 " and ",
                                 describe_type(right)));
                    return;
                }
                scalar->set_result_type(resolved.op.result);
                splice_cast(context_.resource, scalar->params()[0], resolved.lhs_target, resolved.lhs_cast);
                splice_cast(context_.resource, scalar->params()[1], resolved.rhs_target, resolved.rhs_cast);
                last_cardinality_ = combined;
            }

            const expression_context_t& context_;
            bool* saw_reduction_;
            core::error_t error_{core::error_t::no_error()};
            // Cardinality of the operand most recently resolved. Set by every path that resolves
            // one, read by the parent before it resolves the next.
            cardinality_t last_cardinality_{cardinality_t::constant};
        };

    } // namespace

    core::error_t
    resolve_expression(expression_ptr& expression, const expression_context_t& context, bool* saw_reduction) {
        resolver_t resolver{context, saw_reduction};
        if (auto error = resolver.resolve(expression, /*inside_aggregate=*/false); error.contains_error()) {
            return error;
        }
        // A sort expression is a key plus an order
        if (!expression || expression->group() == expression_group::sort) {
            return core::error_t::no_error();
        }
        const auto& result_type = expression->result_type();
        if (result_type.type() == logical_type::UNKNOWN || result_type.type() == logical_type::INVALID) {
            return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"could not resolve a concrete type for expression", context.resource});
        }
        return core::error_t::no_error();
    }

} // namespace services::dispatcher::validation
