#include "binder.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::expressions {

    namespace {

        core::error_t bind_error(std::pmr::memory_resource* resource, core::error_code_t code, const char* what) {
            return core::error_t(code, std::pmr::string{what, resource});
        }

        bool is_two_operand_comparison(compare_type type) noexcept {
            switch (type) {
                case compare_type::eq:
                case compare_type::ne:
                case compare_type::gt:
                case compare_type::lt:
                case compare_type::gte:
                case compare_type::lte:
                    return true;
                default:
                    return false;
            }
        }

    } // namespace

    // ------------------------------------------------------------- bind_schema_t

    bind_schema_t::bind_schema_t(std::pmr::memory_resource* resource)
        : names_(resource)
        , types_(resource) {}

    void bind_schema_t::add(std::string_view name, types::complex_logical_type type) {
        // pmr's uses-allocator construction appends the vector's own allocator, so the element must
        // NOT be handed a resource of its own here.
        names_.emplace_back(name.data(), name.size());
        types_.push_back(std::move(type));
    }

    core::result_wrapper_t<uint32_t> bind_schema_t::resolve(std::string_view name) const {
        uint32_t found = 0;
        size_t matches = 0;
        for (size_t i = 0; i < names_.size(); ++i) {
            if (names_[i] == name) {
                if (matches == 0) {
                    found = static_cast<uint32_t>(i);
                }
                ++matches;
            }
        }
        if (matches == 0) {
            return bind_error(names_.get_allocator().resource(),
                              core::error_code_t::field_not_exists,
                              "binder: no input column carries this name");
        }
        if (matches > 1) {
            return bind_error(names_.get_allocator().resource(),
                              core::error_code_t::ambiguous_name,
                              "binder: more than one input column carries this name");
        }
        return found;
    }

    // A NULL literal has NO type of its own. The transformer registers it as a parameter holding an
    // NA-typed null, so the slot's "current binding" types it as NA -- which is not a type: it has no
    // width, the kernels cannot index it, and expression_executor_t::allocate_slots refuses it.
    //
    // SQL's own answer is that such a literal takes its type from context, and the context is the
    // PEER operand. `x = NULL` types the null as x's type; `1 + NULL` types it as the literal 1's.
    // The value stays null, so the executor still short-circuits it to an invalid row -- what the
    // peer supplies is the WIDTH the slot is read at, nothing more. Without this, every predicate
    // containing a bare NULL is a hard error rather than a row that does not select.
    core::result_wrapper_t<bound_expression_ptr>
    binder_t::retype_untyped_null(bound_expression_ptr operand, const types::complex_logical_type& peer) {
        if (!operand || operand->return_type().type() != types::logical_type::NA) {
            return operand;
        }
        if (peer.type() == types::logical_type::NA) {
            return operand; // nothing to take a type FROM; the caller decides what that means
        }
        if (operand->kind() == bound_kind::parameter) {
            const auto* slot = static_cast<const bound_parameter_t*>(operand.get());
            return bound_expression_ptr{make_bound_parameter(resource_, slot->id(), peer)};
        }
        if (operand->kind() == bound_kind::constant) {
            // A NULL of `peer`, not a value OF peer: logical_value_t(resource, T) is a ZERO of T,
            // never a null, because is_null() is `type == NA` (logical_value.cpp:325).
            return bound_expression_ptr{make_bound_null_constant(resource_, peer)};
        }
        return operand;
    }

    // ----------------------------------------------------------------- binder_t

    binder_t::binder_t(std::pmr::memory_resource* resource)
        : resource_(resource) {}

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind(const expression_ptr& expression,
                                                                const binder_context_t& context) {
        if (!expression) {
            return bind_error(resource_, core::error_code_t::invalid_parameter, "binder: no expression");
        }
        switch (expression->group()) {
            case expression_group::compare:
                return bind_compare(*static_cast<const compare_expression_t*>(expression.get()), context);
            case expression_group::scalar:
                return bind_scalar(*static_cast<const scalar_expression_t*>(expression.get()), context);
            case expression_group::aggregate:
                return bind_aggregate(*static_cast<const aggregate_expression_t*>(expression.get()), context);
            case expression_group::sort:
                return bind_sort(*static_cast<const sort_expression_t*>(expression.get()), context);
            case expression_group::function:
                return bind_function(*static_cast<const function_expression_t*>(expression.get()), context);
            case expression_group::invalid:
                break;
        }
        return bind_error(resource_, core::error_code_t::invalid_parameter, "binder: unknown expression group");
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_key(const key_t& key,
                                                                    const binder_context_t& context) {
        if (key.is_null()) {
            return bind_error(resource_, core::error_code_t::invalid_parameter, "binder: empty key");
        }
        const bool right_side = key.side() == side_t::right;
        const bind_schema_t* schema = right_side ? context.right : context.left;
        if (!schema) {
            return bind_error(resource_,
                              core::error_code_t::schema_error,
                              "binder: the side this key names has no input schema");
        }

        // TWO ways a name becomes an ordinal, and the FIRST one wins.
        //
        // (1) validate_logical_plan already resolved it and stamped the ordinals into key_t::path().
        //     Every key that reaches an operator in production is in this state -- which is also why
        //     '::?' variant selection needs nothing here: choosing among same-named columns by
        //     physical type is precisely what produced that path upstream, so re-deciding it would
        //     be a second, disagreeing answer to a settled question.
        // (2) No path: resolve the name against the schema here. This is the shape a caller without
        //     the validator builds (the unit tests), and it is where duplicate names are refused.
        std::pmr::vector<size_t> path{resource_};
        types::complex_logical_type column_type{types::logical_type::NA};
        if (!key.path().empty()) {
            path.assign(key.path().begin(), key.path().end());
            if (path.front() >= schema->size()) {
                return bind_error(resource_,
                                  core::error_code_t::field_not_exists,
                                  "binder: the resolved column ordinal is past the input schema");
            }
            // The LEAF type of the address: the same walk data_chunk_t::at() performs over the
            // vectors, performed over the types.
            column_type = types::complex_logical_type::type_from_path(schema->types(), path);
        } else {
            if (key.storage().size() != 1) {
                // A dotted NAME with no resolved path: nothing here can turn `a/b/c` into ordinals
                // without the nested column shape the validator owns.
                return bind_error(resource_,
                                  core::error_code_t::unimplemented_yet,
                                  "binder: an unresolved nested field path cannot be bound");
            }
            auto index = schema->resolve(std::string_view{key.storage().front()});
            if (index.has_error()) {
                return index.convert_error<bound_expression_ptr>();
            }
            path.push_back(index.value());
            column_type = schema->type_at(index.value());
        }
        if (column_type.type() == types::logical_type::NA) {
            return bind_error(resource_,
                              core::error_code_t::schema_error,
                              "binder: the addressed column has no type");
        }
        bound_expression_ptr reference = make_bound_reference(resource_,
                                                              std::move(column_type),
                                                              std::move(path),
                                                              right_side ? side_t::right : side_t::left);
        // '::?' selects a column, it does NOT convert a value, so it must not become a cast node.
        if (!key.has_cast_type() || key.is_variant_select()) {
            return reference;
        }
        // A '::' cast becomes part of the compiled tree instead of being rediscovered per row.
        return make_bound_cast(resource_, key.cast_type(), std::move(reference));
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_operand(const param_storage& operand,
                                                                        const binder_context_t& context) {
        return bind_param(operand, context);
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_column_path(const std::pmr::vector<size_t>& path,
                                                                            side_t side,
                                                                            const binder_context_t& context) {
        if (path.empty()) {
            return bind_error(resource_, core::error_code_t::invalid_parameter, "binder: empty column path");
        }
        const bool right_side = side == side_t::right;
        const bind_schema_t* schema = right_side ? context.right : context.left;
        if (!schema) {
            return bind_error(resource_,
                              core::error_code_t::schema_error,
                              "binder: the side this column path names has no input schema");
        }
        if (path.front() >= schema->size()) {
            return bind_error(resource_,
                              core::error_code_t::field_not_exists,
                              "binder: the column ordinal is past the input schema");
        }
        std::pmr::vector<size_t> owned{resource_};
        owned.assign(path.begin(), path.end());
        auto leaf = types::complex_logical_type::type_from_path(schema->types(), owned);
        if (leaf.type() == types::logical_type::NA) {
            return bind_error(resource_, core::error_code_t::schema_error, "binder: the column has no type");
        }
        return bound_expression_ptr{make_bound_reference(resource_,
                                                         std::move(leaf),
                                                         std::move(owned),
                                                         right_side ? side_t::right : side_t::left)};
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_param(const param_storage& param,
                                                                      const binder_context_t& context) {
        if (is_key(param)) {
            return bind_key(as_key(param), context);
        }
        if (is_expr(param)) {
            return bind(as_expr(param), context);
        }
        if (!is_parameter(param)) {
            return bind_error(resource_, core::error_code_t::invalid_parameter, "binder: unknown operand kind");
        }
        const auto id = as_parameter(param);
        if (!context.parameters) {
            return bind_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "binder: a parameter slot needs the parameter map to be typed");
        }
        const auto* value = logical_plan::get_parameter(context.parameters, id);
        if (!value) {
            return bind_error(resource_, core::error_code_t::invalid_parameter, "binder: parameter slot is not bound");
        }
        // TYPED here, from the current binding; the VALUE stays live. Both a literal and a real
        // placeholder are parameters today (the transformer registers literals as parameters too),
        // so the binder cannot yet tell which of them could become a bound_constant_t that owns its
        // value. Binding every slot live is the choice that is correct for both.
        return bound_expression_ptr{make_bound_parameter(resource_, id, value->type())};
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_compare(const compare_expression_t& expression,
                                                                        const binder_context_t& context) {
        const auto op = expression.type();
        if (op == compare_type::union_and || op == compare_type::union_or || op == compare_type::union_not) {
            std::pmr::vector<bound_expression_ptr> children{resource_};
            children.reserve(expression.children().size());
            for (const auto& child : expression.children()) {
                auto bound = bind(child, context);
                if (bound.has_error()) {
                    return bound;
                }
                children.push_back(std::move(bound.value()));
            }
            return make_bound_conjunction(resource_, op, std::move(children));
        }
        // A constant predicate. all_true is what an unconditional filter lowers to (a DML whose
        // WHERE the scan already applied); all_false is the empty-result sentinel. Both are
        // BOOLEAN expressions that read nothing, so they ARE constants -- binding them as such
        // makes them foldable, and the executor evaluates them once in create() instead of
        // per chunk. No node kind of their own: a constant is already the right shape.
        // NB: their left()/right() slots hold a NULL expression_ptr, so this must precede any
        // operand inspection.
        if (op == compare_type::all_true || op == compare_type::all_false) {
            return bound_expression_ptr{
                make_bound_constant(resource_,
                                    types::logical_value_t{resource_, op == compare_type::all_true})};
        }
        if (op == compare_type::is_null || op == compare_type::is_not_null) {
            auto operand = bind_param(expression.left(), context);
            if (operand.has_error()) {
                return operand;
            }
            return make_bound_null_test(resource_, op, std::move(operand.value()));
        }
        if (op == compare_type::regex) {
            return bind_regex(expression, context);
        }
        if (op == compare_type::any || op == compare_type::all) {
            return bind_any_all(expression, context);
        }
        if (!is_two_operand_comparison(op)) {
            return bind_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "binder: unknown comparison kind");
        }
        auto left = bind_param(expression.left(), context);
        if (left.has_error()) {
            return left;
        }
        auto right = bind_param(expression.right(), context);
        if (right.has_error()) {
            return right;
        }
        auto typed_left = retype_untyped_null(std::move(left.value()), right.value()->return_type());
        if (typed_left.has_error()) {
            return typed_left;
        }
        auto typed_right = retype_untyped_null(std::move(right.value()), typed_left.value()->return_type());
        if (typed_right.has_error()) {
            return typed_right;
        }
        if (typed_left.value()->return_type().type() == types::logical_type::NA &&
            typed_right.value()->return_type().type() == types::logical_type::NA) {
            // `NULL = NULL`: neither side can type the other, and the answer does not depend on the
            // row -- it is UNKNOWN everywhere, which selects nothing. A typed NULL BOOLEAN says
            // exactly that, and being foldable it is evaluated once instead of per chunk.
            return bound_expression_ptr{make_bound_constant(
                resource_,
                types::logical_value_t{resource_, types::complex_logical_type{types::logical_type::BOOLEAN}})};
        }
        return make_bound_comparison(resource_, op, std::move(typed_left.value()), std::move(typed_right.value()));
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_regex(const compare_expression_t& expression,
                                                                      const binder_context_t& context) {
        auto subject = bind_param(expression.left(), context);
        if (subject.has_error()) {
            return subject;
        }
        // THE point of the node: when the pattern is a bound parameter its bytes are known NOW, so
        // the regex is compiled once here rather than per row -- and, because a bound node is a class,
        // the move-only compiled object is simply a member. That is what removes the predicate
        // subclass whose only reason to exist was that a regex_t does not fit in a std::function.
        // An `x LIKE <column>` pattern genuinely varies per row and is bound as `dynamic`.
        if (is_parameter(expression.right())) {
            if (!context.parameters) {
                return bind_error(resource_,
                                  core::error_code_t::invalid_parameter,
                                  "binder: a regex pattern parameter needs the parameter map");
            }
            const auto* pattern = logical_plan::get_parameter(context.parameters, as_parameter(expression.right()));
            if (!pattern) {
                return bind_error(resource_,
                                  core::error_code_t::invalid_parameter,
                                  "binder: the regex pattern slot is not bound");
            }
            const bool is_string = types::is_string(pattern->type().type());
            // The transformer has already run like_to_regex on a scalar LIKE, so the stored pattern
            // is regex source: convert nothing here (regex_like() marks the per-ELEMENT conversion an
            // ANY/ALL needs, which is why bind_any_all passes it on and this does not).
            return make_bound_regex(resource_,
                                    std::move(subject.value()),
                                    is_string && !pattern->is_null() ? pattern->value<std::string_view>()
                                                                     : std::string_view{},
                                    pattern->is_null(),
                                    is_string,
                                    false,
                                    expression.regex_icase());
        }
        auto pattern = bind_param(expression.right(), context);
        if (pattern.has_error()) {
            return pattern;
        }
        return make_bound_dynamic_regex(resource_,
                                        std::move(subject.value()),
                                        std::move(pattern.value()),
                                        false,
                                        expression.regex_icase());
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_any_all(const compare_expression_t& expression,
                                                                        const binder_context_t& context) {
        // The array operand is a PARAMETER slot holding the flattened sub-query result. It stays a
        // slot: a correlated sub-query refills it per outer row, so binding its VALUE would freeze
        // the first outer row's set into the plan.
        if (!is_parameter(expression.right())) {
            return bind_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "binder: an ANY/ALL right operand must be a sub-query array slot");
        }
        auto subject = bind_param(expression.left(), context);
        if (subject.has_error()) {
            return subject;
        }
        return make_bound_any_all(resource_,
                                  expression.type() == compare_type::any,
                                  expression.inner_op(),
                                  as_parameter(expression.right()),
                                  expression.regex_like(),
                                  expression.regex_icase(),
                                  expression.regex_negate(),
                                  std::move(subject.value()));
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_scalar(const scalar_expression_t& expression,
                                                                       const binder_context_t& context) {
        const auto type = expression.type();
        if (type == scalar_type::get_field || type == scalar_type::group_field) {
            return bind_key(expression.key(), context);
        }
        if (type == scalar_type::constant) {
            if (expression.params().size() != 1) {
                return bind_error(resource_,
                                  core::error_code_t::invalid_parameter,
                                  "binder: a constant takes exactly one operand");
            }
            return bind_param(expression.params().front(), context);
        }
        return bind_scalar_operands(type, expression.params(), context);
    }

    core::result_wrapper_t<bound_expression_ptr>
    binder_t::bind_scalar_operands(scalar_type type,
                                   const std::pmr::vector<param_storage>& params,
                                   const binder_context_t& context) {
        const auto& expression_params = params;
        if (const auto arith = to_arithmetic_op(type)) {
            const vector::arithmetic_op op = *arith;
            if (expression_params.size() != 2) {
                return bind_error(resource_,
                                  core::error_code_t::invalid_parameter,
                                  "binder: binary arithmetic takes exactly two operands");
            }
            auto left = bind_param(expression_params[0], context);
            if (left.has_error()) {
                return left;
            }
            auto right = bind_param(expression_params[1], context);
            if (right.has_error()) {
                return right;
            }
            auto typed_left = retype_untyped_null(std::move(left.value()), right.value()->return_type());
            if (typed_left.has_error()) {
                return typed_left;
            }
            auto typed_right = retype_untyped_null(std::move(right.value()), typed_left.value()->return_type());
            if (typed_right.has_error()) {
                return typed_right;
            }
            return make_bound_arithmetic(resource_,
                                         op,
                                         std::move(typed_left.value()),
                                         std::move(typed_right.value()));
        }
        if (type == scalar_type::unary_minus) {
            if (expression_params.empty()) {
                return bind_error(resource_,
                                  core::error_code_t::arithmetics_failure,
                                  "binder: unary minus requires one operand");
            }
            auto operand = bind_param(expression_params[0], context);
            if (operand.has_error()) {
                return operand;
            }
            return make_bound_negate(resource_, std::move(operand.value()));
        }
        if (type == scalar_type::case_expr || type == scalar_type::case_when) {
            // Same operand layout the boxed evaluator uses: when0, then0, when1, then1, ... and a
            // trailing ELSE when the count is odd.
            const auto& operands = expression_params;
            if (operands.size() < 2) {
                return bind_error(resource_,
                                  core::error_code_t::invalid_parameter,
                                  "binder: CASE needs at least one WHEN/THEN pair");
            }
            std::pmr::vector<bound_expression_ptr> branches{resource_};
            branches.reserve(operands.size());
            for (const auto& operand : operands) {
                auto bound = bind_param(operand, context);
                if (bound.has_error()) {
                    return bound;
                }
                branches.push_back(std::move(bound.value()));
            }
            // A NULL THEN / ELSE takes its type from the first RESULT branch that has one -- the
            // same context rule a comparison applies, over the branch list. Results sit at the odd
            // positions, plus a trailing ELSE when the count is odd.
            types::complex_logical_type result_type{types::logical_type::NA};
            for (size_t i = 1; i < branches.size(); i += 2) {
                if (branches[i]->return_type().type() != types::logical_type::NA) {
                    result_type = branches[i]->return_type();
                    break;
                }
            }
            if (result_type.type() == types::logical_type::NA && branches.size() % 2 == 1) {
                result_type = branches.back()->return_type();
            }
            if (result_type.type() != types::logical_type::NA) {
                for (size_t i = 1; i < branches.size(); i += 2) {
                    auto typed = retype_untyped_null(std::move(branches[i]), result_type);
                    if (typed.has_error()) {
                        return typed;
                    }
                    branches[i] = std::move(typed.value());
                }
                if (branches.size() % 2 == 1) {
                    auto typed = retype_untyped_null(std::move(branches.back()), result_type);
                    if (typed.has_error()) {
                        return typed;
                    }
                    branches.back() = std::move(typed.value());
                }
            }
            return make_bound_case(resource_, std::move(branches));
        }
        return bind_error(resource_,
                          core::error_code_t::unimplemented_yet,
                          "binder: this scalar operation is not bound yet");
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_aggregate(const aggregate_expression_t& expression,
                                                                          const binder_context_t& context) {
        // An aggregate is not itself a per-row expression: what the bound layer can answer is the
        // expression whose value it consumes per row -- exactly the thing pushed_aggregate_spec_t
        // re-derives. COUNT(*) has no such argument.
        if (expression.params().size() != 1) {
            return bind_error(resource_,
                              core::error_code_t::unimplemented_yet,
                              "binder: only a single-argument aggregate has a bound per-row input");
        }
        return bind_param(expression.params().front(), context);
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_function(const function_expression_t& expression,
                                                                         const binder_context_t& context) {
        if (!context.functions) {
            return bind_error(resource_,
                              core::error_code_t::function_registry_error,
                              "binder: a function call needs the function registry");
        }
        if (expression.function_uid() == compute::invalid_function_uid) {
            return bind_error(resource_,
                              core::error_code_t::unrecognized_function,
                              "binder: the function call carries no resolved uid");
        }
        const auto* function = context.functions->get_function(expression.function_uid());
        if (!function) {
            return bind_error(resource_,
                              core::error_code_t::unrecognized_function,
                              "binder: the function uid is not registered");
        }
        std::pmr::vector<bound_expression_ptr> arguments{resource_};
        arguments.reserve(expression.args().size());
        std::pmr::vector<types::complex_logical_type> argument_types{resource_};
        argument_types.reserve(expression.args().size());
        for (const auto& argument : expression.args()) {
            auto bound = bind_param(argument, context);
            if (bound.has_error()) {
                return bound;
            }
            argument_types.push_back(bound.value()->return_type());
            arguments.push_back(std::move(bound.value()));
        }
        // THE point of binding a call: the kernel is chosen HERE, from the argument types the tree
        // promises, and its declared output type becomes the node's return type. The boxed path
        // re-dispatched per batch off whatever the first row's value happened to be typed as, so a
        // column whose first row was NULL could pick a different kernel than the same column with a
        // non-null head. An argument set with no matching kernel is refused at bind time instead of
        // failing on the first chunk that reaches it.
        auto kernel = function->dispatch_exact(resource_, argument_types);
        if (kernel.has_error()) {
            return kernel.convert_error<bound_expression_ptr>();
        }
        const auto& signature = kernel.value().get().signature();
        if (signature.output_types.empty()) {
            return bind_error(resource_,
                              core::error_code_t::incorrect_function_return_type,
                              "binder: the matched kernel declares no output type");
        }
        auto return_type = signature.output_types.front().resolve(resource_, argument_types);
        if (return_type.has_error()) {
            return return_type.convert_error<bound_expression_ptr>();
        }
        if (return_type.value().type() == types::logical_type::NA) {
            return bind_error(resource_,
                              core::error_code_t::incorrect_function_return_type,
                              "binder: the matched kernel resolves to no result type");
        }
        return make_bound_function(resource_,
                                   context.functions,
                                   expression.function_uid(),
                                   std::move(return_type.value()),
                                   std::move(arguments));
    }

    core::result_wrapper_t<bound_expression_ptr> binder_t::bind_sort(const sort_expression_t& expression,
                                                                     const binder_context_t& context) {
        // The ORDER the sort applies is the operator's business; what binds is the VALUE it orders
        // by -- the thing sort_key re-derives at execution time today.
        return bind_key(expression.key(), context);
    }

} // namespace components::expressions
