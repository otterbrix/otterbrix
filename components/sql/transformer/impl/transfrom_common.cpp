#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/jsonb_path.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/logical_value.hpp>
#include <core/regex/like_to_regex.hpp>

using namespace components::expressions;

namespace components::sql::transform {

    namespace {
        // The aggregate a HAVING names may sit ANYWHERE inside a target-list entry — as the entry
        // itself, under a CAST, inside a CASE arm, or as an operand of arithmetic — so matching it
        // means descending the whole expression rather than inspecting its top node. A call matches
        // on name AND arguments: two aggregates of the same name are different aggregates.
        const expressions::expression_i* find_call(const expressions::expression_i* expr,
                                                   const std::string& name,
                                                   const std::pmr::vector<expressions::param_storage>& args,
                                                   bool args_comparable);

        const expressions::expression_i* find_call_in(const expressions::param_storage& param,
                                                      const std::string& name,
                                                      const std::pmr::vector<expressions::param_storage>& args,
                                                      bool args_comparable) {
            if (!std::holds_alternative<expressions::expression_ptr>(param)) {
                return nullptr;
            }
            const auto& nested = std::get<expressions::expression_ptr>(param);
            return nested ? find_call(nested.get(), name, args, args_comparable) : nullptr;
        }

        const expressions::expression_i* find_call(const expressions::expression_i* expr,
                                                   const std::string& name,
                                                   const std::pmr::vector<expressions::param_storage>& args,
                                                   bool args_comparable) {
            if (expr == nullptr) {
                return nullptr;
            }
            switch (expr->group()) {
                case expression_group::aggregate: {
                    const auto* agg = static_cast<const aggregate_expression_t*>(expr);
                    if (agg->function_name() == name && (!args_comparable || agg->params() == args)) {
                        return expr;
                    }
                    for (const auto& param : agg->params()) {
                        if (const auto* found = find_call_in(param, name, args, args_comparable)) {
                            return found;
                        }
                    }
                    return nullptr;
                }
                case expression_group::function: {
                    const auto* call = static_cast<const function_expression_t*>(expr);
                    if (call->name() == name && (!args_comparable || call->args() == args)) {
                        return expr;
                    }
                    for (const auto& param : call->args()) {
                        if (const auto* found = find_call_in(param, name, args, args_comparable)) {
                            return found;
                        }
                    }
                    return nullptr;
                }
                case expression_group::cast:
                    return find_call_in(static_cast<const expressions::cast_expression_t*>(expr)->child(),
                                        name,
                                        args,
                                        args_comparable);
                case expression_group::scalar: {
                    for (const auto& param : static_cast<const scalar_expression_t*>(expr)->params()) {
                        if (const auto* found = find_call_in(param, name, args, args_comparable)) {
                            return found;
                        }
                    }
                    return nullptr;
                }
                case expression_group::compare: {
                    const auto* cmp = static_cast<const compare_expression_t*>(expr);
                    if (const auto* found = find_call_in(cmp->left(), name, args, args_comparable)) {
                        return found;
                    }
                    if (const auto* found = find_call_in(cmp->right(), name, args, args_comparable)) {
                        return found;
                    }
                    for (const auto& child : cmp->children()) {
                        if (const auto* found = find_call(child.get(), name, args, args_comparable)) {
                            return found;
                        }
                    }
                    return nullptr;
                }
                default:
                    return nullptr;
            }
        }
    } // namespace

    namespace {
        // Logical negation (complement) of a scalar comparison operator: eq<->ne, lt<->gte, gt<->lte.
        // NOT distributes over an ANY/ALL membership by De Morgan only when the per-element comparison
        // is itself flipped to its complement (see the AEXPR_NOT membership rewrite below). Returns
        // compare_type::invalid for an operator with no scalar complement (regex handled separately).
        compare_type negate_scalar_compare(compare_type op) noexcept {
            switch (op) {
                case compare_type::eq:
                    return compare_type::ne;
                case compare_type::ne:
                    return compare_type::eq;
                case compare_type::lt:
                    return compare_type::gte;
                case compare_type::lte:
                    return compare_type::gt;
                case compare_type::gt:
                    return compare_type::lte;
                case compare_type::gte:
                    return compare_type::lt;
                default:
                    return compare_type::invalid;
            }
        }
    } // namespace

    expression_ptr transformer::transform_a_expr_arithmetic(A_Expr* node,
                                                            const name_collection_t& names,
                                                            logical_plan::parameter_node_t* params) {
        auto op_str = std::string_view(strVal(node->name->lst.front().data));
        auto stype = get_arithmetic_scalar_type(op_str);
        if (stype == scalar_type::invalid) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"invalid arithmetics operator", resource_});
            return nullptr;
        }

        auto expr = make_scalar_expression(resource_, stype);

        if (node->lexpr) {
            expr->append_param(transform_a_expr_operand(node->lexpr, names, params));
            expr->append_param(transform_a_expr_operand(node->rexpr, names, params));
        } else if (op_str == "+") {
            // Unary plus in an expression slot: the identity, encoded as 0 + x (an
            // expression is required here; the pure strip happens in operand position).
            auto zero_id = params->add_parameter(types::logical_value_t(resource_, int64_t(0)));
            expr = make_scalar_expression(resource_, scalar_type::add);
            expr->append_param(zero_id);
            expr->append_param(transform_a_expr_operand(node->rexpr, names, params));
        } else {
            if (stype == scalar_type::add) {
                auto operand = transform_a_expr_operand(node->rexpr, names, params);
                if (std::holds_alternative<expression_ptr>(operand)) {
                    return std::get<expression_ptr>(operand);
                }
                auto value =
                    make_scalar_expression(resource_,
                                           std::holds_alternative<expressions::key_t>(operand) ? scalar_type::get_field
                                                                                               : scalar_type::constant);
                value->append_param(std::move(operand));
                return value;
            }
            if (stype == scalar_type::subtract) {
                expr = make_scalar_expression(resource_, scalar_type::unary_minus);
            }
            expr->append_param(transform_a_expr_operand(node->rexpr, names, params));
        }
        return expr;
    }

    param_storage transformer::transform_a_expr_operand(Node* node,
                                                        const name_collection_t& names,
                                                        logical_plan::parameter_node_t* params) {
        // `+x` is x in operand position too: peel the identity layers so the stripped node
        // takes its own arm below.
        node = strip_unary_plus(node);
        if (!node) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"operator is missing its operand", resource_});
            return nullptr;
        }
        switch (nodeTag(node)) {
            case T_ColumnRef: {
                // Predicate-arithmetic operand: a correlated outer column is lowered to a
                // parameter here. Predicate value getters read parameters live per row
                // check (see create_value_getter), so the lateral join's per-outer-row
                // rebind is honoured even for a correlation nested in arithmetic.
                if (auto corr = try_lateral_correlate(pg_ptr_cast<ColumnRef>(node), names)) {
                    return *corr;
                }
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node), names);
                key.deduce_side(names);
                return key.field;
            }
            case T_A_Indirection: {
                auto res = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node), names);
                if (res.has_error()) {
                    error_ = res.error();
                    return nullptr;
                }
                auto key = std::move(res.value());
                key.deduce_side(names);
                return key.field;
            }
            case T_ParamRef:
            case T_A_Const:
            case T_TypeCast:
            case T_RowExpr:
            case T_A_ArrayExpr:
                return add_param_value(node, params);
            case T_A_Expr: {
                auto sub_expr = pg_ptr_cast<A_Expr>(node);
                if (sub_expr->kind == AEXPR_OP) {
                    auto sub_op = std::string_view(strVal(sub_expr->name->lst.front().data));
                    if (is_arithmetic_operator(sub_op)) {
                        return transform_a_expr_arithmetic(sub_expr, names, params);
                    }
                    if (auto function_name = operator_function_name(sub_op); !function_name.empty()) {
                        auto call = make_function_expression(resource_, std::string{function_name});
                        if (sub_expr->lexpr) {
                            call->args().push_back(transform_a_expr_operand(sub_expr->lexpr, names, params));
                        }
                        if (sub_expr->rexpr) {
                            call->args().push_back(transform_a_expr_operand(sub_expr->rexpr, names, params));
                        }
                        if (has_error()) {
                            return nullptr;
                        }
                        return expression_ptr{call};
                    }
                    if (is_jsonb_nav_operator(sub_op)) {
                        expressions::key_t k{resource_};
                        if (!resolve_jsonb_scalar_key(sub_expr, names, k)) {
                            return nullptr;
                        }
                        return k;
                    }
                }
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"Unsupported A_Expr in arithmetic operand", resource_});
                return nullptr;
            }
            case T_FuncCall:
                return transform_a_expr_func(pg_ptr_cast<FuncCall>(node), names, params);
            default:
                error_ =
                    core::error_t(core::error_code_t::sql_parse_error,
                                  std::pmr::string{"Unsupported operand type in arithmetic expression", resource_});
                return nullptr;
        }
    }

    void transformer::transform_select_a_expr(A_Expr* node,
                                              const char* alias,
                                              const name_collection_t& names,
                                              logical_plan::execution_plan_t* plan,
                                              logical_plan::node_ptr& group) {
        auto op_str = std::string_view(strVal(node->name->lst.front().data));
        if (!is_arithmetic_operator(op_str)) {
            error_ =
                core::error_t(core::error_code_t::sql_parse_error,
                              std::pmr::string{"Unsupported operator in SELECT: " + std::string(op_str), resource_});
            return;
        }
        std::string expr_name = alias ? alias : std::string(op_str);
        scalar_expression_ptr expr;

        if (node->lexpr) {
            auto stype = get_arithmetic_scalar_type(op_str);
            if (stype == scalar_type::invalid) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"invalid arithmetics operand", resource_});
                return;
            }
            expr = make_scalar_expression(resource_, stype, expressions::key_t{resource_, std::move(expr_name)});
            expr->append_param(resolve_select_operand(node->lexpr, names, plan, group));
            expr->append_param(resolve_select_operand(node->rexpr, names, plan, group));
        } else if (op_str == "+") {
            // Unary plus in an expression slot: the identity, encoded as 0 + x (the SELECT
            // field-clause strip handles the top-level `SELECT +x` form).
            auto zero_id = plan->parameters->add_parameter(types::logical_value_t(resource_, int64_t(0)));
            expr = make_scalar_expression(resource_,
                                          scalar_type::add,
                                          expressions::key_t{resource_, std::move(expr_name)});
            expr->append_param(zero_id);
            expr->append_param(resolve_select_operand(node->rexpr, names, plan, group));
        } else {
            // Unary minus: proper unary operator with single operand
            expr = make_scalar_expression(resource_,
                                          scalar_type::unary_minus,
                                          expressions::key_t{resource_, std::move(expr_name)});
            expr->append_param(resolve_select_operand(node->rexpr, names, plan, group));
        }

        group->append_expression(expr);
    }

    param_storage transformer::resolve_select_operand(Node* node,
                                                      const name_collection_t& names,
                                                      logical_plan::execution_plan_t* plan,
                                                      logical_plan::node_ptr& group) {
        switch (nodeTag(node)) {
            case T_ColumnRef: {
                // Correlated outer column in a SELECT-list operand: lower to the
                // correlation parameter. operator_select / evaluate_arithmetic read
                // parameters live per row, so the lateral per-outer-row rebind holds.
                if (auto corr = try_lateral_correlate(pg_ptr_cast<ColumnRef>(node), names)) {
                    return *corr;
                }
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node), names);
                key.deduce_side(names);
                return key.field;
            }
            case T_A_Indirection: {
                auto res = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node), names);
                if (res.has_error()) {
                    error_ = res.error();
                    return nullptr;
                }
                auto key = std::move(res.value());
                key.deduce_side(names);
                return key.field;
            }
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                if (cast->arg && nodeTag(cast->arg) == T_ColumnRef) {
                    auto target_type_res = get_type(resource_, cast->typeName);
                    if (target_type_res.has_error()) {
                        error_ = target_type_res.error();
                        return nullptr;
                    }
                    auto col_ref = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(cast->arg), names);
                    col_ref.deduce_side(names);
                    return param_storage{expression_ptr{
                        make_cast_expression(resource_,
                                             param_storage{std::move(col_ref.field)},
                                             target_type_res.value(),
                                             casts::cast_t{},
                                             cast->try_cast ? casts::cast_kind::try_cast : casts::cast_kind::cast)}};
                }
                // A cast over a scalar jsonb navigation, e.g. (t #>> 'a.c')::bigint:
                // resolve the navigation to its flattened column key and annotate it,
                // exactly like a column cast. Without this the cast node fell through
                // to add_param_value, which tried to fold the navigation A_Expr into a
                // constant parameter and read uninitialized memory — a per-run garbage
                // constant, identical on every row.
                if (cast->arg && nodeTag(cast->arg) == T_A_Expr) {
                    auto* sub = pg_ptr_cast<A_Expr>(cast->arg);
                    if (sub->kind == AEXPR_OP && sub->name && nodeTag(sub->name->lst.front().data) == T_String &&
                        is_jsonb_nav_operator(strVal(sub->name->lst.front().data))) {
                        auto target_type_res = get_type(resource_, cast->typeName);
                        if (target_type_res.has_error()) {
                            error_ = target_type_res.error();
                            return nullptr;
                        }
                        expressions::key_t navigated{resource_};
                        if (!resolve_jsonb_scalar_key(sub, names, navigated)) {
                            return nullptr;
                        }
                        return param_storage{expression_ptr{make_cast_expression(
                            resource_,
                            param_storage{std::move(navigated)},
                            target_type_res.value(),
                            casts::cast_t{},
                            cast->try_cast ? casts::cast_kind::try_cast : casts::cast_kind::cast)}};
                    }
                }
                return add_param_value(node, plan->parameters.get());
            }
            case T_ParamRef:
            case T_A_Const:
                return add_param_value(node, plan->parameters.get());
            case T_A_Expr: {
                auto sub_expr = pg_ptr_cast<A_Expr>(node);
                if (sub_expr->kind == AEXPR_OP) {
                    auto sub_op = std::string_view(strVal(sub_expr->name->lst.front().data));
                    if (is_jsonb_nav_operator(sub_op)) {
                        expressions::key_t k{resource_};
                        if (!resolve_jsonb_scalar_key(sub_expr, names, k)) {
                            return nullptr;
                        }
                        return k;
                    }
                    if (is_arithmetic_operator(sub_op)) {
                        auto sub_stype = get_arithmetic_scalar_type(sub_op);
                        if (sub_stype == scalar_type::invalid) {
                            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                                   std::pmr::string{"invalid arithmetics operand", resource_});
                            return nullptr;
                        }
                        auto sub_scalar = make_scalar_expression(resource_, sub_stype);
                        if (sub_expr->lexpr) {
                            sub_scalar->append_param(resolve_select_operand(sub_expr->lexpr, names, plan, group));
                        } else {
                            auto zero_id =
                                plan->parameters->add_parameter(types::logical_value_t(resource_, int64_t(0)));
                            sub_scalar->append_param(zero_id);
                        }
                        sub_scalar->append_param(resolve_select_operand(sub_expr->rexpr, names, plan, group));
                        return sub_scalar;
                    }
                }
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"Unsupported A_Expr in SELECT operand", resource_});
                return nullptr;
            }
            case T_FuncCall: {
                // In SELECT context, FuncCall is an aggregate
                auto func = pg_ptr_cast<FuncCall>(node);
                auto funcname = std::string{strVal(linitial(func->funcname))};

                std::pmr::vector<param_storage> args(resource_);
                if (!func->agg_star) {
                    args.reserve(func->args->lst.size());
                    for (const auto& arg : func->args->lst) {
                        args.emplace_back(resolve_select_operand(pg_ptr_cast<Node>(arg.data), names, plan, group));
                    }
                }

                // FILTER (WHERE p): lower to a CASE over each argument (or COUNT(CASE ...) for a
                // bare aggregate) so only qualifying rows reach the aggregate.
                args = apply_aggregate_filter(func->agg_filter, std::move(args), names, plan);

                auto call = make_function_expression(resource_, std::move(funcname), std::move(args));
                call->set_star_argument(func->agg_star);
                return param_storage{expressions::expression_ptr{call}};
            }
            case T_CaseExpr:
                return case_expr_to_scalar(pg_ptr_cast<CaseExpr>(node), nullptr, names, plan, group);
            case T_SubLink: {
                auto sub = pg_ptr_cast<SubLink>(node);
                if (sub->subLinkType != EXPR_SUBLINK) {
                    error_ =
                        core::error_t(core::error_code_t::sql_parse_error,
                                      std::pmr::string{"Unsupported operand type in SELECT arithmetic", resource_});
                    return nullptr;
                }
                // Scalar sub-query as an arithmetic operand: flatten it and return the bound parameter id
                // (read live per row by evaluate_arithmetic). Save/restore the pending internal-aggregate
                // stash around the inner transform so it does not drop this level's aggregates.
                auto param_id =
                    plan->parameters->add_parameter(types::logical_value_t{resource_, types::logical_type::NA});
                auto prev_pending = std::move(pending_internal_aggs_);
                pending_internal_aggs_.clear();
                auto sub_node = transform(*sub->subselect, plan);
                pending_internal_aggs_ = std::move(prev_pending);
                if (has_error()) {
                    return nullptr;
                }
                plan->sub_query_results.emplace_back(&vector::compact_to_single_value, param_id);
                plan->sub_queries.emplace_back(std::move(sub_node));
                return param_id;
            }
            default:
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"Unsupported operand type in SELECT arithmetic", resource_});
                return nullptr;
        }
    }

    // Render a jsonb operator's right-hand key/path operand into its textual form.
    // The operand is always a literal: a bare string/number, a cast of one, or a
    // ParamRef; a bare column reference (`t -> x`) contributes the column's *name*.
    // The switch is exhaustive and never dereferences a node as the wrong type —
    // every unhandled shape reports a clean parse error instead.
    std::string transformer::get_str_value(Node* node) {
        switch (nodeTag(node)) {
            case T_TypeCast:
                // A cast key is just its underlying constant rendered as text:
                // 'x'::text -> "x", 5::bigint -> "5", TRUE -> "t". Recurse so the
                // operand's real node type drives the conversion. (This arm used to
                // collapse EVERY cast to the boolean strings "true"/"false", which
                // both mis-keyed 'x'::text and dereferenced a non-string cast
                // argument's integer union member as a char* — a segfault.)
                return get_str_value(pg_ptr_cast<TypeCast>(node)->arg);
            case T_A_Const: {
                auto value = &(pg_ptr_cast<A_Const>(node)->val);
                switch (nodeTag(value)) {
                    case T_String:
                        return strVal(value);
                    case T_Integer:
                        return std::to_string(intVal(value));
                    case T_Float:
                        return strVal(value);
                    case T_Null:
                        error_ =
                            core::error_t(core::error_code_t::sql_parse_error,
                                          std::pmr::string{"jsonb key must be a constant value, not NULL", resource_});
                        return {};
                    default:
                        break;
                }
                // No fall-through into the ColumnRef arm below: an unexpected
                // constant kind is a clean error, not a wild reinterpret-cast.
                break;
            }
            case T_ColumnRef:
                // `t -> col`: the key is the column's own name (its identifier),
                // not its per-row value. Kept for compatibility with that spelling.
                return strVal(pg_ptr_cast<ColumnRef>(node)->fields->lst.back().data);
            case T_ParamRef:
                return "$" + std::to_string(pg_ptr_cast<ParamRef>(node)->number);
            default:
                break;
        }
        error_ = core::error_t(core::error_code_t::sql_parse_error,
                               std::pmr::string{"incorrect string value in get_str_value", resource_});
        return {};
    }

    bool transformer::references_lateral_outer(ColumnRef* ref, const name_collection_t& inner_names) const {
        if (!lateral_join_ || !lateral_outer_names_ || !lateral_plan_) {
            return false;
        }
        auto& lst = ref->fields->lst;
        // Only a qualified reference (table.column) can name an outer relation; an
        // unqualified column always resolves against the inner scope.
        if (lst.size() < 2 || nodeTag(lst.front().data) != T_String) {
            return false;
        }
        const std::string qualifier = strVal(lst.front().data);
        // A qualifier the inner scope owns is an ordinary in-scope column, not a
        // correlation (inner names shadow outer ones, matching SQL scoping).
        if (inner_names.is_left_table(qualifier) || inner_names.is_right_table(qualifier)) {
            return false;
        }
        return lateral_outer_names_->is_left_table(qualifier) || lateral_outer_names_->is_right_table(qualifier);
    }

    std::optional<core::parameter_id_t> transformer::try_lateral_correlate(ColumnRef* ref,
                                                                           const name_collection_t& inner_names) {
        if (!references_lateral_outer(ref, inner_names)) {
            return std::nullopt;
        }
        // Resolve against the OUTER scope so the key's path + side match how the
        // lateral join operator locates the column in the outer row's chunk.
        auto outer_col = columnref_to_field(resource_, ref, *lateral_outer_names_);
        outer_col.deduce_side(*lateral_outer_names_);
        const std::string dedup_key =
            std::string(strVal(ref->fields->lst.front().data)) + "." + std::string(outer_col.field.as_string());
        if (auto it = lateral_correlation_map_.find(dedup_key); it != lateral_correlation_map_.end()) {
            return it->second;
        }
        // Placeholder value: the lateral join operator rebinds the real outer value
        // (with its true type) into this slot before each inner-sub-plan re-run.
        auto param_id = lateral_plan_->parameters->add_parameter(
            types::logical_value_t{resource_, types::complex_logical_type{types::logical_type::NA}});
        lateral_join_->add_correlation(param_id, outer_col.field);
        lateral_correlation_map_.emplace(dedup_key, param_id);
        return param_id;
    }

    void transformer::note_cast_type(const types::complex_logical_type& target) {
        if (target.type() != types::logical_type::UNKNOWN) {
            // a built-in target needs no catalog trip
            return;
        }
        const std::string& name = target.type_name();
        if (std::find(cast_type_names_.begin(), cast_type_names_.end(), name) == cast_type_names_.end()) {
            cast_type_names_.push_back(name);
        }
    }

    core::parameter_id_t transformer::add_param_value(Node* node, logical_plan::parameter_node_t* params) {
        if (nodeTag(node) == T_ParamRef) {
            auto ref = pg_ptr_cast<ParamRef>(node);
            if (auto it = parameter_map_.find(ref->number); it != parameter_map_.end()) {
                return it->second;
            } else {
                auto id = params->add_parameter(
                    types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA}));
                parameter_map_.emplace(ref->number, id);
                return id;
            }
        }

        if (auto res = get_value(resource_, node); res.has_error()) {
            error_ = res.error();
            return core::parameter_id_t{};
        } else {
            return params->add_parameter(std::move(res.value()));
        }
    }

    expression_ptr
    transformer::transform_a_expr(A_Expr* node, const name_collection_t& names, logical_plan::execution_plan_t* plan) {
        switch (node->kind) {
            case AEXPR_AND: // fall-through
            case AEXPR_OR: {
                auto expr = make_compare_union_expression(resource_,
                                                          node->kind == AEXPR_AND ? compare_type::union_and
                                                                                  : compare_type::union_or);
                auto append = [this, &plan, &expr, &names](Node* node) {
                    expression_ptr child_expr;
                    if (nodeTag(node) == T_A_Expr) {
                        child_expr = transform_a_expr(pg_ptr_cast<A_Expr>(node), names, plan);
                    } else if (nodeTag(node) == T_A_Indirection) {
                        child_expr = transform_a_indirection(pg_ptr_cast<A_Indirection>(node), names, plan);
                    } else if (nodeTag(node) == T_FuncCall) {
                        child_expr = transform_a_expr_func(pg_ptr_cast<FuncCall>(node), names, plan->parameters.get());
                    } else if (nodeTag(node) == T_NullTest) {
                        child_expr = transform_null_test(pg_ptr_cast<NullTest>(node), names, plan->parameters.get());
                    } else if (nodeTag(node) == T_SubLink) {
                        child_expr = transform_sublink_expr(pg_ptr_cast<SubLink>(node), names, plan);
                    } else {
                        error_ = core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"Unsupported expression: unknown expr type in transform_a_expr",
                                             resource_});
                        return;
                    }
                    // A child transform may fail (unsupported sub-query form → nullptr + error_).
                    // Guard before child_expr->group() below null-derefs.
                    if (has_error() || !child_expr) {
                        return;
                    }
                    if (expr->group() == child_expr->group()) {
                        auto comp_expr = reinterpret_cast<const compare_expression_ptr&>(child_expr);
                        if (expr->type() == comp_expr->type()) {
                            for (auto& child : comp_expr->children()) {
                                expr->append_child(child);
                            }
                            return;
                        }
                    }
                    expr->append_child(child_expr);
                };

                append(node->lexpr);
                append(node->rexpr);
                return expr;
            }
            case AEXPR_OP: {
                if (nodeTag(node) == T_A_Indirection) {
                    return transform_a_indirection(pg_ptr_cast<A_Indirection>(node), names, plan);
                }
                // The operator symbol is the LAST element of the name list — a schema-qualified operator
                // (OPERATOR(pg_catalog.<>)) prepends the schema, so read .back(), not .front().
                if (!node->name || nodeTag(node->name->lst.back().data) != T_String) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"Unsupported expr in transform_a_exr", resource_});
                    return nullptr;
                }
                auto op_str = std::string_view(strVal(node->name->lst.back().data));

                // Check if this is arithmetic (+, -, *, /, %)
                if (is_arithmetic_operator(op_str)) {
                    return transform_a_expr_arithmetic(node, names, plan->parameters.get());
                }

                // Check for LIKE / NOT LIKE
                // LIKE ~~ / NOT LIKE !~~ / ILIKE ~~* / NOT ILIKE !~~*.
                if (op_str == "~~" || op_str == "!~~" || op_str == "~~*" || op_str == "!~~*") {
                    column_ref_t key_left(resource_);
                    if (nodeTag(node->lexpr) == T_ColumnRef) {
                        key_left = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node->lexpr), names);
                    } else if (nodeTag(node->lexpr) == T_A_Indirection) {
                        auto res = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node->lexpr), names);
                        if (res.has_error()) {
                            error_ = res.error();
                            return nullptr;
                        }
                        key_left = std::move(res.value());
                    } else {
                        error_ =
                            core::error_t(core::error_code_t::sql_parse_error,
                                          std::pmr::string{"LIKE: left side must be a column reference", resource_});
                        return nullptr;
                    }
                    key_left.deduce_side(names);
                    auto raw_val = get_value(resource_, node->rexpr);
                    if (raw_val.has_error()) {
                        error_ = raw_val.error();
                        return nullptr;
                    }
                    if (raw_val.value().is_null()) {
                        // `col [NOT] [I]LIKE NULL` is UNKNOWN for every row (three-valued logic,
                        // and NOT UNKNOWN is still UNKNOWN) -> zero rows for BOTH the plain and
                        // the negated form. all_false is the canonical no-rows predicate (the
                        // scan short-circuits it); it must NOT be wrapped in union_not here —
                        // that would turn match-nothing into match-everything.
                        return make_compare_expression(resource_, compare_type::all_false);
                    }
                    // LIKE patterns are text-only. Reading a numeric/bool payload through
                    // value<std::string_view>() below treats it as a std::string pointer and
                    // crashes before the executor's regex operand guards can report an error.
                    if (!types::is_string(raw_val.value().type().type())) {
                        error_ = core::error_t(core::error_code_t::sql_parse_error,
                                               std::pmr::string{"LIKE: right side must be a string", resource_});
                        return nullptr;
                    }
                    auto pattern = std::string(raw_val.value().value<std::string_view>());
                    auto param_id = plan->parameters->add_parameter(types::logical_value_t(resource_, pattern));
                    const bool icase = (op_str == "~~*" || op_str == "!~~*");  // ILIKE / NOT ILIKE
                    const bool negate = (op_str == "!~~" || op_str == "!~~*"); // NOT LIKE / NOT ILIKE

                    // Convert to regex function call. NOT [I]LIKE is the `n` flag rather than a
                    // union_not: the match itself inverts, so a NULL subject stays UNKNOWN (the row
                    // is dropped, as PostgreSQL does) without an is_not_null guard around it.
                    std::string flags{"l"};
                    if (icase) {
                        flags += 'i';
                    }
                    if (negate) {
                        flags += 'n';
                    }
                    std::pmr::vector<expressions::param_storage> args{resource_};
                    args.emplace_back(key_left.field);
                    args.emplace_back(param_id);
                    args.emplace_back(plan->parameters->add_parameter(types::logical_value_t(resource_, flags)));
                    return make_function_expression(resource_, "regexp_like", std::move(args));
                }

                // JSONB key existence: '?' / '?|' / '?&'. Desugars to IS NOT NULL.
                if (op_str == "?" || op_str == "?|" || op_str == "?&") {
                    return transform_jsonb_exists(node, names, plan->parameters.get(), op_str);
                }

                auto comp_type = get_compare_type(op_str);
                if (comp_type == compare_type::invalid) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"invalid compare operand", resource_});
                    return nullptr;
                }

                // Set when a comparison operand is an ARRAY(SELECT ...): such a compare must be marked
                // unfoldable so it evaluates in-memory (length-aware array equality) and is never pushed to
                // the storage constant_filter path (which does no length reconcile).
                bool array_operand = false;
                auto get_arg = [this, &names, &plan, &array_operand](Node* node) -> param_storage {
                    switch (nodeTag(node)) {
                        case T_ColumnRef: {
                            if (auto corr = try_lateral_correlate(pg_ptr_cast<ColumnRef>(node), names)) {
                                return *corr;
                            }
                            auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node), names);
                            key.deduce_side(names);
                            return key.field;
                        }
                        case T_A_Indirection: {
                            auto res = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node), names);
                            if (res.has_error()) {
                                error_ = res.error();
                                return nullptr;
                            }
                            auto key = std::move(res.value());
                            key.deduce_side(names);
                            return key.field;
                        }
                        case T_TypeCast: {
                            auto cast = pg_ptr_cast<TypeCast>(node);
                            if (cast->arg && nodeTag(cast->arg) == T_ColumnRef) {
                                auto target_type_res = get_type(resource_, cast->typeName);
                                if (target_type_res.has_error()) {
                                    error_ = target_type_res.error();
                                    return nullptr;
                                }
                                auto col_ref = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(cast->arg), names);
                                col_ref.deduce_side(names);
                                note_cast_type(target_type_res.value());
                                auto conversion = make_cast_expression(resource_,
                                                                       param_storage{std::move(col_ref.field)},
                                                                       target_type_res.value(),
                                                                       casts::cast_t{},
                                                                       cast->try_cast ? casts::cast_kind::try_cast
                                                                                      : casts::cast_kind::cast);
                                return param_storage{expressions::expression_ptr{conversion}};
                            }
                            // '<jsonb nav chain> ::? type' in a predicate, e.g.
                            // WHERE m -> 'a' ->> 'b' ::? bigint > 0.
                            if (cast->arg && nodeTag(cast->arg) == T_A_Expr) {
                                auto* sub = pg_ptr_cast<A_Expr>(cast->arg);
                                if (sub->kind == AEXPR_OP && sub->name &&
                                    nodeTag(sub->name->lst.front().data) == T_String &&
                                    is_jsonb_nav_operator(strVal(sub->name->lst.front().data))) {
                                    auto target_type_res = get_type(resource_, cast->typeName);
                                    if (target_type_res.has_error()) {
                                        error_ = target_type_res.error();
                                        return nullptr;
                                    }
                                    expressions::key_t k{resource_};
                                    if (!resolve_jsonb_scalar_key(sub, names, k)) {
                                        return nullptr;
                                    }
                                    k.set_cast_type(target_type_res.value());
                                    if (cast->variant_select) {
                                        k.set_variant_select(true);
                                    }
                                    return k;
                                }
                            }
                            if (cast->arg && nodeTag(cast->arg) == T_A_Const) {
                                auto target_type_res = get_type(resource_, cast->typeName);
                                if (target_type_res.has_error()) {
                                    error_ = target_type_res.error();
                                    return nullptr;
                                }
                                auto value_res = get_value(resource_, node);
                                if (value_res.has_error()) {
                                    error_ = value_res.error();
                                    return nullptr;
                                }
                                bool converts = value_res.value().type() != target_type_res.value();
                                param_storage bound{plan->parameters->add_parameter(std::move(value_res.value()))};
                                if (!converts) {
                                    return bound;
                                }
                                note_cast_type(target_type_res.value());
                                auto conversion = make_cast_expression(resource_,
                                                                       bound,
                                                                       target_type_res.value(),
                                                                       casts::cast_t{},
                                                                       cast->try_cast ? casts::cast_kind::try_cast
                                                                                      : casts::cast_kind::cast);
                                return param_storage{expressions::expression_ptr{conversion}};
                            }
                            return add_param_value(node, plan->parameters.get());
                        }
                        case T_ParamRef:
                        case T_A_Const:
                        case T_RowExpr:
                        case T_A_ArrayExpr:
                            return add_param_value(node, plan->parameters.get());
                        case T_FuncCall:
                            return transform_a_expr_func(pg_ptr_cast<FuncCall>(node), names, plan->parameters.get());
                        case T_A_Expr: {
                            auto sub = pg_ptr_cast<A_Expr>(node);
                            if (sub->kind == AEXPR_OP) {
                                auto sub_op = std::string_view(strVal(sub->name->lst.front().data));
                                if (is_arithmetic_operator(sub_op)) {
                                    return transform_a_expr_arithmetic(sub, names, plan->parameters.get());
                                }
                                if (is_jsonb_nav_operator(sub_op)) {
                                    expressions::key_t k{resource_};
                                    if (!resolve_jsonb_scalar_key(sub, names, k)) {
                                        return nullptr;
                                    }
                                    return k;
                                }
                            }
                            error_ = core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"unrecognized expression in transform_a_expr", resource_});
                            return nullptr;
                        }
                        case T_MinMaxExpr: {
                            auto expr = pg_ptr_cast<MinMaxExpr>(node);
                            std::string funcname = expr->op == MinMaxOp::IS_GREATEST ? "greatest" : "least";
                            std::pmr::vector<param_storage> args{resource_};
                            args.reserve(expr->args->lst.size());
                            for (const auto& arg : expr->args->lst) {
                                if (nodeTag(arg.data) == T_ColumnRef) {
                                    auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg.data), names);
                                    key.deduce_side(names);
                                    args.emplace_back(std::move(key.field));
                                } else if (nodeTag(arg.data) == T_A_Indirection) {
                                    auto res =
                                        indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(arg.data), names);
                                    if (res.has_error()) {
                                        error_ = res.error();
                                        return nullptr;
                                    }
                                    auto key = std::move(res.value());
                                    key.deduce_side(names);
                                    args.emplace_back(std::move(key.field));
                                } else if (nodeTag(arg.data) == T_FuncCall) {
                                    args.emplace_back(transform_a_expr_func(pg_ptr_cast<FuncCall>(arg.data),
                                                                            names,
                                                                            plan->parameters.get()));
                                } else if (nodeTag(arg.data) == T_A_Expr) {
                                    auto sub = pg_ptr_cast<A_Expr>(arg.data);
                                    if (sub->kind == AEXPR_OP &&
                                        is_arithmetic_operator(strVal(sub->name->lst.front().data))) {
                                        args.emplace_back(
                                            transform_a_expr_arithmetic(sub, names, plan->parameters.get()));
                                    } else {
                                        args.emplace_back(
                                            add_param_value(pg_ptr_cast<Node>(arg.data), plan->parameters.get()));
                                    }
                                } else {
                                    args.emplace_back(
                                        add_param_value(pg_ptr_cast<Node>(arg.data), plan->parameters.get()));
                                }
                            }
                            return make_function_expression(resource_, std::move(funcname), std::move(args));
                        }
                        case T_SubLink: {
                            auto sub = pg_ptr_cast<SubLink>(node);
                            // Transform first so nested sub_queries/sub_query_results are appended before this
                            // level's entries — the executor runs sub_queries front-to-back and
                            // sub_query_results[i] must correspond to sub_queries[i]. Dispatch on the sub-link
                            // kind: a bare comparison operand only ever carries EXPR / EXISTS / ARRAY (ANY/ALL
                            // arrive as whole predicates via transform_sublink_expr and reach here only through
                            // unusual parenthesised nesting).
                            auto param_id = plan->parameters->add_parameter(
                                types::logical_value_t{resource_, types::logical_type::NA});
                            // Save/restore the pending internal-aggregate stash around each inner
                            // transform: the inner transform_select epilogue flushes + clears the
                            // stash, which would steal this level's SELECT-list aggregates.
                            switch (sub->subLinkType) {
                                case EXPR_SUBLINK: {
                                    auto prev_pending = std::move(pending_internal_aggs_);
                                    pending_internal_aggs_.clear();
                                    auto sub_node = transform(*sub->subselect, plan);
                                    pending_internal_aggs_ = std::move(prev_pending);
                                    plan->sub_query_results.emplace_back(&vector::compact_to_single_value, param_id);
                                    plan->sub_queries.emplace_back(std::move(sub_node));
                                    return param_id;
                                }
                                case EXISTS_SUBLINK: {
                                    // `col = EXISTS (SELECT ...)`: compare against the boolean EXISTS result.
                                    auto prev_pending = std::move(pending_internal_aggs_);
                                    pending_internal_aggs_.clear();
                                    auto sub_node = transform(*sub->subselect, plan);
                                    pending_internal_aggs_ = std::move(prev_pending);
                                    plan->sub_query_results.emplace_back(&vector::compact_to_bool_value, param_id);
                                    plan->sub_queries.emplace_back(std::move(sub_node));
                                    return param_id;
                                }
                                case ARRAY_SUBLINK: {
                                    // `col = ARRAY (SELECT ...)`: array equality against the compacted array.
                                    // array_equality lets the executor rebuild a 0-row result as a typed empty
                                    // {}; array_operand marks the enclosing compare unfoldable (see below).
                                    auto prev_pending = std::move(pending_internal_aggs_);
                                    pending_internal_aggs_.clear();
                                    auto sub_node = transform(*sub->subselect, plan);
                                    pending_internal_aggs_ = std::move(prev_pending);
                                    plan->sub_query_results.emplace_back(&vector::compact_to_array_value,
                                                                         param_id,
                                                                         /*boolean_required=*/false,
                                                                         /*array_equality=*/true);
                                    plan->sub_queries.emplace_back(std::move(sub_node));
                                    array_operand = true;
                                    return param_id;
                                }
                                default:
                                    error_ = core::error_t(
                                        core::error_code_t::sql_parse_error,
                                        std::pmr::string{"unsupported subquery form as a comparison operand",
                                                         resource_});
                                    return nullptr;
                            }
                        }
                        default:
                            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                                   std::pmr::string{"Unsupported expression", resource_});
                            return nullptr;
                    }
                };

                param_storage left = get_arg(node->lexpr);
                param_storage right = get_arg(node->rexpr);
                auto cmp = make_compare_expression(resource_, comp_type, left, right);
                if (array_operand) {
                    // Route array equality to the in-memory operator_match (length-aware
                    // logical_value_t::operator==), never the storage constant_filter pushdown.
                    cmp->make_unfoldable();
                }
                return cmp;
            }
            case AEXPR_NOT: {
                expression_ptr right;
                if (nodeTag(node->rexpr) == T_A_Expr) {
                    right = transform_a_expr(pg_ptr_cast<A_Expr>(node->rexpr), names, plan);
                } else if (nodeTag(node->rexpr) == T_A_Indirection) {
                    right = transform_a_indirection(pg_ptr_cast<A_Indirection>(node->rexpr), names, plan);
                } else if (nodeTag(node->rexpr) == T_FuncCall) {
                    right = transform_a_expr_func(pg_ptr_cast<FuncCall>(node->rexpr), names, plan->parameters.get());
                } else if (nodeTag(node->rexpr) == T_SubLink) {
                    right = transform_sublink_expr(pg_ptr_cast<SubLink>(node->rexpr), names, plan);
                } else {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"Unsupported expression: unknown expr type in transform_a_expr", resource_});
                    return nullptr;
                }
                // A child transform may have failed (unsupported sub-query form → nullptr + error_);
                // guard before right->group() null-derefs.
                if (has_error() || !right) {
                    return nullptr;
                }
                // De Morgan for a negated membership sub-query: `NOT (x op ANY S)` ≡ `x !op ALL S` and
                // `NOT (x op ALL S)` ≡ `x !op ANY S` (!op = the scalar complement). This preserves the
                // three-valued NULL semantics: a NULL element of S makes a non-matched outer row UNKNOWN
                // under BOTH the membership and its negation, so the row is dropped either way — the
                // classic `x NOT IN (SELECT ... containing a NULL)` correctly yields NO rows. A plain
                // union_not over the boolean membership would instead see only "false" (NULLs already
                // folded away) and flip it to true, wrongly keeping the row. Regex (LIKE/ILIKE) ANY/ALL
                // keeps its own per-element negation (regex_negate) and is left to the union_not wrapper.
                if (right->group() == expression_group::compare) {
                    auto& membership = reinterpret_cast<compare_expression_ptr&>(right);
                    const auto ctype = membership->type();
                    if ((ctype == compare_type::any || ctype == compare_type::all) &&
                        membership->inner_op() != compare_type::regex) {
                        const auto negated = negate_scalar_compare(membership->inner_op());
                        if (negated != compare_type::invalid) {
                            membership->set_type(ctype == compare_type::any ? compare_type::all : compare_type::any);
                            membership->set_inner_op(negated);
                            return right;
                        }
                    }
                }
                if (right->group() == expression_group::compare) {
                    auto& inner = reinterpret_cast<const compare_expression_ptr&>(right);
                    if (inner->type() == compare_type::union_not && inner->children().size() == 1) {
                        return inner->children().front();
                    }
                }
                auto expr = make_compare_union_expression(resource_, compare_type::union_not);
                expr->append_child(right);
                return expr;
            }
            case AEXPR_IN: {
                // col IN (1,2,3) → union_or(col=1, col=2, col=3)
                // col NOT IN (1,2,3) → union_and(col<>1, col<>2, col<>3)
                if (nodeTag(node->lexpr) != T_ColumnRef && nodeTag(node->lexpr) != T_A_Indirection) {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"IN expression: left side must be a column reference", resource_});
                    return nullptr;
                }
                auto key_in_res = node_to_field(resource_, node->lexpr, names);
                if (key_in_res.has_error()) {
                    error_ = key_in_res.error();
                    return nullptr;
                }
                auto key_in = std::move(key_in_res.value());
                key_in.deduce_side(names);

                auto op_str = std::string(strVal(node->name->lst.front().data));
                bool is_not_in = (op_str == "<>");
                auto union_type = is_not_in ? compare_type::union_and : compare_type::union_or;
                auto cmp_type = is_not_in ? compare_type::ne : compare_type::eq;

                auto list_node = pg_ptr_cast<List>(node->rexpr);
                auto union_expr = make_compare_union_expression(resource_, union_type);
                for (const auto& elem : list_node->lst) {
                    auto param_id = add_param_value(pg_ptr_cast<Node>(elem.data), plan->parameters.get());
                    union_expr->append_child(make_compare_expression(resource_, cmp_type, key_in.field, param_id));
                }
                return union_expr;
            }
            default:
                error_ = core::error_t(
                    core::error_code_t::sql_parse_error,
                    std::pmr::string{"Unsupported node type: " + expr_kind_to_string(node->kind), resource_});
                return nullptr;
        }
    }

    expression_ptr
    transformer::transform_predicate(Node* node, const name_collection_t& names, logical_plan::execution_plan_t* plan) {
        switch (nodeTag(node)) {
            case T_A_Expr:
                return transform_a_expr(pg_ptr_cast<A_Expr>(node), names, plan);
            case T_A_Indirection:
                return transform_a_indirection(pg_ptr_cast<A_Indirection>(node), names, plan);
            case T_FuncCall:
                return transform_a_expr_func(pg_ptr_cast<FuncCall>(node), names, plan->parameters.get());
            case T_NullTest:
                return transform_null_test(pg_ptr_cast<NullTest>(node), names, plan->parameters.get());
            case T_SubLink:
                return transform_sublink_expr(pg_ptr_cast<SubLink>(node), names, plan);
            case T_TypeCast: {
                // Boolean literal: TRUE/FALSE parse as TypeCast(A_Const{"t"|"f"}, bool)
                // and reduce to the constant all_true / all_false predicate.
                auto cast = pg_ptr_cast<TypeCast>(node);
                if (cast->arg && nodeTag(cast->arg) == T_A_Const) {
                    auto constant = pg_ptr_cast<A_Const>(cast->arg);
                    auto target_type_res = get_type(resource_, cast->typeName);
                    if (!target_type_res.has_error() &&
                        target_type_res.value().type() == types::logical_type::BOOLEAN &&
                        constant->val.type == T_String) {
                        std::string_view literal = strVal(&constant->val);
                        if (literal == "t") {
                            return make_compare_expression(resource_, compare_type::all_true);
                        }
                        if (literal == "f") {
                            return make_compare_expression(resource_, compare_type::all_false);
                        }
                    }
                }
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"Unsupported predicate expression", resource_});
                return nullptr;
            }
            default:
                error_ = core::error_t(
                    core::error_code_t::sql_parse_error,
                    std::pmr::string{"Unsupported predicate expression: " + node_tag_to_string(nodeTag(node)),
                                     resource_});
                return nullptr;
        }
    }

    expression_ptr transformer::transform_sublink_expr(SubLink* node,
                                                       const name_collection_t& names,
                                                       logical_plan::execution_plan_t* plan) {
        switch (node->subLinkType) {
            case EXISTS_SUBLINK: {
                auto param_id1 = plan->parameters->add_parameter(types::logical_value_t{resource_, true});
                auto param_id2 =
                    plan->parameters->add_parameter(types::logical_value_t{resource_, types::logical_type::NA});
                // Transform before appending so nested sub_queries/sub_query_results come first.
                // Save/restore the pending internal-aggregate stash so the inner epilogue's
                // flush + clear does not steal this level's SELECT-list aggregates.
                auto prev_pending = std::move(pending_internal_aggs_);
                pending_internal_aggs_.clear();
                auto sub_node = transform(*node->subselect, plan);
                pending_internal_aggs_ = std::move(prev_pending);
                plan->sub_query_results.emplace_back(&vector::compact_to_bool_value, param_id2);
                plan->sub_queries.emplace_back(std::move(sub_node));
                auto expr = make_compare_expression(resource_, compare_type::eq, param_id1, param_id2);
                expr->make_unfoldable();
                return expr;
            }
            case NOT_EXISTS_SUBLINK:
                break;
            case ALL_SUBLINK:
            case ANY_SUBLINK: {
                if (nodeTag(node->testexpr) != T_ColumnRef && nodeTag(node->testexpr) != T_A_Indirection) {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"IN expression: left side must be a column reference", resource_});
                    return nullptr;
                }
                auto key_res = node_to_field(resource_, node->testexpr, names);
                if (key_res.has_error()) {
                    error_ = key_res.error();
                    return nullptr;
                }
                auto key = std::move(key_res.value());
                key.deduce_side(names);
                // Operator symbol is last (schema-qualified OPERATOR(schema.op) prepends the schema).
                auto op_str = std::string_view(strVal(node->operName->lst.back().data));
                // LIKE family: ~~ (LIKE), ~~* (ILIKE), !~~ (NOT LIKE), !~~* (NOT ILIKE) all map to a regex
                // inner_op whose LIKE glob is converted per element at eval time (regex_like), matched
                // case-insensitively for ILIKE (regex_icase), and negated per element before the any/all
                // fold for NOT LIKE (regex_negate). Everything else (=, <>, <, ~~->regexp, ...) goes through
                // get_compare_type; an unmapped operator is rejected (never silently `=`).
                compare_type inner_op;
                bool re_like = false;
                bool re_icase = false;
                bool re_negate = false;
                if (op_str == "~~" || op_str == "~~*" || op_str == "!~~" || op_str == "!~~*") {
                    inner_op = compare_type::regex;
                    re_like = true;
                    re_icase = (op_str == "~~*" || op_str == "!~~*");
                    re_negate = (op_str == "!~~" || op_str == "!~~*");
                } else {
                    inner_op = get_compare_type(op_str);
                    if (inner_op == compare_type::invalid) {
                        error_ = core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"unsupported operator in ANY/ALL subquery comparison", resource_});
                        return nullptr;
                    }
                }
                auto param_id =
                    plan->parameters->add_parameter(types::logical_value_t{resource_, types::logical_type::NA});
                // Transform before appending so nested sub_queries/sub_query_results come first.
                // Save/restore the pending internal-aggregate stash so the inner epilogue's
                // flush + clear does not steal this level's SELECT-list aggregates.
                auto prev_pending = std::move(pending_internal_aggs_);
                pending_internal_aggs_.clear();
                auto sub_node = transform(*node->subselect, plan);
                pending_internal_aggs_ = std::move(prev_pending);
                plan->sub_query_results.emplace_back(&vector::compact_to_array_value, param_id);
                plan->sub_queries.emplace_back(std::move(sub_node));
                auto ctype = node->subLinkType == ANY_SUBLINK ? compare_type::any : compare_type::all;
                auto expr = make_compare_expression(resource_, ctype, key.field, param_id);
                expr->set_inner_op(inner_op);
                if (inner_op == compare_type::regex) {
                    std::string flags;
                    if (re_like) {
                        flags += 'l';
                    }
                    if (re_icase) {
                        flags += 'i';
                    }
                    if (re_negate) {
                        flags += 'n';
                    }
                    expr->set_regex_flags(plan->parameters->add_parameter(types::logical_value_t(resource_, flags)));
                }
                // ANY/ALL pushes into the disk scan as a conjunction of per-element filters
                // (transform_predicate: constant_filter for comparisons, regex_filter for LIKE/ILIKE). The
                // sub-query array is bound once (the executor runs sub_queries before the main plan), so a
                // once-built filter is correct for these non-correlated arrays. Comparisons, positive AND
                // negated LIKE/ILIKE ANY|ALL all push down: NOT LIKE ALL -> conjunction_not, NOT LIKE ANY ->
                // OR of per-element conjunction_not, both guarded by is_not_null (see transform_predicate).
                return expr;
            }
            case EXPR_SUBLINK: {
                // Scalar sub-query as a bare boolean predicate: `WHERE (SELECT flag ...)`. PostgreSQL
                // supports this; it is EXISTS-shaped. Compact the sub-query to its single scalar value and
                // compare it against true. (>1 row errors inside compact_to_single_value, as PostgreSQL does.)
                auto param_true = plan->parameters->add_parameter(types::logical_value_t{resource_, true});
                auto param_result =
                    plan->parameters->add_parameter(types::logical_value_t{resource_, types::logical_type::NA});
                // Transform before appending so nested sub_queries/sub_query_results come first.
                // Save/restore the pending internal-aggregate stash so the inner epilogue's
                // flush + clear does not steal this level's SELECT-list aggregates.
                auto prev_pending = std::move(pending_internal_aggs_);
                pending_internal_aggs_.clear();
                auto sub_node = transform(*node->subselect, plan);
                pending_internal_aggs_ = std::move(prev_pending);
                // boolean_required: WHERE's argument must be type boolean (PostgreSQL). The
                // executor rejects a non-boolean static output type of this sub-query before
                // binding, so `WHERE (SELECT 1)` errors instead of silently coercing to bool.
                plan->sub_query_results.emplace_back(&vector::compact_to_single_value,
                                                     param_result,
                                                     /*boolean_required=*/true);
                plan->sub_queries.emplace_back(std::move(sub_node));
                auto expr = make_compare_expression(resource_, compare_type::eq, param_true, param_result);
                expr->make_unfoldable();
                return expr;
            }
            case ROWCOMPARE_SUBLINK:
            case ARRAY_SUBLINK:
            case CTE_SUBLINK:
            case INITPLAN_FUNC_SUBLINK:
                break;
        }
        // Unsupported / parser-unreachable sub-query form. In this fork the grammar never emits
        // NOT_EXISTS/ROWCOMPARE/CTE/INITPLAN_FUNC (NOT EXISTS is AEXPR_NOT+EXISTS; WITH rides withClause);
        // ARRAY(SELECT ...) as a predicate is meaningless (array != bool) and target-list ARRAY is a
        // separate deferred feature.
        error_ = core::error_t(core::error_code_t::sql_parse_error,
                               std::pmr::string{"unsupported subquery expression in this context", resource_});
        return nullptr;
    }

    expression_ptr transformer::transform_a_expr_func(FuncCall* node,
                                                      const name_collection_t& names,
                                                      logical_plan::parameter_node_t* params) {
        std::string funcname = strVal(node->funcname->lst.front().data);
        std::pmr::vector<param_storage> args;
        args.reserve(node->args->lst.size());
        // create_value_getter rejects keys whose side is still undefined at runtime.
        // For unqualified column refs inside a function call in a non-JOIN query
        // (no right table set), default the side to left so the predicate can read
        // the value. Joins keep the original ambiguity-aware behaviour.
        const bool no_right_side = names.right_name.empty() && names.right_alias.empty();
        auto pin_side_to_left_if_unset = [no_right_side](expressions::key_t& field) {
            if (no_right_side && field.side() == expressions::side_t::undefined) {
                field.set_side(expressions::side_t::left);
            }
        };
        for (const auto& arg : node->args->lst) {
            if (nodeTag(arg.data) == T_ColumnRef) {
                // Correlated outer column as a function argument: lower to the
                // correlation parameter (read live per row by the function predicate /
                // projection evaluators, so the lateral per-outer-row rebind holds).
                if (auto corr = try_lateral_correlate(pg_ptr_cast<ColumnRef>(arg.data), names)) {
                    args.emplace_back(*corr);
                    continue;
                }
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg.data), names);
                key.deduce_side(names);
                pin_side_to_left_if_unset(key.field);
                args.emplace_back(std::move(key.field));
            } else if (nodeTag(arg.data) == T_A_Indirection) {
                auto res = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(arg.data), names);
                if (res.has_error()) {
                    error_ = res.error();
                    return nullptr;
                }
                auto key = std::move(res.value());
                key.deduce_side(names);
                pin_side_to_left_if_unset(key.field);
                args.emplace_back(std::move(key.field));
            } else if (nodeTag(arg.data) == T_FuncCall) {
                args.emplace_back(transform_a_expr_func(pg_ptr_cast<FuncCall>(arg.data), names, params));
            } else if (nodeTag(arg.data) == T_A_Expr) {
                auto sub = pg_ptr_cast<A_Expr>(arg.data);
                if (sub->kind == AEXPR_OP && is_arithmetic_operator(strVal(sub->name->lst.front().data))) {
                    args.emplace_back(transform_a_expr_arithmetic(sub, names, params));
                } else {
                    args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
                }
            } else {
                args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
            }
        }
        return make_function_expression(resource_, std::move(funcname), std::move(args));
    }

    expression_ptr transformer::transform_a_indirection(A_Indirection* node,
                                                        const name_collection_t& names,
                                                        logical_plan::execution_plan_t* plan) {
        if (node->arg->type == T_A_Expr) {
            return transform_a_expr(pg_ptr_cast<A_Expr>(node->arg), names, plan);
        } else if (node->arg->type == T_A_Indirection) {
            return transform_a_indirection(pg_ptr_cast<A_Indirection>(node->arg), names, plan);
        } else if (node->arg->type == T_FuncCall) {
            return transform_a_expr_func(pg_ptr_cast<FuncCall>(node->arg), names, plan->parameters.get());
        } else {
            error_ =
                core::error_t(core::error_code_t::sql_parse_error,
                              std::pmr::string{"Unsupported node type: " + node_tag_to_string(node->type), resource_});
            return nullptr;
        }
    }

    bool transformer::resolve_jsonb_base(Node* lexpr,
                                         const name_collection_t& names,
                                         std::pmr::vector<std::pmr::string>& segments,
                                         expressions::side_t& side) {
        if (nodeTag(lexpr) == T_ColumnRef) {
            auto* ref = pg_ptr_cast<ColumnRef>(lexpr);
            auto& lst = ref->fields->lst;
            if (lst.size() == 1 && nodeTag(lst.back().data) == T_String) {
                std::string base_name = strVal(lst.back().data);
                if (names.is_left_table(base_name)) {
                    side = expressions::side_t::left; // bare table name -> document root
                } else if (names.is_right_table(base_name)) {
                    side = expressions::side_t::right;
                } else {
                    segments.emplace_back(std::pmr::string{base_name.c_str(), resource_}); // column at root
                }
            } else {
                auto cr = columnref_to_field(resource_, ref, names);
                cr.deduce_side(names);
                side = cr.field.side();
                for (const auto& s : cr.field.storage()) {
                    segments.emplace_back(s);
                }
            }
            return true;
        }
        if (nodeTag(lexpr) == T_A_Indirection) {
            auto res = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(lexpr), names);
            if (res.has_error()) {
                error_ = res.error();
                return false;
            }
            auto cr = std::move(res.value());
            cr.deduce_side(names);
            side = cr.field.side();
            for (const auto& s : cr.field.storage()) {
                segments.emplace_back(s);
            }
            return true;
        }
        error_ = core::error_t(core::error_code_t::sql_parse_error,
                               std::pmr::string{"unsupported base operand for jsonb operator", resource_});
        return false;
    }

    bool transformer::collect_jsonb_path(A_Expr* node,
                                         const name_collection_t& names,
                                         std::pmr::vector<std::pmr::string>& segments,
                                         expressions::side_t& side) {
        auto op = std::string_view(strVal(node->name->lst.front().data));

        // Left operand: either a deeper jsonb navigation step, or the base
        // (table name / column) the whole chain is rooted at.
        Node* lexpr = node->lexpr;
        if (nodeTag(lexpr) == T_A_Expr) {
            auto* sub = pg_ptr_cast<A_Expr>(lexpr);
            if (sub->kind == AEXPR_OP && sub->name && nodeTag(sub->name->lst.front().data) == T_String &&
                is_jsonb_nav_operator(strVal(sub->name->lst.front().data))) {
                if (!collect_jsonb_path(sub, names, segments, side)) {
                    return false;
                }
            } else {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"unsupported left operand in jsonb operator chain", resource_});
                return false;
            }
        } else if (!resolve_jsonb_base(lexpr, names, segments, side)) {
            return false;
        }

        // Right operand: the key(s) this step navigates into.
        std::string key_str = get_str_value(node->rexpr);
        if (has_error()) {
            return false;
        }
        if (jsonb_op_takes_path(op)) {
            // '#>' / '#>>' / '#-' : a whole path. Accept PG array '{a,b}' or dotted 'a.b'.
            for (auto& seg : jsonb_path::split_operand(key_str, resource_)) {
                segments.emplace_back(std::move(seg));
            }
        } else {
            // '->' / '->>' : a single key, taken verbatim (no splitting).
            segments.emplace_back(std::pmr::string{key_str.c_str(), resource_});
        }
        return true;
    }

    bool transformer::jsonb_lhs_is_table(Node* node, const name_collection_t& names) const {
        if (!node || nodeTag(node) != T_ColumnRef) {
            return false;
        }
        auto& lst = pg_ptr_cast<ColumnRef>(node)->fields->lst;
        if (lst.size() != 1 || nodeTag(lst.back().data) != T_String) {
            return false;
        }
        std::string nm = strVal(lst.back().data);
        return names.is_left_table(nm) || names.is_right_table(nm);
    }

    bool
    transformer::resolve_jsonb_prefix_key(A_Expr* node, const name_collection_t& names, expressions::key_t& out_key) {
        std::pmr::vector<std::pmr::string> segments(resource_);
        expressions::side_t side = expressions::side_t::undefined;
        if (!collect_jsonb_path(node, names, segments, side)) {
            return false;
        }
        if (segments.empty()) {
            error_ =
                core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{"empty jsonb path", resource_});
            return false;
        }
        out_key = expressions::key_t(resource_, jsonb_path::flatten(segments, resource_), side);
        if (out_key.side() == expressions::side_t::undefined && names.right_name.empty() && names.right_alias.empty()) {
            out_key.set_side(expressions::side_t::left);
        }
        return true;
    }

    bool
    transformer::resolve_jsonb_scalar_key(A_Expr* node, const name_collection_t& names, expressions::key_t& out_key) {
        auto op = std::string_view(strVal(node->name->lst.front().data));
        if (!jsonb_nav_returns_scalar(op)) {
            // '->' / '#>' return jsonb (a sub-table) — only valid in a relation
            // position (FROM/JOIN), not as a scalar in SELECT/WHERE.
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"jsonb operator '" + std::string(op) +
                                                        "' returns a table and cannot be used as a scalar value; "
                                                        "terminate the chain with '->>' or '#>>'",
                                                    resource_});
            return false;
        }
        // The only difference from a prefix key is the scalar-vs-table guard above;
        // the flattening itself is identical, so share it.
        return resolve_jsonb_prefix_key(node, names, out_key);
    }

    expression_ptr transformer::transform_jsonb_exists(A_Expr* node,
                                                       const name_collection_t& names,
                                                       logical_plan::parameter_node_t* params,
                                                       std::string_view op) {
        // Left operand: document (table root) or a navigation prefix.
        std::pmr::vector<std::pmr::string> prefix(resource_);
        expressions::side_t side = expressions::side_t::undefined;
        Node* lexpr = node->lexpr;
        if (nodeTag(lexpr) == T_A_Expr) {
            auto* sub = pg_ptr_cast<A_Expr>(lexpr);
            if (sub->kind == AEXPR_OP && sub->name && nodeTag(sub->name->lst.front().data) == T_String &&
                is_jsonb_nav_operator(strVal(sub->name->lst.front().data))) {
                if (!collect_jsonb_path(sub, names, prefix, side)) {
                    return nullptr;
                }
            } else {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"unsupported left operand for jsonb '?'", resource_});
                return nullptr;
            }
        } else if (!resolve_jsonb_base(lexpr, names, prefix, side)) {
            return nullptr;
        }

        // Right operand: a single key ('?') or a text array '{x,y}' ('?|','?&').
        std::string rhs = get_str_value(node->rexpr);
        if (has_error()) {
            return nullptr;
        }
        std::pmr::vector<std::pmr::string> keys(resource_);
        auto push_key = [&](const std::string& raw) {
            size_t b = raw.find_first_not_of(" \"");
            size_t e = raw.find_last_not_of(" \"");
            if (b == std::string::npos) {
                return;
            }
            keys.emplace_back(std::pmr::string{raw.substr(b, e - b + 1).c_str(), resource_});
        };
        if (op == "?") {
            keys.emplace_back(std::pmr::string{rhs.c_str(), resource_});
        } else {
            std::string body = rhs;
            if (body.size() >= 2 && body.front() == '{' && body.back() == '}') {
                body = body.substr(1, body.size() - 2);
            }
            size_t start = 0;
            while (true) {
                size_t comma = body.find(',', start);
                push_key(body.substr(start, comma - start));
                if (comma == std::string::npos) {
                    break;
                }
                start = comma + 1;
            }
        }

        if (keys.empty()) {
            // '?&' over no keys is vacuously true; '?|' over no keys is false.
            return make_compare_expression(params->parameters().resource(),
                                           op == "?&" ? compare_type::all_true : compare_type::all_false);
        }

        expressions::side_t use_side = side;
        if (use_side == expressions::side_t::undefined && names.right_name.empty() && names.right_alias.empty()) {
            use_side = expressions::side_t::left;
        }
        auto build_exists = [&](const std::pmr::string& k) -> compare_expression_ptr {
            std::pmr::vector<std::pmr::string> segments(prefix);
            segments.emplace_back(k);
            expressions::key_t key(resource_, jsonb_path::flatten(segments, resource_), use_side);
            // A jsonb existence test: a key that names no column is legally absent
            // (yields false), and an intermediate object key is present if a child
            // is — the validator resolves both, so it must not hard-error here.
            key.set_absent_ok(true);
            auto dummy = params->add_parameter(
                types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA}));
            return make_compare_expression(params->parameters().resource(), compare_type::is_not_null, key, dummy);
        };
        if (keys.size() == 1) {
            return build_exists(keys[0]);
        }
        auto combined = make_compare_union_expression(params->parameters().resource(),
                                                      op == "?&" ? compare_type::union_and : compare_type::union_or);
        for (const auto& k : keys) {
            combined->append_child(build_exists(k));
        }
        return combined;
    }

    logical_plan::node_ptr transformer::transform_function(RangeFunction& node,
                                                           const name_collection_t& names,
                                                           logical_plan::parameter_node_t* params) {
        auto list = pg_ptr_cast<List>(node.functions->lst.front().data);
        auto func_call = pg_ptr_cast<FuncCall>(list->lst.front().data);
        return transform_function(*func_call, names, params);
    }

    logical_plan::node_ptr transformer::transform_from_function(RangeFunction& node,
                                                                const name_collection_t& names,
                                                                logical_plan::node_join_ptr& node_join,
                                                                logical_plan::execution_plan_t* plan) {
        if (!node.functions || node.functions->lst.empty()) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"table function: empty function list in FROM clause", resource_});
            return nullptr;
        }
        auto list = pg_ptr_cast<List>(node.functions->lst.front().data);
        auto func_call = pg_ptr_cast<FuncCall>(list->lst.front().data);
        std::string funcname = strVal(func_call->funcname->lst.front().data);
        std::pmr::vector<param_storage> args{resource_};
        // func_call->args is null for a zero-argument call (e.g. `FROM foo()`); leave
        // args empty and let validation reject the arity mismatch rather than deref it.
        if (func_call->args) {
            args.reserve(func_call->args->lst.size());
            for (const auto& arg : func_call->args->lst) {
                if (nodeTag(arg.data) == T_ColumnRef) {
                    // Correlated outer reference: bind per outer row via a parameter.
                    auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg.data), names);
                    // Placeholder type only used to satisfy validation; the real value (with
                    // its true type) is bound from the outer row at runtime. BIGINT keeps
                    // integer-typed table functions like generate_series matchable.
                    auto param_id =
                        plan->parameters->add_parameter(types::logical_value_t{resource_, static_cast<int64_t>(0)});
                    node_join->add_correlation(param_id, key.field);
                    node_join->set_lateral(true);
                    args.emplace_back(param_id);
                } else {
                    args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), plan->parameters.get()));
                }
            }
        }
        return logical_plan::make_node_function(resource_, std::move(funcname), std::move(args));
    }

    logical_plan::node_ptr transformer::transform_function(FuncCall& node,
                                                           const name_collection_t& names,
                                                           logical_plan::parameter_node_t* params) {
        std::string funcname = strVal(node.funcname->lst.front().data);
        std::pmr::vector<param_storage> args;
        args.reserve(node.args->lst.size());
        for (const auto& arg : node.args->lst) {
            if (nodeTag(arg.data) == T_ColumnRef) {
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg.data), names);
                key.deduce_side(names);
                args.emplace_back(std::move(key.field));
            } else {
                args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
            }
        }
        return logical_plan::make_node_function(resource_, std::move(funcname), std::move(args));
    }

    expression_ptr transformer::case_expr_to_scalar(CaseExpr* node,
                                                    const char* alias,
                                                    const name_collection_t& names,
                                                    logical_plan::execution_plan_t* plan,
                                                    logical_plan::node_ptr group) {
        std::string expr_name = alias ? alias : "case_" + std::to_string(aggregate_counter_++);
        auto expr = make_scalar_expression(resource_,
                                           scalar_type::case_expr,
                                           expressions::key_t{resource_, std::move(expr_name)});

        // Process WHEN clauses: params layout is [cond1, result1, cond2, result2, ..., default]
        for (auto& arg : node->args->lst) {
            auto when = pg_ptr_cast<CaseWhen>(arg.data);

            // Condition
            if (node->arg) {
                // Simple CASE: CASE col WHEN val THEN ... → generate equality: col = val.
                // The operand must be a column reference — a blind pg_ptr_cast<ColumnRef> on any other
                // node (e.g. a SubLink) reinterprets it and crashes; guard the tag first, mirroring
                // transform_null_test.
                if (nodeTag(node->arg) != T_ColumnRef && nodeTag(node->arg) != T_A_Indirection) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"CASE operand must be a column reference", resource_});
                    return nullptr;
                }
                auto col_key_res = node_to_field(resource_, pg_ptr_cast<Node>(node->arg), names);
                if (col_key_res.has_error()) {
                    error_ = col_key_res.error();
                    return nullptr;
                }
                auto col_key = std::move(col_key_res.value());
                col_key.deduce_side(names);
                auto param_id = add_param_value(pg_ptr_cast<Node>(when->expr), plan->parameters.get());
                auto cond = make_compare_expression(resource_, compare_type::eq, col_key.field, param_id);
                expr->append_param(expression_ptr(cond));
            } else {
                // Searched CASE: CASE WHEN condition THEN ... → boolean expression
                auto cond_node = pg_ptr_cast<Node>(when->expr);
                if (nodeTag(cond_node) == T_A_Expr) {
                    auto condition = transform_a_expr(pg_ptr_cast<A_Expr>(cond_node), names, plan);
                    expr->append_param(condition);
                } else if (nodeTag(cond_node) == T_FuncCall) {
                    auto condition =
                        transform_a_expr_func(pg_ptr_cast<FuncCall>(cond_node), names, plan->parameters.get());
                    expr->append_param(condition);
                } else if (nodeTag(cond_node) == T_NullTest) {
                    // CASE WHEN col IS [NOT] NULL THEN ...
                    auto condition =
                        transform_null_test(pg_ptr_cast<NullTest>(cond_node), names, plan->parameters.get());
                    expr->append_param(condition);
                } else {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"Unsupported WHEN condition type", resource_});
                    return nullptr;
                }
            }

            // Result: any value expression
            auto result_node = pg_ptr_cast<Node>(when->result);
            expr->append_param(resolve_select_operand(result_node, names, plan, group));
        }

        // Default (ELSE clause)
        if (node->defresult) {
            auto def_node = pg_ptr_cast<Node>(node->defresult);
            expr->append_param(resolve_select_operand(def_node, names, plan, group));
        }

        return expr;
    }

    std::pmr::vector<param_storage> transformer::apply_aggregate_filter(Node* agg_filter,
                                                                        std::pmr::vector<param_storage> args,
                                                                        const name_collection_t& names,
                                                                        logical_plan::execution_plan_t* plan) {
        if (!agg_filter) {
            return args;
        }
        // Wrap one aggregate argument `x` into `CASE WHEN p THEN x END`. The predicate is
        // re-transformed per argument so each CASE owns an independent condition tree (constant
        // folding mutates compare nodes in place, so a shared one would be unsafe); aggregates
        // almost always take a single argument, so the duplication is immaterial.
        auto wrap = [&](param_storage result) -> param_storage {
            auto cond = transform_predicate(agg_filter, names, plan);
            if (has_error()) {
                return result;
            }
            auto case_expr = make_scalar_expression(
                resource_,
                scalar_type::case_expr,
                expressions::key_t{resource_, "__aggfilter_" + std::to_string(aggregate_counter_++)});
            case_expr->append_param(cond);              // WHEN p
            case_expr->append_param(std::move(result)); // THEN <arg>   (no ELSE -> NULL when p not TRUE)
            return expressions::expression_ptr(case_expr);
        };

        if (args.empty()) {
            // count(*) / a parameterless aggregate: count the rows where p by counting the non-NULL
            // results of CASE WHEN p THEN 1 END.
            auto one = plan->parameters->add_parameter(static_cast<int64_t>(1));
            std::pmr::vector<param_storage> filtered(resource_);
            filtered.emplace_back(wrap(one));
            return filtered;
        }
        for (auto& arg : args) {
            arg = wrap(std::move(arg));
        }
        return args;
    }

    void transformer::transform_select_case_expr(CaseExpr* node,
                                                 const char* alias,
                                                 const name_collection_t& names,
                                                 logical_plan::execution_plan_t* plan,
                                                 logical_plan::node_ptr& group) {
        auto expr = case_expr_to_scalar(node, alias, names, plan, group);
        if (expr) {
            group->append_expression(expr);
        }
    }

    // Resolve a HAVING operand: FuncCall → find matching aggregate alias in group
    param_storage transformer::resolve_having_operand(Node* node,
                                                      const name_collection_t& names,
                                                      logical_plan::execution_plan_t* plan,
                                                      const logical_plan::node_ptr& group) {
        switch (nodeTag(node)) {
            case T_FuncCall: {
                auto func = pg_ptr_cast<FuncCall>(node);
                auto funcname = std::string{strVal(linitial(func->funcname))};
                // The arguments have to be built before anything can be matched: two aggregates of
                // the same name are different aggregates. Binding HAVING sum(g) to a projected
                // sum(v) on the name alone made the group answer about the wrong column.
                std::pmr::vector<param_storage> args(resource_);
                if (func->args) {
                    for (const auto& arg : func->args->lst) {
                        auto* arg_node = pg_ptr_cast<Node>(arg.data);
                        if (nodeTag(arg_node) == T_ColumnRef) {
                            auto col = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg_node), names);
                            col.deduce_side(names);
                            args.emplace_back(std::move(col.field));
                        } else if (nodeTag(arg_node) == T_A_Expr) {
                            args.emplace_back(resolve_having_operand(arg_node, names, plan, group));
                        } else {
                            args.emplace_back(add_param_value(arg_node, plan->parameters.get()));
                        }
                    }
                }
                // FILTER (WHERE p): lower to a CASE over each argument (or COUNT(CASE ...) for a
                // bare aggregate) so only qualifying rows reach the aggregate.
                args = apply_aggregate_filter(func->agg_filter, std::move(args), names, plan);

                const bool args_comparable = std::none_of(args.begin(), args.end(), [](const param_storage& p) {
                    return std::holds_alternative<expressions::expression_ptr>(p);
                });
                for (const auto& expr : group->expressions()) {
                    if (const auto* found = find_call(expr.get(), funcname, args, args_comparable)) {
                        return found->key();
                    }
                }
                // Not in SELECT — add to group so operator_group_t computes it for HAVING
                // (mirrors PostgreSQL: aggregates in HAVING need not appear in SELECT).
                std::string alias = "__having_" + funcname + "_" + std::to_string(aggregate_counter_++);
                auto agg_expr = make_aggregate_expression(resource_, funcname, expressions::key_t{resource_, alias});
                for (auto& arg : args) {
                    agg_expr->append_param(arg);
                }
                group->append_expression(agg_expr);
                return expressions::key_t{resource_, alias};
            }
            case T_ColumnRef: {
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node), names);
                key.deduce_side(names);
                return key.field;
            }
            case T_A_Const:
            case T_ParamRef:
            case T_TypeCast:
                return add_param_value(node, plan->parameters.get());
            case T_A_Expr: {
                auto sub = pg_ptr_cast<A_Expr>(node);
                if (sub->kind == AEXPR_OP) {
                    auto sub_op = std::string_view(strVal(sub->name->lst.front().data));
                    if (is_jsonb_nav_operator(sub_op)) {
                        expressions::key_t k{resource_};
                        if (!resolve_jsonb_scalar_key(sub, names, k)) {
                            return nullptr;
                        }
                        return k;
                    }
                    if (is_arithmetic_operator(sub_op)) {
                        auto stype = get_arithmetic_scalar_type(sub_op);
                        auto expr = make_scalar_expression(resource_, stype);
                        if (sub->lexpr) {
                            expr->append_param(resolve_having_operand(sub->lexpr, names, plan, group));
                            expr->append_param(resolve_having_operand(sub->rexpr, names, plan, group));
                        } else {
                            if (stype == scalar_type::add) {
                                return resolve_having_operand(sub->rexpr, names, plan, group);
                            }
                            if (stype == scalar_type::subtract) {
                                expr = make_scalar_expression(resource_, scalar_type::unary_minus);
                            }
                            expr->append_param(resolve_having_operand(sub->rexpr, names, plan, group));
                        }
                        return expr;
                    }
                }
                return add_param_value(node, plan->parameters.get());
            }
            case T_SubLink: {
                auto param_id =
                    plan->parameters->add_parameter(types::logical_value_t{resource_, types::logical_type::NA});
                // Transform before appending so nested sub_queries/sub_query_results come first.
                // Save/restore the pending internal-aggregate stash so the inner epilogue's
                // flush + clear does not steal this level's SELECT-list aggregates.
                auto prev_pending = std::move(pending_internal_aggs_);
                pending_internal_aggs_.clear();
                auto sub_node = transform(*pg_ptr_cast<SubLink>(node)->subselect, plan);
                pending_internal_aggs_ = std::move(prev_pending);
                plan->sub_query_results.emplace_back(&vector::compact_to_single_value, param_id);
                plan->sub_queries.emplace_back(std::move(sub_node));
                return param_id;
            }
            default:
                return add_param_value(node, plan->parameters.get());
        }
    }

    expression_ptr transformer::transform_having_expr(Node* node,
                                                      const name_collection_t& names,
                                                      logical_plan::execution_plan_t* plan,
                                                      const logical_plan::node_ptr& group) {
        if (nodeTag(node) == T_TypeCast) {
            // HAVING TRUE / FALSE — constant predicate, no aggregate involved.
            return transform_predicate(node, names, plan);
        }
        if (nodeTag(node) == T_SubLink) {
            // A sub-query as a bare HAVING predicate — `HAVING (SELECT flag ...)`, and
            // EXISTS / IN / ANY / ALL — is transformed exactly as in WHERE. For a bare
            // EXPR_SUBLINK this yields the compact-to-single-value `== true` predicate with
            // boolean_required set, so a non-boolean `HAVING (SELECT 1)` is rejected
            // (PostgreSQL: argument of HAVING must be type boolean). A sub-query as a
            // comparison OPERAND (`HAVING sum(x) > (SELECT ...)`) is a different path
            // (resolve_having_operand) and stays untyped, since any type is legal there.
            return transform_sublink_expr(pg_ptr_cast<SubLink>(node), names, plan);
        }
        if (nodeTag(node) == T_A_Expr) {
            auto a_expr = pg_ptr_cast<A_Expr>(node);
            if (a_expr->kind == AEXPR_OP) {
                // Operator symbol is last (schema-qualified OPERATOR(schema.op) prepends the schema).
                auto op_str = std::string_view(strVal(a_expr->name->lst.back().data));
                if (!is_arithmetic_operator(op_str)) {
                    auto comp_type = get_compare_type(op_str);
                    if (comp_type == compare_type::invalid) {
                        error_ = core::error_t(core::error_code_t::sql_parse_error,
                                               std::pmr::string{"invalid comparison operand", resource_});
                        return nullptr;
                    }
                    auto left = resolve_having_operand(a_expr->lexpr, names, plan, group);
                    auto right = resolve_having_operand(a_expr->rexpr, names, plan, group);
                    return make_compare_expression(resource_, comp_type, left, right);
                }
            } else if (a_expr->kind == AEXPR_AND || a_expr->kind == AEXPR_OR) {
                auto expr = make_compare_union_expression(resource_,
                                                          a_expr->kind == AEXPR_AND ? compare_type::union_and
                                                                                    : compare_type::union_or);
                expr->append_child(transform_having_expr(a_expr->lexpr, names, plan, group));
                expr->append_child(transform_having_expr(a_expr->rexpr, names, plan, group));
                return expr;
            }
        }
        error_ = core::error_t(core::error_code_t::sql_parse_error,
                               std::pmr::string{"Unsupported expression in HAVING clause", resource_});
        return nullptr;
    }

    expression_ptr transformer::transform_null_test(NullTest* node,
                                                    const name_collection_t& names,
                                                    logical_plan::parameter_node_t* params) {
        auto cmp = node->nulltesttype == IS_NULL ? compare_type::is_null : compare_type::is_not_null;
        // is_null/is_not_null don't need a value, use a dummy parameter
        auto param_id = params->add_parameter(
            types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA}));

        // A bare column keeps the fast validity-bitmap path in the predicate operator.
        if (nodeTag(node->arg) == T_ColumnRef || nodeTag(node->arg) == T_A_Indirection) {
            auto key_res = node_to_field(resource_, pg_ptr_cast<Node>(node->arg), names);
            if (key_res.has_error()) {
                error_ = key_res.error();
                return nullptr;
            }
            auto key = std::move(key_res.value());
            key.deduce_side(names);
            return make_compare_expression(resource_, cmp, key.field, param_id);
        }

        // A computed argument — `(expr) IS NULL`
        auto operand = transform_a_expr_operand(pg_ptr_cast<Node>(node->arg), names, params);
        if (error_.contains_error()) {
            return nullptr;
        }
        return make_compare_expression(resource_, cmp, operand, param_id);
    }

} // namespace components::sql::transform
