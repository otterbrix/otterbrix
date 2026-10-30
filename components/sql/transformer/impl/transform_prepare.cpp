#include "../transformer.hpp"
#include "view_body_text.hpp"

#include <components/logical_plan/node_deallocate.hpp>
#include <components/logical_plan/node_execute.hpp>
#include <components/logical_plan/node_prepare.hpp>
#include <components/vector/vector_builders.hpp>

namespace components::sql::transform {

    namespace {

        // What PostgreSQL 18 refuses in an EXECUTE parameter before the expression has a type (parse_expr.c:1852,
        // :893; parse_agg.c:1003): a sub-query, a $n and a window function, the first in transform order.
        // Aggregates and set-returning functions are found in the argument's plan by the executor.
        core::error_t refusal_of(std::pmr::memory_resource* resource, Node* node) {
            if (node == nullptr) {
                return core::error_t::no_error();
            }
            switch (nodeTag(node)) {
                case T_SubLink:
                    return core::error_t(core::error_code_t::unimplemented_yet,
                                         std::pmr::string{"cannot use subquery in EXECUTE parameter", resource});
                case T_ParamRef:
                    return core::error_t(core::error_code_t::undefined_parameter,
                                         std::pmr::string{"there is no parameter $" +
                                                              std::to_string(pg_ptr_cast<ParamRef>(node)->number),
                                                          resource});
                default:
                    break;
            }
            core::error_t refused = core::error_t::no_error();
            for_each_operand(node, [&](Node* operand) {
                if (!refused.contains_error()) {
                    refused = refusal_of(resource, operand);
                }
            });
            // A call's arguments are transformed before the call is (transformFuncCall).
            if (!refused.contains_error() && nodeTag(node) == T_FuncCall &&
                pg_ptr_cast<FuncCall>(node)->over != nullptr) {
                return core::error_t(
                    core::error_code_t::windowing_error,
                    std::pmr::string{"window functions are not allowed in EXECUTE parameters", resource});
            }
            return refused;
        }

    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_prepare(PrepareStmt& node) {
        auto body = view_body_text(resource_, raw_sql_, node.query_location, node.query_end_location, "PREPARE");
        if (body.has_error()) {
            return body.error();
        }
        std::pmr::vector<types::complex_logical_type> declared{resource_};
        if (!is_empty_list(node.argtypes)) {
            for (const auto& cell : node.argtypes->lst) {
                auto type = get_type(resource_, pg_ptr_cast<TypeName>(cell.data));
                if (type.has_error()) {
                    return type.error();
                }
                if (type.value().type() == types::logical_type::UNKNOWN) {
                    return core::error_t(core::error_code_t::unimplemented_yet,
                                         std::pmr::string{"PREPARE: a parameter of type " + type.value().type_name() +
                                                              " is not supported yet: only built-in types are",
                                                          resource_});
                }
                declared.push_back(std::move(type.value()));
            }
        }
        return logical_plan::node_ptr{logical_plan::make_node_prepare(resource_,
                                                                      construct(node.name),
                                                                      std::move(body.value()),
                                                                      std::move(declared))};
    }

    core::result_wrapper_t<logical_plan::node_ptr>
    transformer::transform_execute(ExecuteStmt& node, logical_plan::execution_plan_t* plan) {
        std::pmr::vector<logical_plan::execute_argument_t> arguments{resource_};
        core::error_t refusal = core::error_t::no_error();
        if (!is_empty_list(node.params)) {
            auto* arena = node.params->lst.get_allocator().resource();
            for (const auto& cell : node.params->lst) {
                auto* argument = pg_ptr_cast<Node>(cell.data);
                const auto slot =
                    plan->parameters->add_parameter(vector::make_null(resource_, sub_query_placeholder(EXPR_SUBLINK)),
                                                    vector::parameter_kind::execution);
                arguments.push_back(logical_plan::execute_argument_t{slot, is_quoted_literal(argument)});
                if (refusal.contains_error()) {
                    continue;
                }
                refusal = refusal_of(resource_, argument);
                if (refusal.contains_error()) {
                    continue;
                }
                // The argument is the one column of SELECT <argument>, a sub-plan of the statement (EvaluateParams).
                auto* target = makeNode(arena, ResTarget);
                target->name = nullptr;
                target->indirection = NIL;
                target->val = argument;
                target->location = -1;
                auto* select = makeNode(arena, SelectStmt);
                select->distinctClause = NIL;
                select->targetList = list_make1(arena, target);
                select->fromClause = NIL;
                select->groupClause = NIL;
                select->windowClause = NIL;
                auto lowered = transform(pg_cast<Node>(*pg_ptr_cast<Node>(select)), plan);
                if (lowered.has_error()) {
                    refusal = core::error_on(resource_, lowered.error());
                    continue;
                }
                plan->sub_query_results.push_back({&vector::compact_to_single_value, slot});
                plan->sub_queries.emplace_back(std::move(lowered.value()));
            }
        }
        return logical_plan::node_ptr{
            logical_plan::make_node_execute(resource_, construct(node.name), std::move(arguments), refusal)};
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_deallocate(DeallocateStmt& node) {
        if (node.name == nullptr) {
            return logical_plan::node_ptr{logical_plan::make_node_deallocate_all(resource_)};
        }
        return logical_plan::node_ptr{logical_plan::make_node_deallocate(resource_, construct(node.name))};
    }

} // namespace components::sql::transform
