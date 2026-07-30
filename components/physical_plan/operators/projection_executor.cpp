#include "projection_executor.hpp"

#include <components/vector/vector_operations.hpp>

namespace components::operators {

    namespace {

        namespace expr = components::expressions;

        core::error_t plan_error(std::pmr::memory_resource* resource, core::error_code_t code, const char* what) {
            return core::error_t(code, std::pmr::string{what, resource});
        }

        // A group_key_t addresses its columns by BARE ORDINAL PATH and stamps a side; the constant
        // arms carry a value the struct owns. Both map straight onto bound leaves.
        core::result_wrapper_t<expr::bound_expression_ptr> bind_key_column(std::pmr::memory_resource* resource,
                                                                           expr::binder_t& binder,
                                                                           const group_key_t& key,
                                                                           const expr::binder_context_t& context) {
            if (key.full_path.empty()) {
                return plan_error(resource,
                                  core::error_code_t::invalid_parameter,
                                  "projection: field_ref path must be resolved before execution");
            }
            return binder.bind_column_path(key.full_path, key.side, context);
        }

        // COALESCE: an ordered list of columns and constants, first non-null wins. The struct's own
        // shape, bound to the node that shares it.
        core::result_wrapper_t<expr::bound_expression_ptr> bind_key_coalesce(std::pmr::memory_resource* resource,
                                                                             expr::binder_t& binder,
                                                                             const group_key_t& key,
                                                                             const types::complex_logical_type& result,
                                                                             const expr::binder_context_t& context) {
            std::pmr::vector<expr::bound_expression_ptr> operands{resource};
            operands.reserve(key.coalesce_entries.size());
            for (const auto& entry : key.coalesce_entries) {
                if (entry.type == group_key_t::coalesce_entry::source::constant) {
                    operands.push_back(
                        expr::bound_expression_ptr{expr::make_bound_constant(resource, entry.constant)});
                    continue;
                }
                std::pmr::vector<size_t> path{resource};
                path.push_back(entry.col_index);
                auto bound = binder.bind_column_path(path, key.side, context);
                if (bound.has_error()) {
                    return bound;
                }
                operands.push_back(std::move(bound.value()));
            }
            return expr::make_bound_coalesce(resource, result, std::move(operands));
        }

        // CASE WHEN: the struct stores each clause as (condition column, comparison, condition
        // value) -> (result column | result constant), plus an ELSE. That is a CASE with its WHENs
        // pre-decomposed, so each clause becomes one comparison child and one result child in the
        // when0,then0,when1,then1,...[,else] layout bound_case_t already uses.
        core::result_wrapper_t<expr::bound_expression_ptr> bind_key_case(std::pmr::memory_resource* resource,
                                                                         expr::binder_t& binder,
                                                                         const group_key_t& key,
                                                                         const expr::binder_context_t& context) {
            std::pmr::vector<expr::bound_expression_ptr> branches{resource};
            branches.reserve(key.case_clauses.size() * 2 + 1);
            for (const auto& clause : key.case_clauses) {
                std::pmr::vector<size_t> path{resource};
                path.push_back(clause.condition_col);
                auto condition_column = binder.bind_column_path(path, key.side, context);
                if (condition_column.has_error()) {
                    return condition_column;
                }
                switch (clause.cmp) {
                    case expr::compare_type::eq:
                    case expr::compare_type::ne:
                    case expr::compare_type::gt:
                    case expr::compare_type::gte:
                    case expr::compare_type::lt:
                    case expr::compare_type::lte: {
                        auto comparison = expr::make_bound_comparison(
                            resource,
                            clause.cmp,
                            std::move(condition_column.value()),
                            expr::bound_expression_ptr{expr::make_bound_constant(resource, clause.condition_value)});
                        if (comparison.has_error()) {
                            return comparison;
                        }
                        branches.push_back(std::move(comparison.value()));
                        break;
                    }
                    default:
                        // An UNCONDITIONAL clause: any non-comparison cmp always fires. A constant
                        // TRUE says that and, being foldable, is evaluated once instead of per row.
                        branches.push_back(expr::bound_expression_ptr{
                            expr::make_bound_constant(resource, types::logical_value_t{resource, true})});
                        break;
                }
                if (clause.res_type == group_key_t::case_clause::result_source::constant) {
                    branches.push_back(
                        expr::bound_expression_ptr{expr::make_bound_constant(resource, clause.res_constant)});
                    continue;
                }
                std::pmr::vector<size_t> result_path{resource};
                result_path.push_back(clause.res_col);
                auto result_column = binder.bind_column_path(result_path, key.side, context);
                if (result_column.has_error()) {
                    return result_column;
                }
                branches.push_back(std::move(result_column.value()));
            }
            // The ELSE is the odd trailing child. A null_value ELSE is left OFF the list: bound_case_t
            // answers NULL for a row no WHEN fired on, which is the same thing said once.
            switch (key.else_type) {
                case group_key_t::else_source::column: {
                    std::pmr::vector<size_t> else_path{resource};
                    else_path.push_back(key.else_col);
                    auto else_column = binder.bind_column_path(else_path, key.side, context);
                    if (else_column.has_error()) {
                        return else_column;
                    }
                    branches.push_back(std::move(else_column.value()));
                    break;
                }
                case group_key_t::else_source::constant:
                    branches.push_back(
                        expr::bound_expression_ptr{expr::make_bound_constant(resource, key.else_constant)});
                    break;
                case group_key_t::else_source::null_value:
                default:
                    break;
            }
            return expr::make_bound_case(resource, std::move(branches));
        }

    } // namespace

    core::result_wrapper_t<vector::vector_t>
    evaluate_scalar(std::pmr::memory_resource* resource,
                    expressions::scalar_type op,
                    const std::pmr::vector<expressions::param_storage>& operands,
                    const vector::data_chunk_t& chunk,
                    const compute::function_registry_t* functions,
                    const logical_plan::storage_parameters& parameters,
                    core::date::timezone_offset_t session_tz) {
        expr::bind_schema_t schema{resource};
        for (const auto& record : chunk.schema()) {
            schema.add(std::string_view{record.name}, record.type);
        }
        expr::binder_context_t context{};
        context.left = &schema;
        context.right = &schema;
        context.functions = functions;
        context.parameters = &parameters;
        context.session_tz = session_tz;

        expr::binder_t binder{resource};
        auto bound = binder.bind_scalar_operands(op, operands, context);
        if (bound.has_error()) {
            return bound.error();
        }
        const uint64_t count = chunk.size();
        const uint64_t capacity = count > 0 ? count : 1;
        auto executor = expr::expression_executor_t::create(resource, std::move(bound.value()), capacity);
        if (executor.has_error()) {
            return executor.error();
        }
        expr::expression_executor_t::context_t execution{};
        execution.parameters = &parameters;
        execution.session_tz = session_tz;
        auto produced = executor.value().execute(chunk, count, execution);
        if (produced.has_error()) {
            return produced.error();
        }
        // The executor OWNS its result slot and reuses it, so the answer is copied out into a vector
        // the caller can move into a chunk.
        vector::vector_t result(resource, produced.value()->type(), capacity);
        result.validity().reset(capacity);
        if (count > 0) {
            if (auto error = vector::vector_ops::copy(*produced.value(), result, count, 0, 0);
                error.contains_error()) {
                return error;
            }
        }
        return result;
    }

    core::result_wrapper_t<expr::bound_expression_ptr>
    bind_select_column(std::pmr::memory_resource* resource,
                       expr::binder_t& binder,
                       const select_column_t& column,
                       const expr::binder_context_t& context) {
        switch (column.type) {
            case select_column_t::kind::field_ref:
                return bind_key_column(resource, binder, column.key, context);
            case select_column_t::kind::coalesce:
                return bind_key_coalesce(resource, binder, column.key, column.result_type, context);
            case select_column_t::kind::case_when:
                return bind_key_case(resource, binder, column.key, context);
            case select_column_t::kind::arithmetic:
                return binder.bind_scalar_operands(column.arith_op, column.operands, context);
            case select_column_t::kind::constant: {
                // THE literal-vs-placeholder distinction, and it is not a guess here: physgen already
                // recorded which it is. A column carrying a parameter id is a slot rebound per outer
                // row by a LATERAL correlation, so it binds LIVE; a column carrying only a value owns
                // that value, so it binds to a constant the expression owns -- the one place a
                // logical_value_t legitimately sits next to execution.
                if (column.constant_param_id.has_value()) {
                    if (!context.parameters) {
                        return plan_error(resource,
                                          core::error_code_t::invalid_parameter,
                                          "projection: a constant parameter slot needs the parameter map");
                    }
                    const auto* value = logical_plan::get_parameter(context.parameters, *column.constant_param_id);
                    // An UNBOUND slot falls through to the column's own baked value below.
                    if (value) {
                        return expr::bound_expression_ptr{
                            expr::make_bound_parameter(resource,
                                                       *column.constant_param_id,
                                                       value->is_null() ? column.result_type : value->type())};
                    }
                }
                // A NULL constant is projected as a TYPED null: the type is the plan-resolved one,
                // authoritative even over zero rows, and the null lives in the validity mask.
                if (column.constant_value.is_null()) {
                    // A NULL of result_type, not a value OF result_type: logical_value_t(resource, T)
                    // is a ZERO of T (is_null() is `type == NA`, logical_value.cpp), so building
                    // it that way would project 0 where the query asked for NULL.
                    return expr::bound_expression_ptr{expr::make_bound_null_constant(resource, column.result_type)};
                }
                return expr::bound_expression_ptr{expr::make_bound_constant(resource, column.constant_value)};
            }
            case select_column_t::kind::star_expand:
                return plan_error(resource,
                                  core::error_code_t::invalid_parameter,
                                  "projection: '*' is not an expression");
        }
        return plan_error(resource, core::error_code_t::invalid_parameter, "projection: unknown column kind");
    }

    vector::data_chunk_t empty_projection(std::pmr::memory_resource* resource,
                                          const std::pmr::vector<select_column_t>& columns) {
        vector::data_chunk_t result(resource, {}, 1);
        for (const auto& column : columns) {
            // Bare '*' over a chunk with no columns expands to no columns.
            if (column.type == select_column_t::kind::star_expand) {
                continue;
            }
            vector::vector_t empty(resource, column.result_type, 1);
            empty.set_name(column.key.name);
            result.data.push_back(std::move(empty));
        }
        result.set_cardinality(0);
        return result;
    }

    projection_executor_t::projection_executor_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , plans_(resource) {}

    core::result_wrapper_t<projection_executor_t>
    projection_executor_t::create(std::pmr::memory_resource* resource,
                                  const std::pmr::vector<select_column_t>& columns,
                                  const vector::schema_t& left_columns,
                                  const vector::schema_t& right_columns,
                                  const compute::function_registry_t* functions,
                                  const logical_plan::storage_parameters* parameters,
                                  core::date::timezone_offset_t session_tz) {
        // Each column's name comes off its schema record, not the type's name slot -- that read
        // asserts on a type carrying no extension, which a computed column of a no-FROM projection
        // does not have. An unnamed column's record answers with an empty name, and that costs
        // nothing: every key that reaches an operator carries the ordinals validation resolved for
        // it, so bind_key never needs the name.
        expressions::bind_schema_t left{resource};
        for (const auto& column : left_columns) {
            left.add(std::string_view{column.name}, column.type);
        }
        expressions::bind_schema_t right{resource};
        for (const auto& column : right_columns) {
            right.add(std::string_view{column.name}, column.type);
        }

        expressions::binder_context_t context{};
        context.left = &left;
        context.right = &right;
        context.functions = functions;
        context.parameters = parameters;
        context.session_tz = session_tz;

        projection_executor_t built{resource};
        expressions::binder_t binder{resource};
        built.plans_.reserve(columns.size());
        for (const auto& column : columns) {
            column_plan_t plan{};
            plan.alias = std::pmr::string{column.key.name, resource};
            if (column.type == select_column_t::kind::star_expand) {
                plan.star_expand = true;
                built.plans_.push_back(std::move(plan));
                continue;
            }
            auto bound = bind_select_column(resource, binder, column, context);
            if (bound.has_error()) {
                return bound.error();
            }
            auto executor = expressions::expression_executor_t::create(resource, std::move(bound.value()));
            if (executor.has_error()) {
                return executor.error();
            }
            plan.executor.emplace(std::move(executor.value()));
            built.plans_.push_back(std::move(plan));
        }
        return core::result_wrapper_t<projection_executor_t>{std::move(built)};
    }

    core::result_wrapper_t<vector::data_chunk_t>
    projection_executor_t::evaluate(vector::data_chunk_t& input,
                                    const logical_plan::storage_parameters& parameters,
                                    core::date::timezone_offset_t session_tz,
                                    const vector::data_chunk_t* right_input) {
        const auto num_rows = input.size();
        const uint64_t capacity = num_rows > 0 ? num_rows : 1;

        expressions::expression_executor_t::context_t execution{};
        execution.parameters = &parameters;
        execution.session_tz = session_tz;
        execution.right_input = right_input;

        // One column per projection entry, pushed as they are built; the chunk derives its types
        // from those columns.
        vector::data_chunk_t result(resource_, {}, capacity);
        for (auto& plan : plans_) {
            if (plan.star_expand) {
                // Bare '*': the input's columns pass through as they are. Qualified `table.*` is
                // pre-expanded to get_field columns at validation, so it never reaches here.
                for (size_t column = 0; column < input.column_count(); ++column) {
                    result.data.push_back(input.data[column]);
                }
                continue;
            }
            auto produced = plan.executor->execute(input, num_rows, execution);
            if (produced.has_error()) {
                return produced.error();
            }
            // The executor OWNS its result vector and reuses it on the next chunk, so the projection
            // takes its own copy into the output chunk rather than referencing it.
            vector::vector_t column(resource_, produced.value()->type(), capacity);
            column.validity().reset(capacity);
            if (num_rows > 0) {
                if (auto error = vector::vector_ops::copy(*produced.value(), column, num_rows, 0, 0);
                    error.contains_error()) {
                    return error;
                }
            }
            column.set_name(plan.alias);
            result.data.push_back(std::move(column));
        }
        result.set_cardinality(num_rows);
        return result;
    }

} // namespace components::operators
