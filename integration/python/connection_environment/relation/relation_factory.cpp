#include "relation_factory.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <memory>
#include <scan/python_replacement_scan.hpp>
#include <string>
#include <vector>

using namespace components;
using namespace components::logical_plan;
using components::table::column_definition_t;
using namespace components::expressions;

namespace otterbrix {

    namespace {
        // ---------------------------------------------------------------------
        // Column-schema derivation.
        //
        // These helpers reproduce, op-by-op, what the former ColumnsVisitor
        // (relation.cpp) computed while walking the Relation variant tree.
        // Instead of walking a tree, each chaining op now recomputes the
        // output schema eagerly from the source schema + the op's expressions,
        // and the result is carried in built_relation_t::columns. The exact
        // name/type results are preserved (count -> UBIGINT, avg(x) -> DOUBLE,
        // field lookups against the source schema, "#"/UNKNOWN sentinels).
        const std::string error_str = "#";

        components::types::complex_logical_type find_type(const std::string& name,
                                                          const std::pmr::vector<column_definition_t>& initial) {
            for (const auto& col : initial) {
                if (col.name() == name) {
                    return col.type();
                }
            }
            return components::types::logical_type::UNKNOWN;
        }

        std::pair<std::string, bool>
        find_param_name(const std::variant<core::parameter_id_t, expressions::key_t, expression_ptr>& param) {
            return std::visit(
                [](const auto& expr) {
                    using type = std::decay_t<decltype(expr)>;
                    if constexpr (std::is_same_v<type, expressions::key_t>) {
                        return std::make_pair(expr.as_string(), true);
                    } else if constexpr (std::is_same_v<type, core::parameter_id_t> ||
                                         std::is_same_v<type, expression_ptr>) {
                        return std::make_pair(error_str, false);
                    }
                    throw std::runtime_error("Unknown parameter type for nodes");
                },
                param);
        }

        column_definition_t process_aggregate(const aggregate_expression_ptr& aggregate_expr,
                                              const std::pmr::vector<column_definition_t>& initial) {
            std::string name = error_str;
            components::types::complex_logical_type type = components::types::logical_type::UNKNOWN;
            if (aggregate_expr->params().size() > 1) {
                return column_definition_t(name, type);
            }
            bool is_count = aggregate_expr->function_name() == "count";
            if (is_count) {
                name = "count";
                type = types::logical_type::UBIGINT;
            } else {
                const auto& param = aggregate_expr->params().front();
                auto founded_name = find_param_name(param);

                if (aggregate_expr->key().is_null()) {
                    std::string agg_str = aggregate_expr->function_name();
                    name = agg_str + "(" + founded_name.first + ")";
                } else {
                    name = aggregate_expr->key().as_string();
                }
                auto base_type = find_type(founded_name.first, initial);
                if (aggregate_expr->function_name() == "avg") {
                    type = types::logical_type::DOUBLE;
                } else {
                    type = base_type;
                }
            }
            return column_definition_t(name, type);
        }

        column_definition_t process_scalar(const scalar_expression_ptr& scalar_expr,
                                           const std::pmr::vector<column_definition_t>& initial) {
            std::string name = error_str;
            components::types::complex_logical_type type = components::types::logical_type::UNKNOWN;

            if (scalar_expr->type() != scalar_type::get_field) {
                return column_definition_t(name, type);
            }
            if (scalar_expr->params().size() > 1) {
                return column_definition_t(name, type);
            }
            if (scalar_expr->params().size() == 1) {
                auto param_name = find_param_name(scalar_expr->params().front());
                name = scalar_expr->key().is_null() ? param_name.first : scalar_expr->key().as_string();
                type = find_type(param_name.first, initial);
            } else {
                if (!scalar_expr->key().is_null()) {
                    name = scalar_expr->key().as_string();
                }
                type = find_type(name, initial);
            }
            return column_definition_t(name, type);
        }

        // Schema for an aggregate that carries a SELECT clause (no group).
        // Mirrors ColumnsVisitor::operator()(Aggregate) with select && !group.
        std::pmr::vector<column_definition_t> select_schema(std::pmr::memory_resource* resource,
                                                            const node_select_ptr& select,
                                                            const std::pmr::vector<column_definition_t>& initial) {
            std::pmr::vector<column_definition_t> result(resource);
            const auto& exprs = select->expressions();
            result.reserve(exprs.size());
            for (const auto& expr : exprs) {
                switch (expr->group()) {
                    case expression_group::scalar:
                        result.push_back(
                            process_scalar(boost::static_pointer_cast<scalar_expression_t>(expr), initial));
                        break;
                    default:
                        result.emplace_back(error_str, components::types::logical_type::UNKNOWN);
                }
            }
            return result;
        }

        // Schema for an aggregate that carries a GROUP clause.
        // Mirrors ColumnsVisitor::operator()(Aggregate) with group present.
        std::pmr::vector<column_definition_t> group_schema(std::pmr::memory_resource* resource,
                                                           const node_group_ptr& group,
                                                           const std::pmr::vector<column_definition_t>& initial) {
            std::pmr::vector<column_definition_t> result(resource);
            const auto& exprs = group->expressions();
            result.reserve(exprs.size());
            for (const auto& expr : exprs) {
                switch (expr->group()) {
                    case expression_group::aggregate:
                        result.push_back(
                            process_aggregate(boost::static_pointer_cast<aggregate_expression_t>(expr), initial));
                        break;
                    case expression_group::scalar:
                        result.push_back(
                            process_scalar(boost::static_pointer_cast<scalar_expression_t>(expr), initial));
                        break;
                    default:
                        result.emplace_back(error_str, components::types::logical_type::UNKNOWN);
                }
            }
            return result;
        }

        // Pass-through schema (copy) for ops that don't change the column set:
        // filter (match), sort, and limit. Mirrors ColumnsVisitor's !group,
        // no-select Aggregate branch (and limit -> resource->get_columns()).
        std::pmr::vector<column_definition_t> passthrough_schema(std::pmr::memory_resource* resource,
                                                                 const std::pmr::vector<column_definition_t>& initial) {
            std::pmr::vector<column_definition_t> result(resource);
            result.reserve(initial.size());
            for (const auto& col : initial) {
                result.emplace_back(col.name(), col.type());
            }
            return result;
        }
    } // namespace

    relation_factory_t::relation_factory_t(const boost::intrusive_ptr<otterbrix_t>& space)
        : space(space) {}

    relation_factory_t::~relation_factory_t() = default;

    void relation_factory_t::set_null_space() { space = nullptr; }

    node_ptr relation_factory_t::make_aggregate_node(const node_ptr& from,
                                                     node_group_ptr group,
                                                     node_match_ptr match,
                                                     node_sort_ptr sort,
                                                     node_select_ptr select,
                                                     node_limit_ptr limit) {
        static int indx = 0;
        auto session = otterbrix::session_id_t();
        std::string name = "t";
        name += std::to_string(indx++);
        space->dispatcher()->execute_sql(session, "CREATE TABLE tmp." + name + "();");

        auto* resource = space->dispatcher()->resource();
        auto aggregator = make_node_aggregate(resource, core::dbname_t{"tmp"}, core::relname_t{name});
        aggregator->append_child(from);
        if (group) {
            aggregator->append_child(group);
        }
        if (match) {
            aggregator->append_child(match);
        }
        if (sort) {
            aggregator->append_child(sort);
        }
        if (select) {
            aggregator->append_child(select);
        }
        if (limit) {
            aggregator->append_child(limit);
        }
        return boost::static_pointer_cast<node_t>(aggregator);
    }

    built_relation_t relation_factory_t::filter_relation(const built_relation_t& relation,
                                                         const expression_wrapper_t& condition) {
        auto* resource = space->dispatcher()->resource();
        node_match_ptr match_node;
        if (condition.is_expression()) {
            if (condition.expression()->group() == expressions::expression_group::compare) {
                match_node = make_node_match(resource, core::dbname_t{}, core::relname_t{}, condition.expression());
            } else {
                throw std::runtime_error("Implementation Error. Undefined expression for filter");
            }
        } else {
            throw std::runtime_error("The method supports only condition expression");
        }
        auto node = make_aggregate_node(relation.node, nullptr, match_node, nullptr, nullptr);
        return {node, passthrough_schema(space->dispatcher()->resource(), relation.columns)};
    }

    built_relation_t relation_factory_t::sort_relation(const built_relation_t& relation,
                                                       const std::vector<expression_wrapper_t>& exprs) {
        if (exprs.empty()) {
            throw std::runtime_error("Please provide at least one expression to sort on");
        }
        std::pmr::vector<expressions::expression_ptr> sort_exprs(space->dispatcher()->resource());
        sort_exprs.reserve(exprs.size());
        for (const auto& expr : exprs) {
            if (expr.is_expression()) {
                if (expr.expression()->group() == expressions::expression_group::sort) {
                    sort_exprs.push_back(expr.expression());
                } else {
                    throw std::runtime_error("Undefined expression type for sort relation");
                }
            } else if (expr.is_key()) {
                throw std::runtime_error("The method supports only sort expressions");
            } else {
                throw std::runtime_error("Implementation Error. Undefined expression type for sort relation");
            }
        }
        auto sort =
            make_node_sort(space->dispatcher()->resource(), core::dbname_t{}, core::relname_t{}, std::move(sort_exprs));

        auto node = make_aggregate_node(relation.node, nullptr, nullptr, sort, nullptr);
        return {node, passthrough_schema(space->dispatcher()->resource(), relation.columns)};
    }

    built_relation_t relation_factory_t::group_relation(const built_relation_t& relation,
                                                        const std::vector<expression_wrapper_t>& exprs) {
        auto* resource = space->dispatcher()->resource();
        std::vector<expressions::expression_ptr> fields;
        fields.reserve(exprs.size());
        for (const auto& expr : exprs) {
            if (expr.is_expression()) {
                const auto& field = expr.expression();
                if (field->group() == expressions::expression_group::aggregate) {
                    fields.push_back(field);
                } else if (field->group() == expressions::expression_group::scalar) {
                    auto scalar = boost::static_pointer_cast<expressions::scalar_expression_t>(field);
                    if (scalar->type() == expressions::scalar_type::get_field) {
                        fields.push_back(scalar);
                    } else {
                        throw std::runtime_error("Could\'t use scalar expression in a group node");
                    }
                } else {
                    throw std::runtime_error("Undefined expression type for group relation");
                }
            } else if (expr.is_key()) {
                fields.push_back(make_scalar_expression(resource, expressions::scalar_type::get_field, expr.key()));
            } else {
                throw std::runtime_error("The method supports only aggregation expressions and fields");
            }
        }
        auto group =
            make_node_group(space->dispatcher()->resource(), core::dbname_t{}, core::relname_t{}, std::move(fields));

        auto schema = group_schema(space->dispatcher()->resource(), group, relation.columns);
        auto node = make_aggregate_node(relation.node, group, nullptr, nullptr, nullptr);
        return {node, std::move(schema)};
    }

    built_relation_t relation_factory_t::select_relation(const built_relation_t& relation,
                                                         const std::vector<expression_wrapper_t>& exprs) {
        auto* resource = space->dispatcher()->resource();
        auto select = make_node_select(resource, core::dbname_t{}, core::relname_t{});
        for (const auto& expr : exprs) {
            expressions::expression_ptr scalar;
            if (expr.is_expression()) {
                const auto& field = expr.expression();
                if (field->group() == expressions::expression_group::scalar) {
                    scalar = field;
                } else if (field->group() == expressions::expression_group::aggregate) {
                    throw std::runtime_error(
                        "Aggregate expressions are not allowed in select(); use groupBy().agg() instead");
                } else {
                    throw std::runtime_error("Undefined expression type for select relation");
                }
            } else if (expr.is_key()) {
                scalar = make_scalar_expression(resource, expressions::scalar_type::get_field, expr.key());
            } else {
                throw std::runtime_error("The method supports only column expressions and fields");
            }
            select->append_expression(scalar);
        }

        auto schema = select_schema(resource, select, relation.columns);
        auto node = make_aggregate_node(relation.node, nullptr, nullptr, nullptr, select);
        return {node, std::move(schema)};
    }

    built_relation_t relation_factory_t::join_relation(const built_relation_t& relation,
                                                       const built_relation_t& other,
                                                       const std::vector<expression_wrapper_t>& exprs,
                                                       components::logical_plan::join_type type) {
        auto* resource = space->dispatcher()->resource();
        std::pmr::vector<expressions::expression_ptr> conditions(resource);
        for (const auto& expr : exprs) {
            if (expr.is_expression()) {
                if (expr.expression()->group() == expressions::expression_group::compare) {
                    conditions.push_back(expr.expression());
                } else {
                    throw std::runtime_error("Undefined expression type for sort relation");
                }
            } else if (expr.is_key()) {
                throw std::runtime_error("The method supports only conditions");
            } else {
                throw std::runtime_error("Implementation Error. Undefined expression type for condition");
            }
        }

        auto join_node = make_node_join(resource, core::dbname_t{}, core::relname_t{}, type);
        join_node->append_child(relation.node);
        join_node->append_child(other.node);
        if (!exprs.empty()) {
            for (const auto& expr : conditions) {
                join_node->append_expression(expr);
            }
        }

        // join schema: left columns followed by right columns.
        std::pmr::vector<column_definition_t> schema(resource);
        schema.reserve(relation.columns.size() + other.columns.size());
        for (const auto& col : relation.columns) {
            schema.emplace_back(col.name(), col.type());
        }
        for (const auto& col : other.columns) {
            schema.emplace_back(col.name(), col.type());
        }
        return {boost::static_pointer_cast<node_t>(join_node), std::move(schema)};
    }

    built_relation_t relation_factory_t::limit_relation(const built_relation_t& relation, int64_t count) {
        auto limit_node =
            make_node_limit(space->dispatcher()->resource(), core::dbname_t{}, core::relname_t{}, limit_t(count));
        auto node = make_aggregate_node(relation.node, nullptr, nullptr, nullptr, nullptr, limit_node);
        return {node, passthrough_schema(space->dispatcher()->resource(), relation.columns)};
    }

    // should be protected because don\'t send external data
    built_relation_t relation_factory_t::create_df_relation(std::unique_ptr<components::tableref::table_ref_t> ref) {
        auto* resource = space->dispatcher()->resource();
        auto tableData = scan_t::fetch_object_data(resource, std::move(ref));

        // Data leaf: the column schema is the data table's own columns.
        std::pmr::vector<column_definition_t> schema(resource);
        if (tableData.second) {
            schema.reserve(tableData.second->size());
            for (const auto& col : *tableData.second) {
                schema.emplace_back(col.name(), col.type());
            }
        }
        return {boost::static_pointer_cast<node_t>(tableData.first), std::move(schema)};
    }

} // namespace otterbrix
