#include <iostream>

#include "relation_factory.hpp"
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_match.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <scan/python_replacement_scan.hpp>
#include <core/types/string.hpp>

using namespace components;
using namespace components::logical_plan;
namespace otterbrix {

    RelationFactory::RelationFactory(const boost::intrusive_ptr<otterbrix_t>& space) : space(space) {

    }

    RelationFactory::~RelationFactory() = default;
    
    void RelationFactory::SetNullSpace() {
        space = nullptr;
    }

    shared_ptr<Relation> RelationFactory::make_data_relation(node_data_ptr data, 
            shared_ptr<ExternalDependency> external_dependency, unique_ptr<vector<components::table::column_definition_t>> columns) {
        return make_shared<Relation>(data, external_dependency, std::move(columns));
    }
    
    shared_ptr<Relation> RelationFactory::make_aggregate_relation(shared_ptr<Relation> from, node_group_ptr group, 
            node_match_ptr match, node_sort_ptr sort) {
        static int indx = 0;
        auto session = otterbrix::session_id_t();
        string name = "t";
        name += to_string(indx++);
        space->dispatcher()->create_collection(session, "tmp", name);
        auto res = make_shared<Relation>(from, group, match, sort, name);
        return res;
    }

    shared_ptr<Relation> RelationFactory::make_join_relation(shared_ptr<Relation> left, shared_ptr<Relation> right, 
            unique_ptr<vector<expression_ptr>> conditions,
            components::logical_plan::join_type type) {
        return make_shared<Relation>(left, right, std::move(conditions), type);
    }

    shared_ptr<Relation> RelationFactory::FilterRelation(shared_ptr<Relation> relation, const Expression& condition) { 
        auto match_node = std::visit([resource = space->dispatcher()->resource()](const auto& expr) -> node_match_ptr {
                   using T = std::decay_t<decltype(expr)>; 
                   if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                       if (expr->group() == expressions::expression_group::compare) {
                          return make_node_match(resource, {}, expr);
                       } else if constexpr (std::is_same_v<T, types::logical_value_t> || 
                               std::is_same_v<T, expressions::key_t>) {
                           throw std::runtime_error("The method supports only condition expressions");
                       }
                       throw std::runtime_error("Implementation Error. Undefined expression for filter");
                   } 
                   throw std::runtime_error("The method supports only condition expression");
               }, condition);
        return make_aggregate_relation(relation, nullptr, match_node, nullptr);
    }

    shared_ptr<Relation> RelationFactory::SortRelation(shared_ptr<Relation> relation, const vector<Expression>& exprs) {
        if (exprs.empty()) {
            throw std::runtime_error("Please provide at least one expression to sort on");
        }
        std::pmr::vector<expressions::expression_ptr> sort_exprs(space->dispatcher()->resource());
        sort_exprs.reserve(exprs.size());
        for (const auto& expr : exprs) {
            sort_exprs.push_back(
                    std::visit([](const auto& sort_expr) -> expressions::expression_ptr {
                        using T = std::decay_t<decltype(sort_expr)>;
                        if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                            if (sort_expr->group() == expressions::expression_group::sort) {
                                return sort_expr;//auto casted = boost::static_pointer_cast<expressions::expression_t> 
                            } else {
                                throw std::runtime_error("Undefined expression type for sort relation");
                            }
                        } else if constexpr (std::is_same_v<T, types::logical_value_t> || 
                                std::is_same_v<T, expressions::key_t>) {
                            throw std::runtime_error("The method supports only sort expressions");
                        } else {
                            throw std::runtime_error("Implementation Error. Undefined expression type for sort relation");
                        }
                    }, expr));
        }
        auto sort = make_node_sort(space->dispatcher()->resource(), {}, std::move(sort_exprs));

        return make_aggregate_relation(relation, nullptr, nullptr, sort);

    }

    shared_ptr<Relation> RelationFactory::GroupRelation(shared_ptr<Relation> relation, const vector<Expression>& exprs) {
        vector<expressions::expression_ptr> fields;
        fields.reserve(exprs.size());
        for (const auto& expr : exprs) {
            fields.push_back(
                    std::visit([resource=space->dispatcher()->resource()](const auto& field) -> expressions::expression_ptr {
                        using T = std::decay_t<decltype(field)>;
                        if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                            if (field->group() == expressions::expression_group::aggregate) {
                                return field;
                            } else if (field->group() == expressions::expression_group::scalar) {
                                auto scalar = boost::static_pointer_cast<expressions::scalar_expression_t>(field);
                                if (scalar->type() == expressions::scalar_type::get_field) {
                                    return scalar;
                                } else {
                                    throw std::runtime_error("Could\'t use scalar expression in a group node");
                                }

                            } else {
                                throw std::runtime_error("Undefined expression type for group relation");
                            }
                        } else if constexpr (std::is_same_v<T, expressions::key_t>) {
                            return make_scalar_expression(resource, expressions::scalar_type::get_field, field);
                        } else if constexpr (std::is_same_v<T, types::logical_value_t>) {
                            throw std::runtime_error("The method supports only aggregation expressions and fields");
                        } else {
                            throw std::runtime_error("Implementation Error. Undefined expression type for group relation");
                        }
                    }, expr));
        }
        auto group = make_node_group(space->dispatcher()->resource(), {}, std::move(fields));

        return make_aggregate_relation(relation, group, nullptr, nullptr);
    }

    shared_ptr<Relation> RelationFactory::JoinRelation(shared_ptr<Relation> relation, shared_ptr<Relation> other, 
            const vector<Expression>& exprs, components::logical_plan::join_type type) {
        auto conditions = make_unique<vector<expressions::expression_ptr>>();
        for (const auto& expr : exprs) {
            conditions->push_back(
                    std::visit([](const auto& cond_expr) -> expressions::expression_ptr {
                        using T = std::decay_t<decltype(cond_expr)>;
                        if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                            if (cond_expr->group() == expressions::expression_group::compare) {
                                return cond_expr;//auto casted = boost::static_pointer_cast<expressions::expression_t> 
                            } else {
                                throw std::runtime_error("Undefined expression type for sort relation");
                            }
                        } else if constexpr (std::is_same_v<T, types::logical_value_t> || 
                                std::is_same_v<T, expressions::key_t>) {
                            throw std::runtime_error("The method supports only conditions");
                        } else {
                            throw std::runtime_error("Implementation Error. Undefined expression type for condition");
                        }
                        
                        
                    }, expr));
        
        }
       
        if (exprs.empty()) {
            conditions = nullptr;
        }
        return make_join_relation(relation, other, std::move(conditions), type);
    }
    // // bad, because no columns info
    // Relation RelationFactory::CreateFromTable(const collection_full_name_t& name) {
    //     auto aggregator = logical_plan::make_node_aggregate(space->dispatcher()->resource(), name);

    //     return Relation{aggregator};
    // }

    shared_ptr<Relation> RelationFactory::CreateFromSelect(components::logical_plan::node_ptr plan) {
        //return Relation::make_relation(boost::static_pointer_cast<components::logical_plan::node_aggregate_t>(plan));
        return nullptr;
    }

    // should be protected because don\'t send external data
    shared_ptr<Relation> RelationFactory::CreateDFRelation(unique_ptr<components::tableref::TableRef> ref) {
        auto external_dependency = ref->external_dependency;

        auto tableData = Scan::FetchObjectData(space->dispatcher()->resource(), std::move(ref));
        return make_shared<Relation>(tableData.first, external_dependency, std::move(tableData.second));
    }

    logical_plan::node_ptr RelationFactory::Execute(const Relation& rel) {
        return std::visit([this, resource = space->dispatcher()->resource()](const auto& rel) {
            using plan_type = std::decay_t<decltype(rel)>;
            if constexpr (std::is_same_v<plan_type, Relation::Aggregate>) {
                const Relation& val = *(rel.resource);
                auto res = RelationFactory::Execute(val);

                auto aggregator = logical_plan::make_node_aggregate(resource, {"tmp", rel.name});
                aggregator->append_child(res);
                
                if (rel.group) {
                    aggregator->append_child(rel.group);
                }
                if (rel.match) {
                    aggregator->append_child(rel.match);
                }
                if (rel.sort) {
                    aggregator->append_child(rel.sort);
                }
                return boost::static_pointer_cast<node_t>(aggregator); 
            } else if constexpr (std::is_same_v<plan_type, Relation::Data>) {
                return boost::static_pointer_cast<node_t>(rel.data);
            } else if constexpr (std::is_same_v<plan_type, Relation::Join>) {
                auto left = RelationFactory::Execute(*(rel.left));
                auto right = RelationFactory::Execute(*(rel.right));
                auto join_node = logical_plan::make_node_join(resource, {}, rel.join_type);
                join_node->append_child(left);
                join_node->append_child(right);
                if (rel.conditions) {
                    const auto cond_vector = *(rel.conditions);
                    for (const auto& expr : cond_vector) {
                        join_node->append_expression(expr);
                    }
                }
                return boost::static_pointer_cast<node_t>(join_node);
            }
            throw std::runtime_error("Implementation error. Undefined executed node");
        }, rel.relation);

    }


} // namespace 
