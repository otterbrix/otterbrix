#pragma once
#include "relation.hpp"
#include "../expression/expression_factory.hpp"
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <core/external_dependencies.hpp>
#include <core/types/memory.hpp>
#include <components/tableref/tableref.hpp>
#include <integration/cpp/otterbrix.hpp>


using namespace components::logical_plan;

namespace otterbrix {
    
    class RelationFactory {
    public:
        RelationFactory(const boost::intrusive_ptr<otterbrix_t>& space);
        virtual ~RelationFactory();
        void SetNullSpace();

        static shared_ptr<Relation> make_data_relation(node_data_ptr data, 
                shared_ptr<ExternalDependency> external_dependency, 
                unique_ptr<vector<components::table::column_definition_t>> columns);
    
        shared_ptr<Relation> make_aggregate_relation(shared_ptr<Relation> from, components::logical_plan::node_group_ptr group, 
                components::logical_plan::node_match_ptr match, components::logical_plan::node_sort_ptr sort);
        
        static shared_ptr<Relation> make_join_relation(shared_ptr<Relation> left, shared_ptr<Relation> right, 
                unique_ptr<vector<components::expressions::expression_ptr>> conditions,
                components::logical_plan::join_type type);

        shared_ptr<Relation> FilterRelation(shared_ptr<Relation> relation, const Expression& condition);
        shared_ptr<Relation> SortRelation(shared_ptr<Relation> relation, const vector<Expression>& exprs);
        shared_ptr<Relation> GroupRelation(shared_ptr<Relation> relation, const vector<Expression>& exprs);

        shared_ptr<Relation> JoinRelation(shared_ptr<Relation> relation, shared_ptr<Relation> other, 
                const vector<Expression>& exprs, components::logical_plan::join_type type);


        shared_ptr<Relation> CreateFromSelect(components::logical_plan::node_ptr plan);
        //Relation CreateFromTable(const collection_full_name_t& name);
        shared_ptr<Relation> CreateDFRelation(unique_ptr<components::tableref::TableRef> tableref);

        components::logical_plan::node_ptr Execute(const Relation& rel);



    private:
        boost::intrusive_ptr<otterbrix_t> space;
    };

} // namespace otterbrix
