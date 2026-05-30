#pragma once

#include <core/external_dependencies.hpp>

#include <core/types/memory.hpp>
#include <core/types/vector.hpp>

#include <components/table/column_definition.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <core/types/string.hpp>
#include <variant>

namespace otterbrix {
    class RelationFactory;
    class ColumnsVisitor;
    
    struct Relation {
        friend class RelationFactory;
        friend class ColumnVisitor;
        friend class ConnectionEnvironment;

    public:

        Relation(components::logical_plan::node_data_ptr data,
                shared_ptr<otterbrix::ExternalDependency> external_dependency,
                unique_ptr<vector<components::table::column_definition_t>> columns);

        Relation(shared_ptr<Relation> resource,
                components::logical_plan::node_group_ptr group,
                components::logical_plan::node_match_ptr match,
                components::logical_plan::node_sort_ptr sort,
                components::logical_plan::node_select_ptr select, string name,
                components::logical_plan::node_limit_ptr limit = nullptr);

        Relation(shared_ptr<Relation> left, shared_ptr<Relation> right,
                unique_ptr<vector<components::expressions::expression_ptr>> conditions,
                components::logical_plan::join_type join_type);

        Relation(shared_ptr<Relation> resource, int64_t limit_count);

    public:

        struct Aggregate {
            Aggregate(shared_ptr<Relation> resource,
                components::logical_plan::node_group_ptr group,
                components::logical_plan::node_match_ptr match,
                components::logical_plan::node_sort_ptr sort,
                components::logical_plan::node_select_ptr select, string name,
                components::logical_plan::node_limit_ptr limit = nullptr)
                : resource(resource), group(group), match(match), sort(sort), select(select),
                  name(std::move(name)), limit(limit) {}
            shared_ptr<Relation> resource;
            components::logical_plan::node_group_ptr group;
            components::logical_plan::node_match_ptr match;
            components::logical_plan::node_sort_ptr sort;
            components::logical_plan::node_select_ptr select;
            string name;
            components::logical_plan::node_limit_ptr limit;
        };
        
        struct Data {
            Data(components::logical_plan::node_data_ptr data,
                shared_ptr<ExternalDependency> external_dependency,
                unique_ptr<vector<components::table::column_definition_t>> columns)
                : data(data), external_dependency(std::move(external_dependency)), columns(std::move(columns)) {}
            components::logical_plan::node_data_ptr data;
            shared_ptr<otterbrix::ExternalDependency> external_dependency;
            unique_ptr<vector<components::table::column_definition_t>> columns;
        };

        struct Join {
            Join(shared_ptr<Relation> left, shared_ptr<Relation> right,
                unique_ptr<vector<components::expressions::expression_ptr>> conditions,
                components::logical_plan::join_type join_type)
                : left(left), right(right), conditions(std::move(conditions)), join_type(join_type) {}
            shared_ptr<Relation> left;
            shared_ptr<Relation> right;
            unique_ptr<vector<components::expressions::expression_ptr>> conditions;
            components::logical_plan::join_type join_type;
        };

        struct Limit {
            Limit(shared_ptr<Relation> resource, int64_t count)
                : resource(resource), count(count) {}
            shared_ptr<Relation> resource;
            int64_t count;
        };

        Relation(Relation&& other) noexcept;

        vector<components::table::column_definition_t> GetColumns();


    private:

        std::variant<Aggregate, Data, Join, Limit> relation;
        

    };
} // namespace otterbrix
