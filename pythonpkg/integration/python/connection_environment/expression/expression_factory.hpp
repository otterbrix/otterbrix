#pragma once

#include <components/types/logical_value.hpp>

#include <components/logical_plan/param_storage.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

#include <core/types/string.hpp>

#include <variant>

#include <integration/cpp/otterbrix.hpp>

#include <unordered_map>


namespace otterbrix {
    using Expression = std::variant<
        core::parameter_id_t,
        components::expressions::key_t,
        //components::document::value_t,
        //components::types::logical_value_t,
        components::expressions::expression_ptr>;

    //using PrepExpression = std::variant<core::parameter_id_t,
    //    components::expressions::key_t,
    //    components::expressions::expression_ptr>;

    
    class ExpressionFactory {
    public:
        ExpressionFactory(const boost::intrusive_ptr<otterbrix_t>& space);
        virtual ~ExpressionFactory();
        void SetNullSpace();

        
        Expression MakeConstant(components::types::logical_value_t&& value);

        Expression MakeCountExpression();

        Expression SortExpression(const string& arg);

        Expression SortExpression(const Expression& arg);
            

        Expression AggregationUnaryExpression(components::expressions::aggregate_type type, 
                const Expression& expr);

        Expression ScalarUnaryExpression(components::expressions::scalar_type type, 
                const Expression& expr);

        Expression ScalarBinaryExpression(components::expressions::scalar_type type, 
                const Expression& left, const Expression& right);


        Expression ComparisonExpression(components::expressions::compare_type type, 
            const Expression& left, const Expression& right);

        Expression ExpressionWithAlias(const Expression& expr, const string& alias);
        
        Expression ComparisonNotExpression(const Expression& expr);

        Expression ComparisonUnionExpression(components::expressions::compare_type type, 
            const Expression& left, const Expression& right);
        Expression TrueExpression();
    public:
        components::expressions::compare_expression_ptr UnionExpressionToExpressionPtr(const Expression& expr);
        string ConvertToString(const Expression& expr);
    
        //PrepExpression PrepareExpression(const Expression& expr);

        components::logical_plan::parameter_node_ptr GetParams(); 

    private:
        core::parameter_id_t AddValue(components::types::logical_value_t&& value);
        std::unordered_map<core::parameter_id_t, components::types::logical_value_t> values; 
        uint64_t counter;
    private:
        boost::intrusive_ptr<otterbrix_t> space;
        //components::logical_plan::parameter_node_ptr params;

    };
} // namespace otterbrix
