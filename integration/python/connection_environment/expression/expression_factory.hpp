#pragma once

#include <components/types/logical_value.hpp>

#include <components/logical_plan/param_storage.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

#include <core/result_wrapper.hpp>
#include <core/types/string.hpp>

#include <optional>

#include <integration/cpp/otterbrix.hpp>

#include <unordered_map>


namespace otterbrix {
    //! R14: hand-written tagged union replacing the former
    //! std::variant<parameter_id_t, key_t, expression_ptr>. The three states are genuinely
    //! distinct (a constant parameter slot, a bare column reference, or an already-built engine
    //! expression node) and must be distinguished at runtime, so a minimal enum-tagged struct is
    //! used instead of std::variant. key_t has no default constructor, so it is held in an
    //! std::optional; the other two members are default-constructible value types.
    class Expression {
    public:
        enum class kind_t : uint8_t
        {
            parameter,
            key,
            expression
        };

        // Implicit converting constructors mirror the previous std::variant behavior so existing
        // call sites (Expression(x), make_shared<PyExpression>(key_t{...}, conn)) keep working.
        Expression(core::parameter_id_t param) // NOLINT: allow implicit creation
            : kind_(kind_t::parameter)
            , param_(param) {}
        Expression(components::expressions::key_t key) // NOLINT: allow implicit creation
            : kind_(kind_t::key)
            , param_(0)
            , key_(std::move(key)) {}
        Expression(components::expressions::expression_ptr expr) // NOLINT: allow implicit creation
            : kind_(kind_t::expression)
            , param_(0)
            , expr_(std::move(expr)) {}

        kind_t kind() const noexcept { return kind_; }

        bool is_parameter() const noexcept { return kind_ == kind_t::parameter; }
        bool is_key() const noexcept { return kind_ == kind_t::key; }
        bool is_expression() const noexcept { return kind_ == kind_t::expression; }

        core::parameter_id_t parameter() const noexcept { return param_; }
        const components::expressions::key_t& key() const noexcept { return *key_; }
        const components::expressions::expression_ptr& expression() const noexcept { return expr_; }

    private:
        kind_t kind_;
        core::parameter_id_t param_;
        std::optional<components::expressions::key_t> key_;
        components::expressions::expression_ptr expr_;
    };


    class ExpressionFactory {
    public:
        ExpressionFactory(const boost::intrusive_ptr<otterbrix_t>& space);
        virtual ~ExpressionFactory();
        void SetNullSpace();

        
        Expression MakeConstant(components::types::logical_value_t&& value);

        Expression MakeCountExpression();

        Expression SortExpression(const string& arg);

        core::result_wrapper_t<Expression>
        SortExpression(const Expression& arg,
                       components::expressions::sort_order order = components::expressions::sort_order::asc);


        core::result_wrapper_t<Expression> AggregationUnaryExpression(const string& function_name,
                const Expression& expr);

        Expression ScalarUnaryExpression(components::expressions::scalar_type type,
                const Expression& expr);

        core::result_wrapper_t<Expression> ScalarBinaryExpression(components::expressions::scalar_type type,
                const Expression& left, const Expression& right);


        core::result_wrapper_t<Expression> ComparisonExpression(components::expressions::compare_type type,
            const Expression& left, const Expression& right);

        core::result_wrapper_t<Expression> ExpressionWithAlias(const Expression& expr, const string& alias);

        core::result_wrapper_t<Expression> ComparisonNotExpression(const Expression& expr);

        core::result_wrapper_t<Expression> ComparisonUnionExpression(components::expressions::compare_type type,
            const Expression& left, const Expression& right);
        Expression TrueExpression();
    public:
        core::result_wrapper_t<components::expressions::compare_expression_ptr>
        UnionExpressionToExpressionPtr(const Expression& expr);
        core::result_wrapper_t<string> ConvertToString(const Expression& expr);
    
        components::logical_plan::parameter_node_ptr GetParams(); 

    private:
        core::parameter_id_t AddValue(components::types::logical_value_t&& value);
        std::unordered_map<core::parameter_id_t, components::types::logical_value_t> values; 
        uint64_t counter;
        boost::intrusive_ptr<otterbrix_t> space;

    };
} // namespace otterbrix
