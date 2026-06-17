#pragma once

#include <components/types/logical_value.hpp>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <core/result_wrapper.hpp>

#include <optional>

#include <integration/cpp/otterbrix.hpp>

#include <string>
#include <unordered_map>

namespace otterbrix {
    //! R14: hand-written tagged union replacing the former
    //! std::variant<parameter_id_t, key_t, expression_ptr>. The three states are genuinely
    //! distinct (a constant parameter slot, a bare column reference, or an already-built engine
    //! expression node) and must be distinguished at runtime, so a minimal enum-tagged struct is
    //! used instead of std::variant. key_t has no default constructor, so it is held in an
    //! std::optional; the other two members are default-constructible value types.
    class expression_wrapper_t {
    public:
        enum class kind_t : uint8_t
        {
            parameter,
            key,
            expression
        };

        // Implicit converting constructors mirror the previous std::variant behavior so existing
        // call sites (expression_wrapper_t(x), make_shared<py_expression_t>(key_t{...}, conn)) keep working.
        expression_wrapper_t(core::parameter_id_t param) // NOLINT: allow implicit creation
            : kind_(kind_t::parameter)
            , param_(param) {}
        expression_wrapper_t(components::expressions::key_t key) // NOLINT: allow implicit creation
            : kind_(kind_t::key)
            , param_(0)
            , key_(std::move(key)) {}
        expression_wrapper_t(components::expressions::expression_ptr expr) // NOLINT: allow implicit creation
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

    class expression_factory_t {
    public:
        expression_factory_t(const boost::intrusive_ptr<otterbrix_t>& space);
        virtual ~expression_factory_t();
        void set_null_space();

        expression_wrapper_t make_constant(components::types::logical_value_t&& value);

        expression_wrapper_t make_count_expression();

        expression_wrapper_t sort_expression(const std::string& arg);

        core::result_wrapper_t<expression_wrapper_t>
        sort_expression(const expression_wrapper_t& arg,
                        components::expressions::sort_order order = components::expressions::sort_order::asc);

        core::result_wrapper_t<expression_wrapper_t> aggregation_unary_expression(const std::string& function_name,
                                                                                  const expression_wrapper_t& expr);

        expression_wrapper_t scalar_unary_expression(components::expressions::scalar_type type,
                                                     const expression_wrapper_t& expr);

        core::result_wrapper_t<expression_wrapper_t> scalar_binary_expression(components::expressions::scalar_type type,
                                                                              const expression_wrapper_t& left,
                                                                              const expression_wrapper_t& right);

        core::result_wrapper_t<expression_wrapper_t> comparison_expression(components::expressions::compare_type type,
                                                                           const expression_wrapper_t& left,
                                                                           const expression_wrapper_t& right);

        core::result_wrapper_t<expression_wrapper_t> expression_with_alias(const expression_wrapper_t& expr,
                                                                           const std::string& alias);

        core::result_wrapper_t<expression_wrapper_t> comparison_not_expression(const expression_wrapper_t& expr);

        core::result_wrapper_t<expression_wrapper_t>
        comparison_union_expression(components::expressions::compare_type type,
                                    const expression_wrapper_t& left,
                                    const expression_wrapper_t& right);
        expression_wrapper_t true_expression();

    public:
        core::result_wrapper_t<components::expressions::compare_expression_ptr>
        union_expression_to_expression_ptr(const expression_wrapper_t& expr);
        core::result_wrapper_t<std::string> convert_to_string(const expression_wrapper_t& expr);

        components::logical_plan::parameter_node_ptr get_params();

    private:
        core::parameter_id_t add_value(components::types::logical_value_t&& value);
        std::unordered_map<core::parameter_id_t, components::types::logical_value_t> values;
        uint64_t counter;
        boost::intrusive_ptr<otterbrix_t> space;
    };
} // namespace otterbrix
