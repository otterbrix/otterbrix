#pragma once

#include "expression.hpp"
#include "key.hpp"
#include <components/compute/function.hpp>
#include <memory_resource>

namespace components::expressions {

    class compare_expression_t;
    using compare_expression_ptr = boost::intrusive_ptr<compare_expression_t>;

    class compare_expression_t final : public expression_i {
    public:
        compare_expression_t(const compare_expression_t&) = delete;
        compare_expression_t(compare_expression_t&&) noexcept = default;
        ~compare_expression_t() override = default;

        compare_expression_t(std::pmr::memory_resource* resource,
                             compare_type type,
                             const param_storage& left,
                             const param_storage& right);

        compare_type type() const;
        // The output alias when the comparison is projected as a column. Deliberately outside
        // hash/equal: two comparisons differing only by alias are the same computation.
        void set_key(const key_t& key);
        param_storage& left();
        const param_storage& left() const;
        param_storage& right();
        const param_storage& right() const;
        const std::pmr::vector<expression_ptr>& children() const;
        std::pmr::vector<expression_ptr>& children();

        void set_type(compare_type type);
        void append_child(const expression_ptr& child);

        bool is_union() const;

        // Only meaningful for compare_type::any and compare_type::all.
        // compare_type::invalid means not set.
        compare_type inner_op() const noexcept;
        void set_inner_op(compare_type op) noexcept;

        bool do_not_fold() const noexcept;
        void make_unfoldable() noexcept;

        // Flags for SQL LIKE -> regex transformation ('i' case-insensitive, 'l' glob, 'n' negated)
        core::parameter_id_t regex_flags_param() const noexcept;
        void set_regex_flags(core::parameter_id_t flags) noexcept;

        // The match function, resolved by validation. Should become a plain function call operand.
        void add_function_uid(compute::function_uid uid) noexcept;
        compute::function_uid function_uid() const noexcept;

    private:
        compare_type type_;
        compare_type inner_op_ = compare_type::invalid;
        bool do_not_fold_ = false;
        core::parameter_id_t regex_flags_param_{0};
        compute::function_uid function_uid_{compute::invalid_function_uid};
        param_storage left_;
        param_storage right_;
        std::pmr::vector<expression_ptr> children_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        bool equal_impl(const expression_i* rhs) const override;
    };

    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource,
                                                   compare_type type,
                                                   const param_storage& left,
                                                   const param_storage& right);
    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource, compare_type type);
    compare_expression_ptr make_compare_union_expression(std::pmr::memory_resource* resource, compare_type type);

    bool is_union_compare_condition(compare_type type);
    compare_type get_compare_type(const std::string& key);

    // Encapsulated access to the `key_t` alternative of `param_storage` (a
    // std::variant type alias). New code names is_key/as_key instead of
    // std::holds_alternative/std::get<key_t>, so no new site names std::variant.
    // as_key must be guarded by is_key (it delegates to std::get, which throws on a
    // mismatched alternative).
    bool is_key(const param_storage& param) noexcept;
    const key_t& as_key(const param_storage& param);
    key_t& as_key(param_storage& param);

    // Same encapsulation for the nested-expression alternative of `param_storage` (a
    // compare/scalar/function operand that is itself an expression). New code reads it
    // via is_expr/as_expr instead of std::holds_alternative/std::get.
    // as_expr must be guarded by is_expr (it delegates to std::get, which throws on a
    // mismatched alternative).
    bool is_expr(const param_storage& param) noexcept;
    const expression_ptr& as_expr(const param_storage& param);
    expression_ptr& as_expr(param_storage& param);

    // Same encapsulation for the parameter-id alternative of `param_storage`.
    // as_parameter must be guarded by is_parameter (it delegates to std::get,
    // which throws on a mismatched alternative).
    bool is_parameter(const param_storage& param) noexcept;
    const core::parameter_id_t& as_parameter(const param_storage& param);
    core::parameter_id_t& as_parameter(param_storage& param);

    // usefull reduction from compare_type
    enum class condition_kind
    {
        computed,
        always,
        never
    };
    condition_kind classify_condition(const expression_ptr& expression) noexcept;

} // namespace components::expressions