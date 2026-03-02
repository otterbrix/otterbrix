#pragma once

#include "expression.hpp"
#include "key.hpp"
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
        param_storage& left();
        const param_storage& left() const;
        param_storage& right();
        const param_storage& right() const;
        const std::pmr::vector<expression_ptr>& children() const;

        void set_type(compare_type type);
        void append_child(const expression_ptr& child);

        bool is_union() const;

    private:
        compare_type type_;
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

} // namespace components::expressions