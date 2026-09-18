#pragma once

#include "expression.hpp"
#include "key.hpp"

namespace components::expressions {

    class sort_expression_t;
    using sort_expression_ptr = boost::intrusive_ptr<sort_expression_t>;

    class sort_expression_t : public expression_i {
    public:
        sort_expression_t(const sort_expression_t&) = delete;
        sort_expression_t(sort_expression_t&&) noexcept = default;

        sort_expression_t(std::pmr::memory_resource* resource,
                          const param_storage& operand,
                          sort_order order,
                          sort_null_order null_order = sort_null_order::nulls_default);

        sort_order order() const;
        sort_null_order null_order() const;
        const param_storage& operand() const noexcept { return operand_; }
        param_storage& operand() noexcept { return operand_; }

    private:
        param_storage operand_;
        sort_order order_;
        sort_null_order null_order_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        bool equal_impl(const expression_i* rhs) const final;
    };

    sort_expression_ptr make_sort_expression(std::pmr::memory_resource* resource,
                                             const param_storage& operand,
                                             sort_order order,
                                             sort_null_order null_order = sort_null_order::nulls_default);
    sort_order get_sort_order(const std::string& key);

} // namespace components::expressions
