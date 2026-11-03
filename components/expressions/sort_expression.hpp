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
        void set_operand(param_storage operand) { operand_ = std::move(operand); }

        size_t ordinal() const noexcept { return ordinal_; }
        void set_ordinal(size_t ordinal) noexcept { ordinal_ = ordinal; }

    private:
        param_storage operand_;
        sort_order order_;
        sort_null_order null_order_;
        size_t ordinal_{0};

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        bool equal_impl(const expression_i* rhs) const final;
    };

    sort_expression_ptr make_sort_expression(std::pmr::memory_resource* resource,
                                             const param_storage& operand,
                                             sort_order order,
                                             sort_null_order null_order = sort_null_order::nulls_default);
    sort_expression_ptr make_ordinal_sort_expression(std::pmr::memory_resource* resource,
                                                     size_t ordinal,
                                                     sort_order order,
                                                     sort_null_order null_order = sort_null_order::nulls_default);
    sort_order get_sort_order(const std::string& key);

} // namespace components::expressions
