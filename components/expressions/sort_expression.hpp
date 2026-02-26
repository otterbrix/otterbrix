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

        sort_expression_t(const key_t& key, sort_order order);

        sort_order order() const;
        const key_t& key() const;

    private:
        sort_order order_;
        key_t key_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        bool equal_impl(const expression_i* rhs) const final;
    };

    sort_expression_ptr make_sort_expression(const key_t& key, sort_order order);
    sort_order get_sort_order(const std::string& key);

} // namespace components::expressions