#include "sort_expression.hpp"
#include <sstream>

namespace components::expressions {

    template<class OStream>
    OStream& operator<<(OStream& stream, const sort_expression_t* sort) {
        stream << sort->key() << ": " << int(sort->order());
        return stream;
    }

    sort_expression_t::sort_expression_t(const key_t& key, sort_order order)
        : expression_i(expression_group::sort)
        , order_(order)
        , key_(key) {}

    sort_order sort_expression_t::order() const { return order_; }

    const key_t& sort_expression_t::key() const { return key_; }

    hash_t sort_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, order_);
        boost::hash_combine(hash_, key_.hash());
        return hash_;
    }

    std::string sort_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << this;
        return stream.str();
    }

    bool sort_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const sort_expression_t*>(rhs);
        return order_ == other->order_ && key_ == other->key_;
    }

    sort_expression_ptr make_sort_expression(const key_t& key, sort_order order) {
        return new sort_expression_t(key, order);
    }

    sort_order get_sort_order(const std::string& key) {
        if (key == "1")
            return sort_order::asc;
        if (key == "-1")
            return sort_order::desc;
        return sort_order::asc;
    }

} // namespace components::expressions