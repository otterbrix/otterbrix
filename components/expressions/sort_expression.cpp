#include "sort_expression.hpp"
#include <sstream>

namespace components::expressions {

    template<class OStream>
    OStream& operator<<(OStream& stream, const sort_expression_t* sort) {
        // A column is spelled bare here, the way a sort key has always been rendered.
        if (std::holds_alternative<key_t>(sort->operand())) {
            stream << std::get<key_t>(sort->operand());
        } else {
            stream << sort->operand();
        }
        stream << ": " << int(sort->order());
        return stream;
    }

    sort_expression_t::sort_expression_t(std::pmr::memory_resource* resource,
                                         const param_storage& operand,
                                         sort_order order,
                                         sort_null_order null_order)
        : expression_i(expression_group::sort, key_t{resource})
        , operand_(operand)
        , order_(order)
        , null_order_(null_order) {}

    sort_order sort_expression_t::order() const { return order_; }

    sort_null_order sort_expression_t::null_order() const { return null_order_; }

    hash_t sort_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, order_);
        boost::hash_combine(hash_, null_order_);
        boost::hash_combine(hash_,
                            std::visit(
                                [](const auto& value) -> std::size_t {
                                    using param_type = std::decay_t<decltype(value)>;
                                    if constexpr (std::is_same_v<param_type, core::parameter_id_t>) {
                                        return std::hash<uint64_t>()(value);
                                    } else if constexpr (std::is_same_v<param_type, key_t>) {
                                        return value.hash();
                                    } else {
                                        return value->hash();
                                    }
                                },
                                operand_));
        return hash_;
    }

    std::string sort_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << this;
        return stream.str();
    }

    bool sort_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const sort_expression_t*>(rhs);
        return order_ == other->order_ && null_order_ == other->null_order_ && operand_ == other->operand_;
    }

    sort_expression_ptr make_sort_expression(std::pmr::memory_resource* resource,
                                             const param_storage& operand,
                                             sort_order order,
                                             sort_null_order null_order) {
        return new sort_expression_t(resource, operand, order, null_order);
    }

    sort_order get_sort_order(const std::string& key) {
        if (key == "1")
            return sort_order::asc;
        if (key == "-1")
            return sort_order::desc;
        return sort_order::asc;
    }

} // namespace components::expressions
