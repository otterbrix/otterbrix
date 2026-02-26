#include "expression.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::expressions {

    expression_group expression_i::group() const { return group_; }

    hash_t expression_i::hash() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, group_);
        boost::hash_combine(hash_, hash_impl());
        return hash_;
    }

    std::string expression_i::to_string() const { return to_string_impl(); }

    bool expression_i::operator==(const expression_i& rhs) const { return group_ == rhs.group_ && equal_impl(&rhs); }

    bool expression_i::operator!=(const expression_i& rhs) const { return !operator==(rhs); }

    expression_i::expression_i(expression_group group)
        : group_(group) {}

} // namespace components::expressions
