#include "cast_expression.hpp"

#include <sstream>

namespace components::expressions {

    cast_expression_t::cast_expression_t(std::pmr::memory_resource* resource,
                                         const param_storage& child,
                                         const types::complex_logical_type& target,
                                         casts::cast_t cast,
                                         casts::cast_kind kind)
        : expression_i(expression_group::cast, key_t{resource})
        , child_(child)
        , cast_(std::move(cast))
        , kind_(kind) {
        set_result_type(target);
    }

    hash_t cast_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, static_cast<uint8_t>(kind_));
        boost::hash_combine(hash_, static_cast<uint8_t>(result_type().type()));
        auto child_hash = std::visit(
            [](const auto& value) {
                using param_type = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<param_type, core::parameter_id_t>) {
                    return std::hash<uint64_t>()(value);
                } else if constexpr (std::is_same_v<param_type, key_t>) {
                    return value.hash();
                } else if constexpr (std::is_same_v<param_type, expression_ptr>) {
                    return value->hash();
                }
            },
            child_);
        boost::hash_combine(hash_, child_hash);
        return hash_;
    }

    std::string cast_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << "{$cast: " << child_ << " AS " << static_cast<int>(result_type().type()) << "}";
        return stream.str();
    }

    bool cast_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const cast_expression_t*>(rhs);
        return kind_ == other->kind_ && result_type() == other->result_type() && child_ == other->child_;
    }

    cast_expression_ptr make_cast_expression(std::pmr::memory_resource* resource,
                                             const param_storage& child,
                                             const types::complex_logical_type& target,
                                             casts::cast_t cast,
                                             casts::cast_kind kind) {
        return new cast_expression_t(resource, child, target, std::move(cast), kind);
    }

} // namespace components::expressions