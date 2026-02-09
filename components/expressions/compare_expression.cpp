#include "compare_expression.hpp"
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <sstream>

namespace std {
    template<>
    struct hash<components::expressions::param_storage> {
        std::size_t operator()(const components::expressions::param_storage& arg) const noexcept {
            if (std::holds_alternative<components::expressions::key_t>(arg)) {
                return std::get<components::expressions::key_t>(arg).hash();
            } else if (std::holds_alternative<core::parameter_id_t>(arg)) {
                return std::hash<uint64_t>()(std::get<core::parameter_id_t>(arg));
            } else {
                assert(std::holds_alternative<components::expressions::expression_ptr>(arg));
                return std::get<components::expressions::expression_ptr>(arg)->hash();
            }
        }
    };
} // namespace std

namespace components::expressions {

    bool is_union_compare_condition(compare_type type) {
        return type == compare_type::union_and || type == compare_type::union_or || type == compare_type::union_not;
    }

    compare_expression_t::compare_expression_t(std::pmr::memory_resource* resource,
                                               compare_type type,
                                               const param_storage& left,
                                               const param_storage& right)
        : expression_i(expression_group::compare)
        , type_(type)
        , left_(left)
        , right_(right)
        , children_(resource) {}

    compare_type compare_expression_t::type() const { return type_; }

    param_storage& compare_expression_t::left() { return left_; }

    const param_storage& compare_expression_t::left() const { return left_; }

    param_storage& compare_expression_t::right() { return right_; }

    const param_storage& compare_expression_t::right() const { return right_; }

    const std::pmr::vector<expression_ptr>& compare_expression_t::children() const { return children_; }

    void compare_expression_t::set_type(compare_type type) { type_ = type; }

    void compare_expression_t::append_child(const expression_ptr& child) { children_.push_back(child); }

    bool compare_expression_t::is_union() const { return is_union_compare_condition(type_); }

    expression_ptr compare_expression_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto type = deserializer->deserialize_enum<compare_type>(1);
        auto arg_left = deserialize_param_storage(deserializer, 2);
        auto arg_right = deserialize_param_storage(deserializer, 3);
        deserializer->advance_array(4);
        std::pmr::vector<expression_ptr> exprs(deserializer->resource());
        exprs.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < deserializer->current_array_size(); i++) {
            deserializer->advance_array(i);
            exprs.emplace_back(expression_i::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();

        compare_expression_ptr res;
        if (is_union_compare_condition(type)) {
            res = make_compare_union_expression(deserializer->resource(), type);
            for (const auto& expr : exprs) {
                res->append_child(expr);
            }
        } else {
            res = make_compare_expression(deserializer->resource(), type, arg_left, arg_right);
        }
        return res;
    }

    hash_t compare_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, type_);
        boost::hash_combine(hash_, std::hash<param_storage>()(left_));
        boost::hash_combine(hash_, std::hash<param_storage>()(right_));
        for (const auto& child : children_) {
            boost::hash_combine(hash_, reinterpret_cast<const compare_expression_ptr&>(child)->hash_impl());
        }
        return hash_;
    }

    std::string compare_expression_t::to_string_impl() const {
        std::stringstream stream;
        if (type() == compare_type::all_true || type() == compare_type::all_false) {
            stream << type();
        } else if (is_union()) {
            stream << type() << ": [";
            for (std::size_t i = 0; i < children().size(); ++i) {
                if (i > 0) {
                    stream << ", ";
                }
                stream << reinterpret_cast<const compare_expression_ptr&>(children().at(i))->to_string_impl();
            }
            stream << "]";
        } else {
            stream << left_ << ": {" << type() << ": " << right_ << "}";
        }
        return stream.str();
    }

    bool compare_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const compare_expression_t*>(rhs);
        return type_ == other->type_ && left_ == other->left_ && right_ == other->right_ &&
               children_.size() == other->children_.size() &&
               std::equal(children_.begin(), children_.end(), other->children_.begin());
    }

    void compare_expression_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(5);
        serializer->append_enum(serializer::serialization_type::expression_compare);
        serializer->append_enum(type_);
        serialize_param_storage(serializer, left_);
        serialize_param_storage(serializer, right_);
        serializer->start_array(children_.size());
        for (const auto& expr : children_) {
            expr->serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource,
                                                   compare_type type,
                                                   const param_storage& left,
                                                   const param_storage& right) {
        return new compare_expression_t(resource, type, left, right);
    }

    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource, compare_type type) {
        assert(!is_union_compare_condition(type));
        return new compare_expression_t(resource, type, nullptr, nullptr);
    }

    compare_expression_ptr make_compare_union_expression(std::pmr::memory_resource* resource, compare_type type) {
        assert(is_union_compare_condition(type));
        return new compare_expression_t(resource, type, nullptr, nullptr);
    }

    compare_type get_compare_type(const std::string& key) {
        if (key.empty())
            return compare_type::invalid;
        auto type = magic_enum::enum_cast<compare_type>(key.substr(1));
        if (type.has_value())
            return type.value();
        if (key == "$and")
            return compare_type::union_and;
        if (key == "$or")
            return compare_type::union_or;
        if (key == "$not")
            return compare_type::union_not;
        return compare_type::invalid;
    }

} // namespace components::expressions
