#include "compare_expression.hpp"
#include <boost/container_hash/hash.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <sstream>

namespace components::expressions {

    bool is_union_compare_condition(compare_type type) {
        return type == compare_type::union_and || type == compare_type::union_or || type == compare_type::union_not;
    }

    compare_expression_t::compare_expression_t(std::pmr::memory_resource* resource,
                                               compare_type type,
                                               side_t side,
                                               const key_t& key,
                                               core::parameter_id_t value)
        : expression_i(expression_group::compare)
        , type_(type)
        , side_(side)
        , value_(value)
        , children_(resource) {
        switch (side_) {
            case side_t::undefined:
            case side_t::left:
                key_left_ = key;
                break;
            case side_t::right:
                key_right_ = key;
                break;
        }
    }

    compare_expression_t::compare_expression_t(std::pmr::memory_resource* resource,
                                               compare_type type,
                                               const key_t& key_left,
                                               const key_t& key_right)
        : expression_i(expression_group::compare)
        , type_(type)
        , side_(side_t::undefined)
        , key_left_(key_left)
        , key_right_(key_right)
        , children_(resource) {}

    compare_type compare_expression_t::type() const { return type_; }

    side_t compare_expression_t::side() const { return side_; }

    const key_t& compare_expression_t::key_left() const { return key_left_; }

    const key_t& compare_expression_t::key_right() const { return key_right_; }

    core::parameter_id_t compare_expression_t::value() const { return value_; }

    const std::pmr::vector<expression_ptr>& compare_expression_t::children() const { return children_; }

    void compare_expression_t::set_type(compare_type type) { type_ = type; }

    void compare_expression_t::append_child(const expression_ptr& child) { children_.push_back(child); }

    bool compare_expression_t::is_union() const { return is_union_compare_condition(type_); }

    expression_ptr compare_expression_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto type = deserializer->deserialize_enum<compare_type>(1);
        auto side = deserializer->deserialize_enum<side_t>(2);
        auto key_left = deserializer->deserialize_key(3);
        auto key_right = deserializer->deserialize_key(4);
        auto param = deserializer->deserialize_param_id(5);
        deserializer->advance_array(6);
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
            if (side == side_t::undefined) {
                res = make_compare_expression(deserializer->resource(), type, key_left, key_right);
            } else {
                if (side == side_t::left) {
                    res = make_compare_expression(deserializer->resource(), type, side, key_left, param);
                } else {
                    res = make_compare_expression(deserializer->resource(), type, side, key_right, param);
                }
            }
        }
        return res;
    }

    hash_t compare_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, type_);
        boost::hash_combine(hash_, key_left_.hash());
        boost::hash_combine(hash_, key_right_.hash());
        boost::hash_combine(hash_, std::hash<uint64_t>()(value_));
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
            if (!key_left().is_null() && !key_right().is_null()) {
                stream << "\"" << key_left() << "\": {" << type() << ": \"" << key_right() << "\"}";
            } else {
                if (key_right().is_null()) {
                    stream << "\"" << key_left() << "\": {" << type() << ": #" << value().t << "}";
                } else {
                    stream << "\"" << key_right() << "\": {" << type() << ": #" << value().t << "}";
                }
            }
        }
        return stream.str();
    }

    bool compare_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const compare_expression_t*>(rhs);
        return type_ == other->type_ && key_left_ == other->key_left_ && key_right_ == other->key_right_ &&
               value_ == other->value_ && children_.size() == other->children_.size() &&
               std::equal(children_.begin(), children_.end(), other->children_.begin());
    }

    void compare_expression_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(7);
        serializer->append_enum(serializer::serialization_type::expression_compare);
        serializer->append_enum(type_);
        serializer->append_enum(side_);
        serializer->append(key_left_);
        serializer->append(key_right_);
        serializer->append(value_);
        serializer->start_array(children_.size());
        for (const auto& expr : children_) {
            expr->serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource,
                                                   compare_type type,
                                                   side_t side,
                                                   const key_t& key,
                                                   core::parameter_id_t id) {
        return new compare_expression_t(resource, type, side, key, id);
    }

    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource,
                                                   compare_type type,
                                                   const key_t& key_left,
                                                   const key_t& key_right) {
        return new compare_expression_t(resource, type, key_left, key_right);
    }

    compare_expression_ptr make_compare_expression(std::pmr::memory_resource* resource, compare_type type) {
        assert(!is_union_compare_condition(type));
        return new compare_expression_t(resource, type, side_t::undefined, key_t{}, core::parameter_id_t{0});
    }

    compare_expression_ptr make_compare_union_expression(std::pmr::memory_resource* resource, compare_type type) {
        assert(is_union_compare_condition(type));
        return new compare_expression_t(resource, type, side_t::undefined, key_t{}, core::parameter_id_t{0});
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
