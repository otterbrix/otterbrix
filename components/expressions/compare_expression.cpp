#include "compare_expression.hpp"
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

    hash_t compare_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, type_);
        boost::hash_combine(hash_, std::hash<param_storage>()(left_));
        boost::hash_combine(hash_, std::hash<param_storage>()(right_));
        for (const auto& child : children_) {
            boost::hash_combine(hash_, child->hash());
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
                stream << children().at(i)->to_string();
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
        if (key.empty()) {
            return compare_type::invalid;
        } else if (key[0] == '$') {
            return get_compare_type(key.substr(1));
        } else if (key == "eq") {
            return compare_type::eq;
        } else if (key == "ne") {
            return compare_type::ne;
        } else if (key == "gt") {
            return compare_type::gt;
        } else if (key == "lt") {
            return compare_type::lt;
        } else if (key == "gte") {
            return compare_type::gte;
        } else if (key == "lte") {
            return compare_type::lte;
        } else if (key == "regex") {
            return compare_type::regex;
        } else if (key == "any") {
            return compare_type::any;
        } else if (key == "all") {
            return compare_type::all;
        } else if (key == "union_and" || key == "and") {
            return compare_type::union_and;
        } else if (key == "union_or" || key == "or") {
            return compare_type::union_or;
        } else if (key == "union_not" || key == "not") {
            return compare_type::union_not;
        } else if (key == "all_true") {
            return compare_type::all_true;
        } else if (key == "all_false") {
            return compare_type::all_false;
        } else if (key == "is_null") {
            return compare_type::is_null;
        } else if (key == "is_not_null") {
            return compare_type::is_not_null;
        } else {
            return compare_type::invalid;
        }
    }

} // namespace components::expressions
