#include "node.hpp"

#include <algorithm>
#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_t::node_t(std::pmr::memory_resource* resource, node_type type)
        : type_(type)
        , children_(resource)
        , expressions_(resource) {}

    node_type node_t::type() const { return type_; }

    const std::string& node_t::result_alias() const { return result_alias_; }

    const std::pmr::vector<node_ptr>& node_t::children() const { return children_; }
    std::pmr::vector<node_ptr>& node_t::children() { return children_; }

    const std::pmr::vector<expression_ptr>& node_t::expressions() const { return expressions_; }
    std::pmr::vector<expression_ptr>& node_t::expressions() { return expressions_; }

    void node_t::set_result_alias(const std::string& alias) { result_alias_ = alias; }

    void node_t::reserve_child(std::size_t count) { children_.reserve(count); }

    void node_t::append_child(const node_ptr& child) { children_.push_back(child); }

    void node_t::append_expression(const expression_ptr& expression) { expressions_.push_back(expression); }

    void node_t::append_expressions(const std::vector<expression_ptr>& expressions) {
        expressions_.reserve(expressions_.size() + expressions.size());
        std::copy(expressions.begin(), expressions.end(), std::back_inserter(expressions_));
    }
    void node_t::append_expressions(const std::pmr::vector<expression_ptr>& expressions) {
        expressions_.reserve(expressions_.size() + expressions.size());
        std::copy(expressions.begin(), expressions.end(), std::back_inserter(expressions_));
    }

    std::unordered_set<components::catalog::oid_t> node_t::table_oid_dependencies() {
        std::unordered_set<components::catalog::oid_t> dependencies;
        table_oid_dependencies_(dependencies);
        return dependencies;
    }

    void node_t::table_oid_dependencies_(std::unordered_set<components::catalog::oid_t>& upper) {
        if (table_oid_ != components::catalog::INVALID_OID) {
            upper.insert(table_oid_);
        }
        for (const auto& child : children_) {
            child->table_oid_dependencies_(upper);
        }
    }

    hash_t node_t::hash() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, type_);
        boost::hash_combine(hash_, hash_impl());
        std::for_each(expressions_.cbegin(), expressions_.cend(), [&hash_](const expression_ptr& expression) {
            boost::hash_combine(hash_, expression->hash());
        });
        std::for_each(children_.cbegin(), children_.cend(), [&hash_](const node_ptr& child) {
            boost::hash_combine(hash_, child->hash());
        });
        return hash_;
    }

    std::string node_t::to_string() const { return to_string_impl(); }

    std::pmr::memory_resource* node_t::resource() const noexcept { return children_.get_allocator().resource(); }

    bool node_t::operator==(const node_t& rhs) const {
        bool result = type_ == rhs.type_ && children_.size() == rhs.children_.size() &&
                      expressions_.size() == rhs.expressions_.size();
        if (result) {
            for (auto it = children_.cbegin(), it2 = rhs.children_.cbegin(), it_end = children_.cend(); it != it_end;
                 ++it, ++it2) {
                result &= (*it == *it2);
            }
            if (result) {
                for (auto it = expressions_.cbegin(), it2 = rhs.expressions_.cbegin(), it_end = expressions_.cend();
                     it != it_end;
                     ++it, ++it2) {
                    result &= (*it == *it2);
                }
            }
        }
        return result;
    }

    bool node_t::operator!=(const node_t& rhs) const { return !operator==(rhs); }

} // namespace components::logical_plan
