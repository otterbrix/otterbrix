#include "aggregate_expression.hpp"

#include <sstream>

namespace components::expressions {

    template<class OStream>
    OStream& operator<<(OStream& stream, const aggregate_expression_t* expr) {
        if (expr->params().empty()) {
            stream << expr->key();
        } else {
            if (!expr->key().is_null()) {
                stream << expr->key() << ": ";
            }
            stream << "{$" << expr->function_name() << ": ";
            if (expr->params().size() > 1) {
                stream << "[";
                bool is_first = true;
                for (const auto& param : expr->params()) {
                    if (is_first) {
                        is_first = false;
                    } else {
                        stream << ", ";
                    }
                    stream << param;
                }
                stream << "]";
            } else {
                stream << expr->params().at(0);
            }
            stream << "}";
        }
        return stream;
    }

    aggregate_expression_t::aggregate_expression_t(std::pmr::memory_resource* resource,
                                                   const std::string& function_name,
                                                   const key_t& key)
        : expression_i(expression_group::aggregate, key)
        , child_(make_function_expression(resource, std::string(function_name))) {}

    aggregate_expression_t::aggregate_expression_t(const expression_ptr& call, const key_t& key)
        : expression_i(expression_group::aggregate, key)
        , child_(call) {}

    function_expression_t* aggregate_expression_t::call() noexcept {
        return static_cast<function_expression_t*>(child_.get());
    }

    const function_expression_t* aggregate_expression_t::call() const noexcept {
        return static_cast<const function_expression_t*>(child_.get());
    }

    const std::string& aggregate_expression_t::function_name() const { return call()->name(); }

    void aggregate_expression_t::add_function_uid(compute::function_uid uid) { call()->add_function_uid(uid); }

    compute::function_uid aggregate_expression_t::function_uid() const { return call()->function_uid(); }

    std::pmr::vector<param_storage>& aggregate_expression_t::params() { return call()->args(); }

    const std::pmr::vector<param_storage>& aggregate_expression_t::params() const { return call()->args(); }

    void aggregate_expression_t::append_param(const param_storage& param) { call()->args().push_back(param); }

    void aggregate_expression_t::set_distinct(bool d) { call()->set_distinct(d); }

    bool aggregate_expression_t::is_distinct() const { return call()->is_distinct(); }

    hash_t aggregate_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, child_->hash());
        boost::hash_combine(hash_, key().hash());
        return hash_;
    }

    std::string aggregate_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << this;
        return stream.str();
    }

    bool aggregate_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const aggregate_expression_t*>(rhs);
        return key() == other->key() && *child_ == *other->child_;
    }

    aggregate_expression_ptr
    make_aggregate_expression(std::pmr::memory_resource* resource, const std::string& function_name, const key_t& key) {
        return new aggregate_expression_t(resource, function_name, key);
    }

    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       const std::string& function_name) {
        return make_aggregate_expression(resource, function_name, key_t(resource));
    }

    aggregate_expression_ptr make_aggregate_over(const expression_ptr& call, const key_t& key) {
        return new aggregate_expression_t(call, key);
    }

    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       const std::string& function_name,
                                                       const key_t& key,
                                                       const key_t& field) {
        auto expr = make_aggregate_expression(resource, function_name, key);
        expr->append_param(field);
        return expr;
    }

} // namespace components::expressions