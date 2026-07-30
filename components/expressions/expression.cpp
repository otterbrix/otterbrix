#include "expression.hpp"

#include <boost/container_hash/hash.hpp>

#include <cassert>

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

    const std::string& expression_i::result_alias() const { return result_alias_; }

    void expression_i::set_result_alias(const std::string& alias) { result_alias_ = alias; }

    expression_i::expression_i(expression_group group)
        : group_(group) {}

    void param_storage::destroy() noexcept {
        switch (kind_) {
            case kind_t::key:
                key_.~key_t();
                break;
            case kind_t::expression:
                expr_.~expression_ptr();
                break;
            case kind_t::parameter:
                break;
        }
    }

    param_storage::~param_storage() { destroy(); }

    param_storage::param_storage(const param_storage& other)
        : kind_(other.kind_) {
        switch (kind_) {
            case kind_t::parameter:
                std::construct_at(&param_, other.param_);
                break;
            case kind_t::key:
                // key_t's own copy constructor, which homes the copy on the SOURCE's resource.
                // Not `= default` there, and deliberately so (key.hpp).
                std::construct_at(&key_, other.key_);
                break;
            case kind_t::expression:
                std::construct_at(&expr_, other.expr_);
                break;
        }
    }

    param_storage::param_storage(param_storage&& other) noexcept
        : kind_(other.kind_) {
        switch (kind_) {
            case kind_t::parameter:
                std::construct_at(&param_, other.param_);
                break;
            case kind_t::key:
                std::construct_at(&key_, std::move(other.key_));
                break;
            case kind_t::expression:
                std::construct_at(&expr_, std::move(other.expr_));
                break;
        }
    }

    param_storage& param_storage::operator=(const param_storage& other) {
        if (this == &other) {
            return *this;
        }
        if (kind_ == other.kind_) {
            switch (kind_) {
                case kind_t::parameter:
                    param_ = other.param_;
                    break;
                case kind_t::key:
                    // pmr copy-assignment keeps the DESTINATION's allocator, which is what an
                    // assigned-into key should do.
                    key_ = other.key_;
                    break;
                case kind_t::expression:
                    expr_ = other.expr_;
                    break;
            }
            return *this;
        }
        // Cross-alternative: construct the new value first. A key or handle copy can throw, and
        // if it does, *this must still hold what it held.
        switch (other.kind_) {
            case kind_t::parameter: {
                const core::parameter_id_t incoming = other.param_;
                destroy();
                kind_ = kind_t::parameter;
                std::construct_at(&param_, incoming);
                break;
            }
            case kind_t::key: {
                key_t incoming(other.key_);
                destroy();
                kind_ = kind_t::key;
                std::construct_at(&key_, std::move(incoming));
                break;
            }
            case kind_t::expression: {
                expression_ptr incoming(other.expr_);
                destroy();
                kind_ = kind_t::expression;
                std::construct_at(&expr_, std::move(incoming));
                break;
            }
        }
        return *this;
    }

    param_storage& param_storage::operator=(param_storage&& other) {
        if (this == &other) {
            return *this;
        }
        if (kind_ == other.kind_) {
            switch (kind_) {
                case kind_t::parameter:
                    param_ = other.param_;
                    break;
                case kind_t::key:
                    // key_t declares no move-assignment (its user-declared copy-assign suppresses
                    // the implicit one), so an rvalue key binds to the copy: `other` is left
                    // intact and this key keeps its own resource. std::variant did exactly this.
                    key_ = other.key_;
                    break;
                case kind_t::expression:
                    expr_ = std::move(other.expr_);
                    break;
            }
            return *this;
        }
        // Cross-alternative: every move constructor here is noexcept, so destroying first is safe.
        switch (other.kind_) {
            case kind_t::parameter: {
                const core::parameter_id_t incoming = other.param_;
                destroy();
                kind_ = kind_t::parameter;
                std::construct_at(&param_, incoming);
                break;
            }
            case kind_t::key: {
                destroy();
                kind_ = kind_t::key;
                std::construct_at(&key_, std::move(other.key_));
                break;
            }
            case kind_t::expression: {
                destroy();
                kind_ = kind_t::expression;
                std::construct_at(&expr_, std::move(other.expr_));
                break;
            }
        }
        return *this;
    }

    bool param_storage::operator==(const param_storage& rhs) const {
        if (kind_ != rhs.kind_) {
            return false;
        }
        switch (kind_) {
            case kind_t::parameter:
                return param_ == rhs.param_;
            case kind_t::key:
                return key_ == rhs.key_;
            case kind_t::expression:
                // Handle identity, as the variant's operator== was: intrusive_ptr compares
                // pointers, it does not descend into expression_i::operator==.
                return expr_ == rhs.expr_;
        }
        return false;
    }

    // The asserts are the Debug replacement for the bad_variant_access the std::variant used
    // to throw: a mis-guarded as_* is otherwise a silent read of an inactive union member.
    bool is_key(const param_storage& param) noexcept { return param.kind_ == param_storage::kind_t::key; }

    const key_t& as_key(const param_storage& param) {
        assert(param.kind_ == param_storage::kind_t::key);
        return param.key_;
    }

    key_t& as_key(param_storage& param) {
        assert(param.kind_ == param_storage::kind_t::key);
        return param.key_;
    }

    bool is_expr(const param_storage& param) noexcept { return param.kind_ == param_storage::kind_t::expression; }

    const expression_ptr& as_expr(const param_storage& param) {
        assert(param.kind_ == param_storage::kind_t::expression);
        return param.expr_;
    }

    expression_ptr& as_expr(param_storage& param) {
        assert(param.kind_ == param_storage::kind_t::expression);
        return param.expr_;
    }

    bool is_parameter(const param_storage& param) noexcept { return param.kind_ == param_storage::kind_t::parameter; }

    const core::parameter_id_t& as_parameter(const param_storage& param) {
        assert(param.kind_ == param_storage::kind_t::parameter);
        return param.param_;
    }

    core::parameter_id_t& as_parameter(param_storage& param) {
        assert(param.kind_ == param_storage::kind_t::parameter);
        return param.param_;
    }

} // namespace components::expressions
