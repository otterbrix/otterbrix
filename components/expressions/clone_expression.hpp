#pragma once

#include "expression.hpp"

#include <memory_resource>

namespace components::expressions {

    // Deep copy of an expression tree onto `resource`: every expression node
    // (compare / scalar / aggregate / sort / function) is rebuilt, including
    // nested expression operands inside param_storage, so mutating the copy's
    // keys (e.g. key_t::set_path re-localization in the filter-pushdown rule)
    // never leaks into the original. key_t / parameter_id_t operands are plain
    // value copies. nullptr clones to nullptr.
    expression_ptr clone_expression(std::pmr::memory_resource* resource, const expression_ptr& expr);

    // An expression tree that crosses an actor mailbox: the sender detaches a deep copy onto the
    // receiver's resource, the receiver attaches its own copy. Nobody keeps a reference into the other
    // side's tree, which is why this is move-only and not an expression_ptr.
    class detached_expression_t final {
    public:
        [[nodiscard]] static detached_expression_t detach(std::pmr::memory_resource* target, const expression_ptr& expr);

        detached_expression_t(detached_expression_t&&) noexcept = default;
        detached_expression_t& operator=(detached_expression_t&&) noexcept = default;
        detached_expression_t(const detached_expression_t&) = delete;
        detached_expression_t& operator=(const detached_expression_t&) = delete;
        ~detached_expression_t() = default;

        [[nodiscard]] expression_ptr attach(std::pmr::memory_resource* resource) const;
        [[nodiscard]] detached_expression_t copy(std::pmr::memory_resource* target) const;
        [[nodiscard]] bool empty() const noexcept { return !tree_; }

    private:
        explicit detached_expression_t(expression_ptr tree) noexcept;
        expression_ptr tree_;
    };

} // namespace components::expressions
