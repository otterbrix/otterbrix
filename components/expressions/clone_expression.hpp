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

} // namespace components::expressions
