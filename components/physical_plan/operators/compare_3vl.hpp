#pragma once

#include <components/expressions/forward.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/tri_bool.hpp>

namespace components::operators {

    // Evaluate a scalar SQL comparison in three-valued logic. A NULL operand makes the comparison
    // UNKNOWN -- distinct from FALSE -- so a CASE WHEN / HAVING / CHECK over a NULL falls through
    // (UNKNOWN does not select, and NOT cannot flip it in). This is the shared spelling that keeps
    // every non-predicate comparison site answering SQL the same way; it must be used instead of
    // logical_value_t::compare(), whose total order (NULLs last) is for sorting and indexing.
    inline types::tri_bool_t eval_compare_3vl(expressions::compare_type op,
                                              const types::logical_value_t& lhs,
                                              const types::logical_value_t& rhs) {
        const auto c = lhs.compare_sql(rhs);
        if (!c) {
            return types::tri_bool_t::unknown;
        }
        switch (op) {
            case expressions::compare_type::eq:
                return types::tri_of(*c == types::compare_t::equals);
            case expressions::compare_type::ne:
                return types::tri_of(*c != types::compare_t::equals);
            case expressions::compare_type::gt:
                return types::tri_of(*c == types::compare_t::more);
            case expressions::compare_type::gte:
                return types::tri_of(*c != types::compare_t::less);
            case expressions::compare_type::lt:
                return types::tri_of(*c == types::compare_t::less);
            case expressions::compare_type::lte:
                return types::tri_of(*c != types::compare_t::more);
            default:
                return types::tri_bool_t::unknown;
        }
    }

} // namespace components::operators
