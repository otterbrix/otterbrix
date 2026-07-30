#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>

namespace components::expressions {

    class bound_expression_t;
    using bound_expression_ptr = boost::intrusive_ptr<bound_expression_t>;

    // O(1) dispatch tag. The bound tree is a class hierarchy (rule 14 forbids std::variant), so the
    // tag is what replaces RTTI: every executor switch keys off kind() and static_casts, and there
    // is no dynamic_cast anywhere in this layer.
    enum class bound_kind : uint8_t
    {
        reference,   // a column of the input chunk
        parameter,   // a plan parameter slot, re-read live on every execution
        constant,    // a value the expression OWNS
        cast,        // child converted to return_type()
        arithmetic,  // vector::arithmetic_op over two children
        comparison,  // two children -> BOOLEAN
        conjunction, // AND / OR / NOT over BOOLEAN children
        case_expr,   // (when, then)+ [, else]
        function,    // a registered compute function over its arguments
        regex,       // subject matched against a pattern compiled at BIND time
        any_all,     // subject folded against a sub-query array read live per execution
        coalesce,    // first operand that is not NULL
        negate       // unary minus, in the OPERAND's own type
    };

    // What the executor and the optimiser are allowed to assume about a node.
    //
    // `foldable` is a property of the SUBTREE, not of the node kind: it means every input the node
    // reads is fixed at bind time, so the node can be evaluated once and replaced by a constant.
    // A reference reads a row and a parameter is re-read live on every execution (LATERAL rebinds
    // the slot between two executions of the same tree), so neither is ever foldable and neither
    // lets an ancestor fold.
    struct bound_traits_t {
        bool propagates_nulls = true; // a NULL in any operand makes the result NULL
        bool deterministic = true;    // same inputs, same output
        bool can_fail = false;        // evaluation can answer core::error_t
        bool foldable = false;        // every input is fixed at bind time
    };

} // namespace components::expressions
