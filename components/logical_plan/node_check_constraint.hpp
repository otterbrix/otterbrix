#pragma once

#include "node.hpp"

#include <components/catalog/constraint_evaluator.hpp>

#include <string>

namespace components::logical_plan {

    // Evaluates a compiled CHECK constraint predicate over the incoming chunk.
    // Inserted by the planner above node_insert / node_update when the target
    // table has CHECK constraints. The predicate is compiled from SQL text at
    // plan time via catalog::compile_check.
    class node_check_constraint_t final : public node_t {
    public:
        explicit node_check_constraint_t(std::pmr::memory_resource*          resource,
                                          const collection_full_name_t&       collection,
                                          components::catalog::row_predicate_fn pred,
                                          std::string                          conexpr);

        const components::catalog::row_predicate_fn& predicate() const { return pred_; }
        const std::string&                           conexpr()   const { return conexpr_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::row_predicate_fn pred_;
        std::string                           conexpr_;
    };

    using node_check_constraint_ptr = boost::intrusive_ptr<node_check_constraint_t>;

} // namespace components::logical_plan
