#pragma once

#include "predicates/predicate.hpp"
#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <expressions/compare_expression.hpp>

namespace components::operators {

    class operator_index_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        operator_index_join_t(std::pmr::memory_resource* resource,
                              log_t log,
                              type join_type,
                              const expressions::expression_ptr& expression,
                              components::catalog::oid_t probe_table_oid);

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        type join_type_;
        expressions::expression_ptr expression_;
        components::catalog::oid_t probe_table_oid_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        void on_execute_impl(pipeline::context_t* context) override;
    };

} // namespace components::operators

