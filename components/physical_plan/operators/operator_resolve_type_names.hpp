#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    // Leaf operator of a kind()==type_names resolve node: the types the session's place sees — pg_catalog's and
    // those of every schema of the place's database — read from pg_type by namespace, and named as format_type
    // names them into each entry's type_names. A SINK like the other resolve operators: its product is the stamp.
    class operator_resolve_type_names_t final : public read_write_operator_t {
    public:
        operator_resolve_type_names_t(std::pmr::memory_resource* resource,
                                      log_t log,
                                      components::logical_plan::node_catalog_resolve_t* node);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::logical_plan::node_catalog_resolve_t* node_;
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
