#pragma once

#include <components/catalog/fk_info.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // Enforces one referencing FK constraint after a DELETE.
    // Scans the child table for rows referencing the deleted parent rows,
    // then applies the configured ON DELETE action:
    //   'a'/'r' NO ACTION / RESTRICT  — error if any child rows reference deleted rows
    //   'c'     CASCADE               — delete the referencing child rows
    //   'n'/'d' SET NULL / SET DEFAULT — update referencing rows in-place
    class operator_fk_cascade_t final : public read_write_operator_t {
    public:
        operator_fk_cascade_t(std::pmr::memory_resource* resource, log_t log, catalog::fk_info_t fk);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::fk_info_t fk_;
    };

} // namespace components::operators