#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/catalog/results/fk_result.hpp>

namespace components::operators {

    // Verifies FK parent existence for INSERT/UPDATE on the referencing table.
    // Disk primitive (point_lookup_by_index) not yet available; operator is a
    // pass-through stub until Etap 5 completes the disk primitive layer.
    class operator_fk_check_t final : public read_write_operator_t {
    public:
        operator_fk_check_t(std::pmr::memory_resource* resource,
                              log_t log,
                              catalog::resolved_fk_t fk);

        const catalog::resolved_fk_t& fk() const noexcept { return fk_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        catalog::resolved_fk_t fk_;
    };

} // namespace components::operators
