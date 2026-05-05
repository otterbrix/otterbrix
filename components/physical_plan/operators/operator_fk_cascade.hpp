#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/catalog/results/fk_result.hpp>

namespace components::operators {

    // Applies FK referential actions (CASCADE/SET NULL) for DELETE/UPDATE on
    // the referenced table. Stub until Etap 5 adds disk.scan_by_key primitive.
    class operator_fk_cascade_t final : public read_write_operator_t {
    public:
        operator_fk_cascade_t(std::pmr::memory_resource* resource,
                               log_t log,
                               catalog::resolved_fk_t fk);

        const catalog::resolved_fk_t& fk() const noexcept { return fk_; }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        catalog::resolved_fk_t fk_;
    };

} // namespace components::operators
