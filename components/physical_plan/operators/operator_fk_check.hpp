#pragma once

#include <components/catalog/fk_info.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // Enforces one outgoing FK constraint on an INSERT or UPDATE chunk.
    // For each row: extracts FK child-col values, calls disk.scan_by_key
    // on the parent table, errors if no matching parent row is found.
    class operator_fk_check_t final : public read_write_operator_t {
    public:
        operator_fk_check_t(std::pmr::memory_resource* resource,
                             log_t                      log,
                             catalog::fk_info_t         fk);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::fk_info_t fk_;
    };

} // namespace components::operators