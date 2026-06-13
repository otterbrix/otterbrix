#pragma once

#include <components/catalog/fk_info.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // Enforces one outgoing FK constraint on an INSERT or UPDATE chunk.
    // Extracts FK child-col values per row into one key batch, calls
    // disk.scan_by_keys on the parent table, and errors on the first row
    // whose key has no matching parent row.
    class operator_fk_check_t final : public read_write_operator_t {
    public:
        operator_fk_check_t(std::pmr::memory_resource* resource, log_t log, catalog::fk_info_t fk);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::fk_info_t fk_;
    };

} // namespace components::operators