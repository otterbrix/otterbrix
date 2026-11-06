#pragma once

#include <components/catalog/settings.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <memory_resource>
#include <vector>

namespace components::operators {

    class operator_set_setting_t final : public read_write_operator_t {
    public:
        operator_set_setting_t(std::pmr::memory_resource* resource,
                               log_t log,
                               catalog::setting_scope_t scope,
                               const std::pmr::vector<catalog::setting_assignment_t>& assignments);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        catalog::setting_scope_t scope_;
        std::pmr::vector<catalog::setting_assignment_t> assignments_;
    };

} // namespace components::operators
