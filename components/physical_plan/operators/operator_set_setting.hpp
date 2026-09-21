#pragma once

#include <components/catalog/settings.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <memory_resource>
#include <string>

namespace components::operators {

    class operator_set_setting_t final : public read_write_operator_t {
    public:
        operator_set_setting_t(std::pmr::memory_resource* resource,
                               log_t log,
                               catalog::setting_id setting,
                               std::pmr::string value);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::setting_id setting() const noexcept { return setting_; }

    private:
        catalog::setting_id setting_;
        std::pmr::string value_;
    };

} // namespace components::operators
