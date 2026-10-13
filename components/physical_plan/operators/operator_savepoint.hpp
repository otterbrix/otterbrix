#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>
#include <string_view>

namespace components::operators {

    class operator_savepoint_t final : public read_write_operator_t {
    public:
        operator_savepoint_t(std::pmr::memory_resource* resource, log_t log, std::string_view name);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        std::string name_;
    };

} // namespace components::operators
