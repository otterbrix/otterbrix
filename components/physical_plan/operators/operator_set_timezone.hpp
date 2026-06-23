#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <memory_resource>
#include <string>

namespace components::operators {

    // SET TIMEZONE — leaf operator. Validates the timezone name (sync phase) so a
    // bad value never reaches disk, then persists a single pg_settings
    // ('TimeZone', <name>) row (async phase). The operator never mutates shared
    // state: the dispatcher updates its own default_tz_cat_ post-success.
    class operator_set_timezone_t final : public read_write_operator_t {
    public:
        operator_set_timezone_t(std::pmr::memory_resource* resource, log_t log, std::pmr::string timezone_name);

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults; replaces the legacy on_execute + find_waiting_operator
        // drive. The sync timezone-name validation now runs at the TOP of
        // await_async_and_resume (one place for both entry points — R6), so an
        // invalid value still aborts before any disk write.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;

        std::pmr::string timezone_name_;
    };

} // namespace components::operators