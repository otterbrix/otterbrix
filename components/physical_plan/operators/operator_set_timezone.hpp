#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <memory_resource>
#include <string>

namespace components::operators {

    // SET TIMEZONE — leaf operator.
    //
    // on_execute_impl: validates the timezone name via a transient
    // session_catalog_t. Failure sets error_t cursor without disk side effects.
    // On success, queues the async disk-write phase via async_wait().
    //
    // await_async_and_resume: builds a single pg_settings row
    //   ('TimeZone', <name>)
    // and sends it to the disk actor via
    // manager_disk_t::append_pg_catalog_row. Awaits the future and
    // mark_executed(). No exceptions, no callbacks, no shared-state mutation.
    //
    // Dispatcher updates its default_tz_cat_ in execute_plan post-success
    // (same actor, single-owner) — operator never touches shared state.
    class operator_set_timezone_t final : public read_write_operator_t {
    public:
        operator_set_timezone_t(std::pmr::memory_resource* resource,
                                log_t log,
                                std::pmr::string timezone_name);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::pmr::string timezone_name_;
    };

} // namespace components::operators