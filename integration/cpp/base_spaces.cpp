#include "base_spaces.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <core/pmr.hpp>

#include <optional>

namespace otterbrix {

    struct base_otterbrix_t::host_t final {
        core::pmr::otterbrix_resource resource;
        log_t log;
        // The pools outlive the engine: it stops them while shutting down, and they are freed after it.
        actor_zeta::scheduler_ptr scheduler_general{new actor_zeta::shared_work(3, 1000)};
        actor_zeta::scheduler_ptr scheduler_exec{new actor_zeta::shared_work(3, 1000)};
        actor_zeta::scheduler_ptr scheduler_disk{new actor_zeta::shared_work(3, 1000)};
        std::optional<services::engine::engine_t> engine;
        std::unique_ptr<wrapper_dispatcher_t, actor_zeta::pmr::deleter_t> wrapper{nullptr,
                                                                                  actor_zeta::pmr::deleter_t(&resource)};
    };

    void base_otterbrix_t::host_deleter_t::operator()(host_t* host) const noexcept { delete host; }

    core::result_wrapper_t<base_otterbrix_t::host_ptr>
    base_otterbrix_t::open(const configuration::config& config, components::planner::primitives_t primitives) {
        host_ptr host{new host_t()};
        auto log = make_log("otterbrix", config.log.path, std::pmr::new_delete_resource());
        if (log.has_error()) {
            return log.error();
        }
        host->log = std::move(log.value());
        host->log.set_level(config.log.level);
        trace(host->log, "base_otterbrix_t::open");

        auto engine = services::engine::open_engine(&host->resource,
                                                    services::engine::schedulers_t{host->scheduler_general.get(),
                                                                                   host->scheduler_exec.get(),
                                                                                   host->scheduler_disk.get()},
                                                    config,
                                                    host->log,
                                                    primitives);
        if (engine.has_error()) {
            return core::error_on(std::pmr::new_delete_resource(), engine.error());
        }
        host->engine.emplace(std::move(engine.value()));
        host->wrapper = actor_zeta::spawn<wrapper_dispatcher_t>(&host->resource,
                                                                host->engine->dispatcher_address(),
                                                                host->scheduler_exec.get(),
                                                                host->log);
        trace(host->log, "base_otterbrix_t::open complete");
        return host;
    }

    base_otterbrix_t::base_otterbrix_t(host_ptr host)
        : host_(std::move(host)) {
        assert(host_ != nullptr && host_->engine.has_value());
    }

    base_otterbrix_t::~base_otterbrix_t() { trace(host_->log, "delete spaces"); }

    log_t& base_otterbrix_t::get_log() { return host_->log; }

    wrapper_dispatcher_t* base_otterbrix_t::dispatcher() { return host_->wrapper.get(); }

    const services::engine::engine_t& base_otterbrix_t::engine() const { return *host_->engine; }

} // namespace otterbrix
