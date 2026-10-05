#pragma once

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/planner/optimizer.hpp>
#include <core/executor.hpp>
#include <core/result_wrapper.hpp>
#include <services/collection/context_storage.hpp>

#include <filesystem>
#include <memory>
#include <memory_resource>
#include <span>

// Engine assembly without a host facade: open_engine validates the configuration, takes the directory
// lock, reads the WAL, spawns the managers, bootstraps the catalog and starts the pools.
namespace services::engine {

    // The host owns the pools and keeps them alive past the engine; the engine starts them when it
    // opens and stops each exactly once while shutting down.
    struct schedulers_t final {
        actor_zeta::scheduler_raw general;
        actor_zeta::scheduler_raw exec;
        actor_zeta::scheduler_raw disk;
    };

    namespace detail {
        struct engine_parts_t;
    } // namespace detail

    // A running engine. Destruction is the shutdown: final checkpoint, pools stopped, managers torn down.
    class engine_t final {
    public:
        // The parts are opaque outside engine.cpp: only open_engine can build them.
        explicit engine_t(std::unique_ptr<detail::engine_parts_t> parts) noexcept;
        engine_t(engine_t&& other) noexcept;
        engine_t& operator=(engine_t&&) = delete;
        ~engine_t();

        actor_zeta::actor::address_t dispatcher_address() const noexcept;
        actor_zeta::actor::address_t disk_address() const noexcept;
        actor_zeta::actor::address_t index_address() const noexcept;
        actor_zeta::actor::address_t wal_address() const noexcept;

    private:
        void shutdown() noexcept;
        std::unique_ptr<detail::engine_parts_t> parts_;
    };

    [[nodiscard]] core::result_wrapper_t<engine_t> open_engine(std::pmr::memory_resource* resource,
                                                               schedulers_t schedulers,
                                                               const configuration::config& config,
                                                               log_t& log,
                                                               components::planner::primitives_t primitives);

} // namespace services::engine
