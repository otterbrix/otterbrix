#pragma once

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/planner/optimizer.hpp>
#include <core/executor.hpp>
#include <core/result_wrapper.hpp>
#include <services/collection/context_storage.hpp>
#include <services/collection/remote_servers.hpp>

#include <filesystem>
#include <memory>
#include <memory_resource>
#include <span>

// Engine assembly without a host facade: prepare_engine -> spawn_engine -> bootstrap -> start.
// Each stage consumes the previous one, so the order is enforced by the types; the dispatcher
// address exists only on a started engine_t.
namespace services::engine {

    // The host owns the pools and keeps them alive past the engine; the engine starts them in
    // start() and stops each exactly once while shutting down.
    struct schedulers_t final {
        actor_zeta::scheduler_raw general;
        actor_zeta::scheduler_raw exec;
        actor_zeta::scheduler_raw disk;
    };

    struct primitives_t final {
        planner::create_plan_rule_t create_plan_rule{&planner::no_custom_lowering};
        components::planner::optimizer_pass_t optimizer_pass{&components::planner::no_op_pass};
        // Server name -> connector type; copied by spawn_engine, checked by bootstrap.
        std::span<const services::remote_server_t> servers{};
    };

    // flock(LOCK_EX | LOCK_NB) on <directory>/.lock, held for the lifetime of the object. Bound to
    // the open file, so a second engine on the same directory is refused within one process too.
    class directory_lock_t final {
    public:
        [[nodiscard]] static core::result_wrapper_t<directory_lock_t> acquire(std::pmr::memory_resource* resource,
                                                                              const std::filesystem::path& directory);

        directory_lock_t(directory_lock_t&& other) noexcept;
        directory_lock_t& operator=(directory_lock_t&& other) noexcept;
        directory_lock_t(const directory_lock_t&) = delete;
        directory_lock_t& operator=(const directory_lock_t&) = delete;
        ~directory_lock_t();

    private:
        explicit directory_lock_t(int fd) noexcept;
        int fd_{-1};
    };

    namespace detail {
        struct engine_parts_t;
        struct engine_parts_deleter_t final {
            void operator()(engine_parts_t* parts) const noexcept;
        };
        using engine_parts_ptr = std::unique_ptr<engine_parts_t, engine_parts_deleter_t>;
    } // namespace detail

    class prepared_engine_t;
    class spawned_engine_t;
    class bootstrapped_engine_t;
    class engine_t;

    // Validates the configuration, takes the directory lock and reads the WAL; spawns nothing.
    [[nodiscard]] core::result_wrapper_t<prepared_engine_t>
    prepare_engine(std::pmr::memory_resource* resource, const configuration::config& config, log_t& log);

    spawned_engine_t spawn_engine(prepared_engine_t prepared,
                                  std::pmr::memory_resource* resource,
                                  schedulers_t schedulers,
                                  const configuration::config& config,
                                  log_t& log,
                                  primitives_t primitives);

    // Catalog, WAL replay, oid/commit clocks, tombstones and indexes; the pools are not running yet. A spawn-time
    // server that repeats a name, lacks a name or a type, or names an existing database refuses the start.
    [[nodiscard]] core::result_wrapper_t<bootstrapped_engine_t> bootstrap(spawned_engine_t spawned);

    engine_t start(bootstrapped_engine_t bootstrapped);

    class prepared_engine_t final {
    public:
        prepared_engine_t(prepared_engine_t&&) noexcept = default;
        prepared_engine_t& operator=(prepared_engine_t&&) noexcept = default;

    private:
        explicit prepared_engine_t(detail::engine_parts_ptr parts) noexcept;
        detail::engine_parts_ptr parts_;

        friend core::result_wrapper_t<prepared_engine_t>
        prepare_engine(std::pmr::memory_resource*, const configuration::config&, log_t&);
        friend spawned_engine_t spawn_engine(prepared_engine_t,
                                             std::pmr::memory_resource*,
                                             schedulers_t,
                                             const configuration::config&,
                                             log_t&,
                                             primitives_t);
    };

    class spawned_engine_t final {
    public:
        spawned_engine_t(spawned_engine_t&&) noexcept = default;
        spawned_engine_t& operator=(spawned_engine_t&&) noexcept = default;

    private:
        explicit spawned_engine_t(detail::engine_parts_ptr parts) noexcept;
        detail::engine_parts_ptr parts_;

        friend spawned_engine_t spawn_engine(prepared_engine_t,
                                             std::pmr::memory_resource*,
                                             schedulers_t,
                                             const configuration::config&,
                                             log_t&,
                                             primitives_t);
        friend core::result_wrapper_t<bootstrapped_engine_t> bootstrap(spawned_engine_t);
    };

    class bootstrapped_engine_t final {
    public:
        bootstrapped_engine_t(bootstrapped_engine_t&&) noexcept = default;
        bootstrapped_engine_t& operator=(bootstrapped_engine_t&&) noexcept = default;

    private:
        explicit bootstrapped_engine_t(detail::engine_parts_ptr parts) noexcept;
        detail::engine_parts_ptr parts_;

        friend core::result_wrapper_t<bootstrapped_engine_t> bootstrap(spawned_engine_t);
        friend engine_t start(bootstrapped_engine_t);
    };

    // A running engine. Destruction is the shutdown: final checkpoint, pools stopped, managers torn down.
    class engine_t final {
    public:
        engine_t(engine_t&&) noexcept = default;
        engine_t& operator=(engine_t&&) = delete;
        ~engine_t();

        actor_zeta::actor::address_t dispatcher_address() const noexcept;
        actor_zeta::actor::address_t disk_address() const noexcept;
        actor_zeta::actor::address_t index_address() const noexcept;
        actor_zeta::actor::address_t wal_address() const noexcept;

    private:
        explicit engine_t(detail::engine_parts_ptr parts) noexcept;
        void shutdown() noexcept;
        detail::engine_parts_ptr parts_;

        friend engine_t start(bootstrapped_engine_t);
    };

} // namespace services::engine
