#pragma once

// Shared fixture for the agent-side aggregate-pushdown REDUCE tests.
//
// Spins up a manager_disk_t on a non-threaded test scheduler over a private temp
// dir, with a synchronous `invoke<>` send helper and the `drive_reduce` helper
// that runs a pushed_aggregate_spec_t reduce and returns its finalized chunks.

#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/log/log.hpp>
#include <components/physical_plan/pushed_aggregate_spec.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <limits>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace pushdown_reduce_test {

    namespace catalog = components::catalog;
    namespace ops = components::operators;
    using session_id_t = components::session::session_id_t;

    inline std::string reduce_dir() {
        static std::string p = test_temp_path("test_otterbrix_pushdown_reduce");
        return p;
    }
    inline void cleanup() { std::filesystem::remove_all(reduce_dir()); }

    struct fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<services::disk::manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", test_temp_path("docker_logs")))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = reduce_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<services::disk::manager_disk_t>(&resource,
                                                                        scheduler,
                                                                        scheduler,
                                                                        disk_config,
                                                                        log)) {
            cleanup();
            std::filesystem::create_directories(reduce_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            manager.reset();
            scheduler->stop();
            delete scheduler;
            cleanup();
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }

        // Drive the aggregate-pushdown reduce over the owning agent's slice via the
        // DEDICATED storage_reduce leg: ONE call, ONE reply carrying all the final
        // aggregated chunks (bounded by #groups — no cursor exists).
        std::pmr::vector<components::vector::data_chunk_t>
        drive_reduce(catalog::oid_t oid, ops::pushed_aggregate_spec_t spec, components::table::transaction_data txn) {
            auto r = invoke(&services::disk::manager_disk_t::storage_reduce,
                            session_id_t{},
                            oid,
                            std::unique_ptr<components::table::table_filter_t>(nullptr),
                            std::vector<size_t>{},
                            txn,
                            std::move(spec));
            REQUIRE_FALSE(r.has_error());
            return std::move(r.value());
        }
    };

} // namespace pushdown_reduce_test
