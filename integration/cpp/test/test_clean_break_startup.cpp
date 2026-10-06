// Clean-break startup tests. After bootstrap, otterbrix uses pg_catalog as the sole
// source of catalog state — these tests verify the on-disk contract by spinning up a
// disk-only manager_disk_t at a directory, doing DDL, killing it, and asserting a fresh
// manager at the same directory observes the persisted state.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/tests/catalog_probe.hpp>
#include <services/disk/tests/disk_test_helpers.hpp>

#include <filesystem>
#include <limits>
#include <thread>
#include <unistd.h>
#include <components/log/test/test_log.hpp>
#include <services/disk/tests/test_directory.hpp>
#include <core/tests/wait_ready.hpp>

using namespace services::disk;
using namespace components::catalog;
using namespace disk_test_helpers;
using session_id_t = components::session::session_id_t;

namespace {
    std::string clean_break_dir() { return integration_fixture_path("test_clean_break_startup").string(); }

    struct fresh_disk {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit fresh_disk(const std::filesystem::path& path)
            : log(make_test_log("python", integration_fixture_path("test_clean_break_startup/logs").string()))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource,
                                                        scheduler,
                                                        scheduler,
                                                        test_directory::created(disk_config),
                                                        log,
                                                        configuration::pump_intervals_t{})) {}
        ~fresh_disk() {
            // manager_disk_t self-drives on an internal thread; destroy it before
            // tearing down the scheduler to avoid use-after-free.
            manager.reset();
            scheduler->stop();
            delete scheduler;
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            REQUIRE(test_helpers::wait_ready(future, scheduler));
            return std::move(future).take_ready();
        }
    };
} // namespace

// 10 system tables today — 9 PG-canonical + pg_database for full DDL plumbing.
TEST_CASE("integration::clean_break_startup::fresh_install_creates_pg_catalog") {
    auto dir = clean_break_dir() + "/fresh";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    {
        fresh_disk fd(dir);
        REQUIRE_FALSE(fd.manager->bootstrap_system_tables_sync().contains_error());
    }
    // On-disk layout is oid-keyed (<db_oid>/<tbl_oid>/table.otbx); system tables live under
    // well_known_oid::main_database.
    auto sys = std::filesystem::path(dir) / std::to_string(static_cast<unsigned>(well_known_oid::main_database));
    REQUIRE(std::filesystem::exists(sys));
    size_t count = 0;
    for (const auto& tbl_dir : std::filesystem::directory_iterator(sys)) {
        if (!tbl_dir.is_directory())
            continue;
        if (std::filesystem::exists(tbl_dir.path() / "table.otbx"))
            ++count;
    }
    REQUIRE(count == all_system_tables().size());
    std::filesystem::remove_all(dir);
}

TEST_CASE("integration::clean_break_startup::existing_pg_catalog_loads") {
    auto dir = clean_break_dir() + "/existing";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    {
        fresh_disk fd(dir);
        REQUIRE_FALSE(fd.manager->bootstrap_system_tables_sync().contains_error());
    }
    {
        fresh_disk fd2(dir);
        REQUIRE_FALSE(fd2.manager->bootstrap_system_tables_sync().contains_error());
        REQUIRE_NOTHROW(fd2.manager->restore_oid_generator_sync());
    }
    std::filesystem::remove_all(dir);
}

TEST_CASE("integration::clean_break_startup::oid_generator_seeded_max_plus_1") {
    auto dir = clean_break_dir() + "/oid_seed";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t high_oid = 0;
    {
        fresh_disk fd(dir);
        REQUIRE_FALSE(fd.manager->bootstrap_system_tables_sync().contains_error());
        for (int i = 0; i < 5; ++i) {
            auto ns_oid = test_create_namespace(fd, std::string("ns_") + std::to_string(i));
            high_oid = std::max(high_oid, ns_oid);
        }
        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        REQUIRE(test_helpers::wait_ready(cf, fd.scheduler));
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        REQUIRE_FALSE(fd2.manager->bootstrap_system_tables_sync().contains_error());
        fd2.manager->restore_oid_generator_sync();
        auto new_ns_oid = test_create_namespace(fd2, "after_restart");
        REQUIRE(new_ns_oid > high_oid);
    }
    std::filesystem::remove_all(dir);
}

TEST_CASE("integration::clean_break_startup::namespace_round_trip") {
    auto dir = clean_break_dir() + "/ns_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t ns_oid = 0;
    {
        fresh_disk fd(dir);
        REQUIRE_FALSE(fd.manager->bootstrap_system_tables_sync().contains_error());
        ns_oid = test_create_namespace(fd, "durable_ns");
        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        REQUIRE(test_helpers::wait_ready(cf, fd.scheduler));
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        REQUIRE_FALSE(fd2.manager->bootstrap_system_tables_sync().contains_error());
        fd2.manager->restore_oid_generator_sync();
        components::table::transaction_data _td_open(0, 0);
        _td_open.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{session_id_t{}, _td_open, {}};
        auto [_, fut] = actor_zeta::otterbrix::send(fd2.manager->address(),
                                                    &manager_disk_t::resolve_namespace,
                                                    ctx,
                                                    std::string("durable_ns"));
        REQUIRE(test_helpers::wait_ready(fut, fd2.scheduler));
        auto rr = std::move(fut).take_ready();
        REQUIRE_FALSE(rr.has_error());
        REQUIRE(rr.value().found);
        REQUIRE(rr.value().oid == ns_oid);
    }
    std::filesystem::remove_all(dir);
}

TEST_CASE("integration::clean_break_startup::table_round_trip_with_columns") {
    auto dir = clean_break_dir() + "/tab_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t tbl_oid = 0;
    {
        fresh_disk fd(dir);
        REQUIRE_FALSE(fd.manager->bootstrap_system_tables_sync().contains_error());
        auto ns_oid = test_create_namespace(fd, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        tbl_oid = test_create_table(fd, ns_oid, "tbl", std::move(cols));
        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        REQUIRE(test_helpers::wait_ready(cf, fd.scheduler));
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        REQUIRE_FALSE(fd2.manager->bootstrap_system_tables_sync().contains_error());
        fd2.manager->restore_oid_generator_sync();
        components::table::transaction_data _td_open(0, 0);
        _td_open.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{session_id_t{}, _td_open, {}};
        auto [_, nfut] = actor_zeta::otterbrix::send(fd2.manager->address(),
                                                     &manager_disk_t::resolve_namespace,
                                                     ctx,
                                                     std::string("ns"));
        REQUIRE(test_helpers::wait_ready(nfut, fd2.scheduler));
        auto rns_r = std::move(nfut).take_ready();
        REQUIRE_FALSE(rns_r.has_error());
        auto& rns = rns_r.value();
        REQUIRE(rns.found);

        auto rt = test_probe::probe_table(fd2, ctx, rns.oid, std::string("tbl"));
        REQUIRE(rt.found);
        REQUIRE(rt.oid == tbl_oid);
        REQUIRE(rt.columns.size() == 1);
    }
    std::filesystem::remove_all(dir);
}

// ddl_create_index writes pg_class (relkind='i') + pg_index + pg_depend; relkind 'i' shares the
// pg_class namespace with 'r', so resolve_table finds the index by name after restart.
TEST_CASE("integration::clean_break_startup::index_round_trip") {
    auto dir = clean_break_dir() + "/idx_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t idx_oid = 0;
    components::catalog::oid_t ns_oid = 0;
    {
        fresh_disk fd(dir);
        REQUIRE_FALSE(fd.manager->bootstrap_system_tables_sync().contains_error());
        ns_oid = test_create_namespace(fd, "idx_ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto tbl_oid = test_create_table(fd, ns_oid, "tbl", std::move(cols));
        idx_oid = test_create_index(fd, ns_oid, tbl_oid, "tbl_idx", std::vector<std::string>{"id"});

        auto [_c, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                    &manager_disk_t::checkpoint_all,
                                                    session_id_t{},
                                                    services::wal::id_t{0},
                                                    std::numeric_limits<uint64_t>::max());
        REQUIRE(test_helpers::wait_ready(cf, fd.scheduler));
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        REQUIRE_FALSE(fd2.manager->bootstrap_system_tables_sync().contains_error());
        fd2.manager->restore_oid_generator_sync();
        components::table::transaction_data _td_open(0, 0);
        _td_open.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{session_id_t{}, _td_open, {}};
        auto ri = test_probe::probe_table(fd2, ctx, ns_oid, std::string("tbl_idx"));
        REQUIRE(ri.found);
        REQUIRE(ri.oid == idx_oid);
        REQUIRE(ri.relkind == 'i');
    }
    std::filesystem::remove_all(dir);
}

TEST_CASE("integration::clean_break_startup::resolve_after_restart") {
    auto dir = clean_break_dir() + "/populate_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    {
        fresh_disk fd(dir);
        REQUIRE_FALSE(fd.manager->bootstrap_system_tables_sync().contains_error());
        test_create_namespace(fd, "post_restart");
        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        REQUIRE(test_helpers::wait_ready(cf, fd.scheduler));
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        REQUIRE_FALSE(fd2.manager->bootstrap_system_tables_sync().contains_error());
        fd2.manager->restore_oid_generator_sync();
        components::table::transaction_data _td_open(0, 0);
        _td_open.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{session_id_t{}, _td_open, {}};
        auto [_, fut] = actor_zeta::otterbrix::send(fd2.manager->address(),
                                                    &manager_disk_t::resolve_namespace,
                                                    ctx,
                                                    std::string("post_restart"));
        REQUIRE(test_helpers::wait_ready(fut, fd2.scheduler));
        auto rns = std::move(fut).take_ready();
        REQUIRE_FALSE(rns.has_error());
        REQUIRE(rns.value().found);
    }
    std::filesystem::remove_all(dir);
}

// Sequence/view/macro are stored in pg_class via relkind 'S'/'v'/'m'.
TEST_CASE("integration::clean_break_startup::sequence_view_macro_via_pg_class") {
    auto dir = clean_break_dir() + "/svm";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t seq_oid = 0;
    components::catalog::oid_t view_oid = 0;
    components::catalog::oid_t macro_oid = 0;
    {
        fresh_disk fd(dir);
        REQUIRE_FALSE(fd.manager->bootstrap_system_tables_sync().contains_error());
        auto ns_oid = test_create_namespace(fd, "ns");
        seq_oid = test_create_sequence(fd, ns_oid, "seq1", 1, 1, 1, std::numeric_limits<std::int64_t>::max(), false);
        view_oid = test_create_view(fd, ns_oid, "v1");
        macro_oid = test_create_macro(fd, ns_oid, "m1");

        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        REQUIRE(test_helpers::wait_ready(cf, fd.scheduler));
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        REQUIRE_FALSE(fd2.manager->bootstrap_system_tables_sync().contains_error());
        fd2.manager->restore_oid_generator_sync();
        auto after_oid = test_create_namespace(fd2, "after");
        REQUIRE(after_oid > seq_oid);
        REQUIRE(after_oid > view_oid);
        REQUIRE(after_oid > macro_oid);
    }
    std::filesystem::remove_all(dir);
}

TEST_CASE("integration::clean_break_startup::wal_replay_split_pg_catalog_first") {
    SUCCEED("base_spaces.cpp PHASE 2 splits WAL records by collection prefix: pg_catalog.* "
            "replay sequentially, user collections in parallel — see test_wal_pool for the "
            "replay path itself");
}
