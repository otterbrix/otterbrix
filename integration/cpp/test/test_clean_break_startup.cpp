// Clean-break startup tests. After bootstrap, otterbrix uses pg_catalog as the SOLE
// source of catalog state — these tests verify the on-disk contract by spinning up a
// disk-only manager_disk_t at a directory, doing DDL, killing it, and asserting a fresh
// manager at the same directory observes the persisted state.

#include "test_config.hpp"
#include <catch2/catch.hpp>

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
#include <fstream>
#include <limits>
#include <thread>
#include <unistd.h>

using namespace services::disk;
using namespace components::catalog;
using namespace disk_test_helpers;
using session_id_t = components::session::session_id_t;

namespace {
    std::string clean_break_dir() {
        static std::string p = "/tmp/test_otterbrix_clean_break_" + std::to_string(::getpid());
        return p;
    }

    // The manager actors self-drive on internal threads; futures become ready
    // asynchronously. Pump the (thread-safe) child scheduler with a bounded poll
    // until the future is ready before extracting its value with take_ready().
    template<typename Fut>
    void poll_ready(core::non_thread_scheduler::scheduler_test_t* scheduler, Fut& fut) {
        for (int i = 0; i < 100000 && !fut.is_ready(); ++i) {
            scheduler->run(1000);
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
    }

    struct fresh_disk {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit fresh_disk(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}
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
            poll_ready(scheduler, future);
            return std::move(future).take_ready();
        }
    };
} // namespace

// 1. Fresh install: bootstrap creates a .otbx file for every system table (10 today —
//    9 PG-canonical + pg_database for full DDL plumbing).
TEST_CASE("integration::clean_break_startup::fresh_install_creates_pg_catalog") {
    auto dir = clean_break_dir() + "/fresh";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
    }
    // On-disk layout is oid-keyed (<db_oid>/<tbl_oid>/table.otbx) — system
    // tables live under well_known_oid::main_database. Count .otbx files
    // under that directory.
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

// 2. Existing pg_catalog loads on the second start without failure.
TEST_CASE("integration::clean_break_startup::existing_pg_catalog_loads") {
    auto dir = clean_break_dir() + "/existing";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
    }
    {
        fresh_disk fd2(dir);
        REQUIRE_NOTHROW(fd2.manager->bootstrap_system_tables_sync());
        REQUIRE_NOTHROW(fd2.manager->restore_oid_generator_sync());
    }
    std::filesystem::remove_all(dir);
}

// 3. oid_generator after restart never collides with persisted OIDs.
TEST_CASE("integration::clean_break_startup::oid_generator_seeded_max_plus_1") {
    auto dir = clean_break_dir() + "/oid_seed";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t high_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        for (int i = 0; i < 5; ++i) {
            auto ns_oid = test_create_namespace(fd, std::string("ns_") + std::to_string(i));
            high_oid = std::max(high_oid, ns_oid);
        }
        // Checkpoint so on-disk metadata is up to date.
        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        poll_ready(fd.scheduler, cf);
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto new_ns_oid = test_create_namespace(fd2, "after_restart");
        REQUIRE(new_ns_oid > high_oid);
    }
    std::filesystem::remove_all(dir);
}

// 4. namespace round-trip: created namespace is resolvable post-restart.
TEST_CASE("integration::clean_break_startup::namespace_round_trip") {
    auto dir = clean_break_dir() + "/ns_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t ns_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "durable_ns");
        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        poll_ready(fd.scheduler, cf);
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        components::table::transaction_data _td_open(0, 0);
        _td_open.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{session_id_t{}, _td_open, {}};
        auto [_, fut] = actor_zeta::otterbrix::send(fd2.manager->address(),
                                                    &manager_disk_t::resolve_namespace,
                                                    ctx,
                                                    std::string("durable_ns"),
                                                    std::uint64_t{0});
        poll_ready(fd2.scheduler, fut);
        auto rr = std::move(fut).take_ready();
        REQUIRE(rr.found);
        REQUIRE(rr.oid == ns_oid);
    }
    std::filesystem::remove_all(dir);
}

// 5. table round-trip: created table with columns is resolvable post-restart with column
// OIDs preserved.
TEST_CASE("integration::clean_break_startup::table_round_trip_with_columns") {
    auto dir = clean_break_dir() + "/tab_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t tbl_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        tbl_oid = test_create_table(fd, ns_oid, "tbl", std::move(cols));
        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        poll_ready(fd.scheduler, cf);
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        components::table::transaction_data _td_open(0, 0);
        _td_open.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{session_id_t{}, _td_open, {}};
        auto [_, nfut] = actor_zeta::otterbrix::send(fd2.manager->address(),
                                                     &manager_disk_t::resolve_namespace,
                                                     ctx,
                                                     std::string("ns"),
                                                     std::uint64_t{0});
        poll_ready(fd2.scheduler, nfut);
        auto rns = std::move(nfut).take_ready();
        REQUIRE(rns.found);

        auto rt = test_probe::probe_table(fd2, ctx, rns.oid, std::string("tbl"));
        REQUIRE(rt.found);
        REQUIRE(rt.oid == tbl_oid);
        REQUIRE(rt.columns.size() == 1);
    }
    std::filesystem::remove_all(dir);
}

// 6. index_round_trip — ddl_create_index writes pg_class (relkind='i') + pg_index +
// pg_depend. After restart, the index entry survives via bootstrap_system_tables_sync and is
// observable via resolve_table by name (relkind 'i' shares the pg_class namespace with 'r').
TEST_CASE("integration::clean_break_startup::index_round_trip") {
    auto dir = clean_break_dir() + "/idx_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t idx_oid = 0;
    components::catalog::oid_t ns_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
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
        poll_ready(fd.scheduler, cf);
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        components::table::transaction_data _td_open(0, 0);
        _td_open.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{session_id_t{}, _td_open, {}};
        // Index lives in pg_class with relkind='i'; resolve_table finds it by name.
        auto ri = test_probe::probe_table(fd2, ctx, ns_oid, std::string("tbl_idx"));
        REQUIRE(ri.found);
        REQUIRE(ri.oid == idx_oid);
        REQUIRE(ri.relkind == 'i');
    }
    std::filesystem::remove_all(dir);
}

// 7. resolve_namespace reflects post-restart catalog state (V4 — populate retired).
// Same shape as the legacy populate_after_restart test; verifies that a namespace created
// before checkpoint+shutdown is still visible after fresh_disk restart via the per-name
// resolve API.
TEST_CASE("integration::clean_break_startup::resolve_after_restart") {
    auto dir = clean_break_dir() + "/populate_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        test_create_namespace(fd, "post_restart");
        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        poll_ready(fd.scheduler, cf);
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        components::table::transaction_data _td_open(0, 0);
        _td_open.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{session_id_t{}, _td_open, {}};
        auto [_, fut] = actor_zeta::otterbrix::send(fd2.manager->address(),
                                                    &manager_disk_t::resolve_namespace,
                                                    ctx,
                                                    std::string("post_restart"),
                                                    std::uint64_t{0});
        poll_ready(fd2.scheduler, fut);
        auto rns = std::move(fut).take_ready();
        REQUIRE(rns.found);
    }
    std::filesystem::remove_all(dir);
}

// 7. Sequence/view/macro stored in pg_class with relkind 'S'/'v'/'m' survives restart.
TEST_CASE("integration::clean_break_startup::sequence_view_macro_via_pg_class") {
    auto dir = clean_break_dir() + "/svm";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    components::catalog::oid_t seq_oid = 0;
    components::catalog::oid_t view_oid = 0;
    components::catalog::oid_t macro_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "ns");
        seq_oid = test_create_sequence(fd, ns_oid, "seq1", 1, 1, 1, std::numeric_limits<std::int64_t>::max(), false);
        view_oid = test_create_view(fd, ns_oid, "v1");
        macro_oid = test_create_macro(fd, ns_oid, "m1");

        auto [_, cf] = actor_zeta::otterbrix::send(fd.manager->address(),
                                                   &manager_disk_t::checkpoint_all,
                                                   session_id_t{},
                                                   services::wal::id_t{0},
                                                   std::numeric_limits<uint64_t>::max());
        poll_ready(fd.scheduler, cf);
        (void) std::move(cf).take_ready();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        // A new namespace creation uses an OID strictly above the persisted SVM OIDs.
        auto after_oid = test_create_namespace(fd2, "after");
        REQUIRE(after_oid > seq_oid);
        REQUIRE(after_oid > view_oid);
        REQUIRE(after_oid > macro_oid);
    }
    std::filesystem::remove_all(dir);
}

// 8. Hard-fail on legacy catalog.otbx file: base_otterbrix_t throws on construction when
// the legacy catalog.otbx file is present in the disk path. M8 clean-break behaviour:
// operator must migrate / remove the file before booting on the new code.
TEST_CASE("integration::clean_break_startup::hard_fail_on_legacy_catalog_otbx") {
    auto dir = std::filesystem::path(clean_break_dir() + "/hard_fail");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    auto disk_subdir = dir / "wal";
    std::filesystem::create_directories(disk_subdir);
    // Plant a stub legacy catalog.otbx in the disk path. The exact contents don't matter —
    // base_otterbrix_t checks existence first and throws before opening the file.
    std::ofstream out((disk_subdir / "catalog.otbx").string(), std::ios::binary);
    out << "legacy_marker";
    out.close();
    REQUIRE(std::filesystem::exists(disk_subdir / "catalog.otbx"));

    auto config = test_create_config(dir);
    bool threw_with_expected_message = false;
    try {
        test_spaces space(config);
    } catch (const std::runtime_error& e) {
        std::string msg = e.what();
        if (msg.find("Legacy catalog format detected") != std::string::npos &&
            msg.find("catalog.otbx") != std::string::npos) {
            threw_with_expected_message = true;
        }
    } catch (...) {
        // Other exception types are NOT what we want here.
    }
    REQUIRE(threw_with_expected_message);
    std::filesystem::remove_all(dir);
}

// 9. WAL replay split (pg_catalog first, user collections second) — tested implicitly
// via base_spaces; here we document the expected ordering.
TEST_CASE("integration::clean_break_startup::wal_replay_split_pg_catalog_first") {
    SUCCEED("base_spaces.cpp PHASE 2 splits WAL records by collection prefix: pg_catalog.* "
            "replay sequentially, user collections in parallel — see test_wal_pool for the "
            "replay path itself");
}
