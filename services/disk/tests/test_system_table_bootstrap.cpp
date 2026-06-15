#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/log/log.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <thread>
#include <unistd.h>

using namespace services::disk;
using namespace components::catalog;

namespace {
    std::string boot_test_dir() {
        static std::string path = "/tmp/test_otterbrix_sysboot_" + std::to_string(::getpid());
        return path;
    }
    void cleanup_boot_dir() { std::filesystem::remove_all(boot_test_dir()); }

    // Mirror of the same helper in services/disk/manager_disk_bootstrap.cpp;
    // the impl keeps it in an anonymous namespace, so duplicate here.
    components::catalog::oid_t well_known_oid_for_system_table(std::string_view name) {
        namespace wk = components::catalog::well_known_oid;
        if (name == "pg_namespace")
            return wk::pg_namespace_table;
        if (name == "pg_class")
            return wk::pg_class_table;
        if (name == "pg_attribute")
            return wk::pg_attribute_table;
        if (name == "pg_type")
            return wk::pg_type_table;
        if (name == "pg_proc")
            return wk::pg_proc_table;
        if (name == "pg_depend")
            return wk::pg_depend_table;
        if (name == "pg_constraint")
            return wk::pg_constraint_table;
        if (name == "pg_index")
            return wk::pg_index_table;
        if (name == "pg_computed_column")
            return wk::pg_computed_column_table;
        if (name == "pg_database")
            return wk::pg_database_table;
        if (name == "pg_sequence")
            return wk::pg_sequence_table;
        if (name == "pg_rewrite")
            return wk::pg_rewrite_table;
        if (name == "pg_settings")
            return wk::pg_settings_table;
        return components::catalog::INVALID_OID;
    }

    std::filesystem::path sys_dir_for(const std::filesystem::path& base) {
        return base / std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::main_database));
    }
    std::filesystem::path otbx_for(const std::filesystem::path& base, std::string_view tbl) {
        return sys_dir_for(base) / std::to_string(static_cast<unsigned>(well_known_oid_for_system_table(tbl))) /
               "table.otbx";
    }
    std::filesystem::path coll_dir_for(const std::filesystem::path& base, std::string_view tbl) {
        return sys_dir_for(base) / std::to_string(static_cast<unsigned>(well_known_oid_for_system_table(tbl)));
    }

    struct disk_only_fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit disk_only_fixture(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}

        ~disk_only_fixture() {
            // Destroy the manager first: its dtor joins the internal loop thread,
            // which may still enqueue children onto the scheduler. Only then is it
            // safe to stop/delete the scheduler.
            manager.reset();
            scheduler->stop();
            delete scheduler;
        }

        // Drive a manager mailbox handler synchronously through the test scheduler.
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
    };
} // namespace

// 1. Fresh start: bootstrap creates one .otbx file per system table under
//    <path>/<main_db_oid>/<tbl_oid>/ (OID-keyed layout from
//    services/disk/manager_disk_bootstrap.cpp).
TEST_CASE("services::disk::sysboot::creates_10_otbx_files") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }

    REQUIRE(std::filesystem::exists(sys_dir_for(base)));
    size_t otbx_count = 0;
    for (const auto& def : all_system_tables()) {
        if (std::filesystem::exists(otbx_for(base, def.name))) {
            otbx_count++;
        }
    }
    REQUIRE(otbx_count == all_system_tables().size());

    cleanup_boot_dir();
}

// 2. Bootstrap is idempotent: a second call with files present does NOT recreate / overwrite.
TEST_CASE("services::disk::sysboot::bootstrap_is_idempotent") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    // First bootstrap.
    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }

    auto pg_class_otbx = otbx_for(base, "pg_class");
    REQUIRE(std::filesystem::exists(pg_class_otbx));
    auto first_size = std::filesystem::file_size(pg_class_otbx);
    auto first_mtime = std::filesystem::last_write_time(pg_class_otbx);

    // Second bootstrap on the same path — must short-circuit, not overwrite.
    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }
    REQUIRE(std::filesystem::file_size(pg_class_otbx) == first_size);
    REQUIRE(std::filesystem::last_write_time(pg_class_otbx) == first_mtime);

    cleanup_boot_dir();
}

// 3. Restart path: bootstrap_system_tables_sync's load path picks up all 10 tables
//    created by a prior bootstrap.
TEST_CASE("services::disk::sysboot::restart_loads_all_10") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }

    {
        disk_only_fixture fx(base);
        // Fresh manager — no in-memory state. The load path picks up the persisted .otbx
        // files. The call must not throw (each .otbx is a valid empty single-file block manager).
        REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());
    }

    cleanup_boot_dir();
}

// 4. Empty config_disk.path — bootstrap is a safe no-op (in-memory deployment).
TEST_CASE("services::disk::sysboot::no_path_is_safe_noop") {
    std::pmr::synchronized_pool_resource resource;
    log_t log = initialization_logger("python", "/tmp/docker_logs/");
    auto* scheduler = new core::non_thread_scheduler::scheduler_test_t(1, 1);
    configuration::config_disk c;
    c.path.clear(); // truly empty — config_disk default is current_path()/wal
    auto m = actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, c, log);

    REQUIRE_NOTHROW(m->bootstrap_system_tables_sync());
    REQUIRE_NOTHROW(m->bootstrap_system_tables_sync()); // idempotent re-run
    REQUIRE_NOTHROW(m->restore_oid_generator_sync());

    // Destroy the manager first: its dtor joins the internal loop thread, which may
    // still enqueue children onto the scheduler. Only then stop/delete the scheduler.
    m.reset();
    scheduler->stop();
    delete scheduler;
}

// 5. The OID generator allocates from FIRST_USER_OID by default and hands out
//    monotonically increasing OIDs. Observed through allocate_oids_batch (restore on an
//    empty catalog should leave the generator at its default seed).
TEST_CASE("services::disk::sysboot::oid_generator_default_seed") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    disk_only_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    fx.manager->restore_oid_generator_sync();

    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{2});
    REQUIRE(oids.size() == 2);
    REQUIRE(oids[0] >= FIRST_USER_OID);
    REQUIRE(oids[1] == oids[0] + 1);

    cleanup_boot_dir();
}

// 6. Each system table has a non-empty column set — no schema accidentally degraded to zero columns.
TEST_CASE("services::disk::sysboot::all_schemas_non_empty") {
    for (const auto& def : all_system_tables()) {
        REQUIRE(def.columns.size() > 0);
    }
}

// 7. Bootstrap dir layout: every system table gets its own subdir (no flat namespace).
TEST_CASE("services::disk::sysboot::dir_layout_per_table") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }

    for (const auto& def : all_system_tables()) {
        REQUIRE(std::filesystem::is_directory(coll_dir_for(base, def.name)));
    }

    cleanup_boot_dir();
}

// 8. Re-running bootstrap on the same in-memory state is idempotent
//    (does not throw, does not crash on already-loaded entries).
TEST_CASE("services::disk::sysboot::load_after_bootstrap_in_same_process") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    disk_only_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());
    REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync()); // double-call

    cleanup_boot_dir();
}
