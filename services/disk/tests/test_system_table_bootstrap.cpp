#include <catch2/catch_test_macros.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/log/log.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include "disk_test_helpers.hpp"

#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace services::disk;
using namespace components::catalog;

namespace {
    std::string boot_test_dir() {
        static std::string path = "/tmp/test_otterbrix_sysboot_" + std::to_string(::getpid());
        return path;
    }
    void cleanup_boot_dir() { std::filesystem::remove_all(boot_test_dir()); }

    std::filesystem::path sys_dir_for(const std::filesystem::path& base) {
        return base / std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::main_database));
    }
    // Layout is OID-keyed: <base>/<db_oid>/<table_oid>/table.otbx.
    std::filesystem::path coll_dir_for(const std::filesystem::path& base, components::catalog::oid_t tbl_oid) {
        return sys_dir_for(base) / std::to_string(static_cast<unsigned>(tbl_oid));
    }
    std::filesystem::path otbx_for(const std::filesystem::path& base, components::catalog::oid_t tbl_oid) {
        return coll_dir_for(base, tbl_oid) / "table.otbx";
    }

    struct disk_only_fixture {
        core::pmr::otterbrix_resource resource;
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
            // Destroy the manager first: its dtor joins the loop thread, which may still enqueue onto the scheduler.
            manager.reset();
            scheduler->stop();
            delete scheduler;
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

        components::execution_context_t ctx() {
            return components::execution_context_t{components::session::session_id_t{},
                                                   components::table::transaction_data::committed(),
                                                   {}};
        }

        void checkpoint(services::wal::id_t wal_id) {
            auto [_, cf] = actor_zeta::otterbrix::send(manager->address(),
                                                       &manager_disk_t::checkpoint_all,
                                                       components::session::session_id_t{},
                                                       wal_id,
                                                       std::numeric_limits<uint64_t>::max());
            for (int i = 0; i < 100000 && !cf.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(cf.is_ready());
            // Sealed floor can never run ahead of the wal id this round reports.
            auto sealed = std::move(cf).take_ready();
            REQUIRE(sealed <= wal_id);
        }
    };

    class one_table_fault_scope_t final
        : public components::table::storage::single_file_block_manager_t::file_handle_interposer_t {
    public:
        one_table_fault_scope_t(otterbrix_test::fault_plan_t& plan, std::string path_marker)
            : plan_(plan)
            , marker_(std::move(path_marker)) {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(this);
        }
        ~one_table_fault_scope_t() override {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(nullptr);
        }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner == nullptr || inner->path().string().find(marker_) == std::string::npos) {
                return inner;
            }
            return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan_);
        }

    private:
        otterbrix_test::fault_plan_t& plan_;
        std::string marker_;
    };

    void append_rows(disk_only_fixture& fx, components::catalog::oid_t table_oid, uint64_t count) {
        std::pmr::vector<components::types::complex_logical_type> types(&fx.resource);
        components::types::complex_logical_type t{components::types::logical_type::BIGINT};
        t.set_alias("value");
        types.push_back(std::move(t));
        components::vector::data_chunk_t chunk(&fx.resource, types, count);
        chunk.set_cardinality(count);
        for (uint64_t i = 0; i < count; i++) {
            chunk.set_value(0, i, static_cast<std::int64_t>(i));
        }
        std::pmr::vector<components::vector::data_chunk_t> batch(&fx.resource);
        batch.emplace_back(std::move(chunk));
        components::execution_context_t append_ctx{components::session::session_id_t{},
                                                   components::table::transaction_data::committed(),
                                                   {},
                                                   table_oid};
        auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
        REQUIRE_FALSE(r.has_error());
    }
} // namespace

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
        if (std::filesystem::exists(otbx_for(base, def.relation_oid))) {
            otbx_count++;
        }
    }
    REQUIRE(otbx_count == all_system_tables().size());

    cleanup_boot_dir();
}

TEST_CASE("services::disk::sysboot::bootstrap_is_idempotent") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }

    auto pg_class_otbx = otbx_for(base, well_known_oid::pg_class_table);
    REQUIRE(std::filesystem::exists(pg_class_otbx));
    auto first_size = std::filesystem::file_size(pg_class_otbx);
    auto first_mtime = std::filesystem::last_write_time(pg_class_otbx);

    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }
    REQUIRE(std::filesystem::file_size(pg_class_otbx) == first_size);
    // Extra parens stop Catch2 from stringifying file_time_type — its __int128 rep has no ostream overload on macOS.
    REQUIRE((std::filesystem::last_write_time(pg_class_otbx) == first_mtime));

    cleanup_boot_dir();
}

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
        REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());
    }

    cleanup_boot_dir();
}

// Empty config_disk.path makes bootstrap refuse, instead of manufacturing a relative-path db under the CWD.
TEST_CASE("services::disk::sysboot::no_path_is_safe_noop") {
    core::pmr::otterbrix_resource resource;
    log_t log = initialization_logger("python", "/tmp/docker_logs/");
    auto* scheduler = new core::non_thread_scheduler::scheduler_test_t(1, 1);
    configuration::config_disk c;
    c.path.clear(); // truly empty — config_disk default is current_path()/wal
    auto m = actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, c, log);

    REQUIRE_NOTHROW(m->bootstrap_system_tables_sync());
    REQUIRE_NOTHROW(m->bootstrap_system_tables_sync());
    REQUIRE_NOTHROW(m->restore_oid_generator_sync());

    m.reset();
    scheduler->stop();
    delete scheduler;
}

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

TEST_CASE("services::disk::sysboot::all_schemas_non_empty") {
    for (const auto& def : all_system_tables()) {
        REQUIRE(def.columns.size() > 0);
    }
}

TEST_CASE("services::disk::sysboot::dir_layout_per_table") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }

    for (const auto& def : all_system_tables()) {
        REQUIRE(std::filesystem::is_directory(coll_dir_for(base, def.relation_oid)));
    }

    cleanup_boot_dir();
}

TEST_CASE("services::disk::sysboot::load_after_bootstrap_in_same_process") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    disk_only_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());
    REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());

    cleanup_boot_dir();
}

// A failed system-table open/create used to come up silently; it must now refuse the start instead.
TEST_CASE("services::disk::sysboot::unopenable_system_table_refuses_the_start") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    constexpr uint64_t kRows = 7;
    components::catalog::oid_t user_table = components::catalog::INVALID_OID;

    {
        disk_only_fixture fd(base);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = disk_test_helpers::test_create_namespace(fd, "ns_one");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        user_table = disk_test_helpers::test_create_table(fd, ns_oid, "t_one", cols);
        fd.invoke(&manager_disk_t::create_storage_disk,
                  components::session::session_id_t{},
                  user_table,
                  components::catalog::well_known_oid::main_database,
                  cols,
                  /*is_computed=*/false);
        append_rows(fd, user_table, kRows);
        fd.checkpoint(services::wal::id_t{100});
    }

    // fail_reads_at_location=0 fails the header read — the knob's off switch is UINT64_MAX, not 0.
    {
        otterbrix_test::fault_plan_t plan;
        plan.fail_reads_at_location = 0;
        one_table_fault_scope_t scope(
            plan,
            "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_class_table)) + "/");

        disk_only_fixture fd2(base);
        INFO("a pg_catalog table that could not be opened must stop the start, not be skipped");
        REQUIRE_THROWS_AS(fd2.manager->bootstrap_system_tables_sync(), std::runtime_error);
        REQUIRE(plan.reads_failed > 0);
    }

    // Survival proof: the refusal wrote nothing, so a repeat open still holds phase 1's content.
    {
        disk_only_fixture fd3(base);
        REQUIRE_NOTHROW(fd3.manager->bootstrap_system_tables_sync());
        fd3.manager->restore_oid_generator_sync();
        fd3.manager->load_user_table_storages_sync();
        auto ns = fd3.invoke(&manager_disk_t::resolve_namespace, fd3.ctx(), std::string("ns_one"));
        REQUIRE_FALSE(ns.has_error());
        CHECK(ns.value().found);
        auto rows = disk_test_helpers::read_ok(
            fd3.invoke(&manager_disk_t::storage_total_rows, components::session::session_id_t{}, user_table));
        CHECK(rows == kRows);
    }

    cleanup_boot_dir();
}

// pg_namespace, not pg_class, is the fault target: its oid lives nowhere else, so skipping it collides the frontier.
TEST_CASE("services::disk::sysboot::a_catalog_that_did_not_come_up_never_lowers_the_oid_frontier") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    components::catalog::oid_t live_ns = components::catalog::INVALID_OID;
    {
        disk_only_fixture fd(base);
        fd.manager->bootstrap_system_tables_sync();
        live_ns = disk_test_helpers::test_create_namespace(fd, "ns_two");
        fd.checkpoint(services::wal::id_t{100});
    }
    REQUIRE(live_ns >= components::catalog::FIRST_USER_OID);

    otterbrix_test::fault_plan_t plan;
    plan.fail_reads_at_location = 0;
    one_table_fault_scope_t scope(
        plan,
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_namespace_table)) + "/");

    disk_only_fixture fd2(base);
    bool refused = false;
    try {
        fd2.manager->bootstrap_system_tables_sync();
    } catch (const std::runtime_error&) {
        refused = true;
    }
    if (!refused) {
        fd2.manager->restore_oid_generator_sync();
        auto oids = fd2.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        REQUIRE(oids.size() == 1);
        INFO("fresh oid " << oids[0] << " would be minted over the live namespace oid " << live_ns);
        CHECK(oids[0] > live_ns);
    }
    INFO("a start over a catalog table that could not be opened must not happen at all");
    CHECK(refused);

    cleanup_boot_dir();
}

// create_storage_disk_sync returns void, so a failed first write used to leave a table reported as created.
TEST_CASE("services::disk::sysboot::uncreatable_system_table_refuses_the_start") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    otterbrix_test::fault_plan_t plan;
    plan.fail_writes_from = 1;
    one_table_fault_scope_t scope(
        plan,
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_namespace_table)) + "/");

    disk_only_fixture fd(base);
    bool refused = false;
    try {
        fd.manager->bootstrap_system_tables_sync();
    } catch (const std::runtime_error&) {
        refused = true;
    }
    if (!refused) {
        plan.fail_writes_from = 0;
        auto ns = fd.invoke(&manager_disk_t::resolve_namespace, fd.ctx(), std::string("public"));
        INFO("a start that came up over a pg_namespace it could not create has no 'public'");
        CHECK((!ns.has_error() && ns.value().found));
    }
    INFO("a pg_catalog table that could not be created must stop the start");
    CHECK(refused);

    plan.fail_writes_from = 0;
    cleanup_boot_dir();
}

// A crash before the first checkpoint leaves a healthy, empty BLOCK_START file that must be reseeded.
TEST_CASE("services::disk::sysboot::a_system_table_that_loads_empty_is_seeded_again") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    {
        disk_only_fixture fd(base);
        fd.manager->bootstrap_system_tables_sync();
    }

    const auto otbx = otbx_for(base, components::catalog::well_known_oid::pg_namespace_table);
    REQUIRE(std::filesystem::exists(otbx));
    std::filesystem::remove(otbx);
    std::filesystem::remove(std::filesystem::path(otbx.string() + ".wal_id"));
    {
        const auto* def = find_system_table(components::catalog::well_known_oid::pg_namespace_table);
        REQUIRE(def != nullptr);
        core::pmr::otterbrix_resource create_resource;
        table_storage_t ts(&create_resource, def->columns, otbx);
        REQUIRE_FALSE(ts.construction_failed());
    }
    REQUIRE(std::filesystem::file_size(otbx) == components::table::storage::BLOCK_START);

    disk_only_fixture fd2(base);
    REQUIRE_NOTHROW(fd2.manager->bootstrap_system_tables_sync());
    auto ns = fd2.invoke(&manager_disk_t::resolve_namespace, fd2.ctx(), std::string("public"));
    REQUIRE_FALSE(ns.has_error());
    INFO("a system table that loaded with zero rows must be seeded, not left silently empty");
    CHECK(ns.value().found);

    cleanup_boot_dir();
}

// Simulates a database with no self-rows to prove reopen still finds the user table and reseeds pg_class's own row.
TEST_CASE("services::disk::sysboot::an_old_database_without_self_rows_is_caught_up") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    constexpr auto pg_class = well_known_oid::pg_class_table;
    constexpr auto pg_attribute = well_known_oid::pg_attribute_table;
    const auto n_sys_tables = all_system_tables().size();
    std::uint64_t n_sys_columns = 0;
    for (const auto& def : all_system_tables()) {
        n_sys_columns += def.columns.size();
    }

    components::catalog::oid_t ns_oid = components::catalog::INVALID_OID;

    {
        disk_only_fixture fd(base);
        fd.manager->bootstrap_system_tables_sync();
        fd.manager->restore_oid_generator_sync();
        ns_oid = disk_test_helpers::test_create_namespace(fd, "ns_old");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        const auto user_table = disk_test_helpers::test_create_table(fd, ns_oid, "t_old", cols);
        fd.invoke(&manager_disk_t::create_storage_disk,
                  components::session::session_id_t{},
                  user_table,
                  components::catalog::well_known_oid::main_database,
                  cols,
                  /*is_computed=*/false);
        append_rows(fd, user_table, 3);

        for (const auto& def : all_system_tables()) {
            fd.invoke(&manager_disk_t::delete_pg_catalog_rows,
                      disk_test_helpers::auto_ctx(),
                      pg_class,
                      std::int64_t{components::catalog::pg_class_col::oid},
                      def.relation_oid);
            fd.invoke(&manager_disk_t::delete_pg_catalog_rows,
                      disk_test_helpers::auto_ctx(),
                      pg_attribute,
                      std::int64_t{components::catalog::pg_attribute_col::attrelid},
                      def.relation_oid);
        }
        auto rk = fd.manager->relkind_for_oid_sync(pg_class);
        REQUIRE_FALSE(rk.has_error());
        REQUIRE(rk.value() == '\0');
        auto t = test_probe::probe_table(fd, fd.ctx(), ns_oid, std::string("t_old"));
        REQUIRE(t.found);
        auto self = test_probe::probe_table(fd,
                                            fd.ctx(),
                                            components::catalog::well_known_oid::pg_catalog_namespace,
                                            std::string("pg_class"));
        REQUIRE_FALSE(self.found);
        fd.checkpoint(services::wal::id_t{100});
    }

    std::uint64_t caught_up_cls_rows = 0;
    std::uint64_t caught_up_att_rows = 0;

    {
        disk_only_fixture fd2(base);
        REQUIRE_NOTHROW(fd2.manager->bootstrap_system_tables_sync());
        fd2.manager->restore_oid_generator_sync();
        fd2.manager->load_user_table_storages_sync();

        auto t = test_probe::probe_table(fd2, fd2.ctx(), ns_oid, std::string("t_old"));
        INFO("catching up the catalog must not lose the user table's pg_class row");
        REQUIRE(t.found);
        CHECK(t.columns.size() == 1);

        // pg_class resolves ITSELF by name again — the row only the catch-up branch writes.
        auto self = test_probe::probe_table(fd2,
                                            fd2.ctx(),
                                            components::catalog::well_known_oid::pg_catalog_namespace,
                                            std::string("pg_class"));
        INFO("an old database must be caught up: pg_class needs its own self-description row");
        REQUIRE(self.found);
        CHECK(self.oid == pg_class);
        const auto* cls_def = find_system_table(pg_class);
        REQUIRE(cls_def != nullptr);
        CHECK(self.columns.size() == cls_def->columns.size());

        caught_up_cls_rows = disk_test_helpers::read_ok(
            fd2.invoke(&manager_disk_t::storage_total_rows, components::session::session_id_t{}, pg_class));
        caught_up_att_rows = disk_test_helpers::read_ok(
            fd2.invoke(&manager_disk_t::storage_total_rows, components::session::session_id_t{}, pg_attribute));
        CHECK(caught_up_cls_rows == 1 + n_sys_tables);
        CHECK(caught_up_att_rows == 1 + n_sys_columns);
    }

    // The catch-up rows persist via their own checkpoint — phase 2 ran no checkpoint_all.
    {
        disk_only_fixture fd3(base);
        REQUIRE_NOTHROW(fd3.manager->bootstrap_system_tables_sync());
        fd3.manager->restore_oid_generator_sync();
        fd3.manager->load_user_table_storages_sync();

        auto cls_rows = disk_test_helpers::read_ok(
            fd3.invoke(&manager_disk_t::storage_total_rows, components::session::session_id_t{}, pg_class));
        auto att_rows = disk_test_helpers::read_ok(
            fd3.invoke(&manager_disk_t::storage_total_rows, components::session::session_id_t{}, pg_attribute));
        INFO("a second open of a caught-up database must not seed the self-rows again");
        CHECK(cls_rows == caught_up_cls_rows);
        CHECK(att_rows == caught_up_att_rows);

        auto t = test_probe::probe_table(fd3, fd3.ctx(), ns_oid, std::string("t_old"));
        CHECK(t.found);
        auto self = test_probe::probe_table(fd3,
                                            fd3.ctx(),
                                            components::catalog::well_known_oid::pg_catalog_namespace,
                                            std::string("pg_class"));
        CHECK(self.found);
    }

    cleanup_boot_dir();
}
