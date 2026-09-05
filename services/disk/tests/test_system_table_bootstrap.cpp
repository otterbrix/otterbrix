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
    // The on-disk layout is OID-keyed: <base>/<db_oid>/<table_oid>/table.otbx. The
    // table OID comes straight off system_table_def_t::relation_oid — there is no
    // name→oid mapping to mirror here.
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

        components::execution_context_t ctx() {
            return components::execution_context_t{components::session::session_id_t{},
                                                   components::table::transaction_data{0, 0},
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
            // Bind the [[nodiscard]] reply and state something true of it: the sealed WAL
            // floor is the oldest root any table could still fall back to, so it can never
            // run ahead of the id this round was told the WAL had reached.
            auto sealed = std::move(cf).take_ready();
            REQUIRE(sealed <= wal_id);
        }
    };

    // The T3 interposer seam is process-wide and a bootstrap opens one .otbx per system
    // table, so filter by path: every handle whose path does not carry the marker is returned
    // unwrapped. Same shape as the scopes in test_persistence.cpp / test_resolve.cpp.
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

    // Append `count` BIGINT rows numbered [0, count) to an existing user table.
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
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
        REQUIRE_FALSE(r.has_error());
    }
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
        if (std::filesystem::exists(otbx_for(base, def.relation_oid))) {
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

    auto pg_class_otbx = otbx_for(base, well_known_oid::pg_class_table);
    REQUIRE(std::filesystem::exists(pg_class_otbx));
    auto first_size = std::filesystem::file_size(pg_class_otbx);
    auto first_mtime = std::filesystem::last_write_time(pg_class_otbx);

    // Second bootstrap on the same path — must short-circuit, not overwrite.
    {
        disk_only_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
    }
    REQUIRE(std::filesystem::file_size(pg_class_otbx) == first_size);
    // Extra parens keep Catch2 from stringifying file_time_type on failure: Apple's
    // filesystem clock uses an __int128 rep, which Catch2's chrono StringMaker
    // cannot stream (ostream has no __int128 overload) and macOS builds break.
    REQUIRE((std::filesystem::last_write_time(pg_class_otbx) == first_mtime));

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

// 4. Empty config_disk.path — bootstrap is a safe no-op. It used to mean "in-memory
// deployment" and built file-less system storages; B4 removed that mode, so an empty
// path now names no directory a .otbx could live in and the call refuses with a logged
// error instead of manufacturing a relative-path database under the process CWD. What
// this case pins is unchanged: the call is safe, idempotent, and leaves nothing behind.
TEST_CASE("services::disk::sysboot::no_path_is_safe_noop") {
    core::pmr::otterbrix_resource resource;
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
        REQUIRE(std::filesystem::is_directory(coll_dir_for(base, def.relation_oid)));
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

// --- D2: A SYSTEM TABLE THAT DID NOT COME UP MUST STOP THE START ------------------------
//
// bootstrap_one (manager_disk_bootstrap.cpp:126-170) had three ways to leave a pg_* table
// absent while reporting nothing an operator could act on:
//   * a failed load_storage_disk_sync was logged at WARN and the lambda returned false
//     ("not freshly created"), which ALSO skipped the seeding branch — so the engine came up
//     with an EMPTY catalog over live storage and the next DDL minted fresh oids on top of it;
//   * create_storage_disk_sync returns void, so a create whose very first write failed left
//     no storage and no word about it;
//   * a system table that LOADED cleanly with zero rows (the crash-before-first-checkpoint
//     shape) was indistinguishable from a table that was already there and full.
// The three cases below pin those three, each on CONTENT, and case A additionally pins that
// the refusal is survivable: the process goes on and a repeat start with the cause removed
// comes up.

// 9. A system table that CANNOT BE OPENED refuses the start, loudly — and the process lives.
TEST_CASE("services::disk::sysboot::unopenable_system_table_refuses_the_start") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    constexpr uint64_t kRows = 7;
    components::catalog::oid_t user_table = components::catalog::INVALID_OID;

    // Phase 1 — a healthy database with a namespace and a user table carrying rows.
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

    // Phase 2 — pg_class cannot be read at all. Offset 0 is the main header, the first read
    // load_existing_database issues, and 0 is a legal value for the knob (its off switch is
    // UINT64_MAX, fault_injection_file.hpp:53-58). The load fails through
    // construction_failed() -> data_corruption (manager_disk_io.cpp:442-446).
    {
        otterbrix_test::fault_plan_t plan;
        plan.fail_reads_at_location = 0;
        one_table_fault_scope_t scope(
            plan,
            "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_class_table)) + "/");

        disk_only_fixture fd2(base);
        INFO("a pg_catalog table that could not be opened must stop the start, not be skipped");
        // RED today: the failure is a WARN line and the engine comes up with no pg_class.
        REQUIRE_THROWS_AS(fd2.manager->bootstrap_system_tables_sync(), std::runtime_error);
        REQUIRE(plan.reads_failed > 0);
    }

    // Phase 3 — THE SURVIVAL PROOF. The refusal above unwound out of a live manager (the
    // first throw on this path to do so) and wrote nothing: the same directory opens again
    // with the cause gone, and it still holds phase 1's CONTENT.
    {
        disk_only_fixture fd3(base);
        REQUIRE_NOTHROW(fd3.manager->bootstrap_system_tables_sync());
        fd3.manager->restore_oid_generator_sync();
        fd3.manager->load_user_table_storages_sync();
        auto ns = fd3.invoke(&manager_disk_t::resolve_namespace, fd3.ctx(), std::string("ns_one"), std::uint64_t{0});
        REQUIRE_FALSE(ns.has_error());
        CHECK(ns.value().found);
        auto rows = disk_test_helpers::read_ok(
            fd3.invoke(&manager_disk_t::storage_total_rows, components::session::session_id_t{}, user_table));
        CHECK(rows == kRows);
    }

    cleanup_boot_dir();
}

// 10. THE CORRUPTION MECHANISM, stated as an assertion. restore_oid_generator_sync skips a
// system table whose entry is null (manager_disk_bootstrap.cpp:313-316), so a catalog table
// that did not come up takes its oids out of the frontier: the generator is left at its
// default seed (FIRST_USER_OID - 1) and the next allocation hands out an oid that is ALIVE on
// disk. After the refusal there is nothing to mint over, because there is no start.
//
// The victim is pg_namespace and the live object is a NAMESPACE on purpose. A namespace's
// oid is written to exactly one place (build_create_namespace_writes touches pg_namespace and
// nothing else), so losing that table loses the only record of the oid. Losing pg_class
// instead does NOT reproduce the collision — a table's attoids are allocated from the same
// batch and land in pg_attribute ABOVE the table's own oid, so the frontier survives in a
// neighbouring table. The hole is real either way; it just is not universal.
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

// 11. The CREATE leg of the same hole. create_storage_disk_sync returns void
// (agent_disk.cpp:151-160 records the construction failure and drops the entry), so a system
// table whose very first write failed leaves bootstrap_one returning "freshly created" over a
// storage that does not exist — and the seeding that follows appends into nothing.
TEST_CASE("services::disk::sysboot::uncreatable_system_table_refuses_the_start") {
    cleanup_boot_dir();
    auto base = std::filesystem::path(boot_test_dir());
    std::filesystem::create_directories(base);

    otterbrix_test::fault_plan_t plan;
    // fail_writes_from is compared with >=, so 1 fails every write on the wrapped handle from
    // the first one on — including the header write that creates the file.
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
        // The content the silent start actually produced: a pg_namespace with no "public".
        plan.fail_writes_from = 0;
        auto ns = fd.invoke(&manager_disk_t::resolve_namespace, fd.ctx(), std::string("public"), std::uint64_t{0});
        INFO("a start that came up over a pg_namespace it could not create has no 'public'");
        CHECK((!ns.has_error() && ns.value().found));
    }
    INFO("a pg_catalog table that could not be created must stop the start");
    CHECK(refused);

    plan.fail_writes_from = 0;
    cleanup_boot_dir();
}

// 12. THE SILENT TWIN — a system table that loads HEALTHY and EMPTY.
//
// A crash between "the system table's .otbx was created" and "its first checkpoint committed"
// leaves a proven-young file: exactly BLOCK_START bytes, no `.wal_id` sidecar. That file opens
// cleanly (A7.6 overlays the builtin schema) and yields ZERO rows, so bootstrap_one returned
// "not freshly created" and every seeding branch was skipped. Nothing anywhere reported an
// error — the catalog was simply empty. This is NOT a refusal case: the file is healthy, and a
// refusal would repeat on every start forever.
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
        // Engine-made, never checkpointed: no file is laid out by hand here.
        const auto* def = find_system_table(components::catalog::well_known_oid::pg_namespace_table);
        REQUIRE(def != nullptr);
        core::pmr::otterbrix_resource create_resource;
        table_storage_t ts(&create_resource, def->columns, otbx);
        REQUIRE_FALSE(ts.construction_failed());
    }
    REQUIRE(std::filesystem::file_size(otbx) == components::table::storage::BLOCK_START);

    disk_only_fixture fd2(base);
    REQUIRE_NOTHROW(fd2.manager->bootstrap_system_tables_sync());
    auto ns = fd2.invoke(&manager_disk_t::resolve_namespace, fd2.ctx(), std::string("public"), std::uint64_t{0});
    REQUIRE_FALSE(ns.has_error());
    INFO("a system table that loaded with zero rows must be seeded, not left silently empty");
    CHECK(ns.value().found);

    cleanup_boot_dir();
}
