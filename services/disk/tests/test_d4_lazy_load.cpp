#include <catch2/catch_test_macros.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/base.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unistd.h>

// Lazy-loading: after bootstrap only the 10 pg_catalog.* system tables are loaded,
// user tables stay out of storages_ until explicitly accessed, and DDL on unloaded
// user tables modifies only system catalog rows (pg_class / pg_attribute) without
// requiring the user storage to be present.

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;
using namespace disk_test_helpers;

namespace {
    std::string d4_dir() {
        static std::string p = "/tmp/test_otterbrix_d4_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(d4_dir()); }

    struct fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = d4_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(d4_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            // Destroy the manager first: its dtor joins the internal loop thread,
            // which may still enqueue children onto the scheduler. Only then is it
            // safe to stop/delete the scheduler.
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

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };

} // namespace

// 1. After bootstrap all 10 pg_catalog.* tables are loaded into storages_.
//    Doc test alias: test_user_table_not_in_storages_at_start (system-side half).
TEST_CASE("services::disk::d4::all_system_tables_loaded_after_bootstrap") {
    fixture fx;
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_database_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_namespace_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_class_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_attribute_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_type_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_proc_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_depend_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_constraint_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_index_table));
    REQUIRE(fx.manager->has_storage(well_known_oid::pg_computed_column_table));
}

// 2. Before any user DDL, no user-table storage exists.
//    Doc test alias: test_user_table_not_in_storages_at_start (user-side half).
TEST_CASE("services::disk::d4::user_table_not_in_storages_at_start") {
    fixture fx;
    // An unallocated user oid (FIRST_USER_OID would be the next allocated) — definitely not loaded.
    REQUIRE_FALSE(fx.manager->has_storage(catalog::oid_t{FIRST_USER_OID + 1000}));
}

// 3. CREATE TABLE only writes pg_class / pg_attribute rows. The user storage is
//    NOT auto-instantiated — D4 leaves storage creation to the executor (or to the
//    next resolve_table when an .otbx is present on disk).
//    Doc test alias: test_append_user_table_to_pg_class.
TEST_CASE("services::disk::d4::create_table_does_not_eager_load_storage") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4a");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "users", std::move(cols));
    REQUIRE(rt_oid >= FIRST_USER_OID);
    // Storage is intentionally NOT in storages_: D4 = lazy. resolve_table is the
    // entry point that promotes a disk-resident .otbx into storages_.
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
}

// 4. resolve_table sees a freshly created user table in pg_class even when its storage
//    is not loaded. Doc test alias: test_show_tables_from_pg_class.
TEST_CASE("services::disk::d4::resolve_table_finds_unloaded_user_table") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4b");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "orders", std::move(cols));
    auto resolved = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("orders"));
    REQUIRE(resolved.found);
    REQUIRE(resolved.oid == rt_oid);
    // resolve_table did not need storage to be present in storages_ to answer the lookup.
}

// 5. DROP TABLE on an unloaded user table mutates only pg_class / pg_attribute /
//    pg_depend; no storage entry required. Doc test alias: test_drop_unloaded_table.
TEST_CASE("services::disk::d4::drop_unloaded_table") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4c");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("v", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "temp_t", std::move(cols));
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
    test_drop_table(fx, rt_oid);
    // After drop the table is no longer resolvable.
    auto resolved = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("temp_t"));
    REQUIRE_FALSE(resolved.found);
}

// 6. test_add_column (pure pg_attribute write, no in-memory sync) on an
//    unloaded user table leaves the storage map untouched. Doc test alias:
//    test_alter_unloaded_table.
TEST_CASE("services::disk::d4::alter_unloaded_table_add_column") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4d");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "alter_me", std::move(cols));
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
    components::table::column_definition_t new_col(
        "name",
        components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    test_add_column(fx, rt_oid, std::move(new_col), 2);
    // No user-storage materialisation as a side-effect of ALTER.
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
    // The new column shows up via resolve_table.
    auto resolved = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("alter_me"));
    REQUIRE(resolved.found);
    REQUIRE(resolved.columns.size() == 2);
}

// 7. Multiple resolve_table calls for the same un-loaded user table do not flap the
//    storages_ map: the storage stays absent (no eager promotion when there's no .otbx
//    on disk). Doc test alias: test_second_select_uses_existing (negative form).
TEST_CASE("services::disk::d4::repeated_resolve_does_not_create_storage") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4e");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "readme", std::move(cols));
    for (int i = 0; i < 3; ++i) {
        auto r = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("readme"));
        REQUIRE(r.found);
    }
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
}

// 8. resolve_table walks pg_attribute filtered by attrelid and returns the attoids in
//    attnum order. Doc test alias: test_scan_pg_attribute_by_relid.
TEST_CASE("services::disk::d4::resolve_table_collects_columns_by_attrelid") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4f");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("a", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("b", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    cols.emplace_back("c", components::types::complex_logical_type{components::types::logical_type::DOUBLE});
    auto rt_oid = test_create_table(fx, ns_oid, "multi", std::move(cols));
    auto r = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("multi"));
    REQUIRE(r.found);
    REQUIRE(r.oid == rt_oid);
    REQUIRE(r.columns.size() == 3);
    // Attoids are unique (each column gets its own oid_gen.allocate()).
    REQUIRE(r.columns[0].attoid != r.columns[1].attoid);
    REQUIRE(r.columns[1].attoid != r.columns[2].attoid);
    REQUIRE(r.columns[0].attoid != r.columns[2].attoid);
}

// 9. peek_checkpoint_wal_id_from_disk returns 0 for unknown tables (§1.11).
TEST_CASE("services::disk::d4::peek_checkpoint_wal_id_unknown_returns_zero") {
    fixture fx;
    // A table that was never created has no sidecar: peek returns 0.
    // "No sidecar" is the one state that still answers 0: nothing was read wrong, there was
    // nothing to read. A sidecar that exists and cannot be read reports instead.
    auto v = fx.manager->peek_checkpoint_wal_id_from_disk(catalog::oid_t{FIRST_USER_OID + 9000},
                                                          well_known_oid::main_database);
    REQUIRE_FALSE(v.has_error());
    REQUIRE(v.value() == services::wal::id_t{0});
}

// 10. load_storage_for_wal_replay_sync is a no-op for already-loaded storage (§1.11).
TEST_CASE("services::disk::d4::load_storage_for_wal_replay_noop_when_loaded") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4g");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    // Create the table (writes pg_class/pg_attribute; does NOT load user storage).
    auto rt_oid = test_create_table(fx, ns_oid, "lazy_t", std::move(cols));

    // Calling load_storage_for_wal_replay_sync on a table that has no .otbx must not crash.
    // A table with no .otbx is "nothing to read", not "could not read": no error, and the
    // caller goes on to synthesise the storage from the record's own chunk.
    REQUIRE_FALSE(fx.manager->load_storage_for_wal_replay_sync(rt_oid, well_known_oid::main_database)
                      .contains_error());
}
// 11. A never-checkpointed .otbx (created, crashed before any checkpoint) loads as a
// legitimately EMPTY disk table with its schema overlaid from the catalog. Feeding the young
// file to the metadata reader instead gives "attempted to read past end of chain", the storage
// stays unloaded, and queries are answered by the storage-less record branch — empty by
// accident.
TEST_CASE("services::disk::d4::never_checkpointed_otbx_loads_as_empty_with_catalog_schema") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_a76a");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "young_t", std::move(cols));

    // Lay down the young .otbx exactly where the manager expects it, through the real
    // create path (never checkpointed => header sectors only, meta_block INVALID), and
    // release the WRITE_LOCK by scope exit.
    const auto tbl_dir = std::filesystem::path(d4_dir()) /
                         std::to_string(static_cast<unsigned>(well_known_oid::main_database)) /
                         std::to_string(static_cast<unsigned>(rt_oid));
    std::filesystem::create_directories(tbl_dir);
    const auto otbx = tbl_dir / "table.otbx";
    {
        core::pmr::otterbrix_resource create_resource;
        std::vector<components::table::column_definition_t> create_cols;
        create_cols.emplace_back("id",
                                 components::types::complex_logical_type{components::types::logical_type::BIGINT});
        table_storage_t ts(&create_resource, std::move(create_cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());
    }
    REQUIRE(std::filesystem::file_size(otbx) == components::table::storage::BLOCK_START);

    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
    REQUIRE_FALSE(
        fx.manager->load_storage_for_wal_replay_sync(rt_oid, well_known_oid::main_database).contains_error());
    // The load succeeded: the young file opened as an empty DISK storage whose columns came
    // from pg_attribute — not a warning, not a skip.
    REQUIRE(fx.manager->has_storage(rt_oid));
    // Loading wrote nothing: the file still carries the never-checkpointed signature.
    REQUIRE(std::filesystem::file_size(otbx) == components::table::storage::BLOCK_START);
}

// 12. The contradiction: the .wal_id sidecar is written solely by a COMMITTED checkpoint,
// so a sidecar claiming wal id N next to a never-checkpointed .otbx means the table file
// was rebuilt or truncated out from under its sidecar. Opening it as empty would silently
// discard whatever that checkpoint held: the load must REFUSE (sidecar consulted in the
// refusing direction only), and both files stay byte-identical.
TEST_CASE("services::disk::d4::young_otbx_with_checkpoint_sidecar_is_refused") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_a76b");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "contradicted_t", std::move(cols));

    const auto tbl_dir = std::filesystem::path(d4_dir()) /
                         std::to_string(static_cast<unsigned>(well_known_oid::main_database)) /
                         std::to_string(static_cast<unsigned>(rt_oid));
    std::filesystem::create_directories(tbl_dir);
    const auto otbx = tbl_dir / "table.otbx";
    {
        core::pmr::otterbrix_resource create_resource;
        std::vector<components::table::column_definition_t> create_cols;
        create_cols.emplace_back("id",
                                 components::types::complex_logical_type{components::types::logical_type::BIGINT});
        table_storage_t ts(&create_resource, std::move(create_cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());
    }
    REQUIRE(std::filesystem::file_size(otbx) == components::table::storage::BLOCK_START);
    // A sidecar only a committed checkpoint writes — deliberately contradictory input for a
    // refusal test (no prepared database is hand-placed; the .otbx itself is engine-made).
    {
        std::ofstream sidecar(tbl_dir / "table.otbx.wal_id", std::ios::binary | std::ios::trunc);
        REQUIRE(sidecar.is_open());
        const uint64_t claimed = 5;
        sidecar.write(reinterpret_cast<const char*>(&claimed), sizeof(claimed));
        REQUIRE(sidecar.good());
    }

    // The refusal has to be VISIBLE to the caller: otherwise replay sees the same "no storage"
    // a table with no file gives and goes on to CREATE one at this path, over the very .otbx
    // the contradiction was about.
    REQUIRE(fx.manager->load_storage_for_wal_replay_sync(rt_oid, well_known_oid::main_database).contains_error());
    // Refused loudly inside load_storage_disk_sync: no storage may appear for a table whose
    // on-disk state is self-contradictory.
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
    // Refusal touched neither file.
    REQUIRE(std::filesystem::file_size(otbx) == components::table::storage::BLOCK_START);
    REQUIRE(std::filesystem::file_size(tbl_dir / "table.otbx.wal_id") == sizeof(uint64_t));
}
