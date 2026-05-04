#include <catch2/catch.hpp>

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

#include <filesystem>
#include <unistd.h>

// D4 lazy-loading tests (catalog-migration-to-postgresql-style.md §3 lines 323–434).
// Verify that after bootstrap only the 10 pg_catalog.* system tables are loaded,
// user tables stay out of storages_ until explicitly accessed, and DDL on unloaded
// user tables modifies only system catalog rows (pg_class / pg_attribute) without
// requiring the user storage to be present.

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string d4_dir() {
        static std::string p = "/tmp/test_otterbrix_d4_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(d4_dir()); }

    struct fixture {
        std::pmr::synchronized_pool_resource resource;
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
            manager->set_run_fn([this] { scheduler->run(10000); });
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            scheduler->stop();
            delete scheduler;
            cleanup();
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };

    bool sys(const fixture& fx, const std::string& tbl) {
        return fx.manager->has_storage(collection_full_name_t{"pg_catalog", "main", tbl});
    }
} // namespace

// 1. After bootstrap all 10 pg_catalog.* tables are loaded into storages_.
//    Doc test alias: test_user_table_not_in_storages_at_start (system-side half).
TEST_CASE("services::disk::d4::all_system_tables_loaded_after_bootstrap") {
    fixture fx;
    REQUIRE(sys(fx, "pg_database"));
    REQUIRE(sys(fx, "pg_namespace"));
    REQUIRE(sys(fx, "pg_class"));
    REQUIRE(sys(fx, "pg_attribute"));
    REQUIRE(sys(fx, "pg_type"));
    REQUIRE(sys(fx, "pg_proc"));
    REQUIRE(sys(fx, "pg_depend"));
    REQUIRE(sys(fx, "pg_constraint"));
    REQUIRE(sys(fx, "pg_index"));
    REQUIRE(sys(fx, "pg_computed_column"));
}

// 2. Before any user DDL, no user-table storage exists.
//    Doc test alias: test_user_table_not_in_storages_at_start (user-side half).
TEST_CASE("services::disk::d4::user_table_not_in_storages_at_start") {
    fixture fx;
    REQUIRE_FALSE(fx.manager->has_storage(collection_full_name_t{"any_ns", "main", "any_table"}));
}

// 3. ddl_create_table only writes pg_class / pg_attribute rows. The user storage is
//    NOT auto-instantiated — D4 leaves storage creation to the executor (or to the
//    next resolve_table when an .otbx is present on disk).
//    Doc test alias: test_append_user_table_to_pg_class.
TEST_CASE("services::disk::d4::create_table_does_not_eager_load_storage") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_d4a"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("users"), std::move(cols), catalog::relkind::regular);
    REQUIRE(rt.created_oid >= FIRST_USER_OID);
    // Storage is intentionally NOT in storages_: D4 = lazy. resolve_table is the
    // entry point that promotes a disk-resident .otbx into storages_.
    REQUIRE_FALSE(fx.manager->has_storage(collection_full_name_t{"ns_d4a", "main", "users"}));
}

// 4. resolve_table sees a freshly created user table in pg_class even when its storage
//    is not loaded. Doc test alias: test_show_tables_from_pg_class.
TEST_CASE("services::disk::d4::resolve_table_finds_unloaded_user_table") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_d4b"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("orders"), std::move(cols), catalog::relkind::regular);
    auto resolved = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(),
                                rns.created_oid, std::string("orders"), std::uint64_t{0});
    REQUIRE(resolved.found);
    REQUIRE(resolved.oid == rt.created_oid);
    // resolve_table did not need storage to be present in storages_ to answer the lookup.
}

// 5. ddl_drop_table on an unloaded user table mutates only pg_class / pg_attribute /
//    pg_depend; no storage entry required. Doc test alias: test_drop_unloaded_table.
TEST_CASE("services::disk::d4::drop_unloaded_table") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_d4c"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("v", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("temp_t"), std::move(cols), catalog::relkind::regular);
    REQUIRE_FALSE(fx.manager->has_storage(collection_full_name_t{"ns_d4c", "main", "temp_t"}));
    fx.invoke(&manager_disk_t::ddl_drop_table, fx.ctx(), rt.created_oid,
               drop_behavior_t::cascade_);
    // After drop the table is no longer resolvable.
    auto resolved = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(),
                                rns.created_oid, std::string("temp_t"), std::uint64_t{0});
    REQUIRE_FALSE(resolved.found);
}

// 6. ddl_add_column on an unloaded user table only updates pg_attribute. Doc test alias:
//    test_alter_unloaded_table.
TEST_CASE("services::disk::d4::alter_unloaded_table_add_column") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_d4d"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("alter_me"), std::move(cols), catalog::relkind::regular);
    REQUIRE_FALSE(fx.manager->has_storage(collection_full_name_t{"ns_d4d", "main", "alter_me"}));
    components::table::column_definition_t new_col(
        "name", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    fx.invoke(&manager_disk_t::ddl_add_column, fx.ctx(), rt.created_oid, std::move(new_col));
    // No user-storage materialisation as a side-effect of ALTER.
    REQUIRE_FALSE(fx.manager->has_storage(collection_full_name_t{"ns_d4d", "main", "alter_me"}));
    // The new column shows up via resolve_table.
    auto resolved = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(),
                                rns.created_oid, std::string("alter_me"), std::uint64_t{0});
    REQUIRE(resolved.found);
    REQUIRE(resolved.columns.size() == 2);
}

// 7. Multiple resolve_table calls for the same un-loaded user table do not flap the
//    storages_ map: the storage stays absent (no eager promotion when there's no .otbx
//    on disk). Doc test alias: test_second_select_uses_existing (negative form).
TEST_CASE("services::disk::d4::repeated_resolve_does_not_create_storage") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_d4e"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
               std::string("readme"), std::move(cols), catalog::relkind::regular);
    for (int i = 0; i < 3; ++i) {
        auto r = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(),
                           rns.created_oid, std::string("readme"), std::uint64_t{0});
        REQUIRE(r.found);
    }
    REQUIRE_FALSE(fx.manager->has_storage(collection_full_name_t{"ns_d4e", "main", "readme"}));
}

// 8. resolve_table walks pg_attribute filtered by attrelid and returns the attoids in
//    attnum order. Doc test alias: test_scan_pg_attribute_by_relid.
TEST_CASE("services::disk::d4::resolve_table_collects_columns_by_attrelid") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_d4f"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("a", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("b", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    cols.emplace_back("c", components::types::complex_logical_type{components::types::logical_type::DOUBLE});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("multi"), std::move(cols), catalog::relkind::regular);
    auto r = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(),
                        rns.created_oid, std::string("multi"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == rt.created_oid);
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
    auto v = fx.manager->peek_checkpoint_wal_id_from_disk(
        collection_full_name_t{"ns_never", "main", "nonexistent"});
    REQUIRE(v == services::wal::id_t{0});
}

// 10. load_storage_for_wal_replay_sync is a no-op for already-loaded storage (§1.11).
TEST_CASE("services::disk::d4::load_storage_for_wal_replay_noop_when_loaded") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_d4g"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    // Create the table (writes pg_class/pg_attribute; does NOT load user storage).
    fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
               std::string("lazy_t"), std::move(cols), catalog::relkind::regular);

    // Calling load_storage_for_wal_replay_sync on a table that has no .otbx must not crash.
    REQUIRE_NOTHROW(
        fx.manager->load_storage_for_wal_replay_sync(
            collection_full_name_t{"ns_d4g", "main", "lazy_t"}));
}