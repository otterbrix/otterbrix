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

// After bootstrap only pg_catalog.* is loaded; user tables stay out of storages_ until accessed.

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
            // manager must be destroyed before the scheduler: its dtor joins the loop thread,
            // which may still enqueue children onto the scheduler.
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
            return components::execution_context_t{session_id_t{}, components::table::transaction_data::committed(), {}};
        }
    };

} // namespace

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

TEST_CASE("services::disk::d4::user_table_not_in_storages_at_start") {
    fixture fx;
    // An unallocated user oid (FIRST_USER_OID would be the next allocated), so definitely not loaded.
    REQUIRE_FALSE(fx.manager->has_storage(catalog::oid_t{FIRST_USER_OID + 1000}));
}

TEST_CASE("services::disk::d4::create_table_does_not_eager_load_storage") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4a");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "users", std::move(cols));
    REQUIRE(rt_oid >= FIRST_USER_OID);
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
}

TEST_CASE("services::disk::d4::resolve_table_finds_unloaded_user_table") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4b");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "orders", std::move(cols));
    auto resolved = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("orders"));
    REQUIRE(resolved.found);
    REQUIRE(resolved.oid == rt_oid);
}

TEST_CASE("services::disk::d4::drop_unloaded_table") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4c");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("v", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "temp_t", std::move(cols));
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
    test_drop_table(fx, rt_oid);
    auto resolved = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("temp_t"));
    REQUIRE_FALSE(resolved.found);
}

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
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
    auto resolved = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("alter_me"));
    REQUIRE(resolved.found);
    REQUIRE(resolved.columns.size() == 2);
}

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
    REQUIRE(r.columns[0].attoid != r.columns[1].attoid);
    REQUIRE(r.columns[1].attoid != r.columns[2].attoid);
    REQUIRE(r.columns[0].attoid != r.columns[2].attoid);
}

TEST_CASE("services::disk::d4::peek_checkpoint_wal_id_unknown_returns_zero") {
    fixture fx;
    // "No sidecar" is the only state that still answers 0; one that exists but can't be read reports an error.
    auto v = fx.manager->peek_checkpoint_wal_id_from_disk(catalog::oid_t{FIRST_USER_OID + 9000},
                                                          well_known_oid::main_database);
    REQUIRE_FALSE(v.has_error());
    REQUIRE(v.value() == services::wal::id_t{0});
}

TEST_CASE("services::disk::d4::load_storage_for_wal_replay_noop_when_loaded") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_d4g");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "lazy_t", std::move(cols));

    REQUIRE_FALSE(fx.manager->load_storage_for_wal_replay_sync(rt_oid, well_known_oid::main_database)
                      .contains_error());
}
// A never-checkpointed .otbx loads as a legitimately empty table with its schema from the catalog;
// the metadata reader instead fails it with "attempted to read past end of chain".
TEST_CASE("services::disk::d4::never_checkpointed_otbx_loads_as_empty_with_catalog_schema") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_a76a");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt_oid = test_create_table(fx, ns_oid, "young_t", std::move(cols));

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
    REQUIRE(fx.manager->has_storage(rt_oid));
    REQUIRE(std::filesystem::file_size(otbx) == components::table::storage::BLOCK_START);
}

// A .wal_id sidecar next to a never-checkpointed .otbx means the file was rebuilt from under it;
// the load must refuse rather than silently open it as empty and discard that checkpoint.
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
    {
        std::ofstream sidecar(tbl_dir / "table.otbx.wal_id", std::ios::binary | std::ios::trunc);
        REQUIRE(sidecar.is_open());
        const uint64_t claimed = 5;
        sidecar.write(reinterpret_cast<const char*>(&claimed), sizeof(claimed));
        REQUIRE(sidecar.good());
    }

    // Must be visible, or replay treats it like a missing file and creates a fresh one over it.
    REQUIRE(fx.manager->load_storage_for_wal_replay_sync(rt_oid, well_known_oid::main_database).contains_error());
    REQUIRE_FALSE(fx.manager->has_storage(rt_oid));
    REQUIRE(std::filesystem::file_size(otbx) == components::table::storage::BLOCK_START);
    REQUIRE(std::filesystem::file_size(tbl_dir / "table.otbx.wal_id") == sizeof(uint64_t));
}
