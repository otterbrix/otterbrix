#include <catch2/catch_test_macros.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"
// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <limits>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <thread>
#include <unistd.h>

// committed_version_operator (row_version_manager.cpp): INSERT is always visible, DELETE stays
// visible while delete_id is uncommitted or newer than min_start_time. System-table scans use
// table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED.

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;
using components::table::transaction_data;
using components::table::TRANSACTION_ID_START;

namespace {
    std::string mvcc_dir() {
        static std::string p = "/tmp/test_otterbrix_mvcc_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(mvcc_dir()); }

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
                c.path = mvcc_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(mvcc_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            // Destroy the manager before the scheduler: its dtor joins the loop thread, which may
            // still enqueue children onto the scheduler.
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

        // Bypass txn_manager: snapshot_horizon=UINT64_MAX makes all committed catalog rows visible.
        components::execution_context_t auto_ctx() {
            transaction_data td(0, 0);
            td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
            return components::execution_context_t{session_id_t{}, td, {}};
        }

        components::execution_context_t txn_ctx(uint64_t txn_id, uint64_t start_time = 1) {
            transaction_data td(txn_id, start_time);
            td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
            return components::execution_context_t{session_id_t{}, td, {}};
        }
    };
} // namespace

TEST_CASE("services::disk::mvcc::auto_commit_create_namespace_visible") {
    fixture fx;
    disk_test_helpers::test_create_namespace(fx, std::string("ns_a"));
    auto rr = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("ns_a"));
    REQUIRE_FALSE(rr.has_error());
    auto& r = rr.value();
    REQUIRE(r.found);
}

TEST_CASE("services::disk::mvcc::uncommitted_insert_invisible_to_other_sessions") {
    fixture fx;
    auto uncommitted = TRANSACTION_ID_START + 1;
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t ns_oid = oids[0];
        auto writes =
            components::catalog::build_create_namespace_writes(&fx.resource, std::string("ns_uncommitted"), ns_oid);
        for (auto& w : writes)
            disk_test_helpers::append_ok(fx.invoke(&manager_disk_t::append_pg_catalog_row,
                                                   fx.txn_ctx(uncommitted),
                                                   w.table_oid,
                                                   std::move(w.row)));
    }
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("ns_uncommitted"));
    REQUIRE_FALSE(r.has_error());
    REQUIRE_FALSE(r.value().found);
}

TEST_CASE("services::disk::mvcc::auto_commit_drop_invisible") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid =
        disk_test_helpers::test_create_table(fx, ns_oid, std::string("t"), cols, catalog::relkind::regular);
    disk_test_helpers::test_drop_table(fx, table_oid);
    auto rr = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("t"));
    REQUIRE_FALSE(rr.found);
}

TEST_CASE("services::disk::mvcc::uncommitted_delete_invisible_to_other_readers") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid =
        disk_test_helpers::test_create_table(fx, ns_oid, std::string("doomed"), cols, catalog::relkind::regular);
    auto uncommitted = TRANSACTION_ID_START + 13;
    {
        constexpr catalog::oid_t pg_class = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_attr = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
                  fx.txn_ctx(uncommitted),
                  pg_class,
                  std::int64_t{0},
                  table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
                  fx.txn_ctx(uncommitted),
                  pg_attr,
                  std::int64_t{1},
                  table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, table_oid);
    }
    auto rr = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("doomed"));
    REQUIRE(rr.found);
}

TEST_CASE("services::disk::mvcc::resolve_includes_uncommitted_deletes") {
    fixture fx;
    disk_test_helpers::test_create_namespace(fx, std::string("kept_ns"));
    const auto drop_ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("dropped_ns"));
    auto uncommitted = TRANSACTION_ID_START + 21;
    {
        constexpr catalog::oid_t pg_ns = catalog::well_known_oid::pg_namespace_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
                  fx.txn_ctx(uncommitted),
                  pg_ns,
                  std::int64_t{0},
                  drop_ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
                  fx.txn_ctx(uncommitted),
                  pg_dep,
                  std::int64_t{1},
                  drop_ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
                  fx.txn_ctx(uncommitted),
                  pg_dep,
                  std::int64_t{3},
                  drop_ns_oid);
    }

    auto kept = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("kept_ns"));
    REQUIRE_FALSE(kept.has_error());
    REQUIRE(kept.value().found);
    auto dropped =
        fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("dropped_ns"));
    REQUIRE_FALSE(dropped.has_error());
    REQUIRE(dropped.value().found);
}

TEST_CASE("services::disk::mvcc::uncommitted_drop_index_invisible") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid =
        disk_test_helpers::test_create_table(fx, ns_oid, std::string("t"), cols, catalog::relkind::regular);
    const auto index_oid = disk_test_helpers::test_create_index(fx,
                                                                ns_oid,
                                                                table_oid,
                                                                std::string("idx_doomed"),
                                                                std::vector<std::string>{"id"},
                                                                std::vector<components::catalog::oid_t>{});
    auto uncommitted = TRANSACTION_ID_START + 77;
    {
        constexpr catalog::oid_t pg_idx = catalog::well_known_oid::pg_index_table;
        constexpr catalog::oid_t pg_cls = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_idx, std::int64_t{0}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_cls, std::int64_t{0}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, index_oid);
    }
    auto rr = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("idx_doomed"));
    REQUIRE(rr.found);
}

TEST_CASE("services::disk::mvcc::uncommitted_drop_type_invisible") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    const auto type_oid = disk_test_helpers::test_create_type(fx, ns_oid, std::string("widget"), std::string{});
    auto uncommitted = TRANSACTION_ID_START + 88;
    {
        constexpr catalog::oid_t pg_type = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_type, std::int64_t{0}, type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, type_oid);
    }
    auto rr = test_probe::probe_type(fx, fx.auto_ctx(), ns_oid, std::string("widget"));
    REQUIRE(rr.found);
}

// Spec §14 line 2766: DDL in a ROLLED-BACK transaction must leave zero catalog rows.
TEST_CASE("services::disk::mvcc::test_ddl_rollback_cleans_up") {
    fixture fx;
    const uint64_t txn = TRANSACTION_ID_START + 500;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("rollback_ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    components::catalog::oid_t table_oid = components::catalog::INVALID_OID;
    std::vector<components::pg_catalog_append_range_t> appends_for_test;
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1 + cols.size()});
        table_oid = oids[0];
        components::catalog::oid_batch_t batch;
        batch.oids = std::move(oids);
        auto writes = components::catalog::build_create_table_writes(&fx.resource,
                                                                     std::string("public"),
                                                                     std::string("ephemeral"),
                                                                     cols,
                                                                     ns_oid,
                                                                     batch,
                                                                     catalog::relkind::regular);
        for (auto& w : writes) {
            auto rng = disk_test_helpers::append_ok(
                fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn), w.table_oid, std::move(w.row)));
            appends_for_test.push_back(std::move(rng));
        }
    }
    REQUIRE(table_oid >= FIRST_USER_OID);
    auto before_other = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("ephemeral"));
    REQUIRE_FALSE(before_other.found);
    fx.invoke(&manager_disk_t::storage_revert_appends, fx.txn_ctx(txn), std::move(appends_for_test), false);
    auto after = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("ephemeral"));
    REQUIRE_FALSE(after.found);
    auto after_same = test_probe::probe_table(fx, fx.txn_ctx(txn), ns_oid, std::string("ephemeral"));
    REQUIRE_FALSE(after_same.found);
}

// Cascaded children's tombstones stay uncommitted too, so they remain visible with the parent.
TEST_CASE("services::disk::mvcc::drop_cascade_uncommitted_invisible_to_other_readers") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid =
        disk_test_helpers::test_create_table(fx, ns_oid, std::string("t"), cols, catalog::relkind::regular);
    disk_test_helpers::test_create_index(fx,
                                         ns_oid,
                                         table_oid,
                                         std::string("child_idx"),
                                         std::vector<std::string>{"id"},
                                         std::vector<components::catalog::oid_t>{});
    auto uncommitted = TRANSACTION_ID_START + 111;
    {
        constexpr catalog::oid_t pg_ns = catalog::well_known_oid::pg_namespace_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_ns, std::int64_t{0}, ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, ns_oid);
    }
    auto rt_after = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("t"));
    auto idx_after = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("child_idx"));
    REQUIRE(rt_after.found);
    REQUIRE(idx_after.found);
}

// resolve_table scans pg_computed_column via the same inline_scan path as pg_attribute;
// storage_publish_commits flips insert_id from txn_id (>= TRANSACTION_ID_START) to commit_id
// (< TRANSACTION_ID_START), which is what makes a register visible.
TEST_CASE("services::disk::mvcc::dynamic_schema_register_invisible_until_commit") {
    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("dyn_ns"));
    auto table_oid = disk_test_helpers::test_create_computing_table(fx, ns_oid, std::string("docs"));

    const uint64_t txn1 = TRANSACTION_ID_START + 901;
    constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
    std::vector<components::pg_catalog_append_range_t> pending_ranges;
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t attoid = oids[0];
        auto row = components::catalog::build_pg_computed_column_row(&fx.resource,
                                                                     table_oid,
                                                                     attoid,
                                                                     std::string("a"),
                                                                     components::catalog::well_known_oid::int64_type,
                                                                     std::int64_t{0},
                                                                     std::int64_t{1});
        auto rng = disk_test_helpers::append_ok(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn1), pg_cc, std::move(row)));
        pending_ranges.push_back(std::move(rng));
    }

    auto resolved_other = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("docs"));
    REQUIRE(resolved_other.found);
    REQUIRE(resolved_other.relkind == components::catalog::relkind::computed);
    REQUIRE(resolved_other.columns.size() == 0);

    fx.invoke(&manager_disk_t::storage_publish_commits,
              fx.txn_ctx(txn1),
              std::uint64_t{1234},
              std::move(pending_ranges));

    auto resolved_after = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("docs"));
    REQUIRE(resolved_after.found);
    REQUIRE(resolved_after.columns.size() == 1);
    REQUIRE(resolved_after.columns[0].attname == "a");
    REQUIRE(resolved_after.columns[0].atttypid == components::catalog::well_known_oid::int64_type);
}

TEST_CASE("services::disk::mvcc::dynamic_schema_register_rollback_undoes") {
    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("dyn_ns"));
    auto table_oid = disk_test_helpers::test_create_computing_table(fx, ns_oid, std::string("docs"));

    const uint64_t txn1 = TRANSACTION_ID_START + 902;
    constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
    std::vector<components::pg_catalog_append_range_t> pending_ranges;
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t attoid = oids[0];
        auto row = components::catalog::build_pg_computed_column_row(&fx.resource,
                                                                     table_oid,
                                                                     attoid,
                                                                     std::string("a"),
                                                                     components::catalog::well_known_oid::int64_type,
                                                                     std::int64_t{0},
                                                                     std::int64_t{1});
        auto rng = disk_test_helpers::append_ok(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn1), pg_cc, std::move(row)));
        pending_ranges.push_back(std::move(rng));
    }

    auto before = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("docs"));
    REQUIRE(before.found);
    REQUIRE(before.columns.size() == 0);

    fx.invoke(&manager_disk_t::storage_revert_appends, fx.txn_ctx(txn1), std::move(pending_ranges), false);

    auto after_other = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("docs"));
    REQUIRE(after_other.found);
    REQUIRE(after_other.columns.size() == 0);
    auto after_same = test_probe::probe_table(fx, fx.txn_ctx(txn1), ns_oid, std::string("docs"));
    REQUIRE(after_same.found);
    REQUIRE(after_same.columns.size() == 0);
}

// resolve_table's inline_scan checks the row_group's local committed counter, not the caller's
// txn, so a txn's own uncommitted writes are invisible to it too — unlike PostgreSQL's
// read-your-own-writes. A future fix belongs in row_group_t::committed_indexing_vector, not here.
TEST_CASE("services::disk::mvcc::dynamic_schema_register_visible_in_same_txn") {
    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("dyn_ns"));
    auto table_oid = disk_test_helpers::test_create_computing_table(fx, ns_oid, std::string("docs"));

    const uint64_t txn1 = TRANSACTION_ID_START + 903;
    constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t attoid = oids[0];
        auto row = components::catalog::build_pg_computed_column_row(&fx.resource,
                                                                     table_oid,
                                                                     attoid,
                                                                     std::string("a"),
                                                                     components::catalog::well_known_oid::int64_type,
                                                                     std::int64_t{0},
                                                                     std::int64_t{1});
        disk_test_helpers::append_ok(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn1), pg_cc, std::move(row)));
    }

    // operator_resolve_table reads with committed_scan=false, so it sees its own in-flight write.
    auto resolved_self =
        test_probe::probe_table(fx, fx.txn_ctx(txn1), ns_oid, std::string("docs"), /*committed_scan=*/false);
    REQUIRE(resolved_self.found);
    REQUIRE(resolved_self.relkind == components::catalog::relkind::computed);
    REQUIRE(resolved_self.columns.size() == 1);
}

// Resolver tolerates the resulting duplicate: picks max(attversion), ties by lowest attoid.
TEST_CASE("services::disk::mvcc::dynamic_field_register_concurrent_duplicate_TODO") {
    WARN("TODO: requires multi-session concurrent test fixture to "
         "exercise pg_computed_column duplicate-registration race");
}

// txn2's register still sees the field live under its pre-tombstone snapshot and no-ops.
TEST_CASE("services::disk::mvcc::dynamic_field_drop_insert_concurrent_TODO") {
    WARN("TODO: requires multi-session concurrent test fixture; the race is "
         "handled by MVCC isolation + resolver max-version filtering — "
         "stale-version GC by VACUUM / physical-compaction later");
}

// VACUUM's ctx->txn snapshot uses lowest_active_start_time, so a concurrent INSERT is invisible.
TEST_CASE("services::disk::mvcc::vacuum_insert_concurrent_TODO") {
    WARN("TODO: requires multi-session concurrent test fixture; race is "
         "handled by VACUUM's lowest_active_start_time horizon — uncommitted "
         "INSERTs invisible to VACUUM, fully-committed rows are stable.");
}

// Result is positional (deleted[i] answers specs[i]); a zero must be an honest count, not an error.
TEST_CASE("services::disk::mvcc::delete_many_counts_each_spec_in_order") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("counted_ns"));
    constexpr catalog::oid_t pg_ns = catalog::well_known_oid::pg_namespace_table;

    // Spec 2 duplicates spec 0, already deleted by it — proves the result is positional.
    const catalog::oid_t absent_oid = ns_oid + 100000;
    std::pmr::vector<pg_catalog_delete_spec_t> specs(&fx.resource);
    specs.push_back({pg_ns, std::int64_t{0}, ns_oid});
    specs.push_back({pg_ns, std::int64_t{0}, absent_oid});
    specs.push_back({pg_ns, std::int64_t{0}, ns_oid});

    auto deleted = fx.invoke(&manager_disk_t::delete_pg_catalog_rows_many, fx.auto_ctx(), std::move(specs));
    REQUIRE_FALSE(deleted.has_error());
    REQUIRE(deleted.value().size() == 3);
    CHECK(deleted.value()[0] == 1);
    CHECK(deleted.value()[1] == 0);
    CHECK(deleted.value()[2] == 0);
}

// Scans under ctx.txn so it sees a same-txn write (insert_id==transaction_id) — otherwise
// ALTER/DROP would misread a 0 count as row-still-present and refuse legal same-txn statements.
TEST_CASE("services::disk::mvcc::delete_many_sees_its_own_uncommitted_row") {
    fixture fx;
    const uint64_t txn1 = TRANSACTION_ID_START + 555;
    constexpr catalog::oid_t pg_ns = catalog::well_known_oid::pg_namespace_table;

    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    const components::catalog::oid_t ns_oid = oids[0];
    auto writes = components::catalog::build_create_namespace_writes(&fx.resource, std::string("own_ns"), ns_oid);
    for (auto& w : writes) {
        disk_test_helpers::append_ok(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn1), w.table_oid, std::move(w.row)));
    }

    {
        std::pmr::vector<pg_catalog_delete_spec_t> other(&fx.resource);
        other.push_back({pg_ns, std::int64_t{0}, ns_oid});
        auto by_other = fx.invoke(&manager_disk_t::delete_pg_catalog_rows_many,
                                  fx.txn_ctx(TRANSACTION_ID_START + 556),
                                  std::move(other));
        REQUIRE_FALSE(by_other.has_error());
        REQUIRE(by_other.value().size() == 1);
        CHECK(by_other.value()[0] == 0);
    }

    std::pmr::vector<pg_catalog_delete_spec_t> specs(&fx.resource);
    specs.push_back({pg_ns, std::int64_t{0}, ns_oid});
    auto deleted = fx.invoke(&manager_disk_t::delete_pg_catalog_rows_many, fx.txn_ctx(txn1), std::move(specs));
    REQUIRE_FALSE(deleted.has_error());
    REQUIRE(deleted.value().size() == 1);
    CHECK(deleted.value()[0] == 1);
}

// Scans on ctx.txn, not the default snapshot — otherwise a namespace created inside an open
// transaction would be invisible to its own resolve, and found==false checks would lie.
TEST_CASE("services::disk::mvcc::resolve_namespace_sees_its_own_uncommitted_row") {
    fixture fx;
    auto uncommitted = TRANSACTION_ID_START + 1;
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t ns_oid = oids[0];
        auto writes =
            components::catalog::build_create_namespace_writes(&fx.resource, std::string("ns_own_txn"), ns_oid);
        for (auto& w : writes)
            disk_test_helpers::append_ok(fx.invoke(&manager_disk_t::append_pg_catalog_row,
                                                   fx.txn_ctx(uncommitted),
                                                   w.table_oid,
                                                   std::move(w.row)));
    }
    INFO("the creating transaction must see its own pg_namespace row");
    auto own = fx.invoke(&manager_disk_t::resolve_namespace,
                         fx.txn_ctx(uncommitted),
                         std::string("ns_own_txn"));
    REQUIRE_FALSE(own.has_error());
    REQUIRE(own.value().found);

    INFO("other sessions still do not (case 2's half must keep holding)");
    auto other =
        fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("ns_own_txn"));
    REQUIRE_FALSE(other.has_error());
    REQUIRE_FALSE(other.value().found);
}

TEST_CASE("services::disk::mvcc::list_namespaces_sees_its_own_uncommitted_row") {
    fixture fx;
    auto uncommitted = TRANSACTION_ID_START + 1;
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t ns_oid = oids[0];
        auto writes =
            components::catalog::build_create_namespace_writes(&fx.resource, std::string("ns_own_list"), ns_oid);
        for (auto& w : writes)
            disk_test_helpers::append_ok(fx.invoke(&manager_disk_t::append_pg_catalog_row,
                                                   fx.txn_ctx(uncommitted),
                                                   w.table_oid,
                                                   std::move(w.row)));
    }
    auto contains = [](const std::pmr::vector<std::string>& names, const char* wanted) {
        for (const auto& n : names) {
            if (n == wanted)
                return true;
        }
        return false;
    };

    INFO("the creating transaction must see its own namespace in the enumeration");
    auto own = fx.invoke(&manager_disk_t::list_namespaces, fx.txn_ctx(uncommitted));
    REQUIRE_FALSE(own.has_error());
    REQUIRE(contains(own.value(), "ns_own_list"));

    INFO("other sessions still do not");
    auto other = fx.invoke(&manager_disk_t::list_namespaces, fx.auto_ctx());
    REQUIRE_FALSE(other.has_error());
    REQUIRE_FALSE(contains(other.value(), "ns_own_list"));
}
