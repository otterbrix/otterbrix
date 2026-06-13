#include <catch2/catch.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"
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

// MVCC visibility tests for DDL.
// Actual semantics (committed_version_operator in row_version_manager.cpp):
//   - use_inserted_version always returns true → INSERT is visible immediately.
//   - use_deleted_version filters out rows whose delete_id is uncommitted (>= TRANSACTION_ID_START)
//     OR whose committed delete is older than min_start_time.
//   System-table scans go through table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED —
//   that's the contract these tests lock in.

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
                c.path = mvcc_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(mvcc_dir());
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

        // Bypass txn_manager — set snapshot_horizon to UINT64_MAX so
        // committed catalog rows (commit_id=1000 etc.) are visible.
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

// 1. CREATE NAMESPACE at txn=0 immediately visible (auto-commit semantics).
TEST_CASE("services::disk::mvcc::auto_commit_create_namespace_visible") {
    fixture fx;
    disk_test_helpers::test_create_namespace(fx, std::string("ns_a"));
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("ns_a"), std::uint64_t{0});
    REQUIRE(r.found);
}

// 2. CREATE NAMESPACE under uncommitted txn is NOT visible to other sessions —
//    standard PostgreSQL MVCC: insert_id >= TRANSACTION_ID_START is hidden until commit.
TEST_CASE("services::disk::mvcc::uncommitted_insert_invisible_to_other_sessions") {
    fixture fx;
    auto uncommitted = TRANSACTION_ID_START + 1;
    // Append the pg_namespace row under the uncommitted txn but do NOT commit.
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t ns_oid = oids[0];
        auto writes =
            components::catalog::build_create_namespace_writes(&fx.resource, std::string("ns_uncommitted"), ns_oid);
        for (auto& w : writes)
            fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(uncommitted), w.table_oid, std::move(w.row));
        // Intentionally no MVCC swap (no storage_publish_commits call).
    }
    // auto_ctx() uses transaction_id=0, so it must NOT see the uncommitted row.
    auto r =
        fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("ns_uncommitted"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 3. DROP TABLE at txn=0 (auto-commit) immediately hides the row.
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

// 4. An uncommitted DELETE is INVISIBLE to other readers — the row stays visible
//    until the deleting txn commits (PostgreSQL MVCC). committed_version_operator's
//    use_deleted_version returns true (= "row visible") when delete_id >= TRANSACTION_ID_START
//    so other-txn readers do not see another in-flight tombstone.
TEST_CASE("services::disk::mvcc::uncommitted_delete_invisible_to_other_readers") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid =
        disk_test_helpers::test_create_table(fx, ns_oid, std::string("doomed"), cols, catalog::relkind::regular);
    auto uncommitted = TRANSACTION_ID_START + 13;
    // Issue uncommitted deletes (tombstone tagged with uncommitted txn_id — no commit).
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
        // Intentionally no MVCC swap (no storage_publish_commits call).
    }
    auto rr = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("doomed"));
    REQUIRE(rr.found);
}

// 5. resolve_namespace mirrors uncommitted-deletion visibility — an other-txn
//    reader still sees a row whose DROP is uncommitted; once the deleting txn commits,
//    the next resolve observes the tombstone-applied state.
TEST_CASE("services::disk::mvcc::resolve_includes_uncommitted_deletes") {
    fixture fx;
    disk_test_helpers::test_create_namespace(fx, std::string("kept_ns"));
    const auto drop_ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("dropped_ns"));
    auto uncommitted = TRANSACTION_ID_START + 21;
    // Issue uncommitted namespace deletes (no commit — tombstones stay uncommitted).
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
        // Intentionally no MVCC swap (no storage_publish_commits call).
    }

    auto kept = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("kept_ns"), std::uint64_t{0});
    REQUIRE(kept.found);
    auto dropped =
        fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(), std::string("dropped_ns"), std::uint64_t{0});
    REQUIRE(dropped.found);
}

// version_monotonic test deleted: catalog_version_ infrastructure removed.

// 7. Uncommitted DROP INDEX is invisible to other readers (same delete-tombstone path as drop table).
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
    // Issue uncommitted index deletes (no commit — tombstones stay uncommitted).
    {
        constexpr catalog::oid_t pg_idx = catalog::well_known_oid::pg_index_table;
        constexpr catalog::oid_t pg_cls = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_idx, std::int64_t{0}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_cls, std::int64_t{0}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, index_oid);
        // Intentionally no MVCC swap (no storage_publish_commits call).
    }
    auto rr = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("idx_doomed"));
    REQUIRE(rr.found);
}

// 8. Uncommitted DROP TYPE is invisible to other readers — type stays visible until commit.
TEST_CASE("services::disk::mvcc::uncommitted_drop_type_invisible") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    const auto type_oid = disk_test_helpers::test_create_type(fx, ns_oid, std::string("widget"), std::string{});
    auto uncommitted = TRANSACTION_ID_START + 88;
    // Issue uncommitted type deletes (no commit — tombstones stay uncommitted).
    {
        constexpr catalog::oid_t pg_type = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_type, std::int64_t{0}, type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, type_oid);
        // Intentionally no MVCC swap (no storage_publish_commits call).
    }
    auto rr = test_probe::probe_type(fx, fx.auto_ctx(), ns_oid, std::string("widget"));
    REQUIRE(rr.found);
}

// 10. test_ddl_rollback_cleans_up (spec §14 line 2766): DDL inside an explicit transaction
//     that is ROLLED BACK must leave zero catalog rows behind.
//     Models BEGIN; CREATE TABLE t; ROLLBACK at the disk-actor level.
TEST_CASE("services::disk::mvcc::test_ddl_rollback_cleans_up") {
    fixture fx;
    const uint64_t txn = TRANSACTION_ID_START + 500;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("rollback_ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    // Create under an explicit (uncommitted) transaction by using append_pg_catalog_row
    // with txn_id >= TRANSACTION_ID_START but NOT calling storage_publish_commits.
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
                                                                     false,
                                                                     ns_oid,
                                                                     batch,
                                                                     catalog::relkind::regular);
        for (auto& w : writes) {
            auto rng =
                fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn), w.table_oid, std::move(w.row));
            appends_for_test.push_back(std::move(rng));
        }
        // Do NOT call storage_publish_commits — rows are pending under txn.
    }
    REQUIRE(table_oid >= FIRST_USER_OID);
    // Before rollback: invisible to other sessions (insert_id >= TRANSACTION_ID_START).
    auto before_other = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("ephemeral"));
    REQUIRE_FALSE(before_other.found);
    // Revert via batched API. The test captured append ranges above
    // (from append_pg_catalog_row return values).
    fx.invoke(&manager_disk_t::storage_revert_appends, fx.txn_ctx(txn), std::move(appends_for_test));
    // After rollback: still not found — no orphan rows.
    auto after = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("ephemeral"));
    REQUIRE_FALSE(after.found);
    // Same txn also cannot find the rolled-back table.
    auto after_same = test_probe::probe_table(fx, fx.txn_ctx(txn), ns_oid, std::string("ephemeral"));
    REQUIRE_FALSE(after_same.found);
}

// 11. Drop cascade preserves uncommitted-deletion semantics — dropping a parent under an
//     uncommitted txn leaves both parent and cascaded children visible to other readers
//     (their tombstones are also uncommitted).
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
    // Issue uncommitted namespace deletes (no commit — tombstones stay uncommitted).
    {
        constexpr catalog::oid_t pg_ns = catalog::well_known_oid::pg_namespace_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_ns, std::int64_t{0}, ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, ns_oid);
        // Intentionally no MVCC swap (no storage_publish_commits call).
    }
    auto rt_after = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("t"));
    auto idx_after = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("child_idx"));
    REQUIRE(rt_after.found);
    REQUIRE(idx_after.found);
}

// pg_computed_column registers obey MVCC visibility. resolve_table for
// relkind='g' tables scans pg_computed_column via the same inline_scan path as
// pg_attribute, so an uncommitted register is invisible to other transactions
// until storage_publish_commits flips insert_id from txn_id (>= TRANSACTION_ID_START)
// to commit_id (< TRANSACTION_ID_START).
TEST_CASE("services::disk::mvcc::dynamic_schema_register_invisible_until_commit") {
    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("dyn_ns"));
    auto table_oid = disk_test_helpers::test_create_computing_table(fx, ns_oid, std::string("docs"));

    // txn1 appends a pg_computed_column row but does NOT call storage_publish_commits.
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
        auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn1), pg_cc, std::move(row));
        pending_ranges.push_back(std::move(rng));
        // Intentionally NO storage_publish_commits — row stays uncommitted.
    }

    // Other-session read (txn=0) must NOT see the uncommitted column.
    auto resolved_other = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("docs"));
    REQUIRE(resolved_other.found);
    REQUIRE(resolved_other.relkind == components::catalog::relkind::computed);
    REQUIRE(resolved_other.columns.size() == 0);

    // txn1 commits — flip MVCC tag (insert_id := commit_id < TRANSACTION_ID_START).
    fx.invoke(&manager_disk_t::storage_publish_commits,
              fx.txn_ctx(txn1),
              std::uint64_t{1234},
              std::move(pending_ranges));

    // Fresh reader (txn=0) now sees the committed column.
    auto resolved_after = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("docs"));
    REQUIRE(resolved_after.found);
    REQUIRE(resolved_after.columns.size() == 1);
    REQUIRE(resolved_after.columns[0].attname == "a");
    REQUIRE(resolved_after.columns[0].atttypid == components::catalog::well_known_oid::int64_type);
}

// 13. Rollback (storage_revert_appends) of an uncommitted pg_computed_column register
//     leaves zero rows behind. Mirrors test #10 (test_ddl_rollback_cleans_up) for the
//     dynamic-schema register path.
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
        auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn1), pg_cc, std::move(row));
        pending_ranges.push_back(std::move(rng));
    }

    // Before rollback: invisible to other readers (uncommitted).
    auto before = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("docs"));
    REQUIRE(before.found);
    REQUIRE(before.columns.size() == 0);

    // Rollback via storage_revert_appends — the appended range is dropped.
    fx.invoke(&manager_disk_t::storage_revert_appends, fx.txn_ctx(txn1), std::move(pending_ranges));

    // After rollback: still no column visible from any reader.
    auto after_other = test_probe::probe_table(fx, fx.auto_ctx(), ns_oid, std::string("docs"));
    REQUIRE(after_other.found);
    REQUIRE(after_other.columns.size() == 0);
    auto after_same = test_probe::probe_table(fx, fx.txn_ctx(txn1), ns_oid, std::string("docs"));
    REQUIRE(after_same.found);
    REQUIRE(after_same.columns.size() == 0);
}

// 14. Own-transaction visibility of a pre-commit pg_computed_column register.
//     NOTE: resolve_table goes through inline_scan, which uses
//     scan_committed(COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED). row_group_t::
//     committed_indexing_vector calls vinfo->committed_indexing_vector with
//     (current_version_, current_version_) — i.e. the row_group's local counter,
//     NOT the caller's transaction_id. So committed_version_operator::
//     use_inserted_version (id < TRANSACTION_ID_START || id == transaction_id) returns
//     true ONLY for committed rows: own uncommitted writes are NOT visible to
//     resolve_table from the same txn either. This locks in the actual contract
//     (resolve_table returns committed snapshot regardless of caller txn id) and
//     documents the deviation from PostgreSQL's "own writes visible mid-txn".
//     If pg_computed_column ever needs read-your-writes for in-flight DDL, the
//     fix lives in row_group_t::committed_indexing_vector (or a separate
//     txn-aware inline_scan variant), not in resolve_table or operator_register.
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
        // Append under txn1 — no commit yet.
        fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn1), pg_cc, std::move(row));
    }

    // Resolve from the SAME txn using PRODUCTION semantics: operator_resolve_table issues
    // read_chunks_by_key with the caller's real ctx->txn (committed_scan=false), so the txn
    // reads its own in-flight catalog write back (read-your-own-writes). This matches the
    // production resolve path; the deleted disk resolve_table scanned committed-only and
    // would have hidden it.
    auto resolved_self =
        test_probe::probe_table(fx, fx.txn_ctx(txn1), ns_oid, std::string("docs"), /*committed_scan=*/false);
    REQUIRE(resolved_self.found);
    REQUIRE(resolved_self.relkind == components::catalog::relkind::computed);
    REQUIRE(resolved_self.columns.size() == 1);
}

// Concurrent INSERTs into a relkind='g' table can register the
// same dynamic field twice. Two sessions A and B each call
// operator_computed_field_register_t::await_async_and_resume; each reads
// pg_computed_column under its own txn_id (uncommitted writes from the other
// session are hidden), classifies the field as new, allocates a
// distinct attoid and appends a row. After both commit, pg_computed_column
// has two rows for (relid, attname).
//
// Resolver tolerance: pick max(attversion), break ties by lowest attoid;
// VACUUM GCs stale versions later. Strict serialization (per-table
// lock or composite actor handler) is deferred — a multi-session concurrent
// fixture is needed to demonstrate the race causes user-visible problems
// before paying the parallelism cost.
TEST_CASE("services::disk::mvcc::dynamic_field_register_concurrent_duplicate_TODO") {
    // Stub: this test requires a multi-session concurrent fixture (two
    // simultaneous append flows on a shared disk actor with overlapping
    // mailbox progression). The current scheduler_test_t harness drains
    // each invoke() to completion before returning, which incidentally
    // serializes register operations and hides the race.
    WARN("TODO: requires multi-session concurrent test fixture to "
         "exercise pg_computed_column duplicate-registration race");
}

// Concurrent ALTER DROP COLUMN + INSERT on a relkind='g' table.
// txn1 runs operator_computed_field_unregister_t (appends tombstone row with
// refcount=0), txn2 concurrently runs operator_computed_field_register_t
// (sees field as live under its pre-tombstone snapshot, no-ops). This is
// handled by MVCC isolation + resolver max(attversion) filtering; strict
// serialization is deferred until a concurrent fixture proves user-visible
// regressions.
TEST_CASE("services::disk::mvcc::dynamic_field_drop_insert_concurrent_TODO") {
    WARN("TODO: requires multi-session concurrent test fixture; the race is "
         "handled by MVCC isolation + resolver max-version filtering — "
         "stale-version GC by VACUUM / physical-compaction later");
}

// Concurrent VACUUM (aggressive-GC) + INSERT on a relkind='g' table.
// VACUUM scans pg_computed_column for dead/stale rows; concurrent INSERT
// registers the same attname. MVCC isolation prevents the race: VACUUM's
// ctx->txn snapshot uses lowest_active_start_time as horizon, so uncommitted
// INSERT writes are invisible. Both read_rows_by_key and delete_pg_catalog_rows
// funnel through ctx.txn (see manager_disk_resolve.cpp /
// manager_disk_ddl.cpp).
TEST_CASE("services::disk::mvcc::vacuum_insert_concurrent_TODO") {
    WARN("TODO: requires multi-session concurrent test fixture; race is "
         "handled by VACUUM's lowest_active_start_time horizon — uncommitted "
         "INSERTs invisible to VACUUM, fully-committed rows are stable.");
}
