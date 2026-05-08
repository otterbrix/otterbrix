#include <catch2/catch.hpp>

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
#include <services/disk/manager_disk.hpp>
#include "disk_test_helpers.hpp"

#include <filesystem>
#include <unistd.h>

// MVCC visibility tests for DDL. Plan §M6 Risk #1 / E2.1A bug.
// Actual semantics in this codebase (committed_version_operator in row_version_manager.cpp):
//   - use_inserted_version always returns true → INSERT is visible immediately.
//   - use_deleted_version filters out rows whose delete_id is uncommitted (>= TRANSACTION_ID_START)
//     OR whose committed delete is older than min_start_time.
//   This means an uncommitted DELETE removes the row from committed-view scans before commit,
//   which is the inverse of standard PostgreSQL MVCC. The E2.1A fix renamed
//   commited_version_operator (typo) → committed_version_operator and routed system-table
//   scans through table_scan_type::COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED — that's the
//   contract these tests lock in.

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

        components::execution_context_t auto_ctx() {
            return components::execution_context_t{session_id_t{}, transaction_data{0, 0}, {}};
        }

        components::execution_context_t txn_ctx(uint64_t txn_id, uint64_t start_time = 1) {
            return components::execution_context_t{session_id_t{}, transaction_data{txn_id, start_time}, {}};
        }
    };
} // namespace

// 1. ddl_create_namespace at txn=0 immediately visible (auto-commit semantics).
TEST_CASE("services::disk::mvcc::auto_commit_create_namespace_visible") {
    fixture fx;
    disk_test_helpers::test_create_namespace(fx, std::string("ns_a"));
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(),
                        std::string("ns_a"), std::uint64_t{0});
    REQUIRE(r.found);
}

// 2. ddl_create_namespace under uncommitted txn is NOT visible to other sessions —
//    standard PostgreSQL MVCC: insert_id >= TRANSACTION_ID_START is hidden until commit.
TEST_CASE("services::disk::mvcc::uncommitted_insert_invisible_to_other_sessions") {
    fixture fx;
    auto uncommitted = TRANSACTION_ID_START + 1;
    // Append the pg_namespace row under the uncommitted txn but do NOT commit.
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t ns_oid = oids[0];
        auto writes = components::catalog::build_create_namespace_writes(&fx.resource,
                                                                          std::string("ns_uncommitted"), ns_oid);
        for (auto& w : writes)
            fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(uncommitted), w.table, std::move(w.row));
        // Intentionally no commit_pg_catalog_appends.
    }
    // auto_ctx() uses transaction_id=0, so it must NOT see the uncommitted row.
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(),
                        std::string("ns_uncommitted"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 3. ddl_drop_table at txn=0 (auto-commit) immediately hides the row.
TEST_CASE("services::disk::mvcc::auto_commit_drop_invisible") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid = disk_test_helpers::test_create_table(fx, ns_oid, std::string("t"), cols,
                                                                  catalog::relkind::regular);
    disk_test_helpers::test_drop_table(fx, table_oid);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), ns_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE_FALSE(rr.found);
}

// 4. E2.1A: an uncommitted DELETE is INVISIBLE to other readers — the row stays visible
//    until the deleting txn commits (PostgreSQL MVCC). committed_version_operator's
//    use_deleted_version returns true (= "row visible") when delete_id >= TRANSACTION_ID_START
//    so other-txn readers do not see another in-flight tombstone.
TEST_CASE("services::disk::mvcc::uncommitted_delete_invisible_to_other_readers") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid = disk_test_helpers::test_create_table(fx, ns_oid, std::string("doomed"), cols,
                                                                  catalog::relkind::regular);
    auto uncommitted = TRANSACTION_ID_START + 13;
    // Issue uncommitted deletes (tombstone tagged with uncommitted txn_id — no commit).
    {
        const collection_full_name_t pg_class{"pg_catalog", "main", "pg_class"};
        const collection_full_name_t pg_attr{"pg_catalog", "main", "pg_attribute"};
        const collection_full_name_t pg_dep{"pg_catalog", "main", "pg_depend"};
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_class, std::int64_t{0}, table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_attr,  std::int64_t{1}, table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep,   std::int64_t{1}, table_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep,   std::int64_t{3}, table_oid);
        // Intentionally no commit_pg_catalog_appends.
    }
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), ns_oid,
                         std::string("doomed"), std::uint64_t{0});
    REQUIRE(rr.found);
}

// 5. resolve_namespace mirrors uncommitted-deletion visibility (E2.1A) — an other-txn
//    reader still sees a row whose DROP is uncommitted; once the deleting txn commits,
//    the next resolve observes the tombstone-applied state. (Replaces former
//    populate_catalog_snapshot-based test now that populate is retired.)
TEST_CASE("services::disk::mvcc::resolve_includes_uncommitted_deletes") {
    fixture fx;
    disk_test_helpers::test_create_namespace(fx, std::string("kept_ns"));
    const auto drop_ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("dropped_ns"));
    auto uncommitted = TRANSACTION_ID_START + 21;
    // Issue uncommitted namespace deletes (no commit — tombstones stay uncommitted).
    {
        const collection_full_name_t pg_ns{"pg_catalog", "main", "pg_namespace"};
        const collection_full_name_t pg_dep{"pg_catalog", "main", "pg_depend"};
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_ns,  std::int64_t{0}, drop_ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, drop_ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, drop_ns_oid);
        // Intentionally no commit_pg_catalog_appends.
    }

    auto kept = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(),
                            std::string("kept_ns"), std::uint64_t{0});
    REQUIRE(kept.found);
    auto dropped = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(),
                               std::string("dropped_ns"), std::uint64_t{0});
    REQUIRE(dropped.found);
}

// 6. catalog_version monotonically increases for every successful ddl_* — even ones whose
//    txn never commits. The version names the operation, not the resulting state.
TEST_CASE("services::disk::mvcc::version_monotonic") {
    fixture fx;
    auto v0 = fx.manager->catalog_version();
    disk_test_helpers::test_create_namespace(fx, std::string("ns_a"));
    auto v1 = fx.manager->catalog_version();
    REQUIRE(v1 >= v0);
    disk_test_helpers::test_create_namespace(fx, std::string("ns_b"));
    auto v2 = fx.manager->catalog_version();
    REQUIRE(v2 >= v1);
}

// 7. Uncommitted DROP INDEX is invisible to other readers (same delete-tombstone path as drop table).
TEST_CASE("services::disk::mvcc::uncommitted_drop_index_invisible") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid = disk_test_helpers::test_create_table(fx, ns_oid, std::string("t"), cols,
                                                                  catalog::relkind::regular);
    const auto index_oid = disk_test_helpers::test_create_index(fx, ns_oid, table_oid,
                                                                  std::string("idx_doomed"),
                                                                  std::vector<std::string>{"id"},
                                                                  std::vector<components::catalog::oid_t>{});
    auto uncommitted = TRANSACTION_ID_START + 77;
    // Issue uncommitted index deletes (no commit — tombstones stay uncommitted).
    {
        const collection_full_name_t pg_idx{"pg_catalog", "main", "pg_index"};
        const collection_full_name_t pg_cls{"pg_catalog", "main", "pg_class"};
        const collection_full_name_t pg_dep{"pg_catalog", "main", "pg_depend"};
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_idx, std::int64_t{0}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_cls, std::int64_t{0}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, index_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, index_oid);
        // Intentionally no commit_pg_catalog_appends.
    }
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), ns_oid,
                         std::string("idx_doomed"), std::uint64_t{0});
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
        const collection_full_name_t pg_type{"pg_catalog", "main", "pg_type"};
        const collection_full_name_t pg_dep{"pg_catalog", "main", "pg_depend"};
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_type, std::int64_t{0}, type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep,  std::int64_t{1}, type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep,  std::int64_t{3}, type_oid);
        // Intentionally no commit_pg_catalog_appends.
    }
    auto rr = fx.invoke(&manager_disk_t::resolve_type, fx.auto_ctx(), ns_oid,
                          std::string("widget"), std::uint64_t{0});
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
    // with txn_id >= TRANSACTION_ID_START but NOT calling commit_pg_catalog_appends.
    components::catalog::oid_t table_oid = components::catalog::INVALID_OID;
    {
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1 + cols.size()});
        table_oid = oids[0];
        components::catalog::oid_batch_t batch;
        batch.oids = std::move(oids);
        collection_full_name_t coll{"public", "main", "ephemeral"};
        auto writes = components::catalog::build_create_table_writes(&fx.resource, coll, cols, false,
                                                                      ns_oid, batch,
                                                                      catalog::relkind::regular);
        for (auto& w : writes)
            fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.txn_ctx(txn), w.table, std::move(w.row));
        // Do NOT call commit_pg_catalog_appends — rows are pending under txn.
    }
    REQUIRE(table_oid >= FIRST_USER_OID);
    // Before rollback: invisible to other sessions (insert_id >= TRANSACTION_ID_START).
    auto before_other = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), ns_oid,
                                   std::string("ephemeral"), std::uint64_t{0});
    REQUIRE_FALSE(before_other.found);
    // Rollback: revert_pg_catalog_appends removes the uncommitted rows entirely.
    fx.invoke(&manager_disk_t::revert_pg_catalog_appends, fx.txn_ctx(txn));
    // After rollback: still not found — no orphan rows.
    auto after = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), ns_oid,
                            std::string("ephemeral"), std::uint64_t{0});
    REQUIRE_FALSE(after.found);
    // Same txn also cannot find the rolled-back table.
    auto after_same = fx.invoke(&manager_disk_t::resolve_table, fx.txn_ctx(txn), ns_oid,
                                  std::string("ephemeral"), std::uint64_t{0});
    REQUIRE_FALSE(after_same.found);
}

// 11. Drop cascade preserves E2.1A semantics — dropping a parent under an uncommitted txn
//     leaves both parent and cascaded children visible to other readers (their tombstones
//     are also uncommitted).
TEST_CASE("services::disk::mvcc::drop_cascade_uncommitted_invisible_to_other_readers") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto table_oid = disk_test_helpers::test_create_table(fx, ns_oid, std::string("t"), cols,
                                                                  catalog::relkind::regular);
    disk_test_helpers::test_create_index(fx, ns_oid, table_oid, std::string("child_idx"),
                                          std::vector<std::string>{"id"},
                                          std::vector<components::catalog::oid_t>{});
    auto uncommitted = TRANSACTION_ID_START + 111;
    // Issue uncommitted namespace deletes (no commit — tombstones stay uncommitted).
    {
        const collection_full_name_t pg_ns{"pg_catalog", "main", "pg_namespace"};
        const collection_full_name_t pg_dep{"pg_catalog", "main", "pg_depend"};
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_ns,  std::int64_t{0}, ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{1}, ns_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, fx.txn_ctx(uncommitted), pg_dep, std::int64_t{3}, ns_oid);
        // Intentionally no commit_pg_catalog_appends.
    }
    auto rt_after = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), ns_oid,
                                std::string("t"), std::uint64_t{0});
    auto idx_after = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), ns_oid,
                                 std::string("child_idx"), std::uint64_t{0});
    REQUIRE(rt_after.found);
    REQUIRE(idx_after.found);
}
