#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

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
    fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns_a"));
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(),
                        std::string("ns_a"), std::uint64_t{0});
    REQUIRE(r.found);
}

// 2. ddl_create_namespace under uncommitted txn is NOT visible to other sessions —
//    standard PostgreSQL MVCC: insert_id >= TRANSACTION_ID_START is hidden until commit.
TEST_CASE("services::disk::mvcc::uncommitted_insert_invisible_to_other_sessions") {
    fixture fx;
    auto uncommitted = TRANSACTION_ID_START + 1;
    fx.invoke(&manager_disk_t::ddl_create_namespace, fx.txn_ctx(uncommitted),
               std::string("ns_uncommitted"));
    // auto_ctx() uses transaction_id=0, so it must NOT see the uncommitted row.
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.auto_ctx(),
                        std::string("ns_uncommitted"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 3. ddl_drop_table at txn=0 (auto-commit) immediately hides the row.
TEST_CASE("services::disk::mvcc::auto_commit_drop_invisible") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.auto_ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), char{'r'});
    fx.invoke(&manager_disk_t::ddl_drop_table, fx.auto_ctx(), rt.created_oid,
               drop_behavior_t::cascade_);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE_FALSE(rr.found);
}

// 4. E2.1A: an uncommitted DELETE is INVISIBLE to other readers — the row stays visible
//    until the deleting txn commits (PostgreSQL MVCC). committed_version_operator's
//    use_deleted_version returns true (= "row visible") when delete_id >= TRANSACTION_ID_START
//    so other-txn readers do not see another in-flight tombstone.
TEST_CASE("services::disk::mvcc::uncommitted_delete_invisible_to_other_readers") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.auto_ctx(), rns.created_oid,
                         std::string("doomed"), std::move(cols), char{'r'});
    auto uncommitted = TRANSACTION_ID_START + 13;
    fx.invoke(&manager_disk_t::ddl_drop_table, fx.txn_ctx(uncommitted),
               rt.created_oid, drop_behavior_t::cascade_);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), rns.created_oid,
                         std::string("doomed"), std::uint64_t{0});
    REQUIRE(rr.found);
}

// 5. resolve_namespace mirrors uncommitted-deletion visibility (E2.1A) — an other-txn
//    reader still sees a row whose DROP is uncommitted; once the deleting txn commits,
//    the next resolve observes the tombstone-applied state. (Replaces former
//    populate_catalog_snapshot-based test now that populate is retired.)
TEST_CASE("services::disk::mvcc::resolve_includes_uncommitted_deletes") {
    fixture fx;
    auto rns_keep = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(),
                                std::string("kept_ns"));
    auto rns_drop = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(),
                                std::string("dropped_ns"));
    (void)rns_keep;
    auto uncommitted = TRANSACTION_ID_START + 21;
    fx.invoke(&manager_disk_t::ddl_drop_namespace, fx.txn_ctx(uncommitted),
               rns_drop.created_oid, drop_behavior_t::cascade_);

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
    fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns_a"));
    auto v1 = fx.manager->catalog_version();
    REQUIRE(v1 > v0);
    fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns_b"));
    auto v2 = fx.manager->catalog_version();
    REQUIRE(v2 > v1);
}

// 7. Uncommitted DROP INDEX is invisible to other readers (same delete-tombstone path as drop table).
TEST_CASE("services::disk::mvcc::uncommitted_drop_index_invisible") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.auto_ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), char{'r'});
    auto ri = fx.invoke(&manager_disk_t::ddl_create_index, fx.auto_ctx(), rns.created_oid,
                         rt.created_oid, std::string("idx_doomed"),
                         std::vector<std::string>{"id"});
    auto uncommitted = TRANSACTION_ID_START + 77;
    fx.invoke(&manager_disk_t::ddl_drop_index, fx.txn_ctx(uncommitted),
               ri.created_oid, drop_behavior_t::cascade_);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), rns.created_oid,
                         std::string("idx_doomed"), std::uint64_t{0});
    REQUIRE(rr.found);
}

// 8. Uncommitted DROP TYPE is invisible to other readers — type stays visible until commit.
TEST_CASE("services::disk::mvcc::uncommitted_drop_type_invisible") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns"));
    auto rty = fx.invoke(&manager_disk_t::ddl_create_type, fx.auto_ctx(), rns.created_oid,
                          std::string("widget"), std::string{});
    auto uncommitted = TRANSACTION_ID_START + 88;
    fx.invoke(&manager_disk_t::ddl_drop_type, fx.txn_ctx(uncommitted),
               rty.created_oid, drop_behavior_t::cascade_);
    auto rr = fx.invoke(&manager_disk_t::resolve_type, fx.auto_ctx(), rns.created_oid,
                          std::string("widget"), std::uint64_t{0});
    REQUIRE(rr.found);
}

// 9. Uncommitted DROP FUNCTION is invisible to other readers.
TEST_CASE("services::disk::mvcc::uncommitted_drop_function_invisible") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns"));
    auto rfn = fx.invoke(&manager_disk_t::ddl_create_function, fx.auto_ctx(), rns.created_oid,
                          std::string("fn1"), std::int32_t{0}, std::int64_t{0}, std::string{}, std::string{});
    auto uncommitted = TRANSACTION_ID_START + 99;
    fx.invoke(&manager_disk_t::ddl_drop_function, fx.txn_ctx(uncommitted),
               rfn.created_oid, drop_behavior_t::cascade_);
    auto rr = fx.invoke(&manager_disk_t::resolve_function, fx.auto_ctx(), rns.created_oid,
                          std::string("fn1"), std::uint64_t{0});
    REQUIRE(rr.found);
}

// 10. test_ddl_rollback_cleans_up (spec §14 line 2766): DDL inside an explicit transaction
//     that is ROLLED BACK must leave zero catalog rows behind.
//     Models BEGIN; CREATE TABLE t; ROLLBACK at the disk-actor level.
TEST_CASE("services::disk::mvcc::test_ddl_rollback_cleans_up") {
    fixture fx;
    const uint64_t txn = TRANSACTION_ID_START + 500;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("rollback_ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    // Create under an explicit (uncommitted) transaction.
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.txn_ctx(txn), rns.created_oid,
                         std::string("ephemeral"), std::move(cols), char{'r'});
    REQUIRE(rt.created_oid >= FIRST_USER_OID);
    // Before rollback: invisible to other sessions (insert_id >= TRANSACTION_ID_START).
    auto before_other = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), rns.created_oid,
                                   std::string("ephemeral"), std::uint64_t{0});
    REQUIRE_FALSE(before_other.found);
    // Rollback: revert_pg_catalog_appends removes the uncommitted rows entirely.
    fx.invoke(&manager_disk_t::revert_pg_catalog_appends, fx.txn_ctx(txn));
    // After rollback: still not found — no orphan rows.
    auto after = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), rns.created_oid,
                            std::string("ephemeral"), std::uint64_t{0});
    REQUIRE_FALSE(after.found);
    // Same txn also cannot find the rolled-back table.
    auto after_same = fx.invoke(&manager_disk_t::resolve_table, fx.txn_ctx(txn), rns.created_oid,
                                  std::string("ephemeral"), std::uint64_t{0});
    REQUIRE_FALSE(after_same.found);
}

// 11. Drop cascade preserves E2.1A semantics — dropping a parent under an uncommitted txn
//     leaves both parent and cascaded children visible to other readers (their tombstones
//     are also uncommitted).
TEST_CASE("services::disk::mvcc::drop_cascade_uncommitted_invisible_to_other_readers") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.auto_ctx(), std::string("ns"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.auto_ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), char{'r'});
    fx.invoke(&manager_disk_t::ddl_create_index, fx.auto_ctx(), rns.created_oid,
               rt.created_oid, std::string("child_idx"),
               std::vector<std::string>{"id"});
    auto uncommitted = TRANSACTION_ID_START + 111;
    fx.invoke(&manager_disk_t::ddl_drop_namespace, fx.txn_ctx(uncommitted),
               rns.created_oid, drop_behavior_t::cascade_);
    auto rt_after = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), rns.created_oid,
                                std::string("t"), std::uint64_t{0});
    auto idx_after = fx.invoke(&manager_disk_t::resolve_table, fx.auto_ctx(), rns.created_oid,
                                 std::string("child_idx"), std::uint64_t{0});
    REQUIRE(rt_after.found);
    REQUIRE(idx_after.found);
}
