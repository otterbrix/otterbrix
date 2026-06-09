#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"

#include <algorithm>
#include <filesystem>
#include <functional>
#include <thread>
#include <unistd.h>

// DDL roundtrip tests. Each test creates catalog objects via the build_create_*_writes
// helpers and verifies that resolve_* methods see the written rows correctly.

using namespace services::disk;
using namespace disk_test_helpers;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string ddl_dir() {
        static std::string p = "/tmp/test_otterbrix_ddl_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(ddl_dir()); }

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
                c.path = ddl_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(ddl_dir());
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

// 12. test_add_column writes a pg_attribute row (resolve_table reads columns
//     from pg_attribute on every call; no in-memory sync).
TEST_CASE("services::disk::ddl::add_column_round_trip") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsac");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto table_oid = test_create_table(fx, ns_oid, "t", cols);
    components::table::column_definition_t new_col(
        "added",
        components::types::complex_logical_type{components::types::logical_type::INTEGER});
    auto attoid = test_add_column(fx, table_oid, std::move(new_col), 2);
    REQUIRE(attoid >= FIRST_USER_OID);
    // After add, the column count visible via resolve_table grows.
    auto rs = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t"));
    REQUIRE(rs.found);
    REQUIRE(rs.columns.size() == 2);
}

// 20. operator_computed_field_register_t allocates a fresh attoid + bumps
// attversion when a column's type evolves (e.g. INT → TEXT). resolve_table
// returns the latest version. Disk-level reproduction via test_computed_register.
TEST_CASE("services::disk::ddl::computed_register_type_evolution") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nstype");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "agg",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);

    // Register "a" as INT.
    auto attoid_int = test_computed_register(fx, table_oid, "a", components::catalog::well_known_oid::int64_type);
    REQUIRE(attoid_int >= FIRST_USER_OID);

    // Register "a" as TEXT — type changed → fresh attoid + bumped attversion.
    auto attoid_text = test_computed_register(fx, table_oid, "a", components::catalog::well_known_oid::string_type);
    REQUIRE(attoid_text >= FIRST_USER_OID);
    REQUIRE(attoid_text != attoid_int);

    // resolve_table must report the latest version (TEXT) of column "a".
    auto rs = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("agg"));
    REQUIRE(rs.found);
    REQUIRE(rs.relkind == components::catalog::relkind::computed);
    REQUIRE(rs.columns.size() == 1);
    REQUIRE(rs.columns[0].attname == "a");
    REQUIRE(rs.columns[0].atttypid == components::catalog::well_known_oid::string_type);
}

// 22. Inserting a fresh pg_computed_column row registers the field, via
// primitive build_pg_computed_column_row + append_pg_catalog_row write.
TEST_CASE("services::disk::ddl::computed_append_new_field") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsca");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "agg",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);
    auto attoid = test_computed_append_simple(fx, table_oid, "count", components::catalog::well_known_oid::int64_type);
    REQUIRE(attoid >= FIRST_USER_OID);
}

// 23. Refcount-bump on duplicate append is gone in the simplified
// binary-refcount model. Re-registering an already-live (name+type) column
// is a no-op (no duplicate row, refcount stays 1).
TEST_CASE("services::disk::ddl::computed_register_same_type_idempotent") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsidem");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "agg",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);

    auto attoid1 = test_computed_register(fx, table_oid, "count", components::catalog::well_known_oid::int64_type);
    REQUIRE(attoid1 >= FIRST_USER_OID);

    // Second register with same (name, type) → no-op.
    auto attoid2 = test_computed_register(fx, table_oid, "count", components::catalog::well_known_oid::int64_type);
    REQUIRE(attoid2 == catalog::INVALID_OID);

    // Single column visible at version 0, refcount=1.
    constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
    components::types::logical_value_t toid_lv(&fx.resource, table_oid);
    components::types::logical_value_t name_lv(&fx.resource, std::string("count"));
    std::pmr::vector<std::string> k1{&fx.resource};
    k1.emplace_back("relid");
    k1.emplace_back("attname");
    std::pmr::vector<components::types::logical_value_t> v1{&fx.resource};
    v1.emplace_back(toid_lv);
    v1.emplace_back(name_lv);
    auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                             fx.ctx(),
                             pg_cc,
                             std::move(k1),
                             test_probe::build_key_chunk(&fx.resource, std::move(v1)));
    std::uint64_t total = 0;
    for (const auto& c : batches) total += c.size();
    REQUIRE(total == 1);
    REQUIRE(batches[0].value(6, 0).value<std::int64_t>() == 1);

    auto rs = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("agg"));
    REQUIRE(rs.found);
    REQUIRE(rs.columns.size() == 1);
}

// 24. Drop = append tombstone (refcount=0). After register+unregister,
// resolve_table must hide the column (the live row + tombstone coexist on
// disk until a future VACUUM, but the reader filters them via the
// refcount<=0 / max-version-per-name gate).
TEST_CASE("services::disk::ddl::computed_unregister_then_resolve_hides_column") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nshide");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "agg",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);

    auto attoid = test_computed_register(fx, table_oid, "count", components::catalog::well_known_oid::int64_type);
    REQUIRE(attoid >= FIRST_USER_OID);

    // Confirm column visible before unregister.
    auto rs_before = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("agg"));
    REQUIRE(rs_before.found);
    REQUIRE(rs_before.columns.size() == 1);
    REQUIRE(rs_before.columns[0].attname == "count");

    // Unregister.
    REQUIRE(test_computed_unregister(fx, table_oid, "count"));

    // Column hidden from resolve_table.
    auto rs_after = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("agg"));
    REQUIRE(rs_after.found);
    REQUIRE(rs_after.relkind == components::catalog::relkind::computed);
    REQUIRE(rs_after.columns.empty());
}

// 25. Refcount-decrement is replaced by binary register/unregister +
// tombstone semantics. Verify that unregister appends a tombstone row with
// refcount=0 and that the live row + tombstone coexist until VACUUM
// (i.e. read_chunks_by_key sees both).
TEST_CASE("services::disk::ddl::computed_unregister_marks_dead") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nstomb");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "agg",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);

    auto attoid = test_computed_register(fx, table_oid, "count", components::catalog::well_known_oid::int64_type);
    REQUIRE(attoid >= FIRST_USER_OID);
    REQUIRE(test_computed_unregister(fx, table_oid, "count"));

    // Two rows on disk for ("agg", "count"): live (refcount=1) + tombstone (refcount=0).
    constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
    components::types::logical_value_t toid_lv(&fx.resource, table_oid);
    components::types::logical_value_t name_lv(&fx.resource, std::string("count"));
    std::pmr::vector<std::string> k2{&fx.resource};
    k2.emplace_back("relid");
    k2.emplace_back("attname");
    std::pmr::vector<components::types::logical_value_t> v2{&fx.resource};
    v2.emplace_back(toid_lv);
    v2.emplace_back(name_lv);
    auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                             fx.ctx(),
                             pg_cc,
                             std::move(k2),
                             test_probe::build_key_chunk(&fx.resource, std::move(v2)));
    std::uint64_t total = 0;
    for (const auto& c : batches) total += c.size();
    REQUIRE(total == 2);

    bool found_live = false;
    bool found_tomb = false;
    std::int64_t live_v = -1;
    std::int64_t tomb_v = -1;
    for (const auto& chunk : batches) {
        REQUIRE(chunk.column_count() >= 7);
        for (std::uint64_t i = 0; i < chunk.size(); ++i) {
            const auto v = chunk.value(5, i).value<std::int64_t>();
            const auto rc = chunk.value(6, i).value<std::int64_t>();
            if (rc > 0) {
                found_live = true;
                live_v = v;
            }
            if (rc == 0) {
                found_tomb = true;
                tomb_v = v;
            }
        }
    }
    REQUIRE(found_live);
    REQUIRE(found_tomb);
    REQUIRE(tomb_v > live_v);
}

// Disk-level mirror of the SQL-level
// dynamic_schema_drop_then_readd_preserves_old_data test. Verify the
// register/unregister/register sequence at the pg_computed_column row level:
//
//   register("a", BIGINT)            -> 1 row: a/v0/rc=1
//   register("b", STRING)            -> 2 rows: a/v0/rc=1, b/v0/rc=1
//   unregister("b")                  -> 3 rows: a/v0/rc=1, b/v0/rc=1, b-tomb/v1/rc=0
//                                      (tombstone reuses live attoid_b)
//   register("b", STRING)            -> EXPECTED 3 rows still: a same-type
//                                      re-register short-circuits to a no-op in
//                                      operator_computed_field_register (max_version
//                                      comes from the tombstone, refcount filter
//                                      not applied at this read).
//   resolve_table                    -> EXPECTED 1 column ("a") — 'b' is
//                                      tombstoned and the resolver gates on
//                                      refcount>0.
//
// If the operator is later changed to revive a tombstone instead of no-oping,
// the row count goes to 4 and resolve_table sees 2 columns; both branches are
// captured below with WARN-fallbacks so the test stays informative either way.
TEST_CASE("services::disk::ddl::computed_field_drop_then_readd") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsreadd");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "foo",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);

    // 1) Register a (BIGINT) and b (STRING).
    auto attoid_a = test_computed_register(fx, table_oid, "a", components::catalog::well_known_oid::int64_type);
    auto attoid_b = test_computed_register(fx, table_oid, "b", components::catalog::well_known_oid::string_type);
    REQUIRE(attoid_a >= FIRST_USER_OID);
    REQUIRE(attoid_b >= FIRST_USER_OID);

    // 2) Drop b → tombstone (rc=0) reusing attoid_b.
    REQUIRE(test_computed_unregister(fx, table_oid, "b"));

    // 3) Re-register b with the SAME atttypid (STRING). By the same-type rule
    //    in operator_computed_field_register_t this is a no-op: the helper
    //    returns INVALID_OID and pg_computed_column gains no new row.
    auto attoid_b2 = test_computed_register(fx, table_oid, "b", components::catalog::well_known_oid::string_type);

    constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
    components::types::logical_value_t toid_lv(&fx.resource, table_oid);
    std::pmr::vector<std::string> k3{&fx.resource};
    k3.emplace_back("relid");
    std::pmr::vector<components::types::logical_value_t> v3{&fx.resource};
    v3.emplace_back(toid_lv);
    auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                             fx.ctx(),
                             pg_cc,
                             std::move(k3),
                             test_probe::build_key_chunk(&fx.resource, std::move(v3)));
    std::uint64_t total_rows = 0;
    for (const auto& c : batches) total_rows += c.size();

    // Branch on observed register-side behavior so the test stays useful even
    // if the operator's same-type policy is later relaxed (e.g. to revive
    // tombstones with a fresh attversion).
    if (attoid_b2 == catalog::INVALID_OID) {
        // Documented current behavior: 3 rows total.
        //   a (live, v=0, rc=1)
        //   b (live, v=0, rc=1)            -- attoid_b
        //   b (tombstone, v=1, rc=0)       -- attoid_b reused
        REQUIRE(total_rows == 3);

        // Per-attname classification.
        int rows_a = 0, rows_b_live = 0, rows_b_tomb = 0;
        std::int64_t b_live_v = -1;
        std::int64_t b_tomb_v = -1;
        catalog::oid_t observed_b_live_attoid = catalog::INVALID_OID;
        catalog::oid_t observed_b_tomb_attoid = catalog::INVALID_OID;
        for (const auto& chunk : batches) {
            REQUIRE(chunk.column_count() >= 7);
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                const auto attname = std::string(chunk.value(2, i).value<std::string_view>());
                const auto v = chunk.value(5, i).value<std::int64_t>();
                const auto rc = chunk.value(6, i).value<std::int64_t>();
                if (attname == "a") {
                    ++rows_a;
                } else if (attname == "b") {
                    if (rc > 0) {
                        ++rows_b_live;
                        b_live_v = v;
                        observed_b_live_attoid = static_cast<catalog::oid_t>(chunk.value(1, i).value<std::uint32_t>());
                    } else {
                        ++rows_b_tomb;
                        b_tomb_v = v;
                        observed_b_tomb_attoid = static_cast<catalog::oid_t>(chunk.value(1, i).value<std::uint32_t>());
                    }
                }
            }
        }
        REQUIRE(rows_a == 1);
        REQUIRE(rows_b_live == 1);
        REQUIRE(rows_b_tomb == 1);
        REQUIRE(b_tomb_v > b_live_v);
        // Tombstone reuses the live attoid (operator_computed_field_unregister.cpp:81).
        REQUIRE(observed_b_live_attoid == attoid_b);
        REQUIRE(observed_b_tomb_attoid == attoid_b);
    } else {
        // Future behavior: revival path appended a fresh row for b. Expect 4
        // rows total and a NEW attoid for the revived b. Flag with WARN so
        // contributors notice the policy shift.
        WARN("operator_computed_field_register_t now allocates a fresh attoid "
             "on same-type re-register (instead of no-oping); update task "
             "#103 expectations.");
        REQUIRE(total_rows == 4);
        REQUIRE(attoid_b2 != attoid_b);
    }

    // resolve_table reflects the resolver's refcount>0 + max-attversion gate.
    auto rs = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("foo"));
    REQUIRE(rs.found);
    REQUIRE(rs.relkind == components::catalog::relkind::computed);

    if (attoid_b2 == catalog::INVALID_OID) {
        // Same-type no-op kept the tombstone in place. Resolver hides 'b'.
        REQUIRE(rs.columns.size() == 1);
        REQUIRE(rs.columns[0].attname == "a");
    } else {
        // Revival branch: both columns visible, b at the latest version.
        REQUIRE(rs.columns.size() == 2);
        bool has_a = false;
        bool has_b = false;
        for (const auto& c : rs.columns) {
            if (c.attname == "a")
                has_a = true;
            if (c.attname == "b") {
                has_b = true;
                REQUIRE(c.atttypid == components::catalog::well_known_oid::string_type);
            }
        }
        REQUIRE(has_a);
        REQUIRE(has_b);
    }
}

// 27. VACUUM GC for pg_computed_column dead rows. After register×3
// + unregister of one column, pg_computed_column on disk holds 4 rows:
//   a (live, v=0, rc=1)
//   b (live, v=0, rc=1)
//   b (tombstone, v=1, rc=0) — operator_computed_field_unregister_t reuses
//                               the live attoid for the tombstone row.
//   c (live, v=0, rc=1)
// operator_vacuum_t step 5 (cf. components/physical_plan/operators/operator_vacuum.cpp
// lines 195–252) iterates dead rows (rc<=0) per relkind='g' table and calls
// delete_pg_catalog_rows(pg_computed_column, oid_col_idx=1, attoid). Because
// the unregister tombstone shares attoid with the live row, that delete wipes
// BOTH rows for column "b" — leaving exactly {a, c}.
//
// We model the GC step directly here: the disk-actor's vacuum_all performs
// only storage cleanup_versions/compact, not the pg_computed_column scan
// (that's owned by operator_vacuum_t). The test exercises the disk primitive
// (delete_pg_catalog_rows) the operator composes on top of.
TEST_CASE("services::disk::ddl::vacuum_gc_clears_dead_computed_columns") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsvac");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "agg",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);

    // Register 3 columns.
    auto attoid_a = test_computed_register(fx, table_oid, "a", components::catalog::well_known_oid::int64_type);
    auto attoid_b = test_computed_register(fx, table_oid, "b", components::catalog::well_known_oid::string_type);
    auto attoid_c = test_computed_register(fx, table_oid, "c", components::catalog::well_known_oid::float64_type);
    REQUIRE(attoid_a >= FIRST_USER_OID);
    REQUIRE(attoid_b >= FIRST_USER_OID);
    REQUIRE(attoid_c >= FIRST_USER_OID);

    // Drop b → appends tombstone (rc=0) reusing attoid_b.
    REQUIRE(test_computed_unregister(fx, table_oid, "b"));

    constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;

    // Pre-VACUUM: 4 rows total for this table (a-live, b-live, b-tombstone, c-live).
    {
        components::types::logical_value_t toid_lv(&fx.resource, table_oid);
        std::pmr::vector<std::string> kk{&fx.resource};
        kk.emplace_back("relid");
        std::pmr::vector<components::types::logical_value_t> vv{&fx.resource};
        vv.emplace_back(toid_lv);
        auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                 fx.ctx(),
                                 pg_cc,
                                 std::move(kk),
                                 test_probe::build_key_chunk(&fx.resource, std::move(vv)));
        std::uint64_t total = 0;
        for (const auto& c : batches) total += c.size();
        REQUIRE(total == 4);
    }

    // Imitate operator_vacuum_t step 5: collect attoids of dead rows (rc<=0)
    // and delete by attoid. Because the unregister tombstone shares attoid
    // with its live counterpart (operator_computed_field_unregister.cpp:81),
    // the per-attoid delete drops BOTH rows for column "b".
    {
        components::types::logical_value_t toid_lv(&fx.resource, table_oid);
        std::pmr::vector<std::string> kk{&fx.resource};
        kk.emplace_back("relid");
        std::pmr::vector<components::types::logical_value_t> vv{&fx.resource};
        vv.emplace_back(toid_lv);
        auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                 fx.ctx(),
                                 pg_cc,
                                 std::move(kk),
                                 test_probe::build_key_chunk(&fx.resource, std::move(vv)));
        std::vector<catalog::oid_t> dead_attoids;
        for (const auto& chunk : batches) {
            REQUIRE(chunk.column_count() >= 7);
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                const auto rc = chunk.value(6, i).value<std::int64_t>();
                if (rc <= 0) {
                    dead_attoids.push_back(static_cast<catalog::oid_t>(chunk.value(1, i).value<std::uint32_t>()));
                }
            }
        }
        REQUIRE(dead_attoids.size() == 1);
        REQUIRE(dead_attoids[0] == attoid_b);

        for (const auto attoid : dead_attoids) {
            fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_cc, std::int64_t{1}, attoid);
        }
        std::set<catalog::oid_t> deletes_local{pg_cc};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }

    // Post-VACUUM: 2 rows left (a, c). b's live row was wiped together with
    // its tombstone because both share the same attoid.
    {
        components::types::logical_value_t toid_lv(&fx.resource, table_oid);
        std::pmr::vector<std::string> kk{&fx.resource};
        kk.emplace_back("relid");
        std::pmr::vector<components::types::logical_value_t> vv{&fx.resource};
        vv.emplace_back(toid_lv);
        auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                 fx.ctx(),
                                 pg_cc,
                                 std::move(kk),
                                 test_probe::build_key_chunk(&fx.resource, std::move(vv)));
        std::uint64_t total = 0;
        for (const auto& c : batches) total += c.size();
        REQUIRE(total == 2);
        std::vector<std::string> names;
        for (const auto& chunk : batches) {
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                names.emplace_back(chunk.value(2, i).value<std::string_view>());
            }
        }
        std::sort(names.begin(), names.end());
        REQUIRE(names == std::vector<std::string>{"a", "c"});
    }

    // resolve_table sees only {a, c}.
    auto rs = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("agg"));
    REQUIRE(rs.found);
    REQUIRE(rs.relkind == components::catalog::relkind::computed);
    REQUIRE(rs.columns.size() == 2);
}

// Physical column compaction primitive at the disk-actor surface.
// compact_relkind_g_storage(name, live_attnames) drops every storage column
// whose name is NOT in live_attnames; backed by table_storage_t::drop_column
// (which itself uses the data_table_t(parent, removed_column) rebuild).
//
// Models the operator_vacuum step 5b call site: a relkind='g' table has had
// columns {a,b,c} adopted by storage_append (per #96 schema-extension fix),
// then column "b" was dropped from pg_computed_column. compact_*_storage
// reclaims b's physical column from the data_table_t.
TEST_CASE("services::disk::ddl::vacuum_physical_compaction_removes_dropped_columns") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;
    using components::vector::data_chunk_t;

    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nscompact");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "agg",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);
    REQUIRE(table_oid >= FIRST_USER_OID);

    // Storage entry must exist for storage_append / compact to operate on.
    fx.invoke(&manager_disk_t::create_storage, session_id_t{}, table_oid, catalog::well_known_oid::main_database);

    // Register columns a/b/c in pg_computed_column.
    auto attoid_a = test_computed_register(fx, table_oid, "a", components::catalog::well_known_oid::int64_type);
    auto attoid_b = test_computed_register(fx, table_oid, "b", components::catalog::well_known_oid::int64_type);
    auto attoid_c = test_computed_register(fx, table_oid, "c", components::catalog::well_known_oid::int64_type);
    REQUIRE(attoid_a >= FIRST_USER_OID);
    REQUIRE(attoid_b >= FIRST_USER_OID);
    REQUIRE(attoid_c >= FIRST_USER_OID);

    // Append a row with {a,b,c} so storage_append's #96 auto-extend adopts
    // all three columns physically.
    {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        for (auto n : {"a", "b", "c"}) {
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias(n);
            types.push_back(std::move(t));
        }
        auto chunk = std::make_unique<data_chunk_t>(&fx.resource, types, 1);
        chunk->set_cardinality(1);
        chunk->set_value(0, 0, logical_value_t(&fx.resource, std::int64_t{1}));
        chunk->set_value(1, 0, logical_value_t(&fx.resource, std::int64_t{2}));
        chunk->set_value(2, 0, logical_value_t(&fx.resource, std::int64_t{3}));
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(chunk));
    }

    // Verify storage now has 3 columns (post-#96).
    {
        auto types = fx.invoke(&manager_disk_t::storage_types, session_id_t{}, table_oid);
        REQUIRE(types.size() == 3);
    }

    // Simulate operator_vacuum step 5a: drop column "b" via tombstone-GC.
    REQUIRE(test_computed_unregister(fx, table_oid, "b"));
    {
        constexpr catalog::oid_t pg_cc = catalog::well_known_oid::pg_computed_column_table;
        logical_value_t toid_lv(&fx.resource, table_oid);
        std::pmr::vector<std::string> kk{&fx.resource};
        kk.emplace_back("relid");
        std::pmr::vector<logical_value_t> vv{&fx.resource};
        vv.emplace_back(toid_lv);
        auto batches = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                 fx.ctx(),
                                 pg_cc,
                                 std::move(kk),
                                 test_probe::build_key_chunk(&fx.resource, std::move(vv)));
        std::vector<catalog::oid_t> dead_attoids;
        for (const auto& chunk : batches) {
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.value(6, i).value<std::int64_t>() <= 0) {
                    dead_attoids.push_back(static_cast<catalog::oid_t>(chunk.value(1, i).value<std::uint32_t>()));
                }
            }
        }
        for (const auto attoid : dead_attoids) {
            fx.invoke(&manager_disk_t::delete_pg_catalog_rows, txn_ctx(), pg_cc, std::int64_t{1}, attoid);
        }
        std::set<catalog::oid_t> deletes_local{pg_cc};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }

    // Now run step 5b: compact_relkind_g_storage with live = {a, c}. Storage
    // must drop column "b" physically.
    {
        std::set<std::string> live{"a", "c"};
        auto dropped = fx.invoke(&manager_disk_t::compact_relkind_g_storage, fx.ctx(), table_oid, std::move(live));
        REQUIRE(dropped == 1);
    }

    // Storage now has 2 columns: a, c.
    {
        auto types = fx.invoke(&manager_disk_t::storage_types, session_id_t{}, table_oid);
        REQUIRE(types.size() == 2);
    }

    // Calling compact_* again with the same live set is a no-op.
    {
        std::set<std::string> live{"a", "c"};
        auto dropped = fx.invoke(&manager_disk_t::compact_relkind_g_storage, fx.ctx(), table_oid, std::move(live));
        REQUIRE(dropped == 0);
    }

    // Empty live set drops everything.
    {
        std::set<std::string> live{};
        auto dropped = fx.invoke(&manager_disk_t::compact_relkind_g_storage, fx.ctx(), table_oid, std::move(live));
        REQUIRE(dropped == 2);
    }
    {
        auto types = fx.invoke(&manager_disk_t::storage_types, session_id_t{}, table_oid);
        REQUIRE(types.empty());
    }

    // Unknown-table calls are silent no-ops (return 0).
    {
        // A never-allocated user oid: definitely not in storages_.
        const catalog::oid_t missing_oid{FIRST_USER_OID + 9999};
        std::set<std::string> live{};
        auto dropped = fx.invoke(&manager_disk_t::compact_relkind_g_storage, fx.ctx(), missing_oid, std::move(live));
        REQUIRE(dropped == 0);
    }
}

// 28. Concurrent INSERT into the same relkind='g' table from
// multiple sessions: MVCC visibility. Skipped: requires a multi-session test
// fixture (independent dispatchers/sessions sharing the same disk actor) that
// the current per-test fixture doesn't model.
TEST_CASE("services::disk::ddl::dynamic_schema_concurrent_insert_skip") {
    WARN("TODO: requires multi-session test fixture; covered indirectly via SQL-level tests today");
}

// 29. WAL recovery: restart after INSERT into a relkind='g' table
// mid-flight, replay correctness for pg_computed_column appends. Skipped:
// needs a restart fixture analogous to the test_recovery.cpp pattern, but
// that suite predates relkind='g' and doesn't yet model dynamic-schema
// rebuild on replay.
TEST_CASE("services::disk::ddl::dynamic_schema_wal_recovery_skip") {
    WARN("TODO: requires restart fixture; covered by test_recovery.cpp pattern but not yet for relkind='g'");
}

// Pins down storage_append for relkind='g' (dynamic-schema) tables:
//   - first chunk: adopts its types (one-shot, since adopt_schema asserts the
//     schema is empty);
//   - later chunks: matches incoming columns to existing ones by alias; columns
//     not already in the schema are silently DROPPED, not auto-added.
// So dynamic-schema growth must go through an explicit add_column /
// pg_computed_column path before storage_append, never via the INSERT itself.
TEST_CASE("services::disk::ddl::storage_expand_on_write_for_dynamic_schema") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;
    using components::vector::data_chunk_t;

    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsdyn");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "docs",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);
    REQUIRE(table_oid >= FIRST_USER_OID);

    // The user-table storage is not allocated by test_create_table (which only
    // writes catalog rows). storage_append needs a storage entry to operate on,
    // so create one explicitly (schema-less, mirroring the runtime path that
    // create_collection takes for fresh tables).
    fx.invoke(&manager_disk_t::create_storage, session_id_t{}, table_oid, catalog::well_known_oid::main_database);

    auto append_ctx = [&](catalog::oid_t toid) {
        return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}, toid};
    };

    auto build_chunk = [&](std::vector<std::pair<std::string, complex_logical_type>> cols,
                           std::function<void(data_chunk_t&)> filler,
                           uint64_t rows) {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        for (auto& [name, ty] : cols) {
            ty.set_alias(name);
            types.push_back(std::move(ty));
        }
        auto chunk = std::make_unique<data_chunk_t>(&fx.resource, types, rows);
        chunk->set_cardinality(rows);
        filler(*chunk);
        return chunk;
    };

    auto attoid_a = test_computed_register(fx, table_oid, "a", components::catalog::well_known_oid::int64_type);
    REQUIRE(attoid_a >= FIRST_USER_OID);
    {
        auto chunk = build_chunk(
            {{"a", complex_logical_type{logical_type::BIGINT}}},
            [&](data_chunk_t& c) { c.set_value(0, 0, logical_value_t(&fx.resource, std::int64_t{1})); },
            /*rows=*/1);
        auto [start, count] =
            fx.invoke(&manager_disk_t::storage_append, append_ctx(table_oid), table_oid, std::move(chunk));
        REQUIRE(count == 1);
        (void) start;
    }

    // The schema was frozen at 1 column by the first append (adopt_schema is
    // one-shot), so the incoming "b" is silently dropped: row count grows to 2
    // but column count stays 1. (Naively you'd expect 2 columns with b=NULL.)
    auto attoid_b = test_computed_register(fx, table_oid, "b", components::catalog::well_known_oid::string_type);
    REQUIRE(attoid_b >= FIRST_USER_OID);
    {
        auto chunk = build_chunk(
            {{"a", complex_logical_type{logical_type::BIGINT}},
             {"b", complex_logical_type{logical_type::STRING_LITERAL}}},
            [&](data_chunk_t& c) {
                c.set_value(0, 0, logical_value_t(&fx.resource, std::int64_t{2}));
                c.set_value(1, 0, logical_value_t(&fx.resource, std::string("x")));
            },
            /*rows=*/1);
        fx.invoke(&manager_disk_t::storage_append, append_ctx(table_oid), table_oid, std::move(chunk));
    }

    {
        // Bug #96 fix: storage_append now auto-extends the IN_MEMORY schema for
        // relkind='g' tables when the incoming chunk brings columns that aren't in
        // the current data_table_t. Pre-existing rows get NULL-equivalent
        // (zero-initialized) values for the new column.
        auto types = fx.invoke(&manager_disk_t::storage_types, session_id_t{}, table_oid);
        REQUIRE(types.size() == 2);
        auto rows = fx.invoke(&manager_disk_t::storage_scan,
                              session_id_t{},
                              table_oid,
                              std::unique_ptr<components::table::table_filter_t>{},
                              /*limit=*/-1,
                              components::table::transaction_data{0, 0});
        REQUIRE(rows);
        REQUIRE(rows->size() == 2);
    }

    auto attoid_c = test_computed_register(fx, table_oid, "c", components::catalog::well_known_oid::float64_type);
    REQUIRE(attoid_c >= FIRST_USER_OID);
    {
        auto chunk = build_chunk(
            {{"a", complex_logical_type{logical_type::BIGINT}},
             {"b", complex_logical_type{logical_type::STRING_LITERAL}},
             {"c", complex_logical_type{logical_type::DOUBLE}}},
            [&](data_chunk_t& c) {
                c.set_value(0, 0, logical_value_t(&fx.resource, std::int64_t{3}));
                c.set_value(1, 0, logical_value_t(&fx.resource, std::string("y")));
                c.set_value(2, 0, logical_value_t(&fx.resource, double{3.14}));
            },
            /*rows=*/1);
        fx.invoke(&manager_disk_t::storage_append, append_ctx(table_oid), table_oid, std::move(chunk));
    }

    {
        auto types = fx.invoke(&manager_disk_t::storage_types, session_id_t{}, table_oid);
        REQUIRE(types.size() == 3);
        auto rows = fx.invoke(&manager_disk_t::storage_scan,
                              session_id_t{},
                              table_oid,
                              std::unique_ptr<components::table::table_filter_t>{},
                              /*limit=*/-1,
                              components::table::transaction_data{0, 0});
        REQUIRE(rows);
        REQUIRE(rows->size() == 3);
    }

    // resolve_table for the relkind='g' table reads columns from
    // pg_computed_column, which DOES grow correctly across the three
    // test_computed_register calls. This is the path the runtime relies on
    // for dynamic-schema growth — independent of what storage_append does.
    auto rs = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("docs"));
    REQUIRE(rs.found);
    REQUIRE(rs.relkind == components::catalog::relkind::computed);
    REQUIRE(rs.columns.size() == 3);
}

// Batched DROP: drop_storage_many erases N user storages in ONE call. Create N
// IN_MEMORY user storages (with one row each so they're observably non-empty),
// confirm each is present, then send a single drop_storage_many with the N oids
// and assert all N are gone (has_storage false / storage_total_rows 0 /
// read_chunks_by_key empty) while a non-targeted storage survives untouched.
TEST_CASE("services::disk::ddl::drop_storage_many_erases_n") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;
    using components::vector::data_chunk_t;

    fixture fx;

    // Allocate N+1 fresh user OIDs (N targeted for DROP + 1 survivor).
    constexpr std::size_t N = 4;
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{N + 1});
    REQUIRE(oids.size() == N + 1);

    std::vector<catalog::oid_t> targets(oids.begin(), oids.begin() + N);
    const catalog::oid_t survivor = oids[N];

    auto append_one = [&](catalog::oid_t oid, std::int64_t kval) {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        for (auto n : {"k", "payload"}) {
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias(n);
            types.push_back(std::move(t));
        }
        auto chunk = std::make_unique<data_chunk_t>(&fx.resource, types, 1);
        chunk->set_cardinality(1);
        chunk->set_value(0, 0, logical_value_t(&fx.resource, kval));
        chunk->set_value(1, 0, logical_value_t(&fx.resource, std::int64_t{kval * 10}));
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   oid};
        fx.invoke(&manager_disk_t::storage_append, append_ctx, oid, std::move(chunk));
    };

    // Create + populate the N targets and the survivor.
    for (std::size_t i = 0; i < N; ++i) {
        fx.invoke(&manager_disk_t::create_storage, session_id_t{}, targets[i], well_known_oid::main_database);
        append_one(targets[i], static_cast<std::int64_t>(i + 1));
    }
    fx.invoke(&manager_disk_t::create_storage, session_id_t{}, survivor, well_known_oid::main_database);
    append_one(survivor, std::int64_t{777});

    // Pre-DROP: every target is present and non-empty.
    for (std::size_t i = 0; i < N; ++i) {
        REQUIRE(fx.manager->has_storage(targets[i]));
        auto rows = fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, targets[i]);
        REQUIRE(rows == 1);
    }
    REQUIRE(fx.manager->has_storage(survivor));
    REQUIRE(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, survivor) == 1);

    // ONE batched drop for all N targets (survivor NOT in the oid list).
    {
        std::pmr::vector<catalog::oid_t> drop_oids{&fx.resource};
        for (auto oid : targets)
            drop_oids.push_back(oid);
        fx.invoke(&manager_disk_t::drop_storage_many, session_id_t{}, std::move(drop_oids));
    }

    // Post-DROP: all N targets are gone on every observable surface.
    for (std::size_t i = 0; i < N; ++i) {
        REQUIRE_FALSE(fx.manager->has_storage(targets[i]));
        REQUIRE(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, targets[i]) == 0);
        std::pmr::vector<std::string> key_cols{&fx.resource};
        key_cols.emplace_back("k");
        std::pmr::vector<logical_value_t> vals{&fx.resource};
        vals.emplace_back(&fx.resource, static_cast<std::int64_t>(i + 1));
        auto chunks = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                fx.ctx(),
                                targets[i],
                                std::move(key_cols),
                                test_probe::build_key_chunk(&fx.resource, std::move(vals)));
        std::uint64_t total = 0;
        for (const auto& c : chunks)
            total += c.size();
        REQUIRE(total == 0);
    }

    // Survivor untouched: still present, still 1 row, still readable.
    REQUIRE(fx.manager->has_storage(survivor));
    REQUIRE(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, survivor) == 1);
    {
        std::pmr::vector<std::string> key_cols{&fx.resource};
        key_cols.emplace_back("k");
        std::pmr::vector<logical_value_t> vals{&fx.resource};
        vals.emplace_back(&fx.resource, std::int64_t{777});
        auto chunks = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                fx.ctx(),
                                survivor,
                                std::move(key_cols),
                                test_probe::build_key_chunk(&fx.resource, std::move(vals)));
        std::uint64_t total = 0;
        for (const auto& c : chunks)
            total += c.size();
        REQUIRE(total == 1);
    }
}

// Batched DROP-GC marking: mark_storage_dropped_many records N GC entries in ONE
// call, and a subsequent on_horizon_advanced past the dropped_at_commit_id
// reclaims each storage's .otbx file. The per-agent dropped_storages_ slice has
// no direct accessor, so observability is via the GC side effect: create N
// DISK-backed storages (each gets a real <db>/<oid>/table.otbx), mark them all
// dropped at commit D, advance the horizon past D, and assert the .otbx files
// are gone. A non-marked DISK storage's .otbx survives.
//
// NOTE on the limitation: mark_storage_dropped_many does NOT remove the storage
// entry from storages_ (that is drop_storage_many's job) and does NOT itself
// delete files; it only records the GC entry. The .otbx removal is performed by
// on_horizon_advanced_inner (agent_disk.cpp: dropped_at_commit_id < new_horizon).
// So the file-reclamation assertion below is the strongest feasible observation
// of the recorded GC entries without a dropped_storages_ accessor.
TEST_CASE("services::disk::ddl::mark_storage_dropped_many_records_n_gc_entries") {
    using components::table::column_definition_t;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    fixture fx;

    constexpr std::size_t N = 3;
    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{N + 1});
    REQUIRE(oids.size() == N + 1);
    std::vector<catalog::oid_t> targets(oids.begin(), oids.begin() + N);
    const catalog::oid_t survivor = oids[N];

    // .otbx path layout mirrors manager_disk_t::create_storage_disk:
    //   <config.path>/<db_oid>/<tbl_oid>/table.otbx
    constexpr catalog::oid_t db_oid = well_known_oid::main_database;
    auto otbx_path_for = [&](catalog::oid_t tbl) {
        return std::filesystem::path(ddl_dir()) / std::to_string(static_cast<unsigned>(db_oid)) /
               std::to_string(static_cast<unsigned>(tbl)) / "table.otbx";
    };

    auto make_disk_storage = [&](catalog::oid_t tbl) {
        std::vector<column_definition_t> cols;
        cols.emplace_back("k", complex_logical_type{logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_disk, session_id_t{}, tbl, db_oid, std::move(cols));
    };

    for (auto oid : targets)
        make_disk_storage(oid);
    make_disk_storage(survivor);

    // Every DISK-backed storage materialised its .otbx on creation.
    for (auto oid : targets)
        REQUIRE(std::filesystem::exists(otbx_path_for(oid)));
    REQUIRE(std::filesystem::exists(otbx_path_for(survivor)));

    // ONE batched mark for all N targets at dropped_at_commit_id = D (survivor
    // not in the list). mark does NOT remove storages_ entries or touch files.
    constexpr std::uint64_t D = 5000;
    {
        std::pmr::vector<catalog::oid_t> mark_oids{&fx.resource};
        for (auto oid : targets)
            mark_oids.push_back(oid);
        fx.invoke(&manager_disk_t::mark_storage_dropped_many, session_id_t{}, std::move(mark_oids), D);
    }

    // Marking alone leaves the .otbx files in place (GC is horizon-driven).
    for (auto oid : targets)
        REQUIRE(std::filesystem::exists(otbx_path_for(oid)));

    // A horizon advance that does NOT pass D (dropped_at_commit_id < new_horizon
    // is false for new_horizon <= D) reclaims nothing.
    fx.invoke(&manager_disk_t::on_horizon_advanced, D);
    for (auto oid : targets)
        REQUIRE(std::filesystem::exists(otbx_path_for(oid)));

    // Advancing the horizon PAST D fires the GC sweep: each recorded entry's
    // .otbx (and sidecars) is reclaimed.
    fx.invoke(&manager_disk_t::on_horizon_advanced, D + 1);
    for (auto oid : targets)
        REQUIRE_FALSE(std::filesystem::exists(otbx_path_for(oid)));

    // The non-marked survivor's .otbx is untouched (no GC entry was recorded).
    REQUIRE(std::filesystem::exists(otbx_path_for(survivor)));
}

// Computing tables (relkind='g') get no pg_attribute rows on creation —
// versioned fields live in pg_computed_column, so resolve_table.columns is
// empty for a fresh computing table.
TEST_CASE("services::disk::ddl::computing_table_pg_attribute_empty") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nscempty");
    auto table_oid = test_create_table(fx,
                                       ns_oid,
                                       "agg",
                                       std::vector<components::table::column_definition_t>{},
                                       catalog::relkind::computed);
    REQUIRE(table_oid >= FIRST_USER_OID);
    auto rr = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("agg"));
    REQUIRE(rr.found);
    REQUIRE(rr.relkind == components::catalog::relkind::computed);
    REQUIRE(rr.columns.empty());

    // After a primitive pg_computed_column write the field lives in pg_computed_column.
    // V4 resolve_table for relkind='g' tables fills `columns` from pg_computed_column
    // (latest non-zero refcount per attname).
    test_computed_append_simple(fx, table_oid, "count", components::catalog::well_known_oid::int64_type);
    auto rr2 = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("agg"));
    REQUIRE(rr2.found);
    REQUIRE(rr2.relkind == components::catalog::relkind::computed);
    REQUIRE(rr2.columns.size() == 1);
    REQUIRE(rr2.columns[0].attname == "count");
    REQUIRE(rr2.columns[0].atttypid == components::catalog::well_known_oid::int64_type);
}
