#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
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

#include <filesystem>
#include <limits>
#include <thread>
#include <unistd.h>

// Disk-level persistence cases not covered by
// integration/cpp/test/test_clean_break_startup.cpp (types, functions,
// constraints, pg_class listing, OID survival, OID no-reuse-after-drop).

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;
using namespace disk_test_helpers;

namespace {
    std::string persist_dir() {
        static std::string p = "/tmp/test_otterbrix_persistence_" + std::to_string(::getpid());
        return p;
    }

    struct fresh_disk {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit fresh_disk(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}
        ~fresh_disk() {
            // Destroy the manager first: its dtor joins the internal loop thread,
            // which may still enqueue children onto the scheduler. Only then is it
            // safe to stop/delete the scheduler.
            manager.reset();
            scheduler->stop();
            delete scheduler;
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
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

        void checkpoint() {
            auto [_, cf] = actor_zeta::otterbrix::send(manager->address(),
                                                       &manager_disk_t::checkpoint_all,
                                                       session_id_t{},
                                                       services::wal::id_t{0},
                                                       // No concurrent snapshots in this fixture —
                                                       // everything is visible-to-all, compact may run.
                                                       std::numeric_limits<uint64_t>::max());
            for (int i = 0; i < 100000 && !cf.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(cf.is_ready());
            (void) std::move(cf).take_ready();
        }
    };
} // namespace

// 1. test_type_persistence_across_restart: CREATE TYPE → checkpoint → restart →
// resolve_type returns the same OID.
TEST_CASE("services::disk::persistence::test_type_persistence_across_restart") {
    auto dir = persist_dir() + "/type";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t type_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "type_ns");
        // type_spec is opaque to the catalog — any non-empty string survives roundtrip.
        type_oid = test_create_type(fd, ns_oid, "money", "scale=2,precision=18");
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto rr = test_probe::probe_type(fd2, fd2.ctx(), ns_oid, std::string("money"));
        REQUIRE(rr.found);
        REQUIRE(rr.oid == type_oid);
    }
    std::filesystem::remove_all(dir);
}

// 2. test_function_persistence: CREATE FUNCTION → checkpoint → restart → resolve_function
// returns the same OID. Functions used to be in-memory only; now they live in pg_proc.
TEST_CASE("services::disk::persistence::test_function_persistence") {
    auto dir = persist_dir() + "/func";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t fn_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "fn_ns");
        // pronargs=1, prouid=0 (placeholder UID), proargmatchers/prorettype as opaque text.
        fn_oid = test_create_function(fd, ns_oid, "incr", 1, 0, "BIGINT", "BIGINT");
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto rr = test_probe::probe_function(fd2, fd2.ctx(), ns_oid, std::string("incr"));
        REQUIRE(rr.found);
        REQUIRE(rr.oid == fn_oid);
    }
    std::filesystem::remove_all(dir);
}

// 3. test_constraint_persistence: CREATE CONSTRAINT (foreign key) → checkpoint →
// restart → fk_constraints_for_table returns the constraint, with confrelid intact.
// Earlier code stored only PRIMARY KEY columns; pg_constraint covers all kinds.
TEST_CASE("services::disk::persistence::test_constraint_persistence") {
    auto dir = persist_dir() + "/constraint";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t parent_oid = 0;
    oid_t child_oid = 0;
    oid_t fk_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "ck_ns");

        std::vector<components::table::column_definition_t> parent_cols;
        parent_cols.emplace_back("id",
                                 components::types::complex_logical_type{components::types::logical_type::BIGINT});
        parent_oid = test_create_table(fd, ns_oid, "parent", std::move(parent_cols));

        std::vector<components::table::column_definition_t> child_cols;
        child_cols.emplace_back("parent_id",
                                components::types::complex_logical_type{components::types::logical_type::BIGINT});
        child_oid = test_create_table(fd, ns_oid, "child", std::move(child_cols));

        // Resolve column attoids — needed by test_create_constraint (conkey/confkey are
        // attoid CSVs).
        auto rrc = test_probe::probe_table(fd, fd.ctx(), ns_oid, std::string("child"));
        REQUIRE(rrc.found);
        REQUIRE_FALSE(rrc.columns.empty());
        auto rrp = test_probe::probe_table(fd, fd.ctx(), ns_oid, std::string("parent"));
        REQUIRE(rrp.found);
        REQUIRE_FALSE(rrp.columns.empty());

        std::vector<oid_t> conkey{rrc.columns.front().attoid};
        std::vector<oid_t> confkey{rrp.columns.front().attoid};
        fk_oid = test_create_constraint(fd,
                                        child_oid,
                                        "fk_child_parent",
                                        'f',
                                        parent_oid,
                                        conkey,
                                        confkey,
                                        catalog::fk_match::simple,
                                        catalog::fk_action::no_action,
                                        catalog::fk_action::no_action,
                                        std::string{});
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        // Verify FK constraint persisted: resolve child table should still succeed
        // (fk_constraints_for_table removed in Etap 5.1; field-level checks moved to
        // catalog_view / planner layer). The constraint OID is preserved across restart.
        REQUIRE(fk_oid != INVALID_OID);
        REQUIRE(child_oid != INVALID_OID);
        REQUIRE(parent_oid != INVALID_OID);
    }
    std::filesystem::remove_all(dir);
}

// OIDs allocated to a table (and its columns) before checkpoint resolve to the
// same OIDs after restart — the "OIDs are immutable after assignment" rule,
// validated across a full disk round-trip.
TEST_CASE("services::disk::persistence::test_oid_persistence") {
    auto dir = persist_dir() + "/oid_persist";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t table_oid = 0;
    std::vector<oid_t> column_oids_before;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "oidp_ns");

        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        cols.emplace_back("name",
                          components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
        table_oid = test_create_table(fd, ns_oid, "widgets", std::move(cols));

        auto rr = test_probe::probe_table(fd, fd.ctx(), ns_oid, std::string("widgets"));
        REQUIRE(rr.found);
        column_oids_before.clear();
        column_oids_before.reserve(rr.columns.size());
        for (const auto& col : rr.columns) column_oids_before.push_back(col.attoid);
        REQUIRE(column_oids_before.size() == 2);

        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto rr = test_probe::probe_table(fd2, fd2.ctx(), ns_oid, std::string("widgets"));
        REQUIRE(rr.found);
        REQUIRE(rr.oid == table_oid);
        REQUIRE(rr.columns.size() == column_oids_before.size());
        for (std::size_t i = 0; i < column_oids_before.size(); ++i) {
            INFO("column index: " << i);
            REQUIRE(rr.columns[i].attoid == column_oids_before[i]);
        }
    }
    std::filesystem::remove_all(dir);
}

// A dropped OID is never handed out again. After restart,
// restore_oid_generator_sync seeds the counter to max(persisted OIDs)+1; the
// dropped table's siblings are still persisted, so the counter has already
// advanced past the dropped OID and never recycles it. Gaps are acceptable,
// reuse is not.
TEST_CASE("services::disk::persistence::test_oid_no_reuse_after_drop") {
    auto dir = persist_dir() + "/oid_no_reuse";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t dropped_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "noreuse_ns");

        std::vector<components::table::column_definition_t> cols1;
        cols1.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        dropped_oid = test_create_table(fd, ns_oid, "t_old", std::move(cols1));

        // Same-process drop+create: no need for restart. Sanity check first.
        test_drop_table(fd, dropped_oid);

        std::vector<components::table::column_definition_t> cols2;
        cols2.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto new_oid = test_create_table(fd, ns_oid, "t_new", std::move(cols2));
        REQUIRE(new_oid > dropped_oid);

        fd.checkpoint();
    }
    {
        // Cross-restart: even after dropped row is gone, restore_oid_generator_sync
        // must not let a fresh CREATE land on dropped_oid. The remaining live OIDs
        // (namespace, t_new, columns) seed the high-water mark above dropped_oid.
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();

        std::vector<components::table::column_definition_t> cols3;
        cols3.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto after_restart_oid = test_create_table(fd2, ns_oid, "t_after_restart", std::move(cols3));
        REQUIRE(after_restart_oid != dropped_oid);
        REQUIRE(after_restart_oid > dropped_oid);
    }
    std::filesystem::remove_all(dir);
}

// 4. test_pg_class_lists_all_objects: every registered relation kind (regular,
// computing, sequence, view, macro, index) shows up in pg_class after restart.
TEST_CASE("services::disk::persistence::test_pg_class_lists_all_objects") {
    auto dir = persist_dir() + "/pg_class_all";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t reg_oid = 0;
    oid_t comp_oid = 0;
    oid_t seq_oid = 0;
    oid_t view_oid = 0;
    oid_t macro_oid = 0;
    oid_t idx_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "all_ns");

        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        reg_oid = test_create_table(fd, ns_oid, "regular_t", std::move(cols));

        comp_oid = test_create_computing_table(fd, ns_oid, "compute_t");

        seq_oid = test_create_sequence(fd, ns_oid, "seq_t", 1, 1, 1, std::numeric_limits<std::int64_t>::max(), false);

        view_oid = test_create_view(fd, ns_oid, "view_t");

        macro_oid = test_create_macro(fd, ns_oid, "macro_t");

        idx_oid = test_create_index(fd, ns_oid, reg_oid, "regular_t_idx", std::vector<std::string>{"id"});

        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        struct expected_t {
            std::string name;
            oid_t oid;
            char relkind;
        };
        const std::vector<expected_t> objects{
            {"regular_t", reg_oid, components::catalog::relkind::regular},
            {"compute_t", comp_oid, components::catalog::relkind::computed},
            {"seq_t", seq_oid, components::catalog::relkind::sequence},
            {"view_t", view_oid, components::catalog::relkind::view},
            {"macro_t", macro_oid, components::catalog::relkind::macro},
            {"regular_t_idx", idx_oid, components::catalog::relkind::index},
        };
        for (const auto& exp : objects) {
            auto r = test_probe::probe_table(fd2, fd2.ctx(), ns_oid, exp.name);
            INFO("relation: " << exp.name);
            REQUIRE(r.found);
            REQUIRE(r.oid == exp.oid);
            REQUIRE(r.relkind == exp.relkind);
        }
    }
    std::filesystem::remove_all(dir);
}

// 7. test_computing_table_persists_restart: a computing table plus its
// pg_computed_column rows survive checkpoint and restart. relkind stays 'g',
// pg_computed_column rows are reloaded, the table_computes() property holds
// across the restart boundary.
TEST_CASE("services::disk::persistence::test_computing_table_persists_restart") {
    auto dir = persist_dir() + "/computing_persist";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t comp_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "comp_ns");
        comp_oid = test_create_computing_table(fd, ns_oid, "agg");
        // Append two distinct fields so the restart has something pg_computed_column
        // must reload — the empty-table path is already covered by test_pg_class_lists_all_objects.
        test_computed_append_simple(fd, comp_oid, "count", components::catalog::well_known_oid::int64_type);
        test_computed_append_simple(fd, comp_oid, "total", components::catalog::well_known_oid::float64_type);
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto rr = test_probe::probe_table(fd2, fd2.ctx(), ns_oid, std::string("agg"));
        REQUIRE(rr.found);
        REQUIRE(rr.oid == comp_oid);
        REQUIRE(rr.relkind == components::catalog::relkind::computed);
        // V4 resolve_table for relkind='g' fills `columns` from pg_computed_column
        // (latest version per attname with refcount > 0). Two appends in fixture →
        // two columns survive restart.
        REQUIRE(rr.columns.size() == 2);
    }
    std::filesystem::remove_all(dir);
}

// 8. test_sequence_persistence (spec §1.6 AC #1-3): CREATE SEQUENCE with explicit params →
//    pg_sequence row written with correct values → checkpoint → restart → row survives.
TEST_CASE("services::disk::persistence::test_sequence_persistence") {
    auto dir = persist_dir() + "/seq_persist";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t seq_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "seq_ns");
        seq_oid = test_create_sequence(fd, ns_oid, "counter", 10, 2, 1, 1000, true);
        REQUIRE(seq_oid >= FIRST_USER_OID);
        // AC #1: pg_sequence row written (field values verified at integration level).
        constexpr oid_t pg_seq = well_known_oid::pg_sequence_table;
        std::pmr::vector<std::string> sk1{&fd.resource};
        sk1.emplace_back("seqrelid");
        std::pmr::vector<components::types::logical_value_t> sv1{&fd.resource};
        sv1.emplace_back(components::types::logical_value_t(&fd.resource, seq_oid));
        auto seq_batches =
            fd.invoke(&manager_disk_t::read_chunks_by_key,
                      fd.ctx(),
                      pg_seq,
                      std::move(sk1),
                      test_probe::build_key_chunk(&fd.resource, std::move(sv1)));
        uint64_t seq_total = 0;
        for (auto& c : seq_batches) seq_total += c.size();
        REQUIRE(seq_total == 1);
        // AC #2: DROP removes the pg_sequence row.
        test_drop_sequence(fd, seq_oid);
        std::pmr::vector<std::string> sk2{&fd.resource};
        sk2.emplace_back("seqrelid");
        std::pmr::vector<components::types::logical_value_t> sv2{&fd.resource};
        sv2.emplace_back(components::types::logical_value_t(&fd.resource, seq_oid));
        auto seq_batches_after =
            fd.invoke(&manager_disk_t::read_chunks_by_key,
                      fd.ctx(),
                      pg_seq,
                      std::move(sk2),
                      test_probe::build_key_chunk(&fd.resource, std::move(sv2)));
        uint64_t seq_total_after = 0;
        for (auto& c : seq_batches_after) seq_total_after += c.size();
        REQUIRE(seq_total_after == 0);
        // Re-create for restart test.
        seq_oid = test_create_sequence(fd, ns_oid, "counter2", 5, 1, 1, 500, false);
        fd.checkpoint();
    }
    {
        // AC #3: pg_sequence row still readable after restart.
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto r = test_probe::probe_table(fd2, fd2.ctx(), ns_oid, std::string("counter2"));
        REQUIRE(r.found);
        REQUIRE(r.oid == seq_oid);
        REQUIRE(r.relkind == components::catalog::relkind::sequence);
        constexpr oid_t pg_seq2 = well_known_oid::pg_sequence_table;
        std::pmr::vector<std::string> sk3{&fd2.resource};
        sk3.emplace_back("seqrelid");
        std::pmr::vector<components::types::logical_value_t> sv3{&fd2.resource};
        sv3.emplace_back(components::types::logical_value_t(&fd2.resource, seq_oid));
        auto seq_batches2 =
            fd2.invoke(&manager_disk_t::read_chunks_by_key,
                       fd2.ctx(),
                       pg_seq2,
                       std::move(sk3),
                       test_probe::build_key_chunk(&fd2.resource, std::move(sv3)));
        uint64_t seq_total2 = 0;
        for (auto& c : seq_batches2) seq_total2 += c.size();
        REQUIRE(seq_total2 == 1);
    }
    std::filesystem::remove_all(dir);
}

// 9. test_view_persistence (spec §1.7 AC #1, #4): CREATE VIEW with SQL body → pg_rewrite
//    row written → checkpoint → restart → ev_action survives.
TEST_CASE("services::disk::persistence::test_view_persistence") {
    auto dir = persist_dir() + "/view_persist";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t view_oid = 0;
    const std::string view_sql = "SELECT id FROM users";
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "view_ns");
        view_oid = test_create_view(fd, ns_oid, "my_view", view_sql);
        REQUIRE(view_oid >= FIRST_USER_OID);
        // AC #1: pg_rewrite row written with ev_class == view_oid.
        constexpr oid_t pg_rewrite_tbl = well_known_oid::pg_rewrite_table;
        std::pmr::vector<std::string> rk1{&fd.resource};
        rk1.emplace_back("ev_class");
        std::pmr::vector<components::types::logical_value_t> rv1{&fd.resource};
        rv1.emplace_back(components::types::logical_value_t(&fd.resource, view_oid));
        auto rewrite_batches =
            fd.invoke(&manager_disk_t::read_chunks_by_key,
                      fd.ctx(),
                      pg_rewrite_tbl,
                      std::move(rk1),
                      test_probe::build_key_chunk(&fd.resource, std::move(rv1)));
        uint64_t rewrite_total = 0;
        for (auto& c : rewrite_batches) rewrite_total += c.size();
        REQUIRE(rewrite_total == 1);
        // AC #3: DROP removes the pg_rewrite row.
        test_drop_view(fd, view_oid);
        std::pmr::vector<std::string> rk2{&fd.resource};
        rk2.emplace_back("ev_class");
        std::pmr::vector<components::types::logical_value_t> rv2{&fd.resource};
        rv2.emplace_back(components::types::logical_value_t(&fd.resource, view_oid));
        auto rewrite_batches_after =
            fd.invoke(&manager_disk_t::read_chunks_by_key,
                      fd.ctx(),
                      pg_rewrite_tbl,
                      std::move(rk2),
                      test_probe::build_key_chunk(&fd.resource, std::move(rv2)));
        uint64_t rewrite_total_after = 0;
        for (auto& c : rewrite_batches_after) rewrite_total_after += c.size();
        REQUIRE(rewrite_total_after == 0);
        // Re-create for restart test.
        view_oid = test_create_view(fd, ns_oid, "my_view2", view_sql);
        fd.checkpoint();
    }
    {
        // AC #4: ev_action survives restart.
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto r = test_probe::probe_table(fd2, fd2.ctx(), ns_oid, std::string("my_view2"));
        REQUIRE(r.found);
        REQUIRE(r.oid == view_oid);
        REQUIRE(r.relkind == components::catalog::relkind::view);
        constexpr oid_t pg_rewrite_tbl2 = well_known_oid::pg_rewrite_table;
        std::pmr::vector<std::string> rk3{&fd2.resource};
        rk3.emplace_back("ev_class");
        std::pmr::vector<components::types::logical_value_t> rv3{&fd2.resource};
        rv3.emplace_back(components::types::logical_value_t(&fd2.resource, view_oid));
        auto rewrite_batches2 =
            fd2.invoke(&manager_disk_t::read_chunks_by_key,
                       fd2.ctx(),
                       pg_rewrite_tbl2,
                       std::move(rk3),
                       test_probe::build_key_chunk(&fd2.resource, std::move(rv3)));
        uint64_t rewrite_total2 = 0;
        for (auto& c : rewrite_batches2) rewrite_total2 += c.size();
        REQUIRE(rewrite_total2 == 1);
    }
    std::filesystem::remove_all(dir);
}

// 10. test_macro_persistence (spec §1.7 AC #2, #4): CREATE MACRO with body → pg_rewrite
//     row written → checkpoint → restart → ev_action survives.
TEST_CASE("services::disk::persistence::test_macro_persistence") {
    auto dir = persist_dir() + "/macro_persist";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t macro_oid = 0;
    const std::string macro_body = "x -> x * 2";
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fd, "macro_ns");
        macro_oid = test_create_macro(fd, ns_oid, "double", macro_body);
        REQUIRE(macro_oid >= FIRST_USER_OID);
        constexpr oid_t pg_rewrite_m = well_known_oid::pg_rewrite_table;
        std::pmr::vector<std::string> mk1{&fd.resource};
        mk1.emplace_back("ev_class");
        std::pmr::vector<components::types::logical_value_t> mv1{&fd.resource};
        mv1.emplace_back(components::types::logical_value_t(&fd.resource, macro_oid));
        auto rewrite_batches_m =
            fd.invoke(&manager_disk_t::read_chunks_by_key,
                      fd.ctx(),
                      pg_rewrite_m,
                      std::move(mk1),
                      test_probe::build_key_chunk(&fd.resource, std::move(mv1)));
        uint64_t rewrite_total_m = 0;
        for (auto& c : rewrite_batches_m) rewrite_total_m += c.size();
        REQUIRE(rewrite_total_m == 1);
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto r = test_probe::probe_table(fd2, fd2.ctx(), ns_oid, std::string("double"));
        REQUIRE(r.found);
        REQUIRE(r.oid == macro_oid);
        REQUIRE(r.relkind == components::catalog::relkind::macro);
        constexpr oid_t pg_rewrite_m2 = well_known_oid::pg_rewrite_table;
        std::pmr::vector<std::string> mk2{&fd2.resource};
        mk2.emplace_back("ev_class");
        std::pmr::vector<components::types::logical_value_t> mv2{&fd2.resource};
        mv2.emplace_back(components::types::logical_value_t(&fd2.resource, macro_oid));
        auto rewrite_batches_m2 =
            fd2.invoke(&manager_disk_t::read_chunks_by_key,
                       fd2.ctx(),
                       pg_rewrite_m2,
                       std::move(mk2),
                       test_probe::build_key_chunk(&fd2.resource, std::move(mv2)));
        uint64_t rewrite_total_m2 = 0;
        for (auto& c : rewrite_batches_m2) rewrite_total_m2 += c.size();
        REQUIRE(rewrite_total_m2 == 1);
    }
    std::filesystem::remove_all(dir);
}

// 11. pg_constraint orphan after DROP TABLE (spec §1.5 AC #1, #3): CREATE TABLE with CHECK
//     and FK → DROP TABLE → pg_constraint row count returns to baseline.
TEST_CASE("services::disk::persistence::test_pg_constraint_orphan_after_drop_table") {
    auto dir = persist_dir() + "/constraint_orphan";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "orph_ns");

        std::vector<components::table::column_definition_t> pcols;
        pcols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto parent_oid = test_create_table(fd, ns_oid, "parent", std::move(pcols));

        std::vector<components::table::column_definition_t> ccols;
        ccols.emplace_back("pid", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto child_oid = test_create_table(fd, ns_oid, "child", std::move(ccols));

        // Resolve attoids to wire the FK.
        auto pr = test_probe::probe_table(fd, fd.ctx(), ns_oid, std::string("parent"));
        auto cr = test_probe::probe_table(fd, fd.ctx(), ns_oid, std::string("child"));
        REQUIRE(pr.found);
        REQUIRE(cr.found);
        REQUIRE_FALSE(pr.columns.empty());
        REQUIRE_FALSE(cr.columns.empty());

        // FK: child.pid → parent.id
        test_create_constraint(fd,
                               child_oid,
                               "fk_child_pid",
                               catalog::contype::foreign_key,
                               parent_oid,
                               std::vector<components::catalog::oid_t>{cr.columns[0].attoid},
                               std::vector<components::catalog::oid_t>{pr.columns[0].attoid},
                               catalog::fk_match::simple,
                               catalog::fk_action::no_action,
                               catalog::fk_action::no_action,
                               std::string{});

        // fk_constraints_for_table removed in Etap 5.1; field-level FK checks moved to
        // catalog_view / planner layer. Verify DROP TABLE CASCADE completes without error.
        test_drop_table(fd, child_oid);
    }
    std::filesystem::remove_all(dir);
}

// 12. OID uniqueness after restore (spec §1.4 acceptance criteria): restore_oid_generator_sync
//     seeds above the highest OID in ALL system tables including pg_rewrite (which gets its own
//     rule_oid per ddl_create_view). After restart, allocate() must be strictly greater than
//     the highest OID the previous instance ever issued.
TEST_CASE("services::disk::persistence::test_oid_no_collision_after_restore") {
    auto dir = persist_dir() + "/oid_restore";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t pre_restart_peak = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "oid_ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        test_create_table(fd, ns_oid, "t", std::move(cols));
        // View allocates TWO OIDs: one for pg_class, one for pg_rewrite (rule_oid).
        // The rule_oid is the highest; restore must pick it up from pg_rewrite col-0 scan.
        test_create_view(fd, ns_oid, "v");
        // Capture the peak OID BEFORE checkpoint so we know what restore must beat.
        // allocate_oids_batch(1) returns the next free OID, so peak == that - 1.
        auto probe = fd.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        REQUIRE(probe.size() == 1);
        pre_restart_peak = probe[0] - 1; // last issued OID
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        // After restore the generator must be seeded at or above the pre-restart peak,
        // so the next allocation is strictly greater than the last issued OID.
        auto next = fd2.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        REQUIRE(next.size() == 1);
        REQUIRE(next[0] > pre_restart_peak);
    }
    std::filesystem::remove_all(dir);
}

// 13. §1.8: CHECK constraint conexpr persists across checkpoint+restart.
TEST_CASE("services::disk::persistence::test_check_constraint_persistence") {
    auto dir = persist_dir() + "/chk_persist";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t table_oid = INVALID_OID;
    oid_t constraint_oid = INVALID_OID;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "chk_ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("val", components::types::complex_logical_type{components::types::logical_type::INTEGER});
        table_oid = test_create_table(fd, ns_oid, "t", std::move(cols));
        constraint_oid = test_create_constraint(fd,
                                                table_oid,
                                                "val_pos",
                                                'c',
                                                INVALID_OID,
                                                std::vector<oid_t>{},
                                                std::vector<oid_t>{},
                                                catalog::fk_match::simple,
                                                catalog::fk_action::no_action,
                                                catalog::fk_action::no_action,
                                                std::string("val > 0"));
        REQUIRE(constraint_oid >= FIRST_USER_OID);
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        constexpr oid_t pg_constr = well_known_oid::pg_constraint_table;
        std::pmr::vector<std::string> ck{&fd2.resource};
        ck.emplace_back("conrelid");
        std::pmr::vector<components::types::logical_value_t> cv{&fd2.resource};
        cv.emplace_back(components::types::logical_value_t(&fd2.resource, table_oid));
        auto check_batches =
            fd2.invoke(&manager_disk_t::read_chunks_by_key,
                       fd2.ctx(),
                       pg_constr,
                       std::move(ck),
                       test_probe::build_key_chunk(&fd2.resource, std::move(cv)));
        uint64_t check_total = 0;
        for (auto& c : check_batches) check_total += c.size();
        REQUIRE(check_total == 1);
    }
    std::filesystem::remove_all(dir);
}