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
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <limits>
#include <unistd.h>

// Phase-5 persistence tests (catalog-migration-to-postgresql-style.md §9, §14 lines
// 2746–2757). These cover the doc's named persistence cases that aren't already
// represented in integration/cpp/test/test_clean_break_startup.cpp:
//   test_type_persistence_across_restart
//   test_function_persistence
//   test_constraint_persistence
//   test_pg_class_lists_all_objects
//   test_oid_persistence              (Phase-0 §14, OID survives checkpoint→load)
//   test_oid_no_reuse_after_drop      (Phase-0 §14, dropped OIDs leave gaps)
// The remaining doc-named tests (sequence/view/macro/index/load_sequence/catalog_otbx_not_needed)
// are already covered there with different names; aliasing is task #7.

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

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
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            manager->set_run_fn([this] { scheduler->run(10000); });
        }
        ~fresh_disk() {
            scheduler->stop();
            delete scheduler;
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        void checkpoint() {
            auto [_, cf] = actor_zeta::otterbrix::send(manager->address(),
                                                        &manager_disk_t::checkpoint_all,
                                                        session_id_t{},
                                                        services::wal::id_t{0});
            scheduler->run(10000);
            (void)std::move(cf).get();
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("type_ns"));
        ns_oid = rns.created_oid;
        // type_spec is opaque to the catalog — any non-empty string survives roundtrip.
        auto rt = fd.invoke(&manager_disk_t::ddl_create_type, fd.ctx(),
                             ns_oid, std::string("money"), std::string("scale=2,precision=18"));
        type_oid = rt.created_oid;
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto rr = fd2.invoke(&manager_disk_t::resolve_type, fd2.ctx(),
                              ns_oid, std::string("money"), std::uint64_t{0});
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("fn_ns"));
        ns_oid = rns.created_oid;
        // pronargs=1, prouid=0 (placeholder UID), proargmatchers/prorettype as opaque text.
        auto rf = fd.invoke(&manager_disk_t::ddl_create_function, fd.ctx(),
                             ns_oid, std::string("incr"),
                             std::int32_t{1}, std::int64_t{0},
                             std::string("BIGINT"), std::string("BIGINT"));
        fn_oid = rf.created_oid;
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto rr = fd2.invoke(&manager_disk_t::resolve_function, fd2.ctx(),
                              ns_oid, std::string("incr"), std::uint64_t{0});
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("ck_ns"));

        std::vector<components::table::column_definition_t> parent_cols;
        parent_cols.emplace_back("id",
                                  components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rp = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), rns.created_oid,
                             std::string("parent"), std::move(parent_cols), catalog::relkind::regular);
        parent_oid = rp.created_oid;

        std::vector<components::table::column_definition_t> child_cols;
        child_cols.emplace_back("parent_id",
                                  components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rc = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), rns.created_oid,
                             std::string("child"), std::move(child_cols), catalog::relkind::regular);
        child_oid = rc.created_oid;

        // Resolve column attoids — needed by ddl_create_constraint (conkey/confkey are
        // attoid CSVs).
        auto rrc = fd.invoke(&manager_disk_t::resolve_table, fd.ctx(),
                              rns.created_oid, std::string("child"), std::uint64_t{0});
        REQUIRE(rrc.found);
        REQUIRE_FALSE(rrc.columns.empty());
        auto rrp = fd.invoke(&manager_disk_t::resolve_table, fd.ctx(),
                              rns.created_oid, std::string("parent"), std::uint64_t{0});
        REQUIRE(rrp.found);
        REQUIRE_FALSE(rrp.columns.empty());

        std::vector<oid_t> conkey{rrc.columns.front().attoid};
        std::vector<oid_t> confkey{rrp.columns.front().attoid};
        auto rcc = fd.invoke(&manager_disk_t::ddl_create_constraint, fd.ctx(),
                              child_oid, std::string("fk_child_parent"), char{'f'},
                              parent_oid, conkey, confkey,
                              catalog::fk_match::simple, catalog::fk_action::no_action, catalog::fk_action::no_action, std::string{});
        fk_oid = rcc.created_oid;
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto fks = fd2.manager->fk_constraints_for_table(child_oid);
        REQUIRE_FALSE(fks.empty());
        bool found = false;
        for (const auto& fk : fks) {
            if (fk.constraint_oid == fk_oid && fk.ref_table_oid == parent_oid) {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }
    std::filesystem::remove_all(dir);
}

// 5. test_oid_persistence: OIDs allocated to a table (and its columns) before
// checkpoint resolve to the same OIDs after restart. Validates Phase-0 design
// rule "OIDs are immutable after assignment" (catalog-migration-to-postgresql-style.md §4)
// across the full disk round-trip.
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("oidp_ns"));
        ns_oid = rns.created_oid;

        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id",
                           components::types::complex_logical_type{components::types::logical_type::BIGINT});
        cols.emplace_back("name",
                           components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
        auto rt = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), ns_oid,
                             std::string("widgets"), std::move(cols), catalog::relkind::regular);
        table_oid = rt.created_oid;

        auto rr = fd.invoke(&manager_disk_t::resolve_table, fd.ctx(),
                              ns_oid, std::string("widgets"), std::uint64_t{0});
        REQUIRE(rr.found);
        column_oids_before.clear();
        column_oids_before.reserve(rr.columns.size());
        for (const auto& col : rr.columns) column_oids_before.push_back(col.attoid);
        REQUIRE(column_oids_before.size() == 2);

        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto rr = fd2.invoke(&manager_disk_t::resolve_table, fd2.ctx(),
                              ns_oid, std::string("widgets"), std::uint64_t{0});
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

// 6. test_oid_no_reuse_after_drop: a dropped OID is never handed out again. After
// restart, restore_oid_generator_sync seeds the counter to max(persisted OIDs)+1
// — but persisted OIDs include the dropped table's siblings, so even though the
// row is gone, the counter has already advanced past it (the OID generator never
// recycles). Validates "OIDs are never reused after DROP (gaps are acceptable)"
// (catalog-migration-to-postgresql-style.md §4 design rule 2).
TEST_CASE("services::disk::persistence::test_oid_no_reuse_after_drop") {
    auto dir = persist_dir() + "/oid_no_reuse";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t dropped_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("noreuse_ns"));
        ns_oid = rns.created_oid;

        std::vector<components::table::column_definition_t> cols1;
        cols1.emplace_back("id",
                            components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt1 = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), ns_oid,
                              std::string("t_old"), std::move(cols1), catalog::relkind::regular);
        dropped_oid = rt1.created_oid;

        // Same-process drop+create: no need for restart. Sanity check first.
        auto rdrop = fd.invoke(&manager_disk_t::ddl_drop_table, fd.ctx(),
                                dropped_oid, drop_behavior_t::cascade_);
        REQUIRE(rdrop.result);
        REQUIRE_FALSE(rdrop.result->is_error());

        std::vector<components::table::column_definition_t> cols2;
        cols2.emplace_back("id",
                            components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt2 = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), ns_oid,
                              std::string("t_new"), std::move(cols2), catalog::relkind::regular);
        REQUIRE(rt2.created_oid > dropped_oid);

        fd.checkpoint();
    }
    {
        // Cross-restart: even after dropped row is gone, restore_oid_generator_sync
        // must not let a fresh CREATE land on dropped_oid. The remaining live OIDs
        // (namespace, t_new, columns) seed the high-water mark above dropped_oid.
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();

        std::vector<components::table::column_definition_t> cols3;
        cols3.emplace_back("id",
                            components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt3 = fd2.invoke(&manager_disk_t::ddl_create_table, fd2.ctx(), ns_oid,
                               std::string("t_after_restart"), std::move(cols3), catalog::relkind::regular);
        REQUIRE(rt3.created_oid != dropped_oid);
        REQUIRE(rt3.created_oid > dropped_oid);
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("all_ns"));
        ns_oid = rns.created_oid;

        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id",
                           components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), ns_oid,
                             std::string("regular_t"), std::move(cols), catalog::relkind::regular);
        reg_oid = rt.created_oid;

        auto rc = fd.invoke(&manager_disk_t::ddl_create_computing_table, fd.ctx(),
                             ns_oid, std::string("compute_t"));
        comp_oid = rc.created_oid;

        auto rs = fd.invoke(&manager_disk_t::ddl_create_sequence, fd.ctx(),
                             ns_oid, std::string("seq_t"),
                             std::int64_t{1}, std::int64_t{1}, std::int64_t{1},
                             std::int64_t{std::numeric_limits<std::int64_t>::max()}, bool{false});
        seq_oid = rs.created_oid;

        auto rv = fd.invoke(&manager_disk_t::ddl_create_view, fd.ctx(),
                             ns_oid, std::string("view_t"), std::string{});
        view_oid = rv.created_oid;

        auto rm = fd.invoke(&manager_disk_t::ddl_create_macro, fd.ctx(),
                             ns_oid, std::string("macro_t"), std::string{});
        macro_oid = rm.created_oid;

        auto ri = fd.invoke(&manager_disk_t::ddl_create_index, fd.ctx(),
                             ns_oid, reg_oid, std::string("regular_t_idx"),
                             std::vector<std::string>{"id"});
        idx_oid = ri.created_oid;

        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        struct expected_t { std::string name; oid_t oid; char relkind; };
        const std::vector<expected_t> objects{
            {"regular_t", reg_oid, 'r'},
            {"compute_t", comp_oid, 'g'},
            {"seq_t",     seq_oid, 'S'},
            {"view_t",    view_oid, 'v'},
            {"macro_t",   macro_oid, 'm'},
            {"regular_t_idx", idx_oid, 'i'},
        };
        for (const auto& exp : objects) {
            auto r = fd2.invoke(&manager_disk_t::resolve_table, fd2.ctx(),
                                  ns_oid, exp.name, std::uint64_t{0});
            INFO("relation: " << exp.name);
            REQUIRE(r.found);
            REQUIRE(r.oid == exp.oid);
            REQUIRE(r.relkind == exp.relkind);
        }
    }
    std::filesystem::remove_all(dir);
}

// 7. test_computing_table_persists_restart: ddl_create_computing_table +
// ddl_computed_append survive checkpoint and restart. relkind stays 'g',
// pg_computed_column rows are reloaded, the table_computes() property holds
// across the restart boundary. Validates Phase-1 C1 (pg_computed_column)
// persistence (catalog-migration-to-postgresql-style.md §14).
TEST_CASE("services::disk::persistence::test_computing_table_persists_restart") {
    auto dir = persist_dir() + "/computing_persist";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    oid_t ns_oid = 0;
    oid_t comp_oid = 0;
    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(),
                              std::string("comp_ns"));
        ns_oid = rns.created_oid;
        auto rc = fd.invoke(&manager_disk_t::ddl_create_computing_table, fd.ctx(),
                              ns_oid, std::string("agg"));
        comp_oid = rc.created_oid;
        // Append two distinct fields so the restart has something pg_computed_column
        // must reload — the empty-table path is already covered by test_pg_class_lists_all_objects.
        fd.invoke(&manager_disk_t::ddl_computed_append, fd.ctx(),
                   comp_oid, std::string("count"),
                   components::catalog::well_known_oid::int64_type);
        fd.invoke(&manager_disk_t::ddl_computed_append, fd.ctx(),
                   comp_oid, std::string("total"),
                   components::catalog::well_known_oid::float64_type);
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto rr = fd2.invoke(&manager_disk_t::resolve_table, fd2.ctx(),
                              ns_oid, std::string("agg"), std::uint64_t{0});
        REQUIRE(rr.found);
        REQUIRE(rr.oid == comp_oid);
        REQUIRE(rr.relkind == 'g');
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("seq_ns"));
        ns_oid = rns.created_oid;
        auto rs = fd.invoke(&manager_disk_t::ddl_create_sequence, fd.ctx(),
                             ns_oid, std::string("counter"),
                             std::int64_t{10},   // start
                             std::int64_t{2},    // increment
                             std::int64_t{1},    // min
                             std::int64_t{1000}, // max
                             bool{true});        // cycle
        seq_oid = rs.created_oid;
        REQUIRE(seq_oid >= FIRST_USER_OID);
        // AC #1: pg_sequence row written with correct parameters.
        auto p = fd.manager->sequence_params_for(seq_oid);
        REQUIRE(p.has_value());
        REQUIRE(p->seqstart == 10);
        REQUIRE(p->seqincrement == 2);
        REQUIRE(p->seqmin == 1);
        REQUIRE(p->seqmax == 1000);
        REQUIRE(p->seqcycle == true);
        // AC #2: DROP removes the pg_sequence row.
        fd.invoke(&manager_disk_t::ddl_drop_sequence, fd.ctx(), seq_oid, drop_behavior_t::cascade_);
        auto p_after_drop = fd.manager->sequence_params_for(seq_oid);
        REQUIRE_FALSE(p_after_drop.has_value());
        // Re-create for restart test.
        auto rs2 = fd.invoke(&manager_disk_t::ddl_create_sequence, fd.ctx(),
                              ns_oid, std::string("counter2"),
                              std::int64_t{5}, std::int64_t{1}, std::int64_t{1},
                              std::int64_t{500}, bool{false});
        seq_oid = rs2.created_oid;
        fd.checkpoint();
    }
    {
        // AC #3: pg_sequence row still readable after restart.
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto r = fd2.invoke(&manager_disk_t::resolve_table, fd2.ctx(),
                             ns_oid, std::string("counter2"), std::uint64_t{0});
        REQUIRE(r.found);
        REQUIRE(r.oid == seq_oid);
        REQUIRE(r.relkind == 'S');
        auto p = fd2.manager->sequence_params_for(seq_oid);
        REQUIRE(p.has_value());
        REQUIRE(p->seqstart == 5);
        REQUIRE(p->seqmax == 500);
        REQUIRE(p->seqcycle == false);
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("view_ns"));
        ns_oid = rns.created_oid;
        auto rv = fd.invoke(&manager_disk_t::ddl_create_view, fd.ctx(),
                             ns_oid, std::string("my_view"), view_sql);
        view_oid = rv.created_oid;
        REQUIRE(view_oid >= FIRST_USER_OID);
        // AC #1: pg_rewrite row written with SQL body.
        auto body = fd.manager->rewrite_ev_action_for(view_oid);
        REQUIRE(body == view_sql);
        // AC #3: DROP removes the pg_rewrite row.
        fd.invoke(&manager_disk_t::ddl_drop_view, fd.ctx(), view_oid, drop_behavior_t::cascade_);
        auto body_after_drop = fd.manager->rewrite_ev_action_for(view_oid);
        REQUIRE(body_after_drop.empty());
        // Re-create for restart test.
        auto rv2 = fd.invoke(&manager_disk_t::ddl_create_view, fd.ctx(),
                              ns_oid, std::string("my_view2"), view_sql);
        view_oid = rv2.created_oid;
        fd.checkpoint();
    }
    {
        // AC #4: ev_action survives restart.
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto r = fd2.invoke(&manager_disk_t::resolve_table, fd2.ctx(),
                             ns_oid, std::string("my_view2"), std::uint64_t{0});
        REQUIRE(r.found);
        REQUIRE(r.oid == view_oid);
        REQUIRE(r.relkind == 'v');
        auto body = fd2.manager->rewrite_ev_action_for(view_oid);
        REQUIRE(body == view_sql);
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("macro_ns"));
        ns_oid = rns.created_oid;
        auto rm = fd.invoke(&manager_disk_t::ddl_create_macro, fd.ctx(),
                             ns_oid, std::string("double"), macro_body);
        macro_oid = rm.created_oid;
        REQUIRE(macro_oid >= FIRST_USER_OID);
        auto body = fd.manager->rewrite_ev_action_for(macro_oid);
        REQUIRE(body == macro_body);
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto r = fd2.invoke(&manager_disk_t::resolve_table, fd2.ctx(),
                             ns_oid, std::string("double"), std::uint64_t{0});
        REQUIRE(r.found);
        REQUIRE(r.oid == macro_oid);
        REQUIRE(r.relkind == 'm');
        auto body = fd2.manager->rewrite_ev_action_for(macro_oid);
        REQUIRE(body == macro_body);
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("orph_ns"));
        auto ns_oid = rns.created_oid;

        std::vector<components::table::column_definition_t> pcols;
        pcols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rp = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), ns_oid,
                             std::string("parent"), std::move(pcols), catalog::relkind::regular);

        std::vector<components::table::column_definition_t> ccols;
        ccols.emplace_back("pid", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rc = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), ns_oid,
                             std::string("child"), std::move(ccols), catalog::relkind::regular);

        // Resolve attoids to wire the FK.
        auto pr = fd.invoke(&manager_disk_t::resolve_table, fd.ctx(), ns_oid,
                             std::string("parent"), std::uint64_t{0});
        auto cr = fd.invoke(&manager_disk_t::resolve_table, fd.ctx(), ns_oid,
                             std::string("child"), std::uint64_t{0});
        REQUIRE(pr.found);
        REQUIRE(cr.found);
        REQUIRE_FALSE(pr.columns.empty());
        REQUIRE_FALSE(cr.columns.empty());

        // FK: child.pid → parent.id
        fd.invoke(&manager_disk_t::ddl_create_constraint, fd.ctx(),
                   rc.created_oid, std::string("fk_child_pid"),
                   catalog::contype::foreign_key, rp.created_oid,
                   std::vector<components::catalog::oid_t>{cr.columns[0].attoid},
                   std::vector<components::catalog::oid_t>{pr.columns[0].attoid},
                   catalog::fk_match::simple, catalog::fk_action::no_action, catalog::fk_action::no_action, std::string{});

        auto fks_before = fd.manager->fk_constraints_for_table(rc.created_oid);
        REQUIRE(fks_before.size() == 1);

        // DROP TABLE child with CASCADE.
        fd.invoke(&manager_disk_t::ddl_drop_table, fd.ctx(), rc.created_oid,
                   drop_behavior_t::cascade_);

        // After drop: no FK constraints referencing the dropped table should remain.
        auto fks_after = fd.manager->fk_constraints_for_table(rc.created_oid);
        REQUIRE(fks_after.empty());
        auto fks_ref_after = fd.manager->fk_constraints_referencing(rp.created_oid);
        REQUIRE(fks_ref_after.empty());
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("oid_ns"));
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), rns.created_oid,
                   std::string("t"), std::move(cols), catalog::relkind::regular);
        // View allocates TWO OIDs: one for pg_class, one for pg_rewrite (rule_oid).
        // The rule_oid is the highest; restore must pick it up from pg_rewrite col-0 scan.
        fd.invoke(&manager_disk_t::ddl_create_view, fd.ctx(),
                   rns.created_oid, std::string("v"), std::string{});
        // Capture the peak OID BEFORE checkpoint so we know what restore must beat.
        pre_restart_peak = fd.manager->oid_gen().peek() - 1; // last issued OID
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        // After restore the generator must be seeded at or above the pre-restart peak.
        auto new_oid = fd2.manager->oid_gen().allocate();
        REQUIRE(new_oid > pre_restart_peak);
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
        auto rns = fd.invoke(&manager_disk_t::ddl_create_namespace, fd.ctx(), std::string("chk_ns"));
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("val", components::types::complex_logical_type{components::types::logical_type::INTEGER});
        auto rt = fd.invoke(&manager_disk_t::ddl_create_table, fd.ctx(), rns.created_oid,
                              std::string("t"), std::move(cols), catalog::relkind::regular);
        table_oid = rt.created_oid;
        auto rc = fd.invoke(&manager_disk_t::ddl_create_constraint, fd.ctx(), table_oid,
                              std::string("val_pos"), char{'c'},
                              INVALID_OID,
                              std::vector<oid_t>{},
                              std::vector<oid_t>{},
                              catalog::fk_match::simple, catalog::fk_action::no_action, char{'a'},
                              std::string("val > 0"));
        constraint_oid = rc.created_oid;
        REQUIRE(constraint_oid >= FIRST_USER_OID);
        fd.checkpoint();
    }
    {
        fresh_disk fd2(dir);
        fd2.manager->load_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        auto checks = fd2.manager->check_constraints_for_table(table_oid);
        REQUIRE(checks.size() == 1);
        REQUIRE(checks[0].constraint_oid == constraint_oid);
        REQUIRE(checks[0].conexpr == "val > 0");
    }
    std::filesystem::remove_all(dir);
}