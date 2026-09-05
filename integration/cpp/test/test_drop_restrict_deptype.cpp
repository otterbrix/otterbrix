// ============================================================================
// WHAT `RESTRICT` AND `CASCADE` MEAN ONCE THE WRITTEN WORD IS HONOURED.
//
// The whole chain is wired: gram.y's opt_drop_behavior yields DROP_RESTRICT for
// the written word and the empty alternative alike (in PostgreSQL they are one
// thing), drop_behavior_of reads it as restrict_ (#638, PostgreSQL parity), and
// node_drop_t::behavior() / alter_table_subcommand_t::behavior reach the two
// operators through components/planner/planner.cpp. This file still builds the
// two plans BY HAND with behavior = restrict_ so the pins hold independently of
// the SQL front end; test_drop_default_restrict.cpp pins the SQL route.
//
// The three cases below were CHARACTERIZATION of three measured defects and are
// now the pins on their fixes:
//
//   (1) operator_computed_field_register.cpp wrote the edge
//       (pg_computed_column, attoid) -> (pg_class, table_oid) with deptype 'n' —
//       a table's dependency on ITSELF. catalog::deptype::blocks_restrict is
//       exactly `dt == 'n'` (components/catalog/dependency_walker.hpp), so a
//       computing table was blocked from being dropped BY ITS OWN COLUMNS. It
//       now writes 'a' (auto), the deptype build_create_index_writes already
//       uses for "owned by the parent"; 'a' still cascades.
//
//   (2) operator_alter_column_drop.cpp refused DROP COLUMN RESTRICT whenever
//       `dependents` was non-empty, and `dependents` was EVERY pg_depend row
//       keyed on the column. It now refuses on `restrict_blockers` — the rows
//       whose deptype actually blocks — so an index on the column, an edge
//       dropped ALONG WITH the column, stops blocking it. The FK-parent gate
//       above it is unconditional and unchanged.
//
//   (3) components/catalog/cascade_planner.cpp's RESTRICT ALLOW-path returned a
//       plan with ZERO steps: the seed was never pushed, so an accepted
//       `DROP TABLE t RESTRICT` deleted nothing and answered success. The leg now
//       falls through to the same seed-last order the CASCADE leg builds.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/sql/transformer/utils.hpp>
#include <services/disk/manager_disk.hpp>

#include <unistd.h>

#include <limits>
#include <string>
#include <thread>

namespace {

    using namespace test_helpers;

    // Fixture roots are qualified by pid through the directory's one root: two binaries
    // sharing a literal /tmp path truncate each other's segment files, and the resulting
    // real I/O failures get read as flakes.
    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_drop_restrict_deptype/") + leaf).string();
    }

    namespace catalog = components::catalog;

    // The two cases that pin a refusal gate need to READ and WRITE pg_depend
    // directly: one dependency shape the gates answer is not producible from SQL
    // (see the forged-edge case below). manager_disk_ is protected on the base,
    // so the fixture opens exactly that one door and nothing else.
    class restrict_spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit restrict_spaces_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(config) {
            components::compute::function_registry_t::reset_default();
        }

        services::disk::manager_disk_t* disk() noexcept { return manager_disk_.get(); }
    };

    // Await a disk future the way the other catalog-probing integration tests do:
    // the disk actor runs on its own scheduler, so the test thread spins.
    template<typename Future>
    void spin_until_ready(Future& fut) {
        for (int i = 0; i < 2000000 && !fut.is_ready(); ++i) {
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
    }

    // Committed rows of `table_oid` whose column `key_col` equals `key`, read
    // through the disk manager's own funnel (every committed row).
    template<typename Key>
    core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
    catalog_chunks_with(restrict_spaces_t& space, catalog::oid_t table_oid, std::uint64_t key_col, Key key) {
        auto* resource = space.disk()->resource();
        components::table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        std::pmr::vector<std::uint64_t> key_cols(resource);
        key_cols.emplace_back(key_col);
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk()->address(),
                                                    &services::disk::manager_disk_t::read_chunks_by_key,
                                                    exec_ctx,
                                                    table_oid,
                                                    std::move(key_cols),
                                                    components::operators::make_key_chunk(resource, key),
                                                    std::pmr::vector<std::uint64_t>{resource});
        spin_until_ready(fut);
        return std::move(fut).take_ready();
    }

    // The relation's oid from the pg_class row that names it.
    catalog::oid_t table_oid_named(restrict_spaces_t& space, const std::string& name) {
        auto batches =
            catalog_chunks_with(space, catalog::well_known_oid::pg_class_table, catalog::pg_class_col::relname,
                                std::string_view{name});
        REQUIRE_FALSE(batches.has_error());
        for (const auto& chunk : batches.value()) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (!chunk.is_null(0, i)) {
                    return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                }
            }
        }
        return catalog::INVALID_OID;
    }

    // The column's attoid, resolved the way operator_alter_column_drop resolves it:
    // pg_attribute keyed on attrelid, matched on attname.
    catalog::oid_t attoid_of(restrict_spaces_t& space, catalog::oid_t table_oid, const std::string& column) {
        auto batches = catalog_chunks_with(space,
                                           catalog::well_known_oid::pg_attribute_table,
                                           catalog::pg_attribute_col::attrelid,
                                           table_oid);
        REQUIRE_FALSE(batches.has_error());
        for (const auto& chunk : batches.value()) {
            if (chunk.column_count() < 3) {
                continue;
            }
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(2, i)) {
                    continue;
                }
                if (chunk.get_value<std::string_view>(2, i) == column) {
                    return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                }
            }
        }
        return catalog::INVALID_OID;
    }

    // Forge one pg_depend edge through the manager's own funnel. The gate under
    // test keys on (refclassid, refobjid) and classifies by (classid, deptype),
    // and no writer in this engine emits the combination the case needs.
    void forge_depend_edge(restrict_spaces_t& space,
                           catalog::oid_t classid,
                           catalog::oid_t objid,
                           catalog::oid_t refclassid,
                           catalog::oid_t refobjid,
                           char deptype) {
        auto* resource = space.disk()->resource();
        components::table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        auto row = catalog::build_pg_depend_row(resource, classid, objid, refclassid, refobjid, deptype);
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk()->address(),
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    catalog::well_known_oid::pg_depend_table,
                                                    std::move(row));
        spin_until_ready(fut);
        auto appended = std::move(fut).take_ready();
        REQUIRE_FALSE(appended.has_error());
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    std::string error_text(const components::cursor::cursor_t_ptr& cur) {
        if (!cur->is_error()) {
            return {};
        }
        return std::string{cur->get_error().what.begin(), cur->get_error().what.end()};
    }

    // `DROP TABLE <db>.<rel> RESTRICT` as a plan. This is exactly the node
    // transform_drop builds (components/sql/transformer/impl/transform_table.cpp,
    // wrap_one), plus the one line it does not have: set_behavior.
    components::cursor::cursor_t_ptr drop_table_restrict(otterbrix::wrapper_dispatcher_t* d,
                                                         const std::string& database,
                                                         const std::string& relname) {
        auto* resource = d->resource();
        auto node = components::logical_plan::make_node_drop(resource,
                                                             components::logical_plan::drop_target_kind::collection);
        node->set_dbname(database);
        node->set_relname(relname);
        node->set_behavior(components::catalog::drop_behavior_t::restrict_);
        components::logical_plan::execution_plan_t plan{resource,
                                                        node,
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, database, relname);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    // `ALTER TABLE <db>.<rel> DROP COLUMN <col> RESTRICT` as a plan — the node
    // transform_alter_table builds for AT_DropColumn, plus `sub.behavior`.
    components::cursor::cursor_t_ptr drop_column_restrict(otterbrix::wrapper_dispatcher_t* d,
                                                          const std::string& database,
                                                          const std::string& relname,
                                                          const std::string& column) {
        auto* resource = d->resource();
        components::logical_plan::alter_table_subcommand_t sub;
        sub.kind = components::logical_plan::alter_table_kind::drop_column;
        sub.column_name = column;
        sub.behavior = components::catalog::drop_behavior_t::restrict_;
        std::vector<components::logical_plan::alter_table_subcommand_t> subs;
        subs.push_back(std::move(sub));
        auto node = components::logical_plan::make_node_alter_table_multi(resource, std::move(subs));
        node->set_dbname(database);
        node->set_relname(relname);
        components::logical_plan::execution_plan_t plan{resource,
                                                        components::logical_plan::node_ptr{node},
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, database, relname);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

} // namespace

// ---------------------------------------------------------------------------
// BLOCKER (1), now the pin on its fix. The test name is the DEFECT's name: a
// computing table's own columns used to block dropping it under RESTRICT.
//
// `CREATE TABLE ns.docs();` makes a relkind='g' table; the first INSERT
// registers its columns through operator_computed_field_register, which writes
// the (pg_computed_column, attoid) -> (pg_class, table_oid) edge. With 'n' on
// that edge the table depended on ITSELF and RESTRICT refused it; with 'a' the
// columns are owned children, cascaded with the table and blocking nothing.
// Nothing outside this table depends on it, so RESTRICT must let it through.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::computing_table_is_blocked_by_its_own_columns") {
    auto config = make_test_config(fixture_path("own_columns"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.docs();");
    run_ok(d, "INSERT INTO dr.docs (id, n) VALUES (1, 42);");

    // Control: the table really is there and really is readable, so a refusal
    // below is a refusal about dependencies and not about a missing target.
    REQUIRE(run_ok(d, "SELECT * FROM dr.docs;")->size() == 1);

    auto dropped = drop_table_restrict(d, "dr", "docs");

    // The only pg_depend edges on this table are its OWN columns. A column row
    // cannot outlive its table, so the edge is 'a' (auto) — it cascades with the
    // table and does not block RESTRICT, exactly as the index→table edge
    // ddl_metadata_builder.cpp writes for a declared table does not.
    INFO("error: " << error_text(dropped));
    CHECK(dropped->is_success());

    // And RESTRICT allowing the drop means the table is GONE, rows and all — not
    // that the statement was waved through over an object it left live.
    auto gone = exec(d, "SELECT * FROM dr.docs;");
    CHECK_FALSE(gone->is_success());
}

// ---------------------------------------------------------------------------
// BLOCKER (2), now the pin on its fix. The test name is the DEFECT's name:
// DROP COLUMN RESTRICT used to count every dependent, not the blocking ones.
//
// An index on the column is an owned dependency: PostgreSQL drops the index
// along with the column and lets RESTRICT through. operator_alter_column_drop
// counted it as a blocker because it never applied the deptype filter; it now
// refuses on `restrict_blockers`, the deptype-filtered subset.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::column_is_blocked_by_its_own_index") {
    auto config = make_test_config(fixture_path("own_index"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.t (a bigint, b bigint);");
    run_ok(d, "INSERT INTO dr.t (a, b) VALUES (1, 10), (2, 20);");
    run_ok(d, "CREATE INDEX ix_a ON dr.t (a);");

    auto dropped = drop_column_restrict(d, "dr", "t", "a");

    // The index depends on the column through an 'i'/'a' edge and is dropped with
    // it; nothing EXTERNAL references column a, so RESTRICT has nothing to refuse.
    INFO("error: " << error_text(dropped));
    CHECK(dropped->is_success());

    // And the column is actually gone — an allowed RESTRICT is a real drop.
    auto after = exec(d, "SELECT a FROM dr.t;");
    CHECK_FALSE(after->is_success());
}

// ---------------------------------------------------------------------------
// The other half of the same gate: an edge that DOES block still blocks. A
// foreign key on ANOTHER table references this column through a 'n' (normal)
// edge, and that one may not be cascaded away by a statement that said RESTRICT.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::column_referenced_by_a_foreign_key_is_refused") {
    auto config = make_test_config(fixture_path("fk_blocks"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.parent (id bigint PRIMARY KEY);");
    run_ok(d, "CREATE TABLE dr.child (pid bigint, FOREIGN KEY (pid) REFERENCES dr.parent (id));");

    auto dropped = drop_column_restrict(d, "dr", "parent", "id");
    CHECK_FALSE(dropped->is_success());

    // WHICH gate answered, spelled out. The FK-parent gate runs BEFORE the
    // RESTRICT gate and runs under every behavior, so this refusal is not
    // evidence about the restrict leg — `restrict_blockers` is pinned separately,
    // by the case below. Naming the gate here is what keeps the two apart: with
    // only `is_success()==false` asserted, deleting either gate would leave this
    // case green.
    INFO("error: " << error_text(dropped));
    CHECK(error_text(dropped).find("foreign key constraint") != std::string::npos);

    // The parent column survived the refusal.
    auto after = run_ok(d, "SELECT id FROM dr.parent;");
    CHECK(after->size() == 0);
}

// ---------------------------------------------------------------------------
// THE RESTRICT LEG OF DROP COLUMN, PINNED ON ITS OWN.
//
// operator_alter_column_drop has two refusal gates and the FK one comes first,
// so every dependency shape TODAY'S writers can produce is answered before the
// RESTRICT gate is reached: the only 'n' edges keyed on a column are the confkey
// rows of build_create_constraint_writes, and those are pg_constraint-classed,
// which is exactly what the FK gate matches. `restrict_blockers` is therefore
// broader than anything the engine writes right now — it is the gate for a
// blocking edge from ANY catalog — and it was reachable by no test at all.
//
// So the edge is forged, through the disk manager's own funnel: a 'n' edge from
// a pg_class-classed object onto the column. `blocking` stays empty (wrong
// classid for the FK gate) and `restrict_blockers` does not, which is the one
// combination that reaches the leg. The control drop of the untouched column in
// the same table is what says the refusal came from the forged edge and not from
// the table's shape.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::a_non_constraint_blocking_edge_refuses_the_column_drop") {
    auto config = make_test_config(fixture_path("foreign_blocker"));
    config.log.level = log_t::level::off;
    restrict_spaces_t space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.t (a bigint, b bigint);");
    run_ok(d, "INSERT INTO dr.t (a, b) VALUES (1, 10);");

    const auto table_oid = table_oid_named(space, "t");
    REQUIRE(table_oid != components::catalog::INVALID_OID);
    const auto att_a = attoid_of(space, table_oid, "a");
    REQUIRE(att_a != components::catalog::INVALID_OID);

    // An object in pg_class depending on column a through a NORMAL edge. Its oid
    // is outside the allocator's range so it can never collide with a real row.
    constexpr components::catalog::oid_t kForeignBlocker = 990001;
    forge_depend_edge(space,
                      components::catalog::well_known_oid::pg_class_table,
                      kForeignBlocker,
                      components::catalog::well_known_oid::pg_attribute_table,
                      att_a,
                      'n');

    auto refused = drop_column_restrict(d, "dr", "t", "a");
    INFO("error: " << error_text(refused));
    CHECK_FALSE(refused->is_success());
    // The RESTRICT gate's own message, naming the blocking oid — not the FK
    // gate's, which this shape does not reach.
    CHECK(error_text(refused).find("DROP COLUMN RESTRICT: column has dependent objects") != std::string::npos);
    CHECK(error_text(refused).find(std::to_string(kForeignBlocker)) != std::string::npos);

    // Refused before the first mutation: the column is still readable.
    CHECK(run_ok(d, "SELECT a FROM dr.t;")->size() == 1);

    // CONTROL: the sibling column carries no blocking edge, so the same statement
    // against it is allowed and really drops.
    auto allowed = drop_column_restrict(d, "dr", "t", "b");
    INFO("error: " << error_text(allowed));
    CHECK(allowed->is_success());
    CHECK_FALSE(exec(d, "SELECT b FROM dr.t;")->is_success());
}

// ---------------------------------------------------------------------------
// THE RESTRICT LEG OF DROP TABLE, PINNED ON ITS OWN.
//
// The refusing half of cascade_planner's gate: a foreign key on another table
// depends on this one through a 'n' edge (constraint→ref_table, written by
// build_create_constraint_writes), so a statement that WROTE RESTRICT may not
// cascade it away. This is the one refusal shape today's writers produce
// unaided, and it is what the message "DROP RESTRICT: object has dependents"
// exists for — the string had no test naming it.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::table_referenced_by_a_foreign_key_is_refused") {
    auto config = make_test_config(fixture_path("fk_table_blocks"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.parent (id bigint PRIMARY KEY);");
    run_ok(d, "INSERT INTO dr.parent (id) VALUES (1);");
    run_ok(d, "CREATE TABLE dr.child (pid bigint, FOREIGN KEY (pid) REFERENCES dr.parent (id));");

    auto refused = drop_table_restrict(d, "dr", "parent");
    INFO("error: " << error_text(refused));
    CHECK_FALSE(refused->is_success());
    CHECK(error_text(refused).find("DROP RESTRICT: object has dependents") != std::string::npos);

    // A refused DROP plans nothing, so the table is untouched — rows included.
    CHECK(run_ok(d, "SELECT id FROM dr.parent;")->size() == 1);
}

// ---------------------------------------------------------------------------
// BLOCKER (3), now the pin on its fix, and worse than either of the other two.
// The test name is the DEFECT's name: the RESTRICT path that ALLOWED a drop
// dropped nothing and reported success.
//
// A DECLARED table owns no pg_computed_column self-edges (build_create_table_writes
// gives its columns only a pg_type edge), so it reaches the allow-path that
// blockers (1) and (2) never got to. That path used to be:
//
//     if (behavior == drop_behavior_t::restrict_) {
//         for (const auto& d : deps) { ...blocks_restrict -> restrict_blocked... }
//         return plan;                     // <-- plan.steps EMPTY
//     }
//
// while the CASCADE leg below it ended with `plan.steps.push_back({seed_classid,
// seed_oid, 'n'})` — the seed step is what makes a drop delete the object's own
// catalog rows. operator_dynamic_cascade_delete iterated zero steps, emitted no
// delete spec, and ran to mark_executed(): success over an object left fully
// live. The allow-path now falls through to the same order computation, so an
// accepted RESTRICT is a real drop.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::allowed_restrict_drop_removes_nothing") {
    auto config = make_test_config(fixture_path("declared_ok"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.plain (id bigint);");
    run_ok(d, "INSERT INTO dr.plain (id) VALUES (1);");

    // The statement is ACCEPTED — nothing blocking depends on this table, so
    // RESTRICT has nothing to refuse. This half is correct and stays.
    auto dropped = drop_table_restrict(d, "dr", "plain");
    INFO("error: " << error_text(dropped));
    CHECK(dropped->is_success());

    // The table was reported dropped, so selecting from it must fail. The
    // allow-path builds the same seed-last step list the CASCADE path does
    // (components/catalog/cascade_planner.cpp) — an accepted RESTRICT is a drop,
    // not a statement that answers success over rows it never touched.
    auto gone = exec(d, "SELECT id FROM dr.plain;");
    CHECK_FALSE(gone->is_success());
}

// ---------------------------------------------------------------------------
// The control the cases above need: a bare DROP TABLE of a computing table
// whose only inbound edges are 'a' (auto) still drops it — the restrict_
// default gates on 'n' edges only. If a restrict_ defect above were ever
// "fixed" by widening the gate to every deptype, this case is what would
// catch it.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::cascade_still_drops_the_computing_table") {
    auto config = make_test_config(fixture_path("cascade_control"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.docs();");
    run_ok(d, "INSERT INTO dr.docs (id, n) VALUES (1, 42);");

    // Plain SQL: bare DROP is RESTRICT, and a computing table's own 'a' edges
    // do not block it.
    run_ok(d, "DROP TABLE dr.docs;");

    auto gone = exec(d, "SELECT * FROM dr.docs;");
    CHECK_FALSE(gone->is_success());
}
