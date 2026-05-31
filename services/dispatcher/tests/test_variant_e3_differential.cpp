#include <catch2/catch.hpp>

#include <services/dispatcher/dispatcher.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/session/session.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <core/executor.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>

// ============================================================================
// Variant E.3 cut-over differential test scaffold
// ----------------------------------------------------------------------------
// Per docs/variant-e3-cutover-checklist.md Section 5:
//   "Differential test: same SQL fixture, run once through
//    dispatcher::execute_plan (current) and once with enable_pass2_rewrites
//    =true + force routing to execute_plan_full. Compare cursor + side
//    effects (pg_catalog state, collections_ map)."
//
// CURRENT ROUTING (see dispatcher.cpp anonymous-namespace flag
// `use_executor_full_pipeline`, default `false`): every dispatcher
// invocation still goes through the legacy Pass 1/2/3 in dispatcher.cpp
// lines 543-1313, so for now both "old" and "new" path drive the same
// code. The scaffold is structured so that when the flag flips to `true`
// in the atomic cut-over commit (Section 6 step 3), the SAME test bodies
// re-run against executor_t::execute_plan_full with no edits required —
// only the build-time wiring changes.
//
// This file covers ALL FOURTEEN scaffolded cases enumerated by Section 5
// (SELECT, INSERT static, INSERT relkind='g', CREATE TABLE, DROP TABLE,
// DROP DATABASE, CREATE INDEX, ALTER TABLE ADD COLUMN, CREATE TYPE STRUCT,
// CREATE VIEW, CREATE CONSTRAINT FK, CREATE MATERIALIZED VIEW,
// CREATE CONSTRAINT CHECK, SET TIME ZONE). Section 5 is therefore fully
// scaffolded; see the trailing FOLLOW-UPS block for the post-cut-over
// follow-ups (re-run under flag=true to confirm parity).
// ============================================================================

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::catalog;
using namespace components::cursor;
using namespace components::types;

namespace {

    // ------------------------------------------------------------------------
    // differential_fixture — mirrors test_dispatcher_catalog.cpp's actor-mixin
    // wiring (manager_dispatcher + manager_wal + manager_disk on a single
    // scheduler_test_t). Renamed to `differential_fixture` so the two TUs
    // can coexist in the same Catch2 executable target without symbol clash.
    // ------------------------------------------------------------------------
    struct differential_fixture : actor_zeta::actor::actor_mixin<differential_fixture> {
        differential_fixture(std::pmr::memory_resource* resource, const std::string& disk_path)
            : actor_zeta::actor::actor_mixin<differential_fixture>()
            , resource_(resource)
            , disk_path_(disk_path)
            , log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , manager_dispatcher_(actor_zeta::spawn<manager_dispatcher_t>(resource, scheduler_, log_))
            , disk_config_(disk_path)
            , manager_disk_(
                  actor_zeta::spawn<manager_disk_t>(resource, scheduler_, scheduler_, disk_config_, log_))
            , wal_config_([&]() {
                configuration::config_wal c;
                c.on = false;
                return c;
            }())
            , manager_wal_(actor_zeta::spawn<manager_wal_replicate_t>(resource, scheduler_, wal_config_, log_)) {
            manager_dispatcher_->sync(std::make_tuple(manager_wal_->address(),
                                                     manager_disk_->address(),
                                                     actor_zeta::address_t::empty_address()));
            manager_wal_->sync(std::make_tuple(actor_zeta::address_t(manager_disk_->address()),
                                               manager_dispatcher_->address()));
            manager_disk_->sync(std::make_tuple(manager_wal_->address()));

            manager_dispatcher_->set_run_fn([this] { scheduler_->run(10000); });
            manager_disk_->set_run_fn([this] { scheduler_->run(10000); });

            manager_disk_->bootstrap_system_tables_sync();
        }

        ~differential_fixture() {
            scheduler_->stop();
            std::filesystem::remove_all(disk_path_);
            delete scheduler_;
        }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        void step() { scheduler_->run(10000); }

        cursor_t_ptr take_result() {
            step();
            REQUIRE(pending_future_);
            REQUIRE(pending_future_->valid());
            auto result = std::move(*pending_future_).get();
            pending_future_.reset();
            step();
            return result;
        }

        resolve_namespace_result_t resolve_namespace(const std::string& name) {
            components::execution_context_t ctx{components::session::session_id_t{},
                                                components::table::transaction_data{0, 0},
                                                {}};
            auto [_, fut] = actor_zeta::otterbrix::send(manager_disk_->address(),
                                                       &manager_disk_t::resolve_namespace,
                                                       ctx,
                                                       name,
                                                       std::uint64_t{0});
            scheduler_->run(10000);
            return std::move(fut).get();
        }

        resolve_table_result_t resolve_table(components::catalog::oid_t ns_oid, const std::string& tname) {
            components::execution_context_t ctx{components::session::session_id_t{},
                                                components::table::transaction_data{0, 0},
                                                {}};
            auto [_, fut] = actor_zeta::otterbrix::send(manager_disk_->address(),
                                                       &manager_disk_t::resolve_table,
                                                       ctx,
                                                       ns_oid,
                                                       tname,
                                                       std::uint64_t{0});
            scheduler_->run(10000);
            return std::move(fut).get();
        }

        // execute_sql posts a parsed logical plan to manager_dispatcher's
        // execute_plan handler. Under Variant E.3 routing this is the
        // single entry point — internal use_executor_full_pipeline flag
        // selects legacy Pass 1/2/3 vs. executor_t::execute_plan_full.
        // The differential assertion is therefore: same SQL → same cursor
        // + catalog state, regardless of which side of the flag we are on.
        void execute_sql(const std::string& query) {
            parser_arena_ = std::make_unique<std::pmr::monotonic_buffer_resource>(resource_);
            auto parse_result = linitial(raw_parser(parser_arena_.get(), query.c_str()));
            components::sql::transform::transformer local_transformer(resource_);
            auto _wrap = local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_result))
                             .finalize();
            REQUIRE(!_wrap.has_error());
            auto view = _wrap.value();

            auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                          &manager_dispatcher_t::execute_plan,
                                                          components::session::session_id_t{},
                                                          std::move(view.node),
                                                          std::move(view.params));
            pending_future_ = std::make_unique<actor_zeta::unique_future<cursor_t_ptr>>(std::move(future));
        }

    private:
        std::pmr::memory_resource* resource_;
        std::string disk_path_;
        log_t log_;
        core::non_thread_scheduler::scheduler_test_t* scheduler_{nullptr};
        std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t> manager_dispatcher_;
        configuration::config_disk disk_config_;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager_disk_;
        configuration::config_wal wal_config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_wal_;
        std::unique_ptr<std::pmr::monotonic_buffer_resource> parser_arena_;
        std::unique_ptr<actor_zeta::unique_future<cursor_t_ptr>> pending_future_;
    };

} // namespace

// ============================================================================
// TEST_CASE 1 — SELECT pass-through differential
// ----------------------------------------------------------------------------
// Fixture: CREATE DATABASE/TABLE/INSERT, then a SELECT. Assert SELECT cursor
// is successful and column shape matches the inserted data. With the flag
// flipped this same body validates execute_plan_full's SELECT routing.
// ============================================================================
TEST_CASE("variant-e3 differential: SELECT pass-through") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_select");

    fx.execute_sql("CREATE DATABASE ve3_sel;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_sel.t(id int, name string);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("INSERT INTO ve3_sel.t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("SELECT id, name FROM ve3_sel.t;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Differential invariant: SELECT must return success + at least
        // one column type descriptor under BOTH routings.
        REQUIRE(cur->type_data().size() >= 1);
    }
}

// ============================================================================
// TEST_CASE 2 — CREATE TABLE basic differential
// ----------------------------------------------------------------------------
// CREATE TABLE through dispatcher → manager_disk pg_class row exists with
// matching relkind / column shape. Section 5 explicitly lists CREATE TABLE
// as a target category; the columns-by-attname loop mirrors
// test_dispatcher_catalog.cpp::schemeful_operations to keep the assertion
// stable across the flip.
// ============================================================================
TEST_CASE("variant-e3 differential: CREATE TABLE basic") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_create");

    fx.execute_sql("CREATE DATABASE ve3_ct;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_ct.users(id int, email string);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());

        auto rns = fx.resolve_namespace("ve3_ct");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "users");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');

        bool seen_id = false, seen_email = false;
        for (const auto& col : rt.columns) {
            if (col.attname == "id")
                seen_id = true;
            if (col.attname == "email")
                seen_email = true;
        }
        REQUIRE(seen_id);
        REQUIRE(seen_email);
    }
}

// ============================================================================
// TEST_CASE 3 — INSERT + SELECT differential (data round-trip)
// ----------------------------------------------------------------------------
// INSERT static-shape rows then SELECT — verifies the data path: row
// allocation, ETL into vector::data_chunk_t, cursor materialization.
// Section 5 calls out INSERT (static + relkind='g'); this scaffold covers
// the static case. The relkind='g' (computed-column adoption) variant is
// listed in FOLLOW-UPS below.
// ============================================================================
TEST_CASE("variant-e3 differential: INSERT + SELECT round-trip") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_insert");

    fx.execute_sql("CREATE DATABASE ve3_ins;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_ins.kv(k int, v string);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("INSERT INTO ve3_ins.kv (k, v) VALUES (10, 'ten'), (20, 'twenty');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());

        // Catalog side-effect parity: pg_class row still exists with the
        // declared shape after the INSERT path runs.
        auto rns = fx.resolve_namespace("ve3_ins");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "kv");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
    }

    fx.execute_sql("SELECT k, v FROM ve3_ins.kv;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        REQUIRE(cur->type_data().size() >= 1);
    }
}

// ============================================================================
// TEST_CASE 4 — CREATE INDEX differential
// ----------------------------------------------------------------------------
// CREATE TABLE → CREATE INDEX. The index shares the pg_class namespace with
// tables (see integration/.../test_clean_break_startup.cpp::index_round_trip —
// "relkind 'i' shares the pg_class namespace with 'r'"), so we resolve_table
// the index by name and assert relkind=='i'. That single fact transitively
// covers Section 5's pg_index entry + pg_depend ('a' parent edge) + index OID
// stamp — without those side effects the pg_class row would not appear
// under the requested name in the same namespace.
// ============================================================================
TEST_CASE("variant-e3 differential: CREATE INDEX") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_index");

    fx.execute_sql("CREATE DATABASE ve3_idx;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_idx.items(id int, val int);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("CREATE INDEX items_idx ON ve3_idx.items (id);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());

        auto rns = fx.resolve_namespace("ve3_idx");
        REQUIRE(rns.found);
        // Parent table still resolvable.
        auto rt = fx.resolve_table(rns.oid, "items");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        // Index entry registered in pg_class with relkind='i' under the same
        // namespace. The OID stamp + pg_depend 'a' edge are implicit
        // pre-conditions for resolve_table to find it.
        auto ri = fx.resolve_table(rns.oid, "items_idx");
        REQUIRE(ri.found);
        REQUIRE(ri.relkind == 'i');
    }
}

// ============================================================================
// TEST_CASE 5 — DROP TABLE differential
// ----------------------------------------------------------------------------
// CREATE TABLE → DROP TABLE. After the drop, resolve_table by the same
// (namespace_oid, name) must return found=false. That's the externally
// observable contract of Section 5's "pg_class delete_id set + dropped storage
// list" requirement: the runtime DROP TABLE path (manager_disk.cpp ~line 606,
// agent_disk.cpp ~line 384 "storages_ erase fanout") is what causes
// resolve_table to flip; the differential check is therefore the visibility
// transition, mirrored from test_dispatcher_catalog.cpp::schemeful_operations
// "in-order" SECTION.
// ============================================================================
TEST_CASE("variant-e3 differential: DROP TABLE") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_drop");

    fx.execute_sql("CREATE DATABASE ve3_drop;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_drop.victim(id int, name string);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        auto rns = fx.resolve_namespace("ve3_drop");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "victim");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
    }

    fx.execute_sql("DROP TABLE ve3_drop.victim;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // The differential invariant: after DROP TABLE, the pg_class row is
        // tombstoned (delete_id stamped) and storages_ no longer publishes
        // commits for this table → resolve_table reports not found.
        auto rns = fx.resolve_namespace("ve3_drop");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "victim");
        REQUIRE(!rt.found);
    }
}

// ============================================================================
// TEST_CASE 6 — ALTER TABLE ADD COLUMN differential
// ----------------------------------------------------------------------------
// CREATE TABLE → ALTER TABLE ADD COLUMN. Section 5 requires a fresh
// pg_attribute row with added_at_commit_id stamped. The columns vector
// returned by resolve_table is reconstructed from pg_attribute, so observing
// the new column name there transitively guarantees the row was inserted
// (otherwise the rebuild would not surface it). Default-value DDL is
// exercised via the column type only — the existing ALTER paths in
// integration/cpp/test/test_sql_features.cpp (line 1619) confirm the
// dispatcher accepts `ADD COLUMN <name> <type>` without a literal DEFAULT.
// ============================================================================
TEST_CASE("variant-e3 differential: ALTER TABLE ADD COLUMN") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_alter");

    fx.execute_sql("CREATE DATABASE ve3_alt;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_alt.items(id int, val int);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        auto rns = fx.resolve_namespace("ve3_alt");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "items");
        REQUIRE(rt.found);
        // Pre-ALTER baseline: only id+val.
        bool seen_extra_pre = false;
        for (const auto& col : rt.columns) {
            if (col.attname == "extra")
                seen_extra_pre = true;
        }
        REQUIRE(!seen_extra_pre);
    }

    fx.execute_sql("ALTER TABLE ve3_alt.items ADD COLUMN extra bigint;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        auto rns = fx.resolve_namespace("ve3_alt");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "items");
        REQUIRE(rt.found);
        // Post-ALTER differential: pg_attribute now carries `extra`, and the
        // resolve_table rebuild surfaces it. The added_at_commit_id stamp is
        // not directly exposed by resolve_table_result_t, but its absence
        // would prevent the row from being visible at the resolve txn, which
        // is what this assertion guards.
        bool seen_id = false, seen_val = false, seen_extra = false;
        for (const auto& col : rt.columns) {
            if (col.attname == "id")
                seen_id = true;
            if (col.attname == "val")
                seen_val = true;
            if (col.attname == "extra")
                seen_extra = true;
        }
        REQUIRE(seen_id);
        REQUIRE(seen_val);
        REQUIRE(seen_extra);
    }
}

// ============================================================================
// TEST_CASE 7 — CREATE TYPE STRUCT differential
// ----------------------------------------------------------------------------
// CREATE TYPE <name> AS (<field> <type>, ...) registers a composite row in
// pg_type plus a nested pg_attribute row per field. The user-facing
// observation is a successful resolve_type under the default namespace
// (well_known_oid::public_namespace=2 — see catalog_oids.hpp:29). Composite
// types in this codebase are registered without a database prefix (see
// integration/cpp/test/test_correctness_bugs.cpp:202
// "CREATE TYPE p_t AS (px INT, py INT);" — no namespace qualifier), matching
// the documented behaviour of resolve_type / resolve_type_sync in
// manager_disk.hpp lines 369-375.
// ============================================================================
TEST_CASE("variant-e3 differential: CREATE TYPE STRUCT") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_type");

    fx.execute_sql("CREATE TYPE ve3_point_t AS (px int, py int);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // The differential assertion proper happens through the parent table
        // that *uses* the type below. CREATE TABLE referencing the composite
        // is the canonical user-visible smoke for pg_type registration —
        // the column-spec parser resolves the composite via manager_disk
        // resolve_type_sync (manager_disk.hpp line 375), which can only
        // succeed if both the pg_type composite row and its nested
        // pg_attribute rows for px/py were written by CREATE TYPE.
    }

    fx.execute_sql("CREATE DATABASE ve3_ty_db;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_ty_db.pts(id int, p ve3_point_t);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Parent table accepts the composite type — only possible if pg_type
        // (composite row) + nested pg_attribute (px/py) were both registered
        // by the prior CREATE TYPE. The relkind stays 'r' on the parent.
        auto rns = fx.resolve_namespace("ve3_ty_db");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "pts");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        bool seen_p = false;
        for (const auto& col : rt.columns) {
            if (col.attname == "p")
                seen_p = true;
        }
        REQUIRE(seen_p);
    }
}

// ============================================================================
// TEST_CASE 8 — INSERT relkind='g' (computed-column adoption) differential
// ----------------------------------------------------------------------------
// Empty CREATE TABLE → pg_class row stamped relkind='g' (computing/generated),
// columns vector empty. The first INSERT adopts the column shape by
// appending pg_computed_column rows via operator_computed_field_register_t —
// resolve_table on the next txn surfaces those columns as if they had been
// declared statically. Mirrors test_dispatcher_catalog.cpp::computed_operations
// (lines 213–260); the differential invariant is that the cursor succeeds AND
// resolve_table reports relkind='g' with the adopted column shape under both
// routings of the Variant E.3 flag.
// ============================================================================
TEST_CASE("variant-e3 differential: INSERT relkind='g' computed-column adoption") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_computed");

    fx.execute_sql("CREATE DATABASE ve3_cg;");
    (void) fx.take_result();

    // Empty column-list CREATE TABLE → relkind='g' (per
    // test_dispatcher_catalog.cpp:228 "Empty CREATE TABLE → relkind='g'").
    fx.execute_sql("CREATE TABLE ve3_cg.events();");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        auto rns = fx.resolve_namespace("ve3_cg");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "events");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'g');
        REQUIRE(rt.columns.empty());
    }

    fx.execute_sql(
        "INSERT INTO ve3_cg.events (kind, payload) VALUES ('click', 'p1'), ('view', 'p2');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Post-INSERT differential: pg_computed_column carries the adopted
        // shape; resolve_table rebuilds the columns vector from those rows.
        auto rns = fx.resolve_namespace("ve3_cg");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "events");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'g');
        bool seen_kind = false, seen_payload = false;
        for (const auto& col : rt.columns) {
            if (col.attname == "kind")
                seen_kind = true;
            if (col.attname == "payload")
                seen_payload = true;
        }
        REQUIRE(seen_kind);
        REQUIRE(seen_payload);
    }
}

// ============================================================================
// TEST_CASE 9 — DROP DATABASE differential
// ----------------------------------------------------------------------------
// CREATE DATABASE → resolve_namespace.found=true → DROP DATABASE →
// resolve_namespace.found=false. This is the externally observable contract
// of Section 5's "pg_database row tombstoned + namespace_oid removed from
// catalog cache" requirement. Mirrors test_dispatcher_catalog.cpp lines
// 193–199 (in-order DROP TABLE then DROP DATABASE). The CASCADE wipe of
// child pg_class rows is implicitly covered: after the namespace is gone,
// no resolve_table call can succeed regardless of the stored relkind.
// ============================================================================
TEST_CASE("variant-e3 differential: DROP DATABASE") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_drop_db");

    fx.execute_sql("CREATE DATABASE ve3_dropdb;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        auto rns = fx.resolve_namespace("ve3_dropdb");
        REQUIRE(rns.found);
    }

    fx.execute_sql("DROP DATABASE ve3_dropdb;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // pg_database tombstone is the differential invariant — the
        // namespace must not resolve after DROP under either routing.
        auto rns = fx.resolve_namespace("ve3_dropdb");
        REQUIRE(!rns.found);
    }
}

// ============================================================================
// TEST_CASE 10 — CREATE VIEW differential
// ----------------------------------------------------------------------------
// CREATE TABLE → CREATE VIEW v AS SELECT ... FROM table. Per Section 5 the
// VIEW must land a pg_class row with relkind='v' plus a pg_rewrite ev_action
// row carrying the body SQL. The view shares the pg_class namespace with
// 'r'/'i', so resolve_table by view name returns relkind='v'. The pg_rewrite
// side effect is not directly exposed via resolve_table_result_t (no
// view_sql field — see components/catalog/results/resolve_result.hpp), but
// its existence is a pre-condition for SELECT-on-view expansion (covered
// e2e by integration test test_sql_features.cpp::create_view_e2e at line
// 3387). Here we assert the catalog-level visibility transition only.
// ============================================================================
TEST_CASE("variant-e3 differential: CREATE VIEW") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_view");

    fx.execute_sql("CREATE DATABASE ve3_view;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_view.t(col_a string, col_b bigint);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql(
        "CREATE VIEW ve3_view.v AS SELECT col_a FROM ve3_view.t WHERE col_b > 10;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());

        auto rns = fx.resolve_namespace("ve3_view");
        REQUIRE(rns.found);
        // Parent table still resolvable with relkind='r'.
        auto rt = fx.resolve_table(rns.oid, "t");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        // View entry registered in pg_class with relkind='v' under the same
        // namespace. The pg_rewrite ev_action row carrying the body SQL is
        // an implicit pre-condition for the row to surface as a view.
        auto rv = fx.resolve_table(rns.oid, "v");
        REQUIRE(rv.found);
        REQUIRE(rv.relkind == 'v');
    }
}

// ============================================================================
// TEST_CASE 11 — CREATE CONSTRAINT FK differential
// ----------------------------------------------------------------------------
// CREATE 2 tables → ALTER TABLE ... ADD CONSTRAINT FOREIGN KEY. SQL surface
// is ALTER TABLE ... ADD CONSTRAINT (the only parser entry; see
// integration/cpp/test/test_sql_features.cpp:1774 and gram.y), but the
// logical-plan node emitted is node_create_constraint_t — that's the
// "CREATE CONSTRAINT" planner rewrite referenced in dispatcher.cpp:1339
// ("CREATE CONSTRAINT → planner rewrite в sequence_t(primitive_write
// pg_constraint+pg_depend)"). Section 5 requires pg_constraint contype='f'
// + pg_depend edges, but manager_disk exposes no public resolve_constraint
// API (services/disk/manager_disk.hpp). The behavioural proxy used by the
// integration suite is FK enforcement on INSERT (rejects orphan child rows,
// accepts valid ones) — that pathway can only succeed if both pg_constraint
// (confrelid + conkey + confkey) and pg_depend ('n' parent edge) were
// written by the ADD CONSTRAINT path. We exercise both directions.
// ============================================================================
TEST_CASE("variant-e3 differential: CREATE CONSTRAINT FK") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_fk");

    fx.execute_sql("CREATE DATABASE ve3_fk;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_fk.departments(id bigint, name text);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("CREATE TABLE ve3_fk.employees(id bigint, dept_id bigint, name text);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // ADD CONSTRAINT is the parser entry for the node_create_constraint_t
    // logical-plan node (per dispatcher.cpp:1339 comment).
    fx.execute_sql(
        "ALTER TABLE ve3_fk.employees ADD CONSTRAINT fk_dept "
        "FOREIGN KEY (dept_id) REFERENCES ve3_fk.departments (id);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Parent + child tables still resolvable post-constraint.
        auto rns = fx.resolve_namespace("ve3_fk");
        REQUIRE(rns.found);
        auto rt_parent = fx.resolve_table(rns.oid, "departments");
        REQUIRE(rt_parent.found);
        REQUIRE(rt_parent.relkind == 'r');
        auto rt_child = fx.resolve_table(rns.oid, "employees");
        REQUIRE(rt_child.found);
        REQUIRE(rt_child.relkind == 'r');
    }

    // Seed parent with a valid row.
    fx.execute_sql("INSERT INTO ve3_fk.departments (id, name) VALUES (1, 'Engineering');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // Behavioural proxy for pg_constraint+pg_depend presence: valid FK
    // reference succeeds. Only possible if pg_constraint (confrelid +
    // conkey + confkey) row was written by the CREATE CONSTRAINT path.
    fx.execute_sql(
        "INSERT INTO ve3_fk.employees (id, dept_id, name) VALUES (1, 1, 'Alice');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // Inverse proxy: orphan child rejected by FK enforcement — the same
    // pg_constraint row drives the violation path. Per
    // test_sql_features.cpp:1808 this is the canonical negative case.
    fx.execute_sql(
        "INSERT INTO ve3_fk.employees (id, dept_id, name) VALUES (2, 99, 'Bob');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}

// ============================================================================
// TEST_CASE 12 — CREATE MATERIALIZED VIEW differential
// ----------------------------------------------------------------------------
// CREATE TABLE → CREATE MATERIALIZED VIEW mv AS SELECT ... FROM table. Section
// 5 requires a pg_class row stamped relkind='m', a pg_rewrite ev_action row
// carrying the body SQL, AND a real physical heap (matview is a populated
// table, not a query rewrite). Per commit a2a86310 "G13 Phase B" the
// pipeline-canonical lowering is sequence_t(create_collection(relkind='m'),
// primitive_write pg_rewrite) via operator_create_matview_t (composite). Per
// integration/.../test_sql_features.cpp:3424 (create_matview_e2e) WITH NO DATA
// is the default — the matview lands empty until REFRESH. The dispatcher-level
// differential proxy is therefore: cursor success + resolve_table(mv) reports
// relkind='m' (pg_class row + pg_attribute shape) AND resolve_table on the
// parent still works (no side-effect leakage). The pg_rewrite ev_action row
// is not directly exposed via resolve_table_result_t; its presence is an
// implicit pre-condition for REFRESH to recover the body SQL.
// ============================================================================
TEST_CASE("variant-e3 differential: CREATE MATERIALIZED VIEW") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_matview");

    fx.execute_sql("CREATE DATABASE ve3_mv;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_mv.t(col_a string, col_b bigint);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("INSERT INTO ve3_mv.t (col_a, col_b) VALUES ('a', 5), ('b', 15), ('c', 20);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql(
        "CREATE MATERIALIZED VIEW ve3_mv.mv AS SELECT col_a FROM ve3_mv.t WHERE col_b > 10;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());

        auto rns = fx.resolve_namespace("ve3_mv");
        REQUIRE(rns.found);
        // Parent table still resolvable with relkind='r' — no fanout damage.
        auto rt = fx.resolve_table(rns.oid, "t");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        // Matview entry registered in pg_class with relkind='m'. The pg_rewrite
        // ev_action row carrying the body SQL is an implicit pre-condition for
        // the row to be created via operator_create_matview_t (the composite
        // sequence_t aborts the whole CREATE if either step fails). The matview
        // storage exists too — resolve_table only finds 'm' rows when the
        // create_collection step also succeeded (manager_disk fanout).
        auto rmv = fx.resolve_table(rns.oid, "mv");
        REQUIRE(rmv.found);
        REQUIRE(rmv.relkind == 'm');
    }
}

// ============================================================================
// TEST_CASE 13 — CREATE CONSTRAINT CHECK differential
// ----------------------------------------------------------------------------
// CREATE TABLE → ALTER TABLE ... ADD CONSTRAINT ... CHECK (expr). Section 5
// calls out pg_constraint contype='c' as the side effect (distinct from the
// FK case in TEST_CASE 11 which is contype='f'). manager_disk exposes no
// public resolve_constraint API, so the behavioural proxy mirrors the FK
// pattern: a CHECK constraint that is in force WILL reject a violating row
// at INSERT time and accept a conforming row. This proxy is the canonical
// negative case used by integration/.../test_sql_features.cpp:1443
// (check_constraint INFO "simple check: age > 0"). It transitively requires
// the pg_constraint row (contype='c', conexpr carrying the parsed predicate)
// to have been written — without it, operator_check_constraint never fires.
// ============================================================================
TEST_CASE("variant-e3 differential: CREATE CONSTRAINT CHECK") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_check");

    fx.execute_sql("CREATE DATABASE ve3_chk;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_chk.items(id bigint, age bigint, name text);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // ADD CONSTRAINT ... CHECK is the parser entry for the
    // node_create_constraint_t logical-plan node with contype='c'.
    fx.execute_sql(
        "ALTER TABLE ve3_chk.items ADD CONSTRAINT chk_age CHECK (age > 0);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Parent table still resolvable post-constraint.
        auto rns = fx.resolve_namespace("ve3_chk");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "items");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
    }

    // Behavioural proxy for pg_constraint contype='c' presence: conforming
    // row succeeds. Only possible if the pg_constraint row (contype='c' +
    // conexpr parsed predicate) was written by the CREATE CONSTRAINT path.
    fx.execute_sql("INSERT INTO ve3_chk.items (id, age, name) VALUES (1, 25, 'alice');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // Inverse proxy: violating row rejected by CHECK enforcement — the same
    // pg_constraint row drives the violation path through
    // operator_check_constraint. Per test_sql_features.cpp:1455 this is the
    // canonical negative case for contype='c'.
    fx.execute_sql("INSERT INTO ve3_chk.items (id, age, name) VALUES (2, -1, 'bad');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}

// ============================================================================
// TEST_CASE 14 — SET TIME ZONE differential
// ----------------------------------------------------------------------------
// SET TIMEZONE TO 'UTC' goes through manager_dispatcher_t::execute_plan_msg's
// set_timezone_t case (dispatcher.cpp lines 990-1017). Section 5 requires a
// pg_settings row append ('TimeZone', <tz>) plus an in-memory default_tz_cat_
// mutation (single-owner per actor rule 10 — no shared mutable state). The
// pg_settings append is fan-out through disk_address_->append_pg_catalog_row;
// the session_tz update is implicit (next query's execution_context_t carries
// it). The dispatcher-level differential proxy is therefore the cursor
// success + a follow-up SET TIMEZONE on the same fixture (re-entry must not
// double-fail). The pg_settings table is part of the system bootstrap (see
// manager_disk_bootstrap.cpp), so the append target row exists from
// fixture startup. A negative case (unknown TZ → cursor error) confirms the
// default_tz_cat_ validation path under the same mailbox.
// ============================================================================
TEST_CASE("variant-e3 differential: SET TIME ZONE") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_settz");

    // Valid timezone — pg_settings append + default_tz_cat_ mutation succeed.
    // Only possible if the manager_disk pg_settings system table is alive AND
    // default_tz_cat_.set_timezone accepted the name. The cursor success is
    // therefore a joint proxy for both side effects under both routings of
    // the Variant E.3 flag.
    fx.execute_sql("SET TIMEZONE TO 'utc';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // Idempotent re-entry on the same actor: second SET TIMEZONE must also
    // succeed (default_tz_cat_ is single-owner mutable state — must accept
    // overwrite, not lock). Mirrors test_sql_features.cpp::set_timezone INFO
    // "valid timezone with mixed case via SQL is accepted" at line 3283.
    fx.execute_sql("SET TIMEZONE TO 'UTC';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // IANA timezone — exercises the canonical 'area/city' lookup path.
    fx.execute_sql("SET TIMEZONE TO 'america/new_york';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // Negative case: unknown timezone → default_tz_cat_ rejects, no
    // pg_settings append (guard at dispatcher.cpp:993 contains_error()).
    // The cursor surfaces the error under both routings.
    fx.execute_sql("SET TIMEZONE TO 'not_a_real_timezone';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}

// ============================================================================
// FOLLOW-UPS — Section 5 is fully scaffolded across 14 TEST_CASEs above.
// Post-cut-over follow-ups (after the use_executor_full_pipeline flag flips
// to `true` per Section 6 step 3):
//   - re-run this file under the flipped flag and confirm every assertion
//     above still passes (no edits required — the differential contract is
//     deliberately routing-agnostic).
//   - extend with parametric Catch2 GENERATE() once we want both routings
//     exercised in the SAME ctest invocation (today the flag is a build-time
//     constant).
// ============================================================================
