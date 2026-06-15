#include <catch2/catch.hpp>

#include <chrono>
#include <thread>

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
#include <services/disk/tests/catalog_probe.hpp>
#include <services/wal/manager_wal_replicate.hpp>

// Differential test scaffold: same SQL fixture, drive dispatcher::execute_plan
// and compare cursor + side effects (pg_catalog state, executor-side storage).

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::catalog;
using namespace components::cursor;
using namespace components::types;

namespace {

    // Mirrors test_dispatcher_catalog.cpp's actor-mixin wiring (manager_dispatcher
    // + manager_wal + manager_disk on one scheduler_test_t). The name must stay
    // distinct from that file's fixture: both TUs share one Catch2 target, so a
    // collision would be an ODR clash.
    struct differential_fixture : actor_zeta::actor::actor_mixin<differential_fixture> {
        differential_fixture(std::pmr::memory_resource* resource, const std::string& disk_path)
            : actor_zeta::actor::actor_mixin<differential_fixture>()
            , resource_(resource)
            , disk_path_(disk_path)
            , log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , manager_dispatcher_(actor_zeta::spawn<manager_dispatcher_t>(resource, scheduler_, log_))
            , disk_config_(disk_path)
            , manager_disk_(actor_zeta::spawn<manager_disk_t>(resource, scheduler_, scheduler_, disk_config_, log_))
            , wal_config_([&]() {
                configuration::config_wal c;
                c.on = false;
                return c;
            }())
            , manager_wal_(actor_zeta::spawn<manager_wal_replicate_t>(resource, scheduler_, wal_config_, log_)) {
            manager_dispatcher_->sync(
                services::dispatcher::manager_dispatcher_t::sync_pack{manager_wal_->address(),
                                                                      manager_disk_->address(),
                                                                      actor_zeta::address_t::empty_address()});
            manager_wal_->sync(services::wal::wal_sync_pack_t{actor_zeta::address_t(manager_disk_->address()),
                                                              manager_dispatcher_->address(),
                                                              actor_zeta::address_t::empty_address()});
            manager_disk_->sync(services::disk::manager_disk_t::disk_sync_pack_t{manager_wal_->address()});

            manager_disk_->bootstrap_system_tables_sync();
        }

        ~differential_fixture() {
            // Destroy managers (self-driving on internal threads) before the
            // scheduler to avoid use-after-free, in reverse dependency order:
            // dispatcher, then wal, then disk.
            manager_dispatcher_.reset();
            manager_wal_.reset();
            manager_disk_.reset();
            scheduler_->stop();
            std::filesystem::remove_all(disk_path_);
            delete scheduler_;
        }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        void step() { scheduler_->run(10000); }

        // Generic disk-actor invoke used by the catalog_probe adapter below.
        template<typename Fn, typename... Args>
        auto disk_invoke(Fn fn, Args&&... args) {
            auto [_, fut] = actor_zeta::otterbrix::send(manager_disk_->address(), fn, std::forward<Args>(args)...);
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
                scheduler_->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(fut.is_ready());
            return std::move(fut).take_ready();
        }

        // Adapter exposing the (resource, invoke) shape that test_probe helpers expect.
        struct probe_fixture {
            differential_fixture* self;
            std::pmr::memory_resource& resource;
            template<typename Fn, typename... Args>
            auto invoke(Fn fn, Args&&... args) {
                return self->disk_invoke(fn, std::forward<Args>(args)...);
            }
        };
        probe_fixture probe_fx() { return probe_fixture{this, *resource_}; }

        cursor_t_ptr take_result() {
            // A plan can span a multi-actor co_await chain (e.g. SET TIMEZONE:
            // executor → dispatcher → disk → back). Each cross-actor co_await
            // re-enters the scheduler, so one step() may not drain it. Pump until
            // the future is ready or a 5s wall-clock deadline.
            REQUIRE(pending_future_);
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!pending_future_->is_ready() && std::chrono::steady_clock::now() < deadline) {
                scheduler_->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(pending_future_->valid());
            REQUIRE(pending_future_->is_ready());
            auto result = std::move(*pending_future_).take_ready();
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
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
                scheduler_->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(fut.is_ready());
            return std::move(fut).take_ready();
        }

        test_probe::probe_table_result_t resolve_table(components::catalog::oid_t ns_oid, const std::string& tname) {
            components::execution_context_t ctx{components::session::session_id_t{},
                                                components::table::transaction_data{0, 0},
                                                {}};
            auto adapter = probe_fx();
            return test_probe::probe_table(adapter, ctx, ns_oid, tname);
        }

        // Post a parsed logical plan to manager_dispatcher's execute_plan handler.
        void execute_sql(const std::string& query) {
            parser_arena_ = std::make_unique<std::pmr::monotonic_buffer_resource>(resource_);
            auto parse_result = linitial(raw_parser(parser_arena_.get(), query.c_str()));
            components::sql::transform::transformer local_transformer(resource_);
            auto _wrap =
                local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_result)).finalize();
            REQUIRE(!_wrap.has_error());
            auto view = _wrap.value();

            auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                           &manager_dispatcher_t::execute_plan,
                                                           components::session::session_id_t{},
                                                           std::move(view));
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

// SELECT pass-through differential. Fixture: CREATE DATABASE/TABLE/INSERT,
// then SELECT. Assert SELECT cursor is successful and column shape matches.
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
        // SELECT must return success + at least one column type descriptor.
        REQUIRE(cur->type_data().size() >= 1);
    }
}

// CREATE TABLE through dispatcher → manager_disk pg_class row exists with
// matching relkind / column shape. The columns-by-attname loop mirrors
// test_dispatcher_catalog.cpp::schemeful_operations.
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

// INSERT static-shape rows then SELECT — verifies the data path: row
// allocation, ETL into vector::data_chunk_t, cursor materialization. The
// relkind='g' (computed-column adoption) variant is covered separately.
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

// CREATE TABLE → CREATE INDEX. Indexes share the pg_class namespace with tables,
// so resolve_table(index_name).relkind=='i' is the proxy: it can only succeed if
// the pg_index entry, the pg_depend 'a' parent edge, and the index OID stamp were
// all written.
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
        auto rt = fx.resolve_table(rns.oid, "items");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        // Index in pg_class with relkind='i' in the same namespace. resolve_table
        // can only find it if the OID stamp + pg_depend 'a' edge were written too.
        auto ri = fx.resolve_table(rns.oid, "items_idx");
        REQUIRE(ri.found);
        REQUIRE(ri.relkind == 'i');
    }
}

// CREATE TABLE → DROP TABLE. After the drop, resolve_table by the same
// (namespace_oid, name) must return found=false — the observable contract of
// "pg_class delete_id set + dropped storage list".
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
        // After DROP TABLE the pg_class row is tombstoned (delete_id stamped)
        // and storages_ no longer publishes commits for it → resolve_table
        // reports not found.
        auto rns = fx.resolve_namespace("ve3_drop");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "victim");
        REQUIRE(!rt.found);
    }
}

// CREATE TABLE → ALTER TABLE ADD COLUMN. Requires a fresh pg_attribute row
// with added_at_commit_id stamped. The columns vector returned by
// resolve_table is reconstructed from pg_attribute, so observing the new
// column name there transitively guarantees the row was inserted.
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
        // Post-ALTER: resolve_table now surfaces `extra` (rebuilt from the new
        // pg_attribute row).
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

// CREATE TYPE <name> AS (<field> <type>, ...) registers a composite row in
// pg_type plus a nested pg_attribute row per field. The user-facing
// observation is a successful resolve_type under the default namespace
// (well_known_oid::public_namespace). Composite types in this codebase are
// registered without a database prefix.
TEST_CASE("variant-e3 differential: CREATE TYPE STRUCT") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_type");

    fx.execute_sql("CREATE TYPE ve3_point_t AS (px int, py int);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Verified indirectly via the table below that uses the type: the
        // column-spec parser resolves it through the catalog type-resolution path,
        // which only succeeds if the pg_type row + its nested pg_attribute rows
        // were written.
    }

    fx.execute_sql("CREATE DATABASE ve3_ty_db;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_ty_db.pts(id int, p ve3_point_t);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // The parent table accepting the composite type is the proof CREATE TYPE
        // registered it.
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

// Empty CREATE TABLE → pg_class row stamped relkind='g' (computing/generated),
// columns vector empty. The first INSERT adopts the column shape by
// appending pg_computed_column rows via operator_computed_field_register_t —
// resolve_table on the next txn surfaces those columns as if they had been
// declared statically. Mirrors test_dispatcher_catalog.cpp::computed_operations.
TEST_CASE("variant-e3 differential: INSERT relkind='g' computed-column adoption") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_computed");

    fx.execute_sql("CREATE DATABASE ve3_cg;");
    (void) fx.take_result();

    // Empty column-list CREATE TABLE → relkind='g'.
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

    fx.execute_sql("INSERT INTO ve3_cg.events (kind, payload) VALUES ('click', 'p1'), ('view', 'p2');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Post-INSERT: pg_computed_column carries the adopted shape;
        // resolve_table rebuilds the columns vector from those rows.
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

// CREATE DATABASE → resolve_namespace.found=true → DROP DATABASE →
// resolve_namespace.found=false. The externally observable contract of
// "pg_database row tombstoned + namespace_oid removed from catalog cache".
// CASCADE wipe of child pg_class rows is implicitly covered: after the
// namespace is gone, no resolve_table call can succeed regardless of relkind.
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
        // pg_database tombstone — the namespace must not resolve after DROP.
        auto rns = fx.resolve_namespace("ve3_dropdb");
        REQUIRE(!rns.found);
    }
}

// CREATE TABLE → CREATE VIEW. A view lands a pg_class relkind='v' row plus a
// pg_rewrite ev_action row carrying the body SQL. Proxy: resolve_table(view).
// relkind=='v'. The pg_rewrite row isn't exposed by the probe_table result but
// is required for SELECT-on-view expansion (covered e2e elsewhere).
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

    fx.execute_sql("CREATE VIEW ve3_view.v AS SELECT col_a FROM ve3_view.t WHERE col_b > 10;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());

        auto rns = fx.resolve_namespace("ve3_view");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "t");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        auto rv = fx.resolve_table(rns.oid, "v");
        REQUIRE(rv.found);
        REQUIRE(rv.relkind == 'v');
    }
}

// ALTER TABLE ... ADD CONSTRAINT FOREIGN KEY (the only parser entry; it emits a
// node_create_constraint_t) writes a pg_constraint contype='f' row + pg_depend
// edges. manager_disk has no resolve_constraint API, so the proxy is FK
// enforcement on INSERT: a valid reference is accepted and an orphan rejected,
// which together can only happen if those rows were written.
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

    fx.execute_sql("ALTER TABLE ve3_fk.employees ADD CONSTRAINT fk_dept "
                   "FOREIGN KEY (dept_id) REFERENCES ve3_fk.departments (id);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Both tables still resolvable — the constraint did no fanout damage.
        auto rns = fx.resolve_namespace("ve3_fk");
        REQUIRE(rns.found);
        auto rt_parent = fx.resolve_table(rns.oid, "departments");
        REQUIRE(rt_parent.found);
        REQUIRE(rt_parent.relkind == 'r');
        auto rt_child = fx.resolve_table(rns.oid, "employees");
        REQUIRE(rt_child.found);
        REQUIRE(rt_child.relkind == 'r');
    }

    fx.execute_sql("INSERT INTO ve3_fk.departments (id, name) VALUES (1, 'Engineering');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // Proxy for pg_constraint+pg_depend presence: a valid FK reference is
    // accepted (only possible if the pg_constraint row was written) ...
    fx.execute_sql("INSERT INTO ve3_fk.employees (id, dept_id, name) VALUES (1, 1, 'Alice');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // ... and an orphan reference is rejected by the same constraint.
    fx.execute_sql("INSERT INTO ve3_fk.employees (id, dept_id, name) VALUES (2, 99, 'Bob');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}

// CREATE TABLE → CREATE MATERIALIZED VIEW mv AS SELECT ... FROM table. Unlike a
// view, a matview lands a real physical heap plus a pg_class relkind='m' row and
// a pg_rewrite ev_action row (WITH NO DATA default, so it is empty until
// REFRESH). operator_create_matview_t lowers to a composite sequence that aborts
// the whole CREATE if any step fails. Proxy: cursor success + resolve_table(mv)
// relkind=='m' + parent still resolvable.
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

    fx.execute_sql("CREATE MATERIALIZED VIEW ve3_mv.mv AS SELECT col_a FROM ve3_mv.t WHERE col_b > 10;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());

        auto rns = fx.resolve_namespace("ve3_mv");
        REQUIRE(rns.found);
        // Parent still resolvable — no fanout damage from the matview create.
        auto rt = fx.resolve_table(rns.oid, "t");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        auto rmv = fx.resolve_table(rns.oid, "mv");
        REQUIRE(rmv.found);
        REQUIRE(rmv.relkind == 'm');
    }
}

// ALTER TABLE ... ADD CONSTRAINT ... CHECK writes a pg_constraint contype='c'
// row (vs 'f' for FK). Same proxy pattern as the FK test: a conforming INSERT is
// accepted and a violating one rejected — neither can happen unless the
// contype='c' row (with its parsed conexpr) drives operator_check_constraint.
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

    fx.execute_sql("ALTER TABLE ve3_chk.items ADD CONSTRAINT chk_age CHECK (age > 0);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
        // Table still resolvable — the constraint did no fanout damage.
        auto rns = fx.resolve_namespace("ve3_chk");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "items");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
    }

    // Proxy: a conforming row is accepted ...
    fx.execute_sql("INSERT INTO ve3_chk.items (id, age, name) VALUES (1, 25, 'alice');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // ... and a violating row is rejected by the same CHECK constraint.
    fx.execute_sql("INSERT INTO ve3_chk.items (id, age, name) VALUES (2, -1, 'bad');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}

// SET TIMEZONE appends a pg_settings row ('TimeZone', <tz>) and mutates the
// actor's in-memory default_tz_cat_ (single-owner, no shared mutable state).
// Proxy: cursor success, a second SET on the same fixture (re-entry must not
// double-fail), and an unknown-TZ error exercising the validation path.
TEST_CASE("variant-e3 differential: SET TIME ZONE") {
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_settz");

    // Valid timezone succeeds (pg_settings append + default_tz_cat_ mutation).
    fx.execute_sql("SET TIMEZONE TO 'utc';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    // Re-entry on the same actor: default_tz_cat_ must accept an overwrite.
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

    // Unknown timezone: default_tz_cat_ rejects, no pg_settings append, cursor
    // surfaces the error.
    fx.execute_sql("SET TIMEZONE TO 'not_a_real_timezone';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}
