#include <catch2/catch_test_macros.hpp>

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
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

// Same SQL fixture drives dispatcher::execute_plan; each case compares the cursor against pg_catalog state.

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::catalog;
using namespace components::cursor;
using namespace components::types;

namespace {

    // Clears on both entry and exit, so a run that died before its destructor can't poison the next one's catalog.
    const std::string& scrubbed(const std::string& path) {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        return path;
    }

    // Mirrors test_dispatcher_catalog.cpp's actor wiring; the name must differ or a shared Catch2 target ODR-clashes.
    struct differential_fixture : actor_zeta::actor::actor_mixin<differential_fixture> {
        differential_fixture(std::pmr::memory_resource* resource, const std::string& disk_path)
            : actor_zeta::actor::actor_mixin<differential_fixture>()
            , resource_(resource)
            , disk_path_(scrubbed(disk_path))
            , log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config_(disk_path)
            , manager_disk_(actor_zeta::spawn<manager_disk_t>(resource, scheduler_, scheduler_, disk_config_, log_))
            // A real index manager: with none wired, backfill used to report success without doing anything.
            , manager_index_(actor_zeta::spawn<services::index::manager_index_t>(resource,
                                                                                 scheduler_,
                                                                                 log_,
                                                                                 disk_config_.path,
                                                                                 disk_config_.bitcask_flush_threshold,
                                                                                 disk_config_.bitcask_segment_record_limit,
                                                                                 disk_config_.btree_flush_threshold))
            , wal_config_(disk_path)
            , manager_wal_(actor_zeta::spawn<manager_wal_replicate_t>(resource,
                                                                      scheduler_,
                                                                      wal_config_,
                                                                      log_,
                                                                      manager_disk_->address(),
                                                                      manager_index_->address()))
            , manager_dispatcher_(actor_zeta::spawn<manager_dispatcher_t>(resource,
                                                                          scheduler_,
                                                                          log_,
                                                                          manager_wal_->address(),
                                                                          manager_disk_->address(),
                                                                          manager_index_->address())) {
            manager_wal_->set_manager_dispatcher_sync(manager_dispatcher_->address());
            manager_disk_->set_manager_wal_sync(manager_wal_->address());
            manager_index_->set_manager_dispatcher_sync(manager_dispatcher_->address());

            manager_disk_->bootstrap_system_tables_sync();
        }

        ~differential_fixture() {
            // Reverse dependency order: index before disk, since index holds disk's address for teardown.
            manager_dispatcher_.reset();
            manager_wal_.reset();
            manager_index_.reset();
            manager_disk_.reset();
            scheduler_->stop();
            std::filesystem::remove_all(disk_path_);
            delete scheduler_;
        }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        void step() { scheduler_->run(10000); }

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
            // A multi-actor co_await chain may not drain in one step(), so pump until ready or a 5s deadline.
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
                                                components::table::transaction_data::committed(),
                                                {}};
            auto [_, fut] = actor_zeta::otterbrix::send(manager_disk_->address(),
                                                        &manager_disk_t::resolve_namespace,
                                                        ctx,
                                                        name);
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
                scheduler_->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(fut.is_ready());
            // The reader has its own error channel; a failed read must not be conflated with found=false.
            auto r = std::move(fut).take_ready();
            REQUIRE_FALSE(r.has_error());
            return std::move(r.value());
        }

        test_probe::probe_table_result_t resolve_table(components::catalog::oid_t ns_oid, const std::string& tname) {
            // probe_see_all_txn, not transaction_data::committed(): a start_time of 0 would hide every ALTER-added column.
            components::execution_context_t ctx{components::session::session_id_t{},
                                                test_probe::probe_see_all_txn(),
                                                {}};
            auto adapter = probe_fx();
            return test_probe::probe_table(adapter, ctx, ns_oid, tname);
        }

        void execute_sql(const std::string& query) {
            parser_arena_ = std::make_unique<std::pmr::monotonic_buffer_resource>(resource_);
            auto parse_result = linitial(raw_parser(parser_arena_.get(), query.c_str()));
            // CREATE VIEW / CREATE MATERIALIZED VIEW slice their body verbatim out of this string.
            components::sql::transform::transformer local_transformer(resource_, query.c_str());
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
        configuration::config_disk disk_config_;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager_disk_;
        std::unique_ptr<services::index::manager_index_t, actor_zeta::pmr::deleter_t> manager_index_;
        configuration::config_wal wal_config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_wal_;
        // Declared after the managers: the dispatcher is spawned with their addresses.
        std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t> manager_dispatcher_;
        std::unique_ptr<std::pmr::monotonic_buffer_resource> parser_arena_;
        std::unique_ptr<actor_zeta::unique_future<cursor_t_ptr>> pending_future_;
    };

} // namespace

TEST_CASE("variant-e3 differential: SELECT pass-through") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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
        REQUIRE(cur->type_data().size() >= 1);
    }
}

// The columns-by-attname loop mirrors test_dispatcher_catalog.cpp::schemeful_operations.
TEST_CASE("variant-e3 differential: CREATE TABLE basic") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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

// The relkind='g' (computed-column adoption) variant is covered separately below.
TEST_CASE("variant-e3 differential: INSERT + SELECT round-trip") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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

// resolve_table(index).relkind=='i' only succeeds if pg_index, the pg_depend edge, and the OID stamp were all written.
TEST_CASE("variant-e3 differential: CREATE INDEX") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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
        auto ri = fx.resolve_table(rns.oid, "items_idx");
        REQUIRE(ri.found);
        REQUIRE(ri.relkind == 'i');
    }
}

// found=false after the drop is the observable proxy for "pg_class delete_id set + dropped storage list".
TEST_CASE("variant-e3 differential: DROP TABLE") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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
        auto rns = fx.resolve_namespace("ve3_drop");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "victim");
        REQUIRE(!rt.found);
    }
}

// resolve_table rebuilds columns from pg_attribute, so seeing the new name there proves the row was inserted.
TEST_CASE("variant-e3 differential: ALTER TABLE ADD COLUMN") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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

// Observed indirectly: the table below only accepts the type if pg_type + its nested rows were written.
TEST_CASE("variant-e3 differential: CREATE TYPE STRUCT") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_type");

    fx.execute_sql("CREATE TYPE ve3_point_t AS (px int, py int);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("CREATE DATABASE ve3_ty_db;");
    (void) fx.take_result();

    fx.execute_sql("CREATE TABLE ve3_ty_db.pts(id int, p ve3_point_t);");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
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

// First INSERT adopts columns via pg_computed_column rows; mirrors test_dispatcher_catalog.cpp::computed_operations.
TEST_CASE("variant-e3 differential: INSERT relkind='g' computed-column adoption") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_computed");

    fx.execute_sql("CREATE DATABASE ve3_cg;");
    (void) fx.take_result();

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

// CASCADE is implicit: once the namespace is gone, no resolve_table call can succeed regardless of relkind.
TEST_CASE("variant-e3 differential: DROP DATABASE") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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
        auto rns = fx.resolve_namespace("ve3_dropdb");
        REQUIRE(!rns.found);
    }
}

// Proxy: resolve_table(view).relkind=='v'; the pg_rewrite body itself is exercised e2e elsewhere.
TEST_CASE("variant-e3 differential: CREATE VIEW") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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

// No resolve_constraint API, so the proxy is enforcement: a valid FK reference is accepted, an orphan rejected.
TEST_CASE("variant-e3 differential: CREATE CONSTRAINT FK") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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

    fx.execute_sql("INSERT INTO ve3_fk.employees (id, dept_id, name) VALUES (1, 1, 'Alice');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("INSERT INTO ve3_fk.employees (id, dept_id, name) VALUES (2, 99, 'Bob');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}

// Implicit WITH DATA is refused rather than silently producing an empty matview (see test_view_expansion.cpp).
TEST_CASE("variant-e3 differential: CREATE MATERIALIZED VIEW") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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
        "CREATE MATERIALIZED VIEW ve3_mv.mv AS SELECT col_a FROM ve3_mv.t WHERE col_b > 10 WITH NO DATA;");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());

        auto rns = fx.resolve_namespace("ve3_mv");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "t");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        auto rmv = fx.resolve_table(rns.oid, "mv");
        REQUIRE(rmv.found);
        REQUIRE(rmv.relkind == 'm');
    }
}

// Same proxy pattern as the FK test: a conforming INSERT is accepted, a violating one rejected.
TEST_CASE("variant-e3 differential: CREATE CONSTRAINT CHECK") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
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
        auto rns = fx.resolve_namespace("ve3_chk");
        REQUIRE(rns.found);
        auto rt = fx.resolve_table(rns.oid, "items");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
    }

    fx.execute_sql("INSERT INTO ve3_chk.items (id, age, name) VALUES (1, 25, 'alice');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("INSERT INTO ve3_chk.items (id, age, name) VALUES (2, -1, 'bad');");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}

// Proxy: cursor success, a same-actor re-entry not double-failing, and an unknown-TZ error hitting validation.
TEST_CASE("variant-e3 differential: SET TIME ZONE") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    differential_fixture fx(mr.get(), "/tmp/test_variant_e3_diff_settz");

    fx.execute_sql("SET TIMEZONE TO 'utc';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("SET TIMEZONE TO 'UTC';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("SET TIMEZONE TO 'america/new_york';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_success());
    }

    fx.execute_sql("SET TIMEZONE TO 'not_a_real_timezone';");
    {
        auto cur = fx.take_result();
        REQUIRE(cur->is_error());
    }
}
