#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

#include <services/dispatcher/dispatcher.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
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

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::catalog;
using namespace components::cursor;
using namespace components::types;

// Dispatcher integration test. Catalog assertions go through manager_disk_t::resolve_namespace
// and the test_probe catalog oracle — there is no in-memory catalog snapshot to read.

namespace {
    // A run that dies before its destructor — an aborting REQUIRE, a crash, a kill, a timeout —
    // leaves its disk directory behind, and the fixture then boots the NEXT run's catalog from
    // those files instead of creating a fresh one, so a later, unrelated run fails and looks like a
    // regression. Clearing on the way IN as well as on the way OUT makes the fixture idempotent.
    // Called from the member-initializer list so it happens BEFORE manager_disk_t is constructed
    // over this path.
    const std::string& scrubbed(const std::string& path) {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        return path;
    }
} // namespace

struct test_dispatcher : actor_zeta::actor::actor_mixin<test_dispatcher> {
    test_dispatcher(std::pmr::memory_resource* resource, const std::string& disk_path)
        : actor_zeta::actor::actor_mixin<test_dispatcher>()
        , resource_(resource)
        , disk_path_(scrubbed(disk_path))
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
        // Pass WAL address — disk's append_pg_catalog_row sends physical_insert to it.
        manager_disk_->sync(services::disk::manager_disk_t::disk_sync_pack_t{manager_wal_->address()});

        // Bootstrap pg_catalog system tables so the disk-side catalog has tables to scan.
        manager_disk_->bootstrap_system_tables_sync();
    }

    ~test_dispatcher() {
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
        test_dispatcher* self;
        std::pmr::memory_resource& resource;
        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            return self->disk_invoke(fn, std::forward<Args>(args)...);
        }
    };
    probe_fixture probe_fx() { return probe_fixture{this, *resource_}; }

    cursor_t_ptr take_result() {
        // execute_plan's future becomes ready asynchronously (the manager actors
        // self-drive on internal threads). Pump the child scheduler until ready,
        // bounded by a 5s wall-clock deadline.
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
        // Drain again so the executor's post-result DDL pipeline (catalog writes,
        // flush, commit_txn, storage_publish_commits) finishes before returning.
        step();
        return result;
    }

    // Resolve a namespace via disk actor — returns {found, oid}.
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
        // The reader carries an error channel ("the catalog could not be READ" is not "the
        // catalog does not have it"); no case here expects a failed read, and letting one
        // through as {found=false} would conflate the two.
        auto r = std::move(fut).take_ready();
        REQUIRE_FALSE(r.has_error());
        return std::move(r.value());
    }

    // Resolve a table via the live read_chunks_by_key path (catalog-read oracle).
    test_probe::probe_table_result_t resolve_table(components::catalog::oid_t ns_oid, const std::string& tname) {
        components::execution_context_t ctx{components::session::session_id_t{},
                                            components::table::transaction_data{0, 0},
                                            {}};
        auto adapter = probe_fx();
        return test_probe::probe_table(adapter, ctx, ns_oid, tname);
    }

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
                                                       session_id_t{},
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

TEST_CASE("services::dispatcher::schemeful_operations") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    test_dispatcher test(mr.get(), "/tmp/test_dispatcher_disk_schemeful");

    test.execute_sql("CREATE DATABASE test;");
    (void) test.take_result();

    test.execute_sql("CREATE TABLE test.test(fld1 int, fld2 string);");
    {
        auto cur = test.take_result();
        REQUIRE(cur->is_success());
        auto rns = test.resolve_namespace("test");
        REQUIRE(rns.found);
        auto rt = test.resolve_table(rns.oid, "test");
        REQUIRE(rt.found);
        REQUIRE(rt.relkind == 'r');
        // Locate columns by attname.
        bool seen_fld1 = false, seen_fld2 = false;
        for (const auto& col : rt.columns) {
            if (col.attname == "fld1")
                seen_fld1 = true;
            if (col.attname == "fld2")
                seen_fld2 = true;
        }
        REQUIRE(seen_fld1);
        REQUIRE(seen_fld2);
    }

    test.execute_sql("INSERT INTO test.test (fld1, fld2) VALUES (1, '1'), (2, '2');");
    {
        auto cur = test.take_result();
        REQUIRE(cur->is_success());
        auto rns = test.resolve_namespace("test");
        REQUIRE(rns.found);
        auto rt = test.resolve_table(rns.oid, "test");
        REQUIRE(rt.found);
    }

    SECTION("in-order") {
        test.execute_sql("DROP TABLE test.test;");
        {
            auto cur = test.take_result();
            REQUIRE(cur->is_success());
            auto rns = test.resolve_namespace("test");
            if (rns.found) {
                auto rt = test.resolve_table(rns.oid, "test");
                REQUIRE(!rt.found);
            }
        }

        test.execute_sql("DROP DATABASE test;");
        {
            auto cur = test.take_result();
            REQUIRE(cur->is_success());
            auto rns = test.resolve_namespace("test");
            REQUIRE(!rns.found);
        }
    }

    SECTION("drop_database") {
        test.execute_sql("DROP DATABASE test;");
        {
            auto cur = test.take_result();
            REQUIRE(cur->is_success());
            auto rns = test.resolve_namespace("test");
            REQUIRE(!rns.found);
        }
    }
}

TEST_CASE("services::dispatcher::computed_operations") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    test_dispatcher test(mr.get(), "/tmp/test_dispatcher_disk_computed");

    test.execute_sql("CREATE DATABASE test;");
    (void) test.take_result();

    test.execute_sql("CREATE TABLE test.test();");
    {
        auto cur = test.take_result();
        REQUIRE(cur->is_success());
        auto rns = test.resolve_namespace("test");
        REQUIRE(rns.found);
        auto rt = test.resolve_table(rns.oid, "test");
        REQUIRE(rt.found);
        // Empty CREATE TABLE → relkind='g' (computing/generated). Columns adopted on insert.
        REQUIRE(rt.relkind == 'g');
        REQUIRE(rt.columns.empty());
    }

    std::stringstream query;
    query << "INSERT INTO test.test (name, count) VALUES ";
    for (int num = 0; num < 100; ++num) {
        query << "('Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
    }

    test.execute_sql(query.str());
    // INSERT into a relkind='g' table — columns visible on next resolve via
    // pg_computed_column (operator_computed_field_register_t).
    {
        auto cur = test.take_result();
        REQUIRE(cur->is_success());
        auto rns = test.resolve_namespace("test");
        REQUIRE(rns.found);
        auto rt = test.resolve_table(rns.oid, "test");
        REQUIRE(rt.found);
        // After adoption the columns reflect the inserted shape.
        bool seen_name = false, seen_count = false;
        for (const auto& col : rt.columns) {
            if (col.attname == "name")
                seen_name = true;
            if (col.attname == "count")
                seen_count = true;
        }
        REQUIRE(seen_name);
        REQUIRE(seen_count);
    }
}

// ===========================================================================
// A conkey THAT IS NOT WHAT encode_oid_csv WROTE MUST STOP THE STATEMENT.
//
// pg_constraint.conkey is a CSV of column attoids, read POSITIONALLY and enforced as an ordered
// tuple. parse_oid_csv answers the decoded list plus an `ok` channel, because the list alone
// cannot carry the loss: every guard downstream compares the resolved column NAMES against the
// very attoid list they were resolved from, so a list that lost a token — or gained a wrong one
// — agrees with itself and passes.
//
// Two shapes must never come back with `ok == true`:
//
//   * TRUNCATED AT A COMMA ("7,11," for "7,11,13"): a loop that stops at the last separator
//     without looking at the token behind it reads a three-column key back as a two-column one,
//     and the engine then enforces a NARROWER key than the one declared;
//   * A TOKEN TOO LARGE FOR AN OID ("4294967297"): read through a 64-bit integer and
//     static_cast down to 32 bits, 2^32 + N reads as N — THE KEY BINDS TO THE NEIGHBOURING
//     COLUMN, one token in and one token out, with nothing for the length guard to notice.
//
// HOW THE ROW IS PRODUCED. The pg_constraint row is built by the engine's own
// build_create_constraint_writes for the constraint being declared and written through the
// engine's own append_pg_catalog_row — the same call operator_insert makes for every DDL row.
// Exactly ONE cell is then different: conkey carries the text a truncated or mis-serialized
// write leaves behind, which is the only way this population is reachable at all (encode_oid_csv,
// the writer, emits neither shape).
// ===========================================================================

namespace {

    // Same shape as test_wave_exec_dispatcher.cpp's wave_dir: ::getpid() in the path so two
    // ctest shards (or two build directories) never boot a catalog out of each other's files.
    std::string catalog_dir(const char* leaf) {
        return "/tmp/test_dispatcher_catalog_" + std::to_string(::getpid()) + "/" + leaf;
    }

    // Write one pg_constraint row (+ its pg_depend rows) for a UNIQUE constraint
    // on `key_attoids` of `table_oid`. `conkey_text` replaces the encoded column
    // list, and a non-null `contype_text` replaces the constraint-kind code.
    void plant_unique_constraint_row(test_dispatcher& test,
                                     std::pmr::memory_resource* resource,
                                     components::catalog::oid_t table_oid,
                                     const std::string& con_name,
                                     const std::vector<components::catalog::oid_t>& key_attoids,
                                     const std::string& conkey_text,
                                     const char* contype_text = nullptr) {
        components::execution_context_t ctx{components::session::session_id_t{},
                                            components::table::transaction_data{0, 0},
                                            {}};
        auto oids = test.disk_invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        REQUIRE_FALSE(oids.empty());
        auto writes_r = components::catalog::build_create_constraint_writes(resource,
                                                                            con_name,
                                                                            table_oid,
                                                                            oids.front(),
                                                                            /*contype=*/'u',
                                                                            components::catalog::INVALID_OID,
                                                                            key_attoids,
                                                                            /*ref_column_attoids=*/{},
                                                                            /*fk_matchtype=*/'s',
                                                                            /*fk_del_action=*/'a',
                                                                            /*fk_upd_action=*/'a',
                                                                            /*check_expr=*/"");
        REQUIRE_FALSE(writes_r.has_error());
        auto writes = std::move(writes_r.value());
        for (auto& w : writes) {
            if (w.table_oid == well_known_oid::pg_constraint_table) {
                w.row.set_value(components::catalog::pg_constraint_col::conkey,
                                std::uint64_t{0},
                                std::string_view{conkey_text});
                if (contype_text != nullptr) {
                    w.row.set_value(components::catalog::pg_constraint_col::contype,
                                    std::uint64_t{0},
                                    std::string_view{contype_text});
                }
            }
            auto appended = test.disk_invoke(&manager_disk_t::append_pg_catalog_row, ctx, w.table_oid, std::move(w.row));
            REQUIRE_FALSE(appended.has_error());
        }
        test.step();
    }

    // (table oid, attoid of `id`, attoid of `code`) for a freshly created
    // two-column table, read back through the live catalog-read path.
    struct planted_table_t {
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t id_attoid{components::catalog::INVALID_OID};
        components::catalog::oid_t code_attoid{components::catalog::INVALID_OID};
    };

    planted_table_t create_two_column_table(test_dispatcher& test) {
        test.execute_sql("CREATE DATABASE conkey_db;");
        REQUIRE(test.take_result()->is_success());
        test.execute_sql("CREATE TABLE conkey_db.t (id bigint, code bigint);");
        REQUIRE(test.take_result()->is_success());

        auto rns = test.resolve_namespace("conkey_db");
        REQUIRE(rns.found);
        auto rt = test.resolve_table(rns.oid, "t");
        REQUIRE(rt.found);
        planted_table_t out;
        out.table_oid = rt.oid;
        for (const auto& col : rt.columns) {
            if (col.attname == "id")
                out.id_attoid = col.attoid;
            if (col.attname == "code")
                out.code_attoid = col.attoid;
        }
        REQUIRE(out.table_oid != components::catalog::INVALID_OID);
        REQUIRE(out.id_attoid != components::catalog::INVALID_OID);
        REQUIRE(out.code_attoid != components::catalog::INVALID_OID);
        return out;
    }

} // namespace

// UNIQUE (id, code) whose conkey lost its tail to a truncation. The declared key
// permits two rows that share `id` and differ in `code`; the truncated key —
// UNIQUE (id) — does not. Refusing that write as a DUPLICATE is the engine
// enforcing a constraint the user never wrote, and saying so in the user's face.
TEST_CASE("services::dispatcher::conkey_csv::a_conkey_truncated_at_a_comma_is_not_a_narrower_key") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    test_dispatcher test(mr.get(), catalog_dir("conkey_truncated"));
    const auto planted = create_two_column_table(test);

    plant_unique_constraint_row(test,
                                mr.get(),
                                planted.table_oid,
                                "uq_id_code",
                                {planted.id_attoid, planted.code_attoid},
                                std::to_string(planted.id_attoid) + ",");

    test.execute_sql("INSERT INTO conkey_db.t (id, code) VALUES (1, 100);");
    auto first = test.take_result();
    INFO("first INSERT: " << (first->is_error() ? std::string(first->get_error().what) : std::string("accepted")));

    test.execute_sql("INSERT INTO conkey_db.t (id, code) VALUES (1, 200);");
    auto permitted = test.take_result();
    const std::string what = permitted->is_error() ? std::string(permitted->get_error().what) : std::string();
    INFO("INSERT the DECLARED key (id, code) permits: " << (permitted->is_error() ? what : std::string("accepted")));

    INFO("a key column list that cannot be read is not a shorter key column list");
    const bool refused_under_a_key_never_declared =
        permitted->is_error() && what.find("UNIQUE constraint violated") != std::string::npos;
    REQUIRE_FALSE(refused_under_a_key_never_declared);
    if (permitted->is_error()) {
        INFO("and a refusal has to name the constraint the user can act on");
        CHECK(what.find("uq_id_code") != std::string::npos);
    }
}

// UNIQUE (id) whose conkey token is 2^32 above the attoid of `code`. Read through
// a 64-bit integer and cast down, it IS the attoid of `code`: the declared key
// silently becomes a key on the neighbouring column, and duplicate `id`s walk in.
TEST_CASE("services::dispatcher::conkey_csv::an_out_of_range_conkey_does_not_bind_the_key_to_another_column") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    test_dispatcher test(mr.get(), catalog_dir("conkey_out_of_range"));
    const auto planted = create_two_column_table(test);

    const std::string shifted =
        std::to_string(static_cast<std::uint64_t>(planted.code_attoid) + (std::uint64_t{1} << 32));
    plant_unique_constraint_row(test, mr.get(), planted.table_oid, "uq_id", {planted.id_attoid}, shifted);

    test.execute_sql("INSERT INTO conkey_db.t (id, code) VALUES (1, 100);");
    auto first = test.take_result();
    INFO("first INSERT: " << (first->is_error() ? std::string(first->get_error().what) : std::string("accepted")));

    test.execute_sql("INSERT INTO conkey_db.t (id, code) VALUES (1, 200);");
    auto dup = test.take_result();
    INFO("duplicate-id INSERT: " << (dup->is_error() ? std::string(dup->get_error().what) : std::string("accepted")));

    // THE USER CONSEQUENCE, read off the table: how many rows carry id = 1 under
    // a declared UNIQUE (id).
    test.execute_sql("SELECT code FROM conkey_db.t WHERE id = 1;");
    auto stored = test.take_result();
    INFO("read error: " << (stored->is_error() ? std::string(stored->get_error().what) : std::string("none")));
    REQUIRE(stored->is_success());
    INFO("rows carrying id = 1: " << stored->size());
    INFO("a UNIQUE (id) that was accepted must be enforced on id, not on whichever column the token decayed to");
    REQUIRE(stored->size() <= 1);
}

// A pg_constraint row whose contype cannot be read is a constraint of UNKNOWN
// KIND — it may be the UNIQUE the user declared. A decode loop that classifies rows
// by that char and skips what it cannot classify drops such a row out of the
// constraint set before any of the refusals below can see it: the same silence as a
// dropped conkey group, one step earlier in the same loop.
TEST_CASE("services::dispatcher::conkey_csv::a_constraint_row_of_unknown_kind_is_not_skipped") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    test_dispatcher test(mr.get(), catalog_dir("conkey_unknown_kind"));
    const auto planted = create_two_column_table(test);

    // A perfectly readable key column list — only the KIND of the constraint is
    // gone, so nothing but the classification step can notice this row at all.
    plant_unique_constraint_row(test,
                                mr.get(),
                                planted.table_oid,
                                "uq_id_kindless",
                                {planted.id_attoid},
                                std::to_string(planted.id_attoid),
                                /*contype_text=*/"");

    test.execute_sql("INSERT INTO conkey_db.t (id, code) VALUES (1, 100);");
    auto first = test.take_result();
    INFO("first INSERT: " << (first->is_error() ? std::string(first->get_error().what) : std::string("accepted")));

    test.execute_sql("INSERT INTO conkey_db.t (id, code) VALUES (1, 200);");
    auto dup = test.take_result();
    INFO("duplicate-id INSERT: " << (dup->is_error() ? std::string(dup->get_error().what) : std::string("accepted")));

    test.execute_sql("SELECT code FROM conkey_db.t WHERE id = 1;");
    auto stored = test.take_result();
    INFO("read error: " << (stored->is_error() ? std::string(stored->get_error().what) : std::string("none")));
    REQUIRE(stored->is_success());
    INFO("rows carrying id = 1: " << stored->size());
    INFO("a constraint row that cannot be classified must stop the statement, not leave the set unannounced");
    REQUIRE(stored->size() <= 1);
}

// ===========================================================================
// A SOURCE COLUMN WITH NO TYPE MUST BE NAMED BY THE STATEMENT THAT NAMED IT,
// NOT BY THE STORAGE SEGMENT THAT CHOKED ON IT.
//
// The VALUES form of this already answers by name: validate_types drops an
// all-NULL column from the chunk (a schemaless table cannot create a column
// from a value that has no type) and says WHICH column went and why. The
// INSERT ... SELECT form never reaches that erase — the projection column is
// typed logical_type::NA (0) and stays in the source schema, bind_computed_rename
// binds it with target_type = NA, the computed-register wrap creates the catalog
// column from it, and the append dies down in column_segment_t with
//
//     "column_segment_t::append: no segment storage for physical type 127"
//
// (127 is physical_type::NA). That sentence names no column, no statement and no
// cause, and it arrives AFTER the register wrap has already put a phantom NA
// column into the target's catalog: the table then reports columns it holds no
// rows for. State survives a failure it should not have survived.
//
// The refusal has to be NARROW, and it is: an unknown key on a schemaless table
// is a different diagnosis with a different message, produced far earlier by
// validate_key, and a plain projection of NULL is not affected at all — the guard
// lives on the INSERT binding, not on the select list.
// ===========================================================================
TEST_CASE("services::dispatcher::null_source_column::insert_select_names_the_typeless_column") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    test_dispatcher test(mr.get(), catalog_dir("null_source_column"));

    auto run = [&test](const char* sql) {
        test.execute_sql(sql);
        return test.take_result();
    };

    REQUIRE(run("CREATE DATABASE nsc;")->is_success());
    REQUIRE(run("CREATE TABLE nsc.src();")->is_success());
    REQUIRE(run("INSERT INTO nsc.src (a, b) VALUES (1, 2);")->is_success());

    // ---- the defect: a written column list ----
    REQUIRE(run("CREATE TABLE nsc.d1();")->is_success());
    {
        auto refused = run("INSERT INTO nsc.d1 (x, y) SELECT a, NULL FROM nsc.src;");
        REQUIRE_FALSE(refused->is_success());
        const std::string what{refused->get_error().what.c_str()};
        INFO("refusal text: " << what);
        // the column the statement named, and the reason, both readable
        CHECK(what.find("\"y\"") != std::string::npos);
        CHECK(what.find("no type to create the column from") != std::string::npos);
        // and NOT the storage segment's sentence
        CHECK(what.find("column_segment_t::append") == std::string::npos);
    }
    {
        // nothing registered: the target is still the empty computing table it was
        auto after = run("SELECT * FROM nsc.d1;");
        REQUIRE(after->is_success());
        INFO("columns registered on the refused target: " << after->column_count());
        CHECK(after->column_count() == 0);
    }

    // ---- same defect without a written column list (the projection names it) ----
    REQUIRE(run("CREATE TABLE nsc.d2();")->is_success());
    {
        auto refused = run("INSERT INTO nsc.d2 SELECT a AS x, NULL AS y FROM nsc.src;");
        REQUIRE_FALSE(refused->is_success());
        const std::string what{refused->get_error().what.c_str()};
        INFO("refusal text: " << what);
        CHECK(what.find("\"y\"") != std::string::npos);
        CHECK(what.find("column_segment_t::append") == std::string::npos);
    }

    // ---- CAST does not give the column a type either, so it is refused the same way ----
    // (NULL::bigint / CAST(NULL AS BIGINT) still resolve to logical_type::NA here; the
    // refusal must therefore not advertise a cast as the way out.)
    REQUIRE(run("CREATE TABLE nsc.d3();")->is_success());
    {
        auto refused = run("INSERT INTO nsc.d3 (x, y) SELECT a, CAST(NULL AS BIGINT) FROM nsc.src;");
        REQUIRE_FALSE(refused->is_success());
        const std::string what{refused->get_error().what.c_str()};
        INFO("refusal text: " << what);
        CHECK(what.find("column_segment_t::append") == std::string::npos);
    }

    // ---- WHAT MUST NOT CHANGE ----
    // A projection of NULL is a legal result column; only writing it into a
    // dynamic-schema table is not.
    {
        auto plain = run("SELECT a, NULL FROM nsc.src;");
        REQUIRE(plain->is_success());
        CHECK(plain->column_count() == 2);
    }
    // A DECLARED target has a column type to store the null under.
    REQUIRE(run("CREATE TABLE nsc.reg (k bigint, v bigint);")->is_success());
    CHECK(run("INSERT INTO nsc.reg (k, v) SELECT a, NULL FROM nsc.src;")->is_success());
    // A UNION branch that supplies a value types the column, so nothing is NA.
    REQUIRE(run("CREATE TABLE nsc.d4();")->is_success());
    CHECK(run("INSERT INTO nsc.d4 (x, y) SELECT a, b FROM nsc.src UNION ALL SELECT a, NULL FROM nsc.src;")
              ->is_success());
    // An unknown key on a schemaless table keeps its OWN, earlier diagnosis —
    // this is the distinction the guard must not blur.
    {
        auto unknown = run("INSERT INTO nsc.d4 (x, y) SELECT a, nosuchkey FROM nsc.src;");
        REQUIRE_FALSE(unknown->is_success());
        const std::string what{unknown->get_error().what.c_str()};
        INFO("refusal text: " << what);
        CHECK(what.find("'nosuchkey' was not found") != std::string::npos);
        CHECK(what.find("no type to create the column from") == std::string::npos);
    }
}
