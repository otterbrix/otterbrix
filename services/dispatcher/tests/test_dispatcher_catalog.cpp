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
#include <components/context/context.hpp>
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

// Catalog assertions go through manager_disk_t::resolve_namespace and test_probe; there is no
// in-memory snapshot to read.

namespace {
    // Clears on the way in too, so a run that died mid-test can't leave its directory for the next to boot from.
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
        , disk_config_(disk_path)
        , manager_disk_(actor_zeta::spawn<manager_disk_t>(resource, scheduler_, scheduler_, disk_config_, log_))
        , wal_config_([&]() {
            configuration::config_wal c;
            c.on = false;
            return c;
        }())
        , manager_wal_(actor_zeta::spawn<manager_wal_replicate_t>(resource,
                                                                   scheduler_,
                                                                   wal_config_,
                                                                   log_,
                                                                   manager_disk_->address(),
                                                                   components::pipeline::no_mailbox()))
        , manager_dispatcher_(actor_zeta::spawn<manager_dispatcher_t>(resource,
                                                                      scheduler_,
                                                                      log_,
                                                                      manager_wal_->address(),
                                                                      manager_disk_->address(),
                                                                      components::pipeline::no_mailbox())) {
        manager_wal_->set_manager_dispatcher_sync(manager_dispatcher_->address());
        manager_disk_->set_manager_wal_sync(manager_wal_->address());

        manager_disk_->bootstrap_system_tables_sync();
    }

    ~test_dispatcher() {
        // Destroy managers before the scheduler, in reverse dependency order, to avoid use-after-free.
        manager_dispatcher_.reset();
        manager_wal_.reset();
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
        test_dispatcher* self;
        std::pmr::memory_resource& resource;
        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            return self->disk_invoke(fn, std::forward<Args>(args)...);
        }
    };
    probe_fixture probe_fx() { return probe_fixture{this, *resource_}; }

    cursor_t_ptr take_result() {
        // The future becomes ready asynchronously; pump the scheduler until ready, bounded by a 5s deadline.
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
        // Drain again so the executor's post-result DDL pipeline finishes before returning.
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
                                                    name);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
            scheduler_->run(1000);
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
        // A failed read isn't {found=false}; no case here expects one, avoiding that conflation.
        auto r = std::move(fut).take_ready();
        REQUIRE_FALSE(r.has_error());
        return std::move(r.value());
    }

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
    configuration::config_disk disk_config_;
    std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager_disk_;
    configuration::config_wal wal_config_;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_wal_;
    // Declared after the managers: the dispatcher is spawned with their addresses.
    std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t> manager_dispatcher_;
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
    {
        auto cur = test.take_result();
        REQUIRE(cur->is_success());
        auto rns = test.resolve_namespace("test");
        REQUIRE(rns.found);
        auto rt = test.resolve_table(rns.oid, "test");
        REQUIRE(rt.found);
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

// A conkey that is not what encode_oid_csv wrote must stop the statement: parse_oid_csv answers
// `ok` even when a token was dropped or corrupted. Two shapes must never pass: a comma
// truncation (a narrower key) and a token 2^32 above an oid (wraps into the neighbouring column).

namespace {

    std::string catalog_dir(const char* leaf) {
        return "/tmp/test_dispatcher_catalog_" + std::to_string(::getpid()) + "/" + leaf;
    }

    // `conkey_text` overwrites the encoded column list; a non-null `contype_text` overwrites the
    // constraint-kind code.
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

    test.execute_sql("SELECT code FROM conkey_db.t WHERE id = 1;");
    auto stored = test.take_result();
    INFO("read error: " << (stored->is_error() ? std::string(stored->get_error().what) : std::string("none")));
    REQUIRE(stored->is_success());
    INFO("rows carrying id = 1: " << stored->size());
    INFO("a UNIQUE (id) that was accepted must be enforced on id, not on whichever column the token decayed to");
    REQUIRE(stored->size() <= 1);
}

TEST_CASE("services::dispatcher::conkey_csv::a_constraint_row_of_unknown_kind_is_not_skipped") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    test_dispatcher test(mr.get(), catalog_dir("conkey_unknown_kind"));
    const auto planted = create_two_column_table(test);

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

// A source column with no type must be named by the statement that named it, not by the storage
// segment that chokes on it: INSERT...SELECT skips validate_types, so a typeless column dies in
// column_segment_t naming no column, after a phantom NA column is already in the catalog. The
// refusal stays narrow — an unknown key keeps its own diagnosis, and a plain NULL is unaffected.
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

    REQUIRE(run("CREATE TABLE nsc.d1();")->is_success());
    {
        auto refused = run("INSERT INTO nsc.d1 (x, y) SELECT a, NULL FROM nsc.src;");
        REQUIRE_FALSE(refused->is_success());
        const std::string what{refused->get_error().what.c_str()};
        INFO("refusal text: " << what);
        CHECK(what.find("\"y\"") != std::string::npos);
        CHECK(what.find("no type to create the column from") != std::string::npos);
        CHECK(what.find("column_segment_t::append") == std::string::npos);
    }
    {
        auto after = run("SELECT * FROM nsc.d1;");
        REQUIRE(after->is_success());
        INFO("columns registered on the refused target: " << after->column_count());
        CHECK(after->column_count() == 0);
    }

    REQUIRE(run("CREATE TABLE nsc.d2();")->is_success());
    {
        auto refused = run("INSERT INTO nsc.d2 SELECT a AS x, NULL AS y FROM nsc.src;");
        REQUIRE_FALSE(refused->is_success());
        const std::string what{refused->get_error().what.c_str()};
        INFO("refusal text: " << what);
        CHECK(what.find("\"y\"") != std::string::npos);
        CHECK(what.find("column_segment_t::append") == std::string::npos);
    }

    // CAST doesn't help either — it still resolves to NA, so the refusal must not suggest a cast as the fix.
    REQUIRE(run("CREATE TABLE nsc.d3();")->is_success());
    {
        auto refused = run("INSERT INTO nsc.d3 (x, y) SELECT a, CAST(NULL AS BIGINT) FROM nsc.src;");
        REQUIRE_FALSE(refused->is_success());
        const std::string what{refused->get_error().what.c_str()};
        INFO("refusal text: " << what);
        CHECK(what.find("column_segment_t::append") == std::string::npos);
    }

    // A projection of NULL is legal; only writing it into a dynamic-schema table is refused.
    {
        auto plain = run("SELECT a, NULL FROM nsc.src;");
        REQUIRE(plain->is_success());
        CHECK(plain->column_count() == 2);
    }
    REQUIRE(run("CREATE TABLE nsc.reg (k bigint, v bigint);")->is_success());
    CHECK(run("INSERT INTO nsc.reg (k, v) SELECT a, NULL FROM nsc.src;")->is_success());
    REQUIRE(run("CREATE TABLE nsc.d4();")->is_success());
    CHECK(run("INSERT INTO nsc.d4 (x, y) SELECT a, b FROM nsc.src UNION ALL SELECT a, NULL FROM nsc.src;")
              ->is_success());
    {
        auto unknown = run("INSERT INTO nsc.d4 (x, y) SELECT a, nosuchkey FROM nsc.src;");
        REQUIRE_FALSE(unknown->is_success());
        const std::string what{unknown->get_error().what.c_str()};
        INFO("refusal text: " << what);
        CHECK(what.find("'nosuchkey' was not found") != std::string::npos);
        CHECK(what.find("no type to create the column from") == std::string::npos);
    }
}
