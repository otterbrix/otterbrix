#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <sys/wait.h>

#include <services/dispatcher/dispatcher.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/compute/function.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/session/session.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/executor.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>

// Wave "executor & dispatcher": red-first tests for the queue entries closed on this
// branch. Each test names the entry it guards in a comment. The fixture path carries
// ::getpid() so parallel ctest shards never share a disk directory.

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::cursor;
using components::session::session_id_t;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    // Entry #23 probe: a host optimizer pass that counts its invocations. A plain
    // fn-ptr per the optimizer_pass_t contract; the counter is a test-local global.
    std::atomic<uint64_t> g_host_pass_calls{0};

    components::logical_plan::node_ptr counting_host_pass(std::pmr::memory_resource*,
                                                          components::logical_plan::node_ptr node) {
        g_host_pass_calls.fetch_add(1, std::memory_order_relaxed);
        return node;
    }

    std::string wave_dir(const char* leaf) {
        return "/tmp/test_wave_exec_dispatcher_" + std::to_string(::getpid()) + "/" + leaf;
    }

    const std::string& scrubbed(const std::string& path) {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        return path;
    }

    bool mentions(const core::error_t& err, const char* needle) {
        return std::string{err.what.c_str()}.find(needle) != std::string::npos;
    }

    // One-arg BIGINT -> BIGINT row UDF (entry #11 probe).
    core::error_t probe_exec(components::compute::kernel_context& ctx,
                             const std::pmr::vector<components::types::logical_value_t>& in,
                             std::pmr::vector<components::types::logical_value_t>& out) {
        out.emplace_back(ctx.exec_context().resource(), in[0].value<int64_t>() * 2);
        return core::error_t::no_error();
    }

    std::unique_ptr<components::compute::row_function> make_probe_func(std::pmr::memory_resource* resource,
                                                                       const std::string& name) {
        using namespace components::compute;
        function_doc doc{"short_doc", "full_doc", {"arg"}, false};
        auto fn = std::make_unique<row_function>(name, arity::unary(), doc, 1);
        kernel_signature_t sig(function_type_t::row,
                               {parameter_type::exact(logical_type::BIGINT)},
                               {output_type::fixed(logical_type::BIGINT)});
        row_kernel k{std::move(sig), probe_exec};
        auto add_err = fn->add_kernel(resource, std::move(k));
        REQUIRE_FALSE(add_err.contains_error());
        return fn;
    }

} // namespace

// Dispatcher + disk + WAL over the non-threading test scheduler; mirrors the fixture in
// test_dispatcher_catalog.cpp, plus a raw execute_plan entry (hand-built plans) and the
// pool-admin helpers from test_dispatcher_admin_errors.cpp.
struct wave_fixture : actor_zeta::actor::actor_mixin<wave_fixture> {
    wave_fixture(std::pmr::memory_resource* resource,
                 const std::string& disk_path,
                 components::planner::optimizer_pass_t optimizer_pass = &components::planner::no_op_pass)
        : actor_zeta::actor::actor_mixin<wave_fixture>()
        , resource_(resource)
        , disk_path_(scrubbed(disk_path))
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
        , manager_dispatcher_(actor_zeta::spawn<manager_dispatcher_t>(resource,
                                                                      scheduler_,
                                                                      log_,
                                                                      &services::planner::no_custom_lowering,
                                                                      optimizer_pass))
        , disk_config_(disk_path)
        , manager_disk_(actor_zeta::spawn<manager_disk_t>(resource, scheduler_, scheduler_, disk_config_, log_))
        , wal_config_([&]() {
            configuration::config_wal c;
            c.on = false;
            return c;
        }())
        , manager_wal_(actor_zeta::spawn<manager_wal_replicate_t>(resource, scheduler_, wal_config_, log_)) {
        manager_dispatcher_->sync(manager_dispatcher_t::sync_pack{manager_wal_->address(),
                                                                  manager_disk_->address(),
                                                                  actor_zeta::address_t::empty_address()});
        manager_wal_->sync(services::wal::wal_sync_pack_t{actor_zeta::address_t(manager_disk_->address()),
                                                          manager_dispatcher_->address(),
                                                          actor_zeta::address_t::empty_address()});
        manager_disk_->sync(manager_disk_t::disk_sync_pack_t{manager_wal_->address()});
        manager_disk_->bootstrap_system_tables_sync();
    }

    ~wave_fixture() {
        manager_dispatcher_.reset();
        manager_wal_.reset();
        manager_disk_.reset();
        scheduler_->stop();
        std::filesystem::remove_all(disk_path_);
        delete scheduler_;
    }

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    template<typename T>
    T pump(actor_zeta::unique_future<T>&& fut) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
            scheduler_->run(1000);
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
        auto out = std::move(fut).take_ready();
        // Drain the post-result DDL/DML tail (catalog writes, commit pipeline).
        scheduler_->run(10000);
        return out;
    }

    cursor_t_ptr execute_sql(const std::string& query) {
        parser_arena_ = std::make_unique<std::pmr::monotonic_buffer_resource>(resource_);
        auto parse_result = linitial(raw_parser(parser_arena_.get(), query.c_str()));
        components::sql::transform::transformer local_transformer(resource_);
        auto wrap =
            local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_result)).finalize();
        REQUIRE(!wrap.has_error());
        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                       &manager_dispatcher_t::execute_plan,
                                                       session_id_t{},
                                                       std::move(wrap.value()));
        return pump(std::move(future));
    }

    cursor_t_ptr execute_plan(components::logical_plan::execution_plan_t plan) {
        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                       &manager_dispatcher_t::execute_plan,
                                                       session_id_t{},
                                                       std::move(plan));
        return pump(std::move(future));
    }

    template<typename Fn, typename... Args>
    auto dispatcher_invoke(Fn fn, Args&&... args) {
        auto [_, fut] = actor_zeta::otterbrix::send(manager_dispatcher_->address(), fn, std::forward<Args>(args)...);
        return pump(std::move(fut));
    }

    template<typename Fn, typename... Args>
    auto disk_invoke(Fn fn, Args&&... args) {
        auto [_, fut] = actor_zeta::otterbrix::send(manager_disk_->address(), fn, std::forward<Args>(args)...);
        return pump(std::move(fut));
    }

    components::execution_context_t read_ctx() {
        return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
    }

    // Write a pg_proc (+ pg_depend) row straight into the catalog, bypassing register_udf.
    // The operator's cross-namespace conflict read then refuses a later CREATE FUNCTION of
    // the same name — AFTER the per-executor fan-out already registered it (entry #11).
    void seed_pg_proc_row(const std::string& fname) {
        auto ctx = read_ctx();
        components::catalog::oid_batch_t batch;
        batch.oids = disk_invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const auto fn_oid = batch.allocate();
        auto writes =
            components::catalog::build_create_function_writes(resource_,
                                                              fname,
                                                              components::catalog::well_known_oid::pg_catalog_namespace,
                                                              fn_oid,
                                                              /*pronargs=*/1,
                                                              /*prouid=*/0,
                                                              /*proargmatchers=*/"",
                                                              /*prorettype=*/"");
        for (auto& w : writes) {
            auto appended = disk_invoke(&manager_disk_t::append_pg_catalog_row, ctx, w.table_oid, std::move(w.row));
            REQUIRE_FALSE(appended.has_error());
        }
        scheduler_->run(10000);
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
};

// ===== Entry #346 =====
// INSERT ... SELECT into a computed (relkind='g') table must register its columns in
// pg_computed_column exactly like the VALUES form does. The observable half of the
// divergence: storage HAS the column (SELECT projects it) while the catalog does not
// (DROP COLUMN refuses).
TEST_CASE("services::dispatcher::wave3::insert_select_registers_computed_columns") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("insert_select_computed"));

    REQUIRE(test.execute_sql("CREATE DATABASE cdc;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE cdc.src (id bigint, price bigint);")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO cdc.src (id, price) VALUES (1, 10), (2, 20);")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE cdc.docs ();")->is_success());

    {
        auto cur = test.execute_sql("INSERT INTO cdc.docs (id, price) SELECT id, price FROM cdc.src;");
        REQUIRE(cur->is_success());
    }
    // Storage side: the column is there.
    {
        auto cur = test.execute_sql("SELECT * FROM cdc.docs;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    // Catalog side: DROP COLUMN must find the column. RED before the fix: the register
    // wrap collected columns only from VALUES chunks, so pg_computed_column got no rows
    // and this refuses with "does not exist".
    {
        auto cur = test.execute_sql("ALTER TABLE cdc.docs DROP COLUMN price;");
        if (cur->is_error()) {
            WARN("DROP COLUMN error: " << cur->get_error().what);
        }
        REQUIRE(cur->is_success());
    }
}

// ===== Entries #107 + #355 =====
// ALTER TABLE on a table the enrich pass could not resolve must be refused loudly, not
// answered with an empty SUCCESS cursor. The planner's rewrite_alter_table already bails
// ("let execute_ddl error out"); the executor guard was the silent half.
TEST_CASE("services::dispatcher::wave3::alter_unresolved_table_is_refused") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("alter_unresolved"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());

    auto cur = test.execute_sql("ALTER TABLE db.no_such_table ADD COLUMN extra bigint;");
    // RED before the fix: empty success cursor — the client is told the ALTER applied.
    REQUIRE(cur->is_error());
    REQUIRE(mentions(cur->get_error(), "no_such_table"));
}

// ===== Entry #106 (site 1: boolean-required) =====
// A boolean-context scalar sub-query whose plan the validator left schema-unstamped
// (an empty resolved schema — reachable through a computed table with no registered
// columns) must be refused with an error cursor. Under NDEBUG the old assert compiled
// away and output_types().front() read an empty vector.
TEST_CASE("services::dispatcher::wave3::boolean_subquery_unstamped_schema_is_refused") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("bool_subq_unstamped"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.t (b bigint);")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.t (b) VALUES (1);")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.docs ();")->is_success());

    auto cur = test.execute_sql("SELECT * FROM db.t WHERE (SELECT * FROM db.docs);");
    // RED before the fix (Debug): assert "boolean-required sub-query must be
    // schema-stamped" aborts the whole binary.
    REQUIRE(cur->is_error());
}

// ===== Entry #106 (site 2: ARRAY-equality) =====
// Same mechanism through the `col = ARRAY(SELECT ...)` form: a 0-row result over an
// unstamped sub-plan reached assert + output_types().front() on an empty vector.
TEST_CASE("services::dispatcher::wave3::array_equality_subquery_unstamped_schema_is_refused") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("array_subq_unstamped"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.t (b bigint);")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.t (b) VALUES (1);")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.docs ();")->is_success());

    auto cur = test.execute_sql("SELECT * FROM db.t WHERE b = ARRAY(SELECT * FROM db.docs);");
    REQUIRE(cur->is_error());
}

// ===== Entry #23 =====
// The host-injected optimizer pass (ctor chain: dispatcher -> executor) must actually be
// forwarded into components::planner::optimize. RED before the fix: the executor stored
// optimizer_pass_ and never passed it — a silently-ignored host customization.
TEST_CASE("services::dispatcher::wave3::host_optimizer_pass_reaches_optimize") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    g_host_pass_calls.store(0, std::memory_order_relaxed);
    wave_fixture test(mr.get(), wave_dir("host_pass"), &counting_host_pass);

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.t (b bigint);")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.t (b) VALUES (7);")->is_success());
    REQUIRE(test.execute_sql("SELECT * FROM db.t;")->is_success());

    REQUIRE(g_host_pass_calls.load(std::memory_order_relaxed) > 0);
}

// ===== Entry #206 =====
// A cross-database foreign key: the transformer registers the referenced table's resolve
// under its own database, but bind_catalog_data used to look it up under the CHILD's
// database — `REFERENCES otherdb.parent` could never bind.
TEST_CASE("services::dispatcher::wave3::cross_db_foreign_key_binds") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("cross_db_fk"));

    REQUIRE(test.execute_sql("CREATE DATABASE db1;")->is_success());
    REQUIRE(test.execute_sql("CREATE DATABASE db2;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db1.parent (id bigint, PRIMARY KEY (id));")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db1.parent (id) VALUES (1);")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db2.child (pid bigint);")->is_success());

    {
        auto cur = test.execute_sql(
            "ALTER TABLE db2.child ADD CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES db1.parent (id);");
        if (cur->is_error()) {
            WARN("ADD CONSTRAINT error: " << cur->get_error().what);
        }
        // RED before the fix: "referenced relation \"db1.parent\" does not exist".
        REQUIRE(cur->is_success());
    }
    // The FK it bound must actually be the cross-database one: an orphan is refused,
    // a matching child row goes in.
    {
        auto orphan = test.execute_sql("INSERT INTO db2.child (pid) VALUES (99);");
        REQUIRE(orphan->is_error());
    }
    {
        auto ok = test.execute_sql("INSERT INTO db2.child (pid) VALUES (1);");
        REQUIRE(ok->is_success());
    }
}

// ===== Entry #11 =====
// register_udf fans the function out to every per-executor registry BEFORE the operator's
// catalog work. When the operator then refuses (here: a pre-existing pg_proc row trips its
// cross-namespace conflict read), the fan-out must be unwound — otherwise a RETRY of the
// same CREATE FUNCTION hits the leaked per-executor entry ("already registered with this
// signature") instead of the operator's own catalog refusal.
TEST_CASE("services::dispatcher::wave3::register_udf_operator_refusal_unwinds_executors") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("udf_unwind"));

    const std::string fname = "wave3_udf_unwind_probe";
    test.seed_pg_proc_row(fname);

    // First attempt: the operator refuses on the seeded catalog row.
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_udf,
                                          session_id_t{},
                                          components::compute::function_ptr{make_probe_func(mr.get(), fname)});
        REQUIRE(err.contains_error());
        REQUIRE(mentions(err, "already exists in the catalog"));
    }
    // Retry: MUST hit the operator's catalog refusal again. RED before the fix: the
    // leaked per-executor registration answers "already registered with this signature"
    // (function_registry_error) instead.
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_udf,
                                          session_id_t{},
                                          components::compute::function_ptr{make_probe_func(mr.get(), fname)});
        REQUIRE(err.contains_error());
        REQUIRE(mentions(err, "already exists in the catalog"));
        REQUIRE(err.type == core::error_code_t::already_exists);
    }
    components::compute::function_registry_t::reset_default();
}

// ===== Entry #208 =====
// ALTER TABLE ADD COLUMN carries a column type into the durable catalog exactly like
// CREATE TABLE does, so it must pass the same gate_persistable_type. The SQL surface
// cannot spell a too-deep type today (CREATE TYPE gates its own depth), so the probe
// hands the dispatcher a hand-built plan — the gate is the last line of defence.
TEST_CASE("services::dispatcher::wave3::alter_add_column_gates_persistable_type") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("alter_add_gate"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.t (b bigint);")->is_success());

    // LIST nested past MAX_SPEC_DEPTH (64) — encode_type_spec refuses it.
    complex_logical_type deep{logical_type::BIGINT};
    for (int i = 0; i < 70; ++i) {
        deep = complex_logical_type::create_list(deep);
    }
    auto node = components::logical_plan::make_node_alter_table_add_column(
        mr.get(),
        components::table::column_definition_t{"too_deep", deep});
    node->set_dbname("db");
    node->set_relname("t");
    components::logical_plan::execution_plan_t plan{mr.get(),
                                                    components::logical_plan::node_ptr{node},
                                                    components::logical_plan::make_parameter_node(mr.get())};

    auto cur = test.execute_plan(std::move(plan));
    // RED before the fix: no validation gate — the statement proceeds into the DDL
    // pipeline with a type the durable form refuses.
    REQUIRE(cur->is_error());
    REQUIRE(mentions(cur->get_error(), "cannot be persisted"));
}

// ===== Entry #227 =====
// core/executor.hpp's otterbrix::send must refuse an empty target LOUDLY. The old
// shorthand answered a ready future with a default value ("answered, with nothing") built
// on the empty address's null resource. The contract now: an empty target dies with a
// message, never answers. The child process exercises it so the abort cannot take the
// test runner down.
TEST_CASE("services::dispatcher::wave3::empty_target_send_dies_loudly") {
    const pid_t child = fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        // CHILD: an empty-target send must never return. Catch2's SIGABRT handler is
        // reset so the abort reaches waitpid as a signal death, not a report.
        ::signal(SIGABRT, SIG_DFL);
        auto res = actor_zeta::otterbrix::send(actor_zeta::address_t::empty_address(),
                                               &services::collection::executor::executor_t::poke_msg);
        // Reached only if the send answered instead of dying — report survival.
        _exit(res.second.is_ready() ? 42 : 43);
    }
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    // The contract: death by signal (abort), not a clean exit with an answer.
    REQUIRE(WIFSIGNALED(status));
}

// ===== ЗАПИСЬ #365 =====
// The column list of INSERT ... SELECT into a computed (relkind='g') table was
// silently IGNORED: validate skipped set_column_bindings for relkind='g', the
// insert operator renames nothing without bindings, so
// `INSERT INTO g (x, y) SELECT a, b` landed and REGISTERED columns a and b — the
// written (x, y) vanished without a word. PR #568 carried the rename through
// rename_targets for every table kind; the #585 bindings rework kept it only for
// relational targets. The name semantics come back here, and a list whose arity
// disagrees with the projection is a refusal, not a silent partial mapping.
TEST_CASE("services::dispatcher::wave4::insert_select_column_list_renames_into_computed_table") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("insert_select_rename_computed"));

    REQUIRE(test.execute_sql("CREATE DATABASE cdd;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE cdd.src (a bigint, b bigint);")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO cdd.src (a, b) VALUES (10, 100), (20, 200);")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE cdd.docs ();")->is_success());

    {
        auto cur = test.execute_sql("INSERT INTO cdd.docs (x, y) SELECT a, b FROM cdd.src;");
        INFO("INSERT error: " << (cur->is_error() ? std::string{cur->get_error().what.c_str()} : std::string{"none"}));
        REQUIRE(cur->is_success());
    }
    // The written names route the values: x carries a's values, y carries b's.
    {
        auto cur = test.execute_sql("SELECT * FROM cdd.docs WHERE x = 10;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    {
        auto cur = test.execute_sql("SELECT * FROM cdd.docs WHERE y = 200;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    // The catalog registered the written names too (DROP COLUMN resolves x).
    {
        auto cur = test.execute_sql("ALTER TABLE cdd.docs DROP COLUMN x;");
        if (cur->is_error()) {
            WARN("DROP COLUMN error: " << cur->get_error().what);
        }
        REQUIRE(cur->is_success());
    }
    // Arity mismatch between the list and the projection is a loud refusal.
    {
        auto cur = test.execute_sql("INSERT INTO cdd.docs (z) SELECT a, b FROM cdd.src;");
        REQUIRE(cur->is_error());
    }
}

// ===== ЗАПИСЬ #235 =====
// Nothing checked the relname of a new index: duplicate detection is by
// (keys, type) only, so CREATE INDEX under a name that pg_class already holds —
// another index, or even a table — минтed a SECOND pg_class row with the same
// relname. DROP INDEX resolves by name and then answers about WHICHEVER row the
// resolve found. The name check rides the same resolve channel DROP INDEX uses:
// the transformer registers a {db, indexname} demand, enrich stamps the
// conflicting oid, and the planner refuses the statement.
TEST_CASE("services::dispatcher::wave4::create_index_refuses_a_taken_name") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("create_index_name_unique"));

    REQUIRE(test.execute_sql("CREATE DATABASE cdi;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE cdi.t (a bigint, b bigint);")->is_success());
    REQUIRE(test.execute_sql("CREATE INDEX idx ON cdi.t (a);")->is_success());

    // The SAME name over a DIFFERENT key set: the (keys,type) duplicate check
    // cannot see it, only the name check can.
    {
        auto cur = test.execute_sql("CREATE INDEX idx ON cdi.t (b);");
        REQUIRE(cur->is_error());
    }
    // A name a TABLE already answers to is just as taken — indexes and tables
    // share pg_class.
    {
        auto cur = test.execute_sql("CREATE INDEX t ON cdi.t (a);");
        REQUIRE(cur->is_error());
    }
    // A fresh name still works.
    REQUIRE(test.execute_sql("CREATE INDEX idx2 ON cdi.t (b);")->is_success());
}
