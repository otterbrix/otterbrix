#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>

#include <services/dispatcher/dispatcher.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/catalog/system_table_schemas.hpp>
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
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

// Dispatcher/executor guards. The fixture path carries ::getpid() so parallel ctest
// shards never share a disk directory.

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::cursor;
using components::session::session_id_t;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    // A host optimizer pass that counts its invocations. A plain fn-ptr per the
    // optimizer_pass_t contract; the counter is a test-local global.
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

    // One-arg BIGINT -> BIGINT vector UDF.
    core::error_t probe_exec(components::compute::kernel_context&,
                             const components::vector::data_chunk_t& in,
                             components::vector::vector_t& out) {
        const auto* source = in.data[0].data<int64_t>();
        auto* destination = out.data<int64_t>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = source[row] * 2;
        }
        return core::error_t::no_error();
    }

    std::unique_ptr<components::compute::vector_function> make_probe_func(std::pmr::memory_resource* resource,
                                                                          const std::string& name) {
        using namespace components::compute;
        function_doc doc{"short_doc", "full_doc", {"arg"}, false};
        auto fn = std::make_unique<vector_function>(name, arity::unary(), doc, 1);
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(logical_type::BIGINT)},
                               {output_type::fixed(logical_type::BIGINT)});
        vector_kernel k{std::move(sig), probe_exec};
        auto add_err = fn->add_kernel(resource, std::move(k));
        REQUIRE_FALSE(add_err.contains_error());
        return fn;
    }

} // namespace

// Dispatcher + disk + WAL over the non-threading test scheduler; mirrors the fixture in
// test_dispatcher_catalog.cpp, plus a raw execute_plan entry (hand-built plans) and the
// pool-admin helpers from test_dispatcher_admin_errors.cpp.
struct wave_fixture : actor_zeta::actor::actor_mixin<wave_fixture> {
    // wire_index=false publishes empty_address() as the executor's index address while the
    // manager itself still exists — the mis-wired-engine seam, kept so one case can pin the
    // refusal operator_create_index_backfill now raises there. Every other case wires it.
    wave_fixture(std::pmr::memory_resource* resource,
                 const std::string& disk_path,
                 components::planner::optimizer_pass_t optimizer_pass = &components::planner::no_op_pass,
                 bool wire_index = true)
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
        , manager_wal_(actor_zeta::spawn<manager_wal_replicate_t>(resource, scheduler_, wal_config_, log_))
        // A REAL index manager, not empty_address(). The empty slot pinned the quiet
        // no-op in operator_create_index_backfill (see the same note in
        // test_variant_e3_differential.cpp): with no index actor wired the operator
        // marked itself executed and reported success without creating anything, and
        // the CREATE INDEX cases below REQUIREd that success. Production always spawns
        // the index manager (integration/cpp/base_spaces.cpp), so the operator now
        // refuses on an empty address and the harness matches production.
        , manager_index_(actor_zeta::spawn<services::index::manager_index_t>(resource,
                                                                            scheduler_,
                                                                            log_,
                                                                            disk_config_.path,
                                                                            disk_config_.bitcask_flush_threshold,
                                                                            disk_config_.bitcask_segment_record_limit,
                                                                            disk_config_.btree_flush_threshold)) {
        const auto index_address =
            wire_index ? manager_index_->address() : actor_zeta::address_t::empty_address();
        manager_dispatcher_->sync(manager_dispatcher_t::sync_pack{manager_wal_->address(),
                                                                  manager_disk_->address(),
                                                                  index_address});
        manager_wal_->sync(services::wal::wal_sync_pack_t{actor_zeta::address_t(manager_disk_->address()),
                                                          manager_dispatcher_->address(),
                                                          index_address});
        manager_disk_->sync(manager_disk_t::disk_sync_pack_t{manager_wal_->address()});
        manager_index_->sync(services::index::index_sync_pack_t{manager_disk_->address()});
        manager_index_->set_manager_dispatcher_sync(manager_dispatcher_->address());
        manager_disk_->bootstrap_system_tables_sync();
    }

    ~wave_fixture() {
        // Index BEFORE disk: it holds manager_disk_'s address and addresses it during
        // teardown.
        manager_dispatcher_.reset();
        manager_wal_.reset();
        manager_index_.reset();
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
    // the same name — AFTER the per-executor fan-out already registered it.
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
    std::unique_ptr<services::index::manager_index_t, actor_zeta::pmr::deleter_t> manager_index_;
    std::unique_ptr<std::pmr::monotonic_buffer_resource> parser_arena_;
};

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
    // Catalog side: DROP COLUMN must find the column. Collecting the registered columns
    // from VALUES chunks alone leaves pg_computed_column empty here, and this refuses
    // with "does not exist".
    {
        auto cur = test.execute_sql("ALTER TABLE cdc.docs DROP COLUMN price;");
        if (cur->is_error()) {
            WARN("DROP COLUMN error: " << cur->get_error().what);
        }
        REQUIRE(cur->is_success());
    }
}

// ALTER TABLE on a table the enrich pass could not resolve must be refused loudly, not
// answered with an empty SUCCESS cursor. The planner's rewrite_alter_table only bails
// ("let execute_ddl error out"), so the refusal has to come from the executor guard.
TEST_CASE("services::dispatcher::wave3::alter_unresolved_table_is_refused") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("alter_unresolved"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());

    auto cur = test.execute_sql("ALTER TABLE db.no_such_table ADD COLUMN extra bigint;");
    // Without the executor guard: an empty success cursor — the client is told the ALTER
    // applied.
    REQUIRE(cur->is_error());
    REQUIRE(mentions(cur->get_error(), "no_such_table"));
}

// A boolean-context scalar sub-query whose plan the validator left schema-unstamped
// (an empty resolved schema — reachable through a computed table with no registered
// columns) must be refused with an error cursor: an assert compiles away under NDEBUG and
// output_types().front() then reads an empty vector.
TEST_CASE("services::dispatcher::wave3::boolean_subquery_unstamped_schema_is_refused") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("bool_subq_unstamped"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.t (b bigint);")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.t (b) VALUES (1);")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.docs ();")->is_success());

    auto cur = test.execute_sql("SELECT * FROM db.t WHERE (SELECT * FROM db.docs);");
    // An assert here ("boolean-required sub-query must be schema-stamped") would abort the
    // whole binary in Debug and read past an empty vector under NDEBUG.
    REQUIRE(cur->is_error());
}

// Same mechanism through the `col = ARRAY(SELECT ...)` form: a 0-row result over an
// unstamped sub-plan reaches output_types().front() on an empty vector.
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

// The host-injected optimizer pass (ctor chain: dispatcher -> executor) must actually be
// forwarded into components::planner::optimize: an executor that stores optimizer_pass_
// and never passes it silently ignores the host customization.
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

// A cross-database foreign key: the transformer registers the referenced table's resolve
// under its OWN database, so bind_catalog_data must look it up there — under the CHILD's
// database `REFERENCES otherdb.parent` can never bind.
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
        // Bound under the child's database instead, this answers
        // "referenced relation \"db1.parent\" does not exist".
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
    // Retry: MUST hit the operator's catalog refusal again. A leaked per-executor
    // registration answers "already registered with this signature"
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
    // Without the validation gate the statement proceeds into the DDL pipeline with a
    // type the durable form refuses.
    REQUIRE(cur->is_error());
    REQUIRE(mentions(cur->get_error(), "cannot be persisted"));
}

// core/executor.hpp's otterbrix::send must refuse an empty target LOUDLY: answering a
// ready future with a default value ("answered, with nothing") builds it on the empty
// address's null resource. The contract is that an empty target dies with a message and
// never answers. The child process exercises it so the abort cannot take the test runner
// down.
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

// The column list of INSERT ... SELECT into a computed (relkind='g') table carries the
// same NAME semantics as everywhere else. Skipping set_column_bindings for relkind='g'
// leaves the insert operator with nothing to rename by, so `INSERT INTO g (x, y) SELECT
// a, b` lands and REGISTERS columns a and b — the written (x, y) vanishing without a
// word. A list whose arity disagrees with the projection is a refusal, not a silent
// partial mapping.
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

// The relname of a new index needs its own check: duplicate detection is by
// (keys, type) only, so CREATE INDEX under a name pg_class already holds — another
// index, or even a table — mints a SECOND pg_class row with the same relname, and
// DROP INDEX resolves by name and then answers about WHICHEVER row it found. The name
// check rides the same resolve channel DROP INDEX uses: the transformer registers a
// {db, indexname} demand, enrich stamps the conflicting oid, and the planner refuses.
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

// A column written NULL in EVERY row of a VALUES source has no type, so it is dropped
// from the source chunk before anything downstream sees it — and the statement then died
// as a bare count disagreement ("INSERT names 2 columns but the source provides 1") that
// named neither the column that went missing nor the reason. The count is a symptom; the
// cause is a typeless column, and the drop site is the last place that still knows which
// written name it belonged to, because the drop is exactly what breaks the 1:1
// correspondence between the written list and the chunk's columns.
//
// Both halves are asserted: the refusal still carries the arity sentence (nothing that
// already reads it changes), and it now also names the column and says why.
TEST_CASE("services::dispatcher::wave4::insert_names_the_all_null_column_it_drops") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("insert_all_null_column_named"));

    REQUIRE(test.execute_sql("CREATE DATABASE anc;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE anc.t ();")->is_success());

    auto cur = test.execute_sql("INSERT INTO anc.t (a, x) VALUES (NULL, 'z');");
    INFO("INSERT result: " << (cur->is_error() ? cur->get_error().what : "accepted"));
    REQUIRE(cur->is_error());
    const std::string what{cur->get_error().what};
    // the arity sentence, unchanged
    CHECK(what.find("INSERT names 2 columns but the source provides 1") != std::string::npos);
    // and the half that was missing: WHICH column, and WHY
    CHECK(what.find("\"a\"") != std::string::npos);
    CHECK(what.find("NULL in every row") != std::string::npos);

    // Two typeless columns are both named, and the sentence stays grammatical.
    auto two = test.execute_sql("INSERT INTO anc.t (a, b) VALUES (NULL, NULL);");
    REQUIRE(two->is_error());
    const std::string what_two{two->get_error().what};
    INFO("INSERT result: " << what_two);
    CHECK(what_two.find("INSERT names 2 columns but the source provides 0") != std::string::npos);
    CHECK(what_two.find("\"a\", \"b\"") != std::string::npos);

    // A column that some row types is NOT dropped: the mixed case still lands, so the
    // refusal above is about typelessness and not about NULLs as such.
    REQUIRE(test.execute_sql("INSERT INTO anc.t (id, v) VALUES (1, NULL), (2, 7);")->is_success());
}

// A CREATE INDEX that reaches an executor with NO index manager wired must be refused,
// not answered with success. operator_create_index_backfill used to mark itself executed
// and return on that branch: the statement registered nothing, created nothing,
// backfilled nothing and never flipped pg_index.indisvalid, and the cursor said SUCCESS.
// The quiet success was invisible because this fixture and test_variant_e3_differential
// both synced empty_address() into the sync_pack's third slot; both wire a real
// manager_index_t now, and this case keeps the seam alive on purpose so the refusal
// itself is pinned rather than the silence.
TEST_CASE("services::dispatcher::wave4::create_index_refuses_without_an_index_manager") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(),
                      wave_dir("create_index_no_index_manager"),
                      &components::planner::no_op_pass,
                      /*wire_index=*/false);

    REQUIRE(test.execute_sql("CREATE DATABASE cim;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE cim.t (a bigint);")->is_success());

    auto cur = test.execute_sql("CREATE INDEX idx ON cim.t (a);");
    INFO("CREATE INDEX result: " << (cur->is_error() ? cur->get_error().what : "accepted"));
    REQUIRE(cur->is_error());
    CHECK(std::string(cur->get_error().what).find("index manager") != std::string::npos);
}

// The two spellings of a DEFAULT AGREE, and this pins the parity by EXECUTION:
//
//   * CREATE TABLE casts. `c integer DEFAULT 7` stores INTEGER 7 although the literal 7 is
//     BIGINT (numeric_literal_value's T_Integer arm), because services/collection's executor
//     hands the column list to convert_column_defaults, which resolves an assignment cast and
//     REPLACES the stored value.
//   * ALTER TABLE ADD COLUMN casts TOO, through the SAME convert_column_defaults and the same
//     cast_registry_ (services/collection/executor.cpp, "ALTER TABLE: DEFAULT coercion", right
//     after the DDL rewrite — only there does each ADD COLUMN clause exist as its own
//     node_alter_column_t whose column() is writable).
//
// THIS SECTION USED TO PIN THE OPPOSITE — that ALTER REFUSED the divergence — and the owner
// flipped it on 2026-09-05 under rule 17, together with the decision to close the parity gap.
// The shape is PostgreSQL's: one cookDefault(), called by both DefineRelation and
// ATExecAddColumn, coercing with COERCION_ASSIGNMENT and erroring only when no assignment cast
// exists. Parity there cannot be broken because there is one path; here it now cannot either,
// because there is one convert_column_defaults.
//
// WHAT STILL REFUSES, AND IS PINNED BELOW: a DEFAULT the registry has no assignment cast to the
// column's type for. Behind that, catalog::alter_column_validators::validate_default_value_type
// (called from operator_alter_column_add) stays as the SECOND line and MUST NOT be read as "now
// removable": the coercion runs on the ALTER STATEMENT path only, so a host-built plan handing a
// node_alter_column_t straight to the operator never passes through it, and the validator is then
// the only check between a divergent DEFAULT and the catalog.
//
// attdefspec is a TYPE-DIRECTED codec: the payload SHAPE is still derived from the column type
// it is decoded against. But read_typed_value (components/index/logical_value_binary_codec.hpp)
// also stores, and checks, one logical tag byte per present value, so it refuses a SAME-WIDTH
// divergence (BIGINT read as TIMESTAMP) as well as the WIDTH divergence (BIGINT read as
// INTEGER) it always caught.
//
// THAT DEMOTES validate_default_value_type TO THE SECOND LINE OF DEFENCE, AND IT MUST NOT BE
// READ AS "NOW REMOVABLE": the two refuse at different moments. The validator refuses at ALTER
// time, before the first catalog write — the divergence never reaches disk and the statement
// fails with "default value type mismatch". The codec guarantees only that a divergence which
// somehow DID reach disk cannot be read back as a valid value of the wrong type, and says so as
// data_corruption. Dropping the validator would turn a rejected statement into a persisted row
// nobody can read afterwards.
//
// Neither codec arm asserts, so Debug and NDEBUG give the same answer. The one pair the section
// below spells out is the worst of a space the next TEST_CASE walks whole.
TEST_CASE("services::dispatcher::wave4::alter_add_column_default_is_coerced_like_create_table") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("alter_add_default_type"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());

    // --- CREATE TABLE leg: the cast happens, and the stored default is the COLUMN's type.
    REQUIRE(test.execute_sql("CREATE TABLE db.created (a bigint, c integer DEFAULT 7);")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.created (a) VALUES (1);")->is_success());
    {
        auto cur = test.execute_sql("SELECT c FROM db.created;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const auto v = cur->value(0, 0);
        REQUIRE_FALSE(v.is_null());
        // INTEGER, not the BIGINT the literal started as: convert_column_defaults ran.
        CHECK(v.type().type() == logical_type::INTEGER);
        CHECK(v.value<int32_t>() == 7);
    }

    // --- ALTER leg, TYPES AGREE: this is the control. ADD COLUMN with a DEFAULT is not
    // refused as such — the write path expands it for a row inserted without the column.
    REQUIRE(test.execute_sql("CREATE TABLE db.agree (a bigint);")->is_success());
    REQUIRE(test.execute_sql("ALTER TABLE db.agree ADD COLUMN c bigint DEFAULT 7;")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.agree (a) VALUES (1);")->is_success());
    {
        auto cur = test.execute_sql("SELECT a, c FROM db.agree;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const auto v = cur->value(1, 0);
        REQUIRE_FALSE(v.is_null());
        CHECK(v.type().type() == logical_type::BIGINT);
        CHECK(v.value<int64_t>() == 7);
    }

    // --- ALTER leg, TYPES DIVERGE: the ONLY difference from the control above is the
    // declared column type. ACCEPTED, and the stored default is the COLUMN's type — the same
    // answer the CREATE TABLE leg above gives for the same spelling. That identity IS the
    // subject: assert it against db.created, not against a literal, so the two legs cannot
    // drift apart without this failing.
    REQUIRE(test.execute_sql("CREATE TABLE db.diverge (a bigint);")->is_success());
    REQUIRE(test.execute_sql("ALTER TABLE db.diverge ADD COLUMN c integer DEFAULT 7;")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.diverge (a) VALUES (1);")->is_success());
    {
        auto cur = test.execute_sql("SELECT c FROM db.diverge;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const auto v = cur->value(0, 0);
        REQUIRE_FALSE(v.is_null());
        // INTEGER, not the BIGINT the literal started as — convert_column_defaults ran on the
        // ALTER path exactly as it does on the CREATE path.
        CHECK(v.type().type() == logical_type::INTEGER);
        CHECK(v.value<int32_t>() == 7);
    }

    // --- WHAT STILL REFUSES, ON BOTH LEGS. A DEFAULT the registry has no ASSIGNMENT cast to the
    // column's type for. STRING -> number is registered explicit_only (components/casts/
    // default_casts.cpp, add_string_to_number), so `integer DEFAULT '7'` has a cast that exists
    // and is nevertheless not usable here — exactly PostgreSQL's rule, where cookDefault coerces
    // with COERCION_ASSIGNMENT and errors otherwise. Asserted on BOTH spellings, because parity
    // that only holds for the accepting direction is not parity.
    {
        auto cur = test.execute_sql("CREATE TABLE db.nocast_create (a bigint, c integer DEFAULT '7');");
        INFO("CREATE TABLE with a default that has no assignment cast");
        CHECK(cur->is_error());
    }
    REQUIRE(test.execute_sql("CREATE TABLE db.nocast (a bigint);")->is_success());
    {
        auto cur = test.execute_sql("ALTER TABLE db.nocast ADD COLUMN c integer DEFAULT '7';");
        INFO("ALTER TABLE with a default that has no assignment cast");
        CHECK(cur->is_error());
    }
    {
        // The refusal landed before the first catalog mutation: no half-added column.
        auto cur = test.execute_sql("SELECT c FROM db.nocast;");
        CHECK(cur->is_error());
    }

    // --- What the codec itself refuses, and why that does not retire the validator.
    // attdefspec reads the payload AGAINST the column type, and checks a stored logical
    // tag byte against it before reading anything else.
    {
        const components::types::logical_value_t bigint_seven{mr.get(), static_cast<int64_t>(7)};
        std::string spec;
        REQUIRE_FALSE(components::catalog::encode_default_spec(mr.get(), bigint_seven, spec).contains_error());

        // Widths differ (8 vs 4). Caught even before the tag byte existed — the four
        // leftover bytes gave it away through `pos != payload.size()` — and still
        // caught. This arm is the standing proof the codec always held THIS class.
        std::optional<components::types::logical_value_t> as_integer;
        auto ec_int = components::catalog::decode_default_spec(mr.get(),
                                                               complex_logical_type{logical_type::INTEGER},
                                                               spec,
                                                               as_integer);
        CHECK(ec_int.contains_error());

        // Widths AGREE (int64 both), so the leftover-bytes check above sees nothing: this
        // is the arm the tag byte carries. Without it the payload decodes into a perfectly
        // valid TIMESTAMP — with no assert to lose under NDEBUG — leaving
        // validate_default_value_type the ONLY thing between this and a persisted
        // `timestamp DEFAULT 7`. The stored tag says BIGINT, the column says TIMESTAMP,
        // and the read is refused before the payload is touched.
        std::optional<components::types::logical_value_t> as_timestamp;
        auto ec_ts = components::catalog::decode_default_spec(mr.get(),
                                                              complex_logical_type{logical_type::TIMESTAMP},
                                                              spec,
                                                              as_timestamp);
        CHECK(ec_ts.contains_error());
        CHECK_FALSE(as_timestamp.has_value());
    }
}


// The type tag, over the WHOLE pair space. The section above pins the single worst pair;
// this pins every one of them — the pair that mattered was found by ENUMERATING the
// space, not by reading the code, and an enumeration is what keeps it closed. The
// sixteen scalars attdefspec can carry give 240 ordered wrong-type pairs; all 240 must
// be refused, and the 16 self-pairs must still round-trip.
//
// Before the tag byte, 50 of the 240 were accepted SILENTLY — they decoded into a
// perfectly valid value of the WRONG type: BIGINT read as UBIGINT / TIME / TIMESTAMP /
// TIMESTAMP_TZ / DOUBLE, INTEGER as UINTEGER / DATE / FLOAT, BOOLEAN / TINYINT /
// UTINYINT interchangeably, and SMALLINT with USMALLINT. BIGINT/DOUBLE was the worst of
// them: a bit-pattern reinterpretation rather than a relabelling. The other 190 were
// caught only because their widths happened to disagree, which is a length check and not
// a type check — it is the reason this case counts pairs instead of trusting that one.
//
// This needs no dispatcher fixture: it drives the catalog encode/decode boundary
// (components/catalog/system_table_schemas.cpp) over the codec in
// components/index/logical_value_binary_codec.hpp. It lives beside the case above
// because it is that case generalised, and because a reader who weakens one must see
// the other.
TEST_CASE("services::dispatcher::wave4::attdefspec_type_tag_refuses_every_wrong_type_pair") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    auto* resource = mr.get();
    using components::types::logical_value_t;

    struct scalar_case {
        const char* name;
        complex_logical_type type;
        logical_value_t value;
    };

    // One value per scalar, all of them SMALL and non-negative on purpose: those are
    // exactly the payloads that reinterpret cleanly into another type of the same width,
    // so a pair that survives here survives on the type check and not on a lucky bit.
    const std::vector<scalar_case> cases{
        {"BOOLEAN", complex_logical_type{logical_type::BOOLEAN}, logical_value_t{resource, true}},
        {"TINYINT", complex_logical_type{logical_type::TINYINT}, logical_value_t{resource, std::int8_t{1}}},
        {"UTINYINT", complex_logical_type{logical_type::UTINYINT}, logical_value_t{resource, std::uint8_t{1}}},
        {"SMALLINT", complex_logical_type{logical_type::SMALLINT}, logical_value_t{resource, std::int16_t{7}}},
        {"USMALLINT", complex_logical_type{logical_type::USMALLINT}, logical_value_t{resource, std::uint16_t{7}}},
        {"INTEGER", complex_logical_type{logical_type::INTEGER}, logical_value_t{resource, std::int32_t{7}}},
        {"UINTEGER", complex_logical_type{logical_type::UINTEGER}, logical_value_t{resource, std::uint32_t{7}}},
        {"BIGINT", complex_logical_type{logical_type::BIGINT}, logical_value_t{resource, std::int64_t{7}}},
        {"UBIGINT", complex_logical_type{logical_type::UBIGINT}, logical_value_t{resource, std::uint64_t{7}}},
        {"FLOAT", complex_logical_type{logical_type::FLOAT}, logical_value_t{resource, 7.0F}},
        {"DOUBLE", complex_logical_type{logical_type::DOUBLE}, logical_value_t{resource, 7.0}},
        {"STRING_LITERAL",
         complex_logical_type{logical_type::STRING_LITERAL},
         logical_value_t{resource, std::string{"seven"}}},
        {"DATE",
         complex_logical_type{logical_type::DATE},
         logical_value_t{resource, core::date::date_t{core::date::days{7}}}},
        {"TIME",
         complex_logical_type{logical_type::TIME},
         logical_value_t{resource, core::date::time_t{core::date::microseconds{7}}}},
        {"TIMESTAMP",
         complex_logical_type{logical_type::TIMESTAMP},
         logical_value_t{resource, core::date::timestamp_t{core::date::microseconds{7}}}},
        {"TIMESTAMP_TZ",
         complex_logical_type{logical_type::TIMESTAMP_TZ},
         logical_value_t{resource, core::date::timestamptz_t{core::date::microseconds{7}}}},
    };
    REQUIRE(cases.size() == 16); // 16 * 15 = 240 ordered wrong-type pairs

    std::string accepted; // every wrong-type pair that still decodes, named
    int accepted_count = 0;
    int refused_count = 0;
    int self_round_trips = 0;

    for (const auto& src : cases) {
        std::string spec;
        INFO("encoding a " << src.name << " default");
        REQUIRE_FALSE(components::catalog::encode_default_spec(resource, src.value, spec).contains_error());
        REQUIRE_FALSE(spec.empty());

        for (const auto& dst : cases) {
            std::optional<logical_value_t> out;
            const auto ec = components::catalog::decode_default_spec(resource, dst.type, spec, out);

            if (&src == &dst) {
                // The value keeps its own type: the tag is a CHECK, never a source. A
                // codec that refused its own output would be worse than the hole.
                INFO("self round trip: " << src.name);
                REQUIRE_FALSE(ec.contains_error());
                REQUIRE(out.has_value());
                CHECK(out->type().type() == dst.type.type());
                ++self_round_trips;
                continue;
            }

            if (ec.contains_error()) {
                CHECK_FALSE(out.has_value()); // a refusal leaves NOTHING behind
                ++refused_count;
            } else {
                ++accepted_count;
                accepted += std::string{src.name} + "->" + dst.name + " ";
            }
        }
    }

    INFO("wrong-type pairs still accepted: " << accepted);
    CHECK(accepted_count == 0);
    CHECK(refused_count == 240);
    CHECK(self_round_trips == 16);
}

// NULL and the nested types, against the same tag. Two things have to stay true that a
// per-value tag could plausibly have broken.
//
// NULL carries NO tag, and must not: presence 0 ends the value, and a NULL is NA-typed
// in this engine (logical_value_t::is_null() IS type() == NA), so there is no type for a
// tag to agree with. Writing the column's type there would invent one.
//
// Nested values carry the tag at EVERY level, not just the outermost, and this is the
// case that earns the extra byte per leaf. The divergence a DEFAULT can carry is
// per-leaf: a STRUCT<BIGINT, STRING> payload read against STRUCT<TIMESTAMP, STRING> is
// the same same-width swap as the scalar one, one level down. An outer-only tag would
// see two STRUCTs, agree, and let the field underneath reinterpret silently.
TEST_CASE("services::dispatcher::wave4::attdefspec_type_tag_spares_null_and_reaches_every_leaf") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    auto* resource = mr.get();
    using components::types::logical_value_t;

    INFO("an explicit DEFAULT NULL still decodes against any column type");
    {
        std::string null_spec;
        REQUIRE_FALSE(components::catalog::encode_default_spec(
                          resource,
                          logical_value_t{resource, complex_logical_type{logical_type::NA}},
                          null_spec)
                          .contains_error());
        for (const auto t : {logical_type::BIGINT, logical_type::TIMESTAMP, logical_type::STRING_LITERAL}) {
            std::optional<logical_value_t> out;
            REQUIRE_FALSE(
                components::catalog::decode_default_spec(resource, complex_logical_type{t}, null_spec, out)
                    .contains_error());
            REQUIRE(out.has_value());
            CHECK(out->is_null());
        }
    }

    INFO("ARRAY / LIST round trip, and refuse on the ELEMENT type");
    {
        const std::vector<logical_value_t> elems{logical_value_t{resource, std::int64_t{7}},
                                                 logical_value_t{resource, std::int64_t{8}}};
        const complex_logical_type bigint{logical_type::BIGINT};
        const complex_logical_type timestamp{logical_type::TIMESTAMP};

        for (const bool as_array : {true, false}) {
            const auto value = as_array ? logical_value_t::create_array(resource, bigint, elems)
                                        : logical_value_t::create_list(resource, bigint, elems);
            const auto good = as_array ? complex_logical_type::create_array(bigint, 2)
                                       : complex_logical_type::create_list(bigint);
            const auto bad = as_array ? complex_logical_type::create_array(timestamp, 2)
                                      : complex_logical_type::create_list(timestamp);

            std::string spec;
            REQUIRE_FALSE(components::catalog::encode_default_spec(resource, value, spec).contains_error());

            std::optional<logical_value_t> out;
            REQUIRE_FALSE(components::catalog::decode_default_spec(resource, good, spec, out).contains_error());
            REQUIRE(out.has_value());
            REQUIRE(out->children().size() == 2);
            CHECK(out->children()[0].value<std::int64_t>() == 7);
            CHECK(out->children()[1].value<std::int64_t>() == 8);

            // Same outer type, same width, WRONG element type. Only a per-leaf tag sees it.
            std::optional<logical_value_t> wrong;
            CHECK(components::catalog::decode_default_spec(resource, bad, spec, wrong).contains_error());
            CHECK_FALSE(wrong.has_value());
        }
    }

    INFO("STRUCT round trip, and refuse on a FIELD type");
    {
        std::pmr::vector<complex_logical_type> good_fields{resource};
        good_fields.emplace_back(complex_logical_type{logical_type::BIGINT, "n"});
        good_fields.emplace_back(complex_logical_type{logical_type::STRING_LITERAL, "s"});
        const auto good = complex_logical_type::create_struct("s", good_fields);

        // The ONLY difference is field 0: BIGINT becomes TIMESTAMP, same eight bytes.
        std::pmr::vector<complex_logical_type> bad_fields{resource};
        bad_fields.emplace_back(complex_logical_type{logical_type::TIMESTAMP, "n"});
        bad_fields.emplace_back(complex_logical_type{logical_type::STRING_LITERAL, "s"});
        const auto bad = complex_logical_type::create_struct("s", bad_fields);

        const std::vector<logical_value_t> fields{logical_value_t{resource, std::int64_t{42}},
                                                  logical_value_t{resource, std::string{"x"}}};
        const auto value = logical_value_t::create_struct(resource, good, fields);

        std::string spec;
        REQUIRE_FALSE(components::catalog::encode_default_spec(resource, value, spec).contains_error());

        std::optional<logical_value_t> out;
        REQUIRE_FALSE(components::catalog::decode_default_spec(resource, good, spec, out).contains_error());
        REQUIRE(out.has_value());
        REQUIRE(out->children().size() == 2);
        CHECK(out->children()[0].value<std::int64_t>() == 42);
        CHECK(out->children()[1].value<std::string_view>() == "x");

        // The outer STRUCT agrees; field 0 does not, and its width does not give it away.
        std::optional<logical_value_t> wrong;
        CHECK(components::catalog::decode_default_spec(resource, bad, spec, wrong).contains_error());
        CHECK_FALSE(wrong.has_value());
    }
}
