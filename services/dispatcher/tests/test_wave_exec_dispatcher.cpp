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
#include <components/context/context.hpp>
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

// wave_dir() carries ::getpid() so parallel ctest shards never share a disk directory.

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::cursor;
using components::session::session_id_t;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

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

struct wave_fixture : actor_zeta::actor::actor_mixin<wave_fixture> {
    wave_fixture(std::pmr::memory_resource* resource,
                 const std::string& disk_path,
                 components::planner::optimizer_pass_t optimizer_pass = &components::planner::no_op_pass,
                 bool wire_index = true)
        : actor_zeta::actor::actor_mixin<wave_fixture>()
        , resource_(resource)
        , disk_path_(scrubbed(disk_path))
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
        , disk_config_(disk_path)
        , manager_disk_(actor_zeta::spawn<manager_disk_t>(resource, scheduler_, scheduler_, disk_config_, log_))
        , manager_index_(actor_zeta::spawn<services::index::manager_index_t>(resource,
                                                                            scheduler_,
                                                                            log_,
                                                                            disk_config_.path,
                                                                            disk_config_.bitcask_flush_threshold,
                                                                            disk_config_.bitcask_segment_record_limit,
                                                                            disk_config_.btree_flush_threshold))
        , wal_config_(disk_path)
        , manager_wal_(actor_zeta::spawn<manager_wal_replicate_t>(
              resource,
              scheduler_,
              wal_config_,
              log_,
              manager_disk_->address(),
              wire_index ? manager_index_->address() : components::pipeline::no_mailbox()))
        , manager_dispatcher_(actor_zeta::spawn<manager_dispatcher_t>(
              resource,
              scheduler_,
              log_,
              manager_wal_->address(),
              manager_disk_->address(),
              wire_index ? manager_index_->address() : components::pipeline::no_mailbox(),
              0,
              &services::planner::no_custom_lowering,
              optimizer_pass)) {
        manager_wal_->set_manager_dispatcher_sync(manager_dispatcher_->address());
        manager_disk_->set_manager_wal_sync(manager_wal_->address());
        manager_index_->set_manager_dispatcher_sync(manager_dispatcher_->address());
        manager_disk_->bootstrap_system_tables_sync();
    }

    ~wave_fixture() {
        // Index resets before disk: it holds manager_disk_'s address and messages it during teardown.
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
    configuration::config_disk disk_config_;
    std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager_disk_;
    std::unique_ptr<services::index::manager_index_t, actor_zeta::pmr::deleter_t> manager_index_;
    configuration::config_wal wal_config_;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_wal_;
    std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t> manager_dispatcher_;
    std::unique_ptr<std::pmr::monotonic_buffer_resource> parser_arena_;
};

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
    {
        auto cur = test.execute_sql("SELECT * FROM cdc.docs;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    // Collecting registered columns from VALUES chunks alone leaves pg_computed_column empty here.
    {
        auto cur = test.execute_sql("ALTER TABLE cdc.docs DROP COLUMN price;");
        if (cur->is_error()) {
            WARN("DROP COLUMN error: " << cur->get_error().what);
        }
        REQUIRE(cur->is_success());
    }
}

// rewrite_alter_table only bails ("let execute_ddl error out"); the executor guard must refuse.
TEST_CASE("services::dispatcher::wave3::alter_unresolved_table_is_refused") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("alter_unresolved"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());

    auto cur = test.execute_sql("ALTER TABLE db.no_such_table ADD COLUMN extra bigint;");
    REQUIRE(cur->is_error());
    REQUIRE(mentions(cur->get_error(), "no_such_table"));
}

// The assert here compiles away under NDEBUG, and output_types().front() reads an empty vector.
TEST_CASE("services::dispatcher::wave3::boolean_subquery_unstamped_schema_is_refused") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("bool_subq_unstamped"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.t (b bigint);")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.t (b) VALUES (1);")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE db.docs ();")->is_success());

    auto cur = test.execute_sql("SELECT * FROM db.t WHERE (SELECT * FROM db.docs);");
    REQUIRE(cur->is_error());
}

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

// Storing optimizer_pass_ without forwarding it into optimize() would silently ignore it.
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

// The transformer registers the referenced table's resolve under its OWN database, not the child's.
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
        REQUIRE(cur->is_success());
    }
    {
        auto orphan = test.execute_sql("INSERT INTO db2.child (pid) VALUES (99);");
        REQUIRE(orphan->is_error());
    }
    {
        auto ok = test.execute_sql("INSERT INTO db2.child (pid) VALUES (1);");
        REQUIRE(ok->is_success());
    }
}

// register_udf fans out to every per-executor registry BEFORE the operator's catalog work.
TEST_CASE("services::dispatcher::wave3::register_udf_operator_refusal_unwinds_executors") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("udf_unwind"));

    const std::string fname = "wave3_udf_unwind_probe";
    test.seed_pg_proc_row(fname);

    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_udf,
                                          session_id_t{},
                                          components::compute::function_ptr{make_probe_func(mr.get(), fname)});
        REQUIRE(err.contains_error());
        REQUIRE(mentions(err, "already exists in the catalog"));
    }
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

// SQL can't spell a too-deep type (CREATE TYPE gates its own depth), so this hands a hand-built plan.
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
    REQUIRE(cur->is_error());
    REQUIRE(mentions(cur->get_error(), "cannot be persisted"));
}

// A ready future answered with a default value would build it on the empty address's null resource.
TEST_CASE("services::dispatcher::wave3::empty_target_send_dies_loudly") {
    const pid_t child = fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        // Catch2 installs a SIGABRT handler; reset it so the abort reaches waitpid as a signal death.
        ::signal(SIGABRT, SIG_DFL);
        auto res = actor_zeta::otterbrix::send(actor_zeta::address_t::empty_address(),
                                               &services::collection::executor::executor_t::poke_msg);
        _exit(res.second.is_ready() ? 42 : 43);
    }
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    REQUIRE(WIFSIGNALED(status));
}

// Skipping set_column_bindings would register a and b instead of the written x and y.
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
    {
        auto cur = test.execute_sql("ALTER TABLE cdd.docs DROP COLUMN x;");
        if (cur->is_error()) {
            WARN("DROP COLUMN error: " << cur->get_error().what);
        }
        REQUIRE(cur->is_success());
    }
    {
        auto cur = test.execute_sql("INSERT INTO cdd.docs (z) SELECT a, b FROM cdd.src;");
        REQUIRE(cur->is_error());
    }
}

// Duplicate detection is by (keys,type) only, so a taken name would mint a second pg_class row.
TEST_CASE("services::dispatcher::wave4::create_index_refuses_a_taken_name") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("create_index_name_unique"));

    REQUIRE(test.execute_sql("CREATE DATABASE cdi;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE cdi.t (a bigint, b bigint);")->is_success());
    REQUIRE(test.execute_sql("CREATE INDEX idx ON cdi.t (a);")->is_success());

    {
        auto cur = test.execute_sql("CREATE INDEX idx ON cdi.t (b);");
        REQUIRE(cur->is_error());
    }
    {
        auto cur = test.execute_sql("CREATE INDEX t ON cdi.t (a);");
        REQUIRE(cur->is_error());
    }
    REQUIRE(test.execute_sql("CREATE INDEX idx2 ON cdi.t (b);")->is_success());
}

// A column written NULL in every row has no type, so it's dropped before anything downstream sees it.
TEST_CASE("services::dispatcher::wave4::insert_names_the_all_null_column_it_drops") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("insert_all_null_column_named"));

    REQUIRE(test.execute_sql("CREATE DATABASE anc;")->is_success());
    REQUIRE(test.execute_sql("CREATE TABLE anc.t ();")->is_success());

    auto cur = test.execute_sql("INSERT INTO anc.t (a, x) VALUES (NULL, 'z');");
    INFO("INSERT result: " << (cur->is_error() ? cur->get_error().what : "accepted"));
    REQUIRE(cur->is_error());
    const std::string what{cur->get_error().what};
    CHECK(what.find("INSERT names 2 columns but the source provides 1") != std::string::npos);
    CHECK(what.find("\"a\"") != std::string::npos);
    CHECK(what.find("NULL in every row") != std::string::npos);

    auto two = test.execute_sql("INSERT INTO anc.t (a, b) VALUES (NULL, NULL);");
    REQUIRE(two->is_error());
    const std::string what_two{two->get_error().what};
    INFO("INSERT result: " << what_two);
    CHECK(what_two.find("INSERT names 2 columns but the source provides 0") != std::string::npos);
    CHECK(what_two.find("\"a\", \"b\"") != std::string::npos);

    // A column some row types is not dropped: refusal above is about typelessness, not NULLs.
    REQUIRE(test.execute_sql("INSERT INTO anc.t (id, v) VALUES (1, NULL), (2, 7);")->is_success());
}

// Without this, operator_create_index_backfill silently answers SUCCESS without doing anything.
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

// A hand-built plan bypasses the ALTER-statement coercion path, so validate_default_value_type stays load-bearing.
TEST_CASE("services::dispatcher::wave4::alter_add_column_default_is_coerced_like_create_table") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    wave_fixture test(mr.get(), wave_dir("alter_add_default_type"));

    REQUIRE(test.execute_sql("CREATE DATABASE db;")->is_success());

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

    // The write path expands a DEFAULT for a row inserted without the column.
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

    REQUIRE(test.execute_sql("CREATE TABLE db.diverge (a bigint);")->is_success());
    REQUIRE(test.execute_sql("ALTER TABLE db.diverge ADD COLUMN c integer DEFAULT 7;")->is_success());
    REQUIRE(test.execute_sql("INSERT INTO db.diverge (a) VALUES (1);")->is_success());
    {
        auto cur = test.execute_sql("SELECT c FROM db.diverge;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const auto v = cur->value(0, 0);
        REQUIRE_FALSE(v.is_null());
        CHECK(v.type().type() == logical_type::INTEGER);
        CHECK(v.value<int32_t>() == 7);
    }

    // STRING -> number is registered explicit_only, so it exists but isn't usable under COERCION_ASSIGNMENT.
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

    // attdefspec checks a stored logical tag byte against the column type before reading anything else.
    {
        const components::types::logical_value_t bigint_seven{mr.get(), static_cast<int64_t>(7)};
        std::string spec;
        REQUIRE_FALSE(components::catalog::encode_default_spec(mr.get(), bigint_seven, spec).contains_error());

        // Widths differ (8 vs 4): already caught pre-tag by the leftover-bytes check.
        std::optional<components::types::logical_value_t> as_integer;
        auto ec_int = components::catalog::decode_default_spec(mr.get(),
                                                               complex_logical_type{logical_type::INTEGER},
                                                               spec,
                                                               as_integer);
        CHECK(ec_int.contains_error());

        // Widths agree (int64 both): the arm the tag byte, not the leftover-bytes check, actually carries.
        std::optional<components::types::logical_value_t> as_timestamp;
        auto ec_ts = components::catalog::decode_default_spec(mr.get(),
                                                              complex_logical_type{logical_type::TIMESTAMP},
                                                              spec,
                                                              as_timestamp);
        CHECK(ec_ts.contains_error());
        CHECK_FALSE(as_timestamp.has_value());
    }
}


// 16 scalars give 240 ordered wrong-type pairs, all must refuse, and the 16 self-pairs must
// still round-trip. Before the tag byte, 50 of the 240 were accepted silently as a valid
// value of the wrong type (bit-pattern reinterpretation, not relabelling); the other 190 were
// only caught because their widths happened to disagree.
TEST_CASE("services::dispatcher::wave4::attdefspec_type_tag_refuses_every_wrong_type_pair") {
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    auto* resource = mr.get();
    using components::types::logical_value_t;

    struct scalar_case {
        const char* name;
        complex_logical_type type;
        logical_value_t value;
    };

    // Values are SMALL and non-negative: they reinterpret cleanly into another type of the same width.
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
    REQUIRE(cases.size() == 16);

    std::string accepted;
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
                // The tag is a CHECK, never a source: refusing its own output would be worse than the hole.
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

// NULL carries no tag (presence 0 ends the value), and nested values carry the tag at EVERY level.
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

        std::optional<logical_value_t> wrong;
        CHECK(components::catalog::decode_default_spec(resource, bad, spec, wrong).contains_error());
        CHECK_FALSE(wrong.has_value());
    }
}
