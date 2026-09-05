#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>
#include <unistd.h>

#include <services/dispatcher/dispatcher.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/casts/cast_registry.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/compute/function.hpp>
#include <components/session/session.hpp>
#include <components/types/types.hpp>
#include <core/executor.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/result_wrapper.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>

// The dispatcher's pool-admin API (register/unregister UDF, register/unregister CAST,
// set_explain_renderer) answers a TYPED error. Flattened to a bare `bool`, every
// distinguishable refusal — a name collision, an unknown overload, an unregistered cast
// source type, a slot id out of range, a catalog write that failed — reaches the caller as
// the same nameless `false`, and the executor's OWN typed error (executor_t::register_udf
// answers core::result_wrapper_t<function_uid>) goes on the floor. These tests pin the
// typed channel: each refusal must name itself.
//
// They also pin the two places the fan-out acks are awaited and must not be ignored
// (unregister_udf / unregister_cast): an executor that refused to drop the overload must
// stop the catalog purge, not ride along with it.
//
// And they pin txn_accumulate_msg: a whole statement's worth of parked ranges arriving at a
// session with no active transaction is a refusal, not a silent drop.

using namespace services;
using namespace services::dispatcher;
using namespace services::disk;
using namespace services::wal;
using components::session::session_id_t;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    // Same shape as test_wave_exec_dispatcher.cpp's wave_dir: ::getpid() in the path so two
    // ctest shards (or two build directories) never boot a catalog out of each other's files.
    std::string admin_dir(const char* leaf) {
        return "/tmp/test_dispatcher_admin_errors_" + std::to_string(::getpid()) + "/" + leaf;
    }

    const std::string& scrubbed(const std::string& path) {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        return path;
    }

    core::error_t noop_cast(const components::vector::vector_t&,
                            components::vector::vector_t*,
                            const components::graph_execution_context&,
                            uint64_t) noexcept {
        return core::error_t::no_error();
    }

    components::casts::cast_entry make_cast_entry() {
        return components::casts::cast_entry{components::casts::cast_function_t{noop_cast, nullptr},
                                             components::casts::cast_cost{.precision_loss = 0, .footprint = 8},
                                             /*convertable_inplace*/ false};
    }

    // BOOLEAN -> DATE has no default cast, so it is a clean slate to register.
    const complex_logical_type kCastSource{logical_type::BOOLEAN};
    const complex_logical_type kCastTarget{logical_type::DATE};

    core::error_t probe_exec(components::compute::kernel_context& ctx,
                             const std::pmr::vector<components::types::logical_value_t>& in,
                             std::pmr::vector<components::types::logical_value_t>& out) {
        out.emplace_back(ctx.exec_context().resource(), in[0].value<int64_t>() * 2);
        return core::error_t::no_error();
    }

    // One-arg BIGINT -> BIGINT row UDF. The name is a parameter so each test owns its own
    // entry in the process-global default registry.
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

    std::pmr::vector<complex_logical_type> bigint_inputs(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> inputs(resource);
        inputs.emplace_back(logical_type::BIGINT);
        return inputs;
    }

    bool mentions(const core::error_t& err, const char* needle) {
        return std::string{err.what.c_str()}.find(needle) != std::string::npos;
    }

} // namespace

// Dispatcher + disk + WAL, driven by the non-threading test scheduler. Mirrors the fixture in
// test_dispatcher_catalog.cpp but exposes the pool-admin entry points instead of SQL.
struct admin_fixture : actor_zeta::actor::actor_mixin<admin_fixture> {
    admin_fixture(std::pmr::memory_resource* resource, const std::string& disk_path)
        : actor_zeta::actor::actor_mixin<admin_fixture>()
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
        manager_dispatcher_->sync(manager_dispatcher_t::sync_pack{manager_wal_->address(),
                                                                  manager_disk_->address(),
                                                                  actor_zeta::address_t::empty_address()});
        manager_wal_->sync(services::wal::wal_sync_pack_t{actor_zeta::address_t(manager_disk_->address()),
                                                          manager_dispatcher_->address(),
                                                          actor_zeta::address_t::empty_address()});
        manager_disk_->sync(manager_disk_t::disk_sync_pack_t{manager_wal_->address()});
        manager_disk_->bootstrap_system_tables_sync();
    }

    ~admin_fixture() {
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
        return std::move(fut).take_ready();
    }

    void drain() { scheduler_->run(10000); }

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

    std::size_t pg_proc_rows(const std::string& fname) {
        auto matches = disk_invoke(&manager_disk_t::resolve_function_by_name, read_ctx(), fname, std::uint64_t{0});
        REQUIRE_FALSE(matches.has_error());
        return matches.value().size();
    }

    components::catalog::oid_t pg_cast_oid(components::catalog::oid_t source_oid,
                                           components::catalog::oid_t target_oid) {
        auto r = disk_invoke(&manager_disk_t::find_cast_oid, read_ctx(), source_oid, target_oid);
        REQUIRE_FALSE(r.has_error());
        return r.value();
    }

    // Write a pg_proc (+ pg_depend) row for `fname` straight into the catalog, WITHOUT
    // going through register_udf. That reproduces the divergence a partial fan-out (or a
    // restart that hydrated the catalog but not the per-executor registries) leaves behind:
    // the catalog says the function exists, no executor's registry holds it.
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
        drain();
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
};

// ===== every refusal names itself =====

TEST_CASE("services::dispatcher::admin_errors::register_udf_duplicate_keeps_executor_error") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    admin_fixture test(mr.get(), admin_dir("udf_dup"));

    const std::string fname = "admin_probe_dup";
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_udf,
                                          session_id_t{},
                                          components::compute::function_ptr{make_probe_func(mr.get(), fname)});
        REQUIRE_FALSE(err.contains_error());
    }
    // Second registration of the same signature: executor_t::register_udf answers a TYPED
    // function_registry_error. That error is the whole point — it must reach the caller
    // instead of being flattened into `false`.
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_udf,
                                          session_id_t{},
                                          components::compute::function_ptr{make_probe_func(mr.get(), fname)});
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::function_registry_error);
        REQUIRE(mentions(err, "already registered"));
    }
    components::compute::function_registry_t::reset_default();
}

TEST_CASE("services::dispatcher::admin_errors::cast_refusals_are_distinguishable") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    admin_fixture test(mr.get(), admin_dir("cast_reasons"));

    // (a) unregistering a cast that was never registered.
    {
        auto err =
            test.dispatcher_invoke(&manager_dispatcher_t::unregister_cast, session_id_t{}, kCastSource, kCastTarget);
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::schema_error);
        REQUIRE(mentions(err, "cast is not registered"));
    }
    // (b) a source type that is not registered at all — a DIFFERENT reason, and it has to
    //     read differently.
    {
        const auto udt = complex_logical_type::create_unknown("admin_probe_udt");
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_cast,
                                          session_id_t{},
                                          udt,
                                          kCastTarget,
                                          make_cast_entry());
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::schema_error);
        REQUIRE(mentions(err, "not registered"));
        REQUIRE_FALSE(mentions(err, "cast is not registered"));
    }
    // (c) the happy path, then the duplicate — a third distinct reason.
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_cast,
                                          session_id_t{},
                                          kCastSource,
                                          kCastTarget,
                                          make_cast_entry());
        REQUIRE_FALSE(err.contains_error());
    }
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_cast,
                                          session_id_t{},
                                          kCastSource,
                                          kCastTarget,
                                          make_cast_entry());
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::schema_error);
        REQUIRE(mentions(err, "already registered"));
    }
    components::compute::function_registry_t::reset_default();
}

TEST_CASE("services::dispatcher::admin_errors::set_explain_renderer_refusals_named") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    admin_fixture test(mr.get(), admin_dir("renderer"));

    // A slot id past the registry limit and a null renderer are both refused by every
    // executor; the dispatcher must say which door closed instead of answering `false`.
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::set_explain_renderer,
                                          uint32_t{4000000000u},
                                          &services::collection::render_postgres);
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::invalid_parameter);
        REQUIRE(mentions(err, "executor"));
    }
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::set_explain_renderer,
                                          uint32_t{5},
                                          services::collection::explain_render_fn{nullptr});
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::invalid_parameter);
    }
    // The success path still reports success.
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::set_explain_renderer,
                                          uint32_t{1},
                                          &services::collection::render_postgres);
        REQUIRE_FALSE(err.contains_error());
    }
    components::compute::function_registry_t::reset_default();
}

// ===== an executor that refused to drop the overload stops the catalog purge =====

TEST_CASE("services::dispatcher::admin_errors::unregister_udf_executor_refusal_keeps_pg_proc") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    admin_fixture test(mr.get(), admin_dir("unreg_refusal"));

    const std::string fname = "admin_probe_orphan";
    // The process-global default registry (what operator_unregister_udf_t probes) knows the
    // overload and the catalog carries its pg_proc row — but NO executor registry holds it.
    {
        auto added =
            components::compute::function_registry_t::get_default()->add_function(make_probe_func(mr.get(), fname));
        REQUIRE_FALSE(added.has_error());
    }
    test.seed_pg_proc_row(fname);
    REQUIRE(test.pg_proc_rows(fname) == 1);

    auto err =
        test.dispatcher_invoke(&manager_dispatcher_t::unregister_udf, session_id_t{}, fname, bigint_inputs(mr.get()));
    // Every executor answered "I did not have that overload". An ack awaited and never looked
    // at lets the pg_proc/pg_depend purge run anyway, and the catalog then claims the function
    // is gone while nothing dropped it. The catalog is asserted FIRST — it is the damage; the
    // typed refusal is only how the caller learns about it.
    REQUIRE(test.pg_proc_rows(fname) == 1);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::unrecognized_function);
    REQUIRE(mentions(err, "executor"));

    components::compute::function_registry_t::reset_default();
}

// Guard on the success path of the cast fan-out. A per-executor DIVERGENCE (one executor
// refusing while another accepts) is not constructible through the public API today:
// register_cast fans the entry out to every executor at once, and unregister_cast's step-1
// validation runs against one of those same registries, so the four registries cannot be made
// to disagree from outside. So this pins the other half: when every executor DOES confirm,
// the pg_cast row must actually go.
TEST_CASE("services::dispatcher::admin_errors::unregister_cast_success_removes_pg_cast_row") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    admin_fixture test(mr.get(), admin_dir("unreg_cast"));

    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_cast,
                                          session_id_t{},
                                          kCastSource,
                                          kCastTarget,
                                          make_cast_entry());
        REQUIRE_FALSE(err.contains_error());
    }
    const auto source_oid = components::catalog::well_known_oid::boolean_type;
    const auto target_oid = components::catalog::well_known_oid::date_type;
    REQUIRE(test.pg_cast_oid(source_oid, target_oid) != components::catalog::INVALID_OID);

    {
        auto err =
            test.dispatcher_invoke(&manager_dispatcher_t::unregister_cast, session_id_t{}, kCastSource, kCastTarget);
        REQUIRE_FALSE(err.contains_error());
    }
    REQUIRE(test.pg_cast_oid(source_oid, target_oid) == components::catalog::INVALID_OID);
    components::compute::function_registry_t::reset_default();
}

// ===== accumulating onto a session with no transaction is a refusal, not silence =====

TEST_CASE("services::dispatcher::admin_errors::txn_accumulate_without_transaction_is_refused") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    admin_fixture test(mr.get(), admin_dir("accumulate"));

    // A session that never began a transaction. The payload below is a whole statement's
    // worth of parked work: base-table insert and delete ranges, catalog row ranges, storage
    // oids created and retired. Without the refusal all of it goes on the floor unsaid.
    const session_id_t orphan_session{};
    txn_accumulate_payload_t payload;
    payload.base_appends.push_back(components::table::dml_append_range_t{4242, 0, 7});
    payload.base_deletes.push_back(components::table::dml_delete_range_t{4242, 99});
    payload.pg_catalog_appends.push_back(components::pg_catalog_append_range_t{1259, 0, 1});
    payload.dropped_storage_oids.push_back(4243);
    payload.created_storage_oids.push_back(4244);
    REQUIRE_FALSE(payload.empty());

    auto err = test.dispatcher_invoke(&manager_dispatcher_t::txn_accumulate_msg, orphan_session, payload);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::transaction_inactive);

    // And the refusal is honest about what it did: nothing was parked anywhere, so a
    // transaction begun afterwards drains empty.
    auto ctx = test.dispatcher_invoke(&manager_dispatcher_t::txn_begin_session_msg, orphan_session);
    REQUIRE(ctx.txn.transaction_id != 0);
    auto drained = test.dispatcher_invoke(&manager_dispatcher_t::txn_commit_drain_msg, orphan_session);
    REQUIRE(drained.base_appends.empty());
    REQUIRE(drained.swap_appends.empty());
    REQUIRE(drained.dropped_storage_oids.empty());
    REQUIRE(drained.created_storage_oids.empty());

    // An active transaction still accepts the same payload — the refusal is about the
    // missing transaction, not about the payload.
    const session_id_t live_session{};
    auto live_ctx = test.dispatcher_invoke(&manager_dispatcher_t::txn_begin_session_msg, live_session);
    REQUIRE(live_ctx.txn.transaction_id != 0);
    auto ok = test.dispatcher_invoke(&manager_dispatcher_t::txn_accumulate_msg, live_session, payload);
    REQUIRE_FALSE(ok.contains_error());
    auto live_drain = test.dispatcher_invoke(&manager_dispatcher_t::txn_commit_drain_msg, live_session);
    REQUIRE(live_drain.base_appends.size() == 1);
    REQUIRE(live_drain.base_appends.front().table_oid == 4242);
    REQUIRE(live_drain.dropped_storage_oids.size() == 1);
    REQUIRE(live_drain.created_storage_oids.size() == 1);

    components::compute::function_registry_t::reset_default();
}
