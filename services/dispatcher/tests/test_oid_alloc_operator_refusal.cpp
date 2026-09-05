#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <services/dispatcher/dispatcher.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/casts/cast_registry.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/session/session.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <core/executor.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/result_wrapper.hpp>
#include <services/collection/executor.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/tests/catalog_probe.hpp>
#include <services/wal/manager_wal_replicate.hpp>

// THREE OPERATORS MINT THEIR OWN CATALOG IDENTITY AND MUST CHECK THAT THE ROUND DELIVERED ONE.
//
// CREATE TABLE and its DDL siblings take their OIDs from the planner's allocation round, which is
// checked twice (oid_batch_t::make against compute_oid_demand, then overrun() after the rewrite).
// operator_register_udf_t (pg_proc), operator_register_cast_t (pg_cast) and
// operator_alter_column_add_t (pg_attribute) do NOT: each runs its own one-OID round against the
// disk actor at execute time —
//     batch.oids = co_await allocate_oids_batch(1); const oid_t id = batch.allocate();
// manager_disk_t::allocate_oids_batch has no error channel, so a round that did not deliver comes
// back as an EMPTY vector; allocate() on an exhausted batch answers INVALID_OID and latches a
// sticky overrun() flag. Spending that unchecked REPORTS SUCCESS and leaves a durable row stamped
// with INVALID_OID (= 0) — rule 16 ("a catalog object always carries an OID") broken durably and
// announced as success:
//   * pg_proc      — a function whose identity is what pg_depend and every later lookup key on;
//   * pg_cast      — worse than useless: find_cast_oid reads that 0 back as "there is no such
//                    cast", so the row is unreachable AND undeletable by DROP CAST;
//   * pg_attribute — a column whose attoid is what the ADD COLUMN backfill hands to the storage
//                    that will materialise it, and what a later DROP COLUMN tombstones on.
//
// THE INJECTION. The round is a message round-trip to the disk actor over an in-memory counter: no
// file, no page, so neither the .otbx interposer nor the WAL one can reach it and there is no
// device to fail. It has its own narrow DEV_MODE seam
// (services::collection::executor::dev_set_oid_alloc_interposer), and an EMPTY batch is not an
// invented state — it is the exact value the round's real failure branches answer with.
// executor_t::allocate_oids_inline consults that seam and these three rounds never pass through
// it, so components/physical_plan/operators/single_oid_round.hpp consults the SAME seam object for
// them, once per round.
//
// SENSITIVITY IS PROVEN INSIDE EACH TEST: the same seam object is installed for the CONTROL
// statement (pass-through — it must succeed and write a real identity) and for the faulted one,
// and each test asserts on the seam's own counters that the round was seen both times and
// substituted exactly once. Every test asserts the CATALOG'S CONTENT, not only the status.

using namespace services;
using namespace services::dispatcher;
using namespace services::disk;
using namespace services::wal;
using components::session::session_id_t;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    namespace catalog = components::catalog;

    // Same shape as test_wave_exec_dispatcher.cpp's wave_dir: ::getpid() in the path so two
    // ctest shards (or two build directories) never boot a catalog out of each other's files.
    std::string oid_alloc_dir(const char* leaf) {
        return "/tmp/test_oid_alloc_operator_refusal_" + std::to_string(::getpid()) + "/" + leaf;
    }

    // A run that dies before its destructor leaves its disk directory behind, and the next run
    // would boot its catalog from those files. Clearing on the way IN as well as OUT makes the
    // fixture idempotent.
    const std::string& scrubbed(const std::string& path) {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        return path;
    }

    // The OID-allocation fault seam, armed per statement. Identical in shape to the one
    // integration/cpp/test/test_oid_alloc_refusal.cpp installs for the planner's round: a plain
    // virtual (rule 14 — not std::function), process-wide, DEV_MODE-only.
    class oid_alloc_fault_scope_t final : public services::collection::executor::oid_alloc_interposer_t {
    public:
        oid_alloc_fault_scope_t() { services::collection::executor::dev_set_oid_alloc_interposer(this); }
        ~oid_alloc_fault_scope_t() override {
            services::collection::executor::dev_set_oid_alloc_interposer(nullptr);
        }

        oid_alloc_fault_scope_t(const oid_alloc_fault_scope_t&) = delete;
        oid_alloc_fault_scope_t& operator=(const oid_alloc_fault_scope_t&) = delete;

        bool arm = false; // armed only around the statement under test

        std::size_t rounds_seen = 0;   // rounds this seam actually observed
        std::size_t rounds_failed = 0; // rounds it substituted

        std::vector<catalog::oid_t> substitute(std::size_t /*requested*/,
                                               std::vector<catalog::oid_t> allocated) override {
            ++rounds_seen;
            if (!arm) {
                return allocated;
            }
            ++rounds_failed;
            // The round delivers nothing at all — the value both real failure branches produce.
            allocated.clear();
            return allocated;
        }
    };

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

    // BOOLEAN has default casts only to the numerics and to string, so both pairs below are a
    // clean slate: the control pair and the faulted pair never collide with each other either.
    const complex_logical_type kCastSource{logical_type::BOOLEAN};
    const complex_logical_type kCastTargetOk{logical_type::DATE};
    const complex_logical_type kCastTargetBroken{logical_type::TIME};

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

    // One-arg BIGINT -> BIGINT vector UDF; the name is a parameter so each registration owns its
    // own entry in the process-global default registry.
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

    bool mentions(const core::error_t& err, const char* needle) {
        return std::string{err.what.c_str()}.find(needle) != std::string::npos;
    }

} // namespace

// Dispatcher + disk + WAL on the non-threading test scheduler. Mirrors the wiring in
// test_dispatcher_catalog.cpp / test_dispatcher_admin_errors.cpp; the name must stay distinct
// from theirs because all three TUs share one Catch2 target.
struct oid_round_fixture : actor_zeta::actor::actor_mixin<oid_round_fixture> {
    oid_round_fixture(std::pmr::memory_resource* resource, const std::string& disk_path)
        : actor_zeta::actor::actor_mixin<oid_round_fixture>()
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

    ~oid_round_fixture() {
        manager_dispatcher_.reset();
        manager_wal_.reset();
        manager_disk_.reset();
        scheduler_->stop();
        std::filesystem::remove_all(disk_path_);
        delete scheduler_;
    }

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    void step() { scheduler_->run(10000); }

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

    // Adapter exposing the (resource, invoke) shape the catalog_probe helpers expect.
    struct probe_fixture {
        oid_round_fixture* self;
        std::pmr::memory_resource& resource;
        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            return self->disk_invoke(fn, std::forward<Args>(args)...);
        }
    };
    probe_fixture probe_fx() { return probe_fixture{this, *resource_}; }

    components::execution_context_t read_ctx() {
        // probe_see_all_txn, not transaction_data{0, 0}: column visibility is judged against
        // start_time, so a 0 there means "a snapshot from before the first commit" and hides
        // every ALTER-added column now that added_at_commit_id carries a real id.
        return components::execution_context_t{session_id_t{}, test_probe::probe_see_all_txn(), {}};
    }

    void execute_sql(const std::string& query) {
        parser_arena_ = std::make_unique<std::pmr::monotonic_buffer_resource>(resource_);
        auto parse_result = linitial(raw_parser(parser_arena_.get(), query.c_str()));
        components::sql::transform::transformer local_transformer(resource_, query.c_str());
        auto _wrap =
            local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_result)).finalize();
        REQUIRE(!_wrap.has_error());
        auto view = _wrap.value();

        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                       &manager_dispatcher_t::execute_plan,
                                                       session_id_t{},
                                                       std::move(view));
        pending_future_ = std::make_unique<actor_zeta::unique_future<components::cursor::cursor_t_ptr>>(
            std::move(future));
    }

    components::cursor::cursor_t_ptr take_result() {
        REQUIRE(pending_future_);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (!pending_future_->is_ready() && std::chrono::steady_clock::now() < deadline) {
            scheduler_->run(1000);
            std::this_thread::yield();
        }
        REQUIRE(pending_future_->valid());
        REQUIRE(pending_future_->is_ready());
        auto result = std::move(*pending_future_).take_ready();
        pending_future_.reset();
        // Drain again so the executor's post-result DDL tail (catalog writes, commit,
        // pg_attribute backfills) finishes before the catalog is read.
        step();
        return result;
    }

    components::cursor::cursor_t_ptr run_sql(const std::string& query) {
        execute_sql(query);
        return take_result();
    }

    // --- catalog content readers (the assertions that matter live on these) ---

    // Every pg_proc row carrying this name, as (oid) — an EMPTY answer means the catalog has
    // no such function at all, which is what a refused registration must leave behind.
    std::vector<catalog::oid_t> pg_proc_oids(const std::string& fname) {
        auto matches = disk_invoke(&manager_disk_t::resolve_function_by_name, read_ctx(), fname, std::uint64_t{0});
        REQUIRE_FALSE(matches.has_error());
        std::vector<catalog::oid_t> out;
        out.reserve(matches.value().size());
        for (const auto& m : matches.value()) {
            out.push_back(m.oid);
        }
        return out;
    }

    // Raw pg_cast read by (castsource, casttarget), answering the row's OWN identity column.
    // find_cast_oid cannot be used for this: it returns the very column under test, so a row
    // stamped 0 is indistinguishable there from no row at all — which is precisely the damage.
    // pg_cast layout (build_create_cast_writes): oid(0), castsource(1), casttarget(2).
    std::vector<catalog::oid_t> pg_cast_oids(catalog::oid_t source_oid, catalog::oid_t target_oid) {
        std::pmr::vector<std::uint64_t> keys{resource_};
        keys.emplace_back(std::uint64_t{1});
        keys.emplace_back(std::uint64_t{2});
        std::pmr::vector<components::types::logical_value_t> vals{resource_};
        vals.emplace_back(resource_, source_oid);
        vals.emplace_back(resource_, target_oid);
        auto adapter = probe_fx();
        auto batches = test_probe::probe_read(adapter,
                                              read_ctx(),
                                              catalog::well_known_oid::pg_cast_table,
                                              std::move(keys),
                                              std::move(vals));
        std::vector<catalog::oid_t> out;
        for (const auto& chunk : batches) {
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                const auto cell = chunk.value(0, i);
                out.push_back(cell.is_null() ? catalog::INVALID_OID
                                             : static_cast<catalog::oid_t>(cell.value<std::uint32_t>()));
            }
        }
        return out;
    }

    catalog::oid_t namespace_oid(const std::string& name) {
        auto r = disk_invoke(&manager_disk_t::resolve_namespace, read_ctx(), name, std::uint64_t{0});
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().found);
        return r.value().oid;
    }

    test_probe::probe_table_result_t table(const std::string& ns, const std::string& tname) {
        auto adapter = probe_fx();
        return test_probe::probe_table(adapter, read_ctx(), namespace_oid(ns), tname);
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
    std::unique_ptr<actor_zeta::unique_future<components::cursor::cursor_t_ptr>> pending_future_;
};

namespace {

    bool has_column(const test_probe::probe_table_result_t& t, const std::string& name) {
        for (const auto& c : t.columns) {
            if (c.attname == name)
                return true;
        }
        return false;
    }

    catalog::oid_t column_attoid(const test_probe::probe_table_result_t& t, const std::string& name) {
        for (const auto& c : t.columns) {
            if (c.attname == name)
                return c.attoid;
        }
        return catalog::INVALID_OID;
    }

} // namespace

// ===========================================================================
// pg_proc — CREATE FUNCTION (manager_dispatcher_t::register_udf)
// ===========================================================================
TEST_CASE("services::dispatcher::oid_alloc_operator_refusal::register_udf_refuses_when_the_round_delivers_nothing") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    oid_round_fixture test(mr.get(), oid_alloc_dir("register_udf"));

    oid_alloc_fault_scope_t fault;

    // CONTROL — the seam is installed and passing through. The registration must succeed AND
    // the pg_proc row must carry a real identity; that is what makes the failure below
    // attributable to the injection rather than to anything else about the statement.
    const std::string ok_name = "oidround_probe_ok";
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_udf,
                                          session_id_t{},
                                          components::compute::function_ptr{make_probe_func(mr.get(), ok_name)});
        REQUIRE_FALSE(err.contains_error());
    }
    REQUIRE(fault.rounds_seen == 1);
    REQUIRE(fault.rounds_failed == 0);
    {
        const auto oids = test.pg_proc_oids(ok_name);
        REQUIRE(oids.size() == 1);
        REQUIRE(oids.front() != catalog::INVALID_OID);
    }

    // FAULT — the round delivers nothing at all.
    const std::string broken_name = "oidround_probe_broken";
    fault.arm = true;
    auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_udf,
                                      session_id_t{},
                                      components::compute::function_ptr{make_probe_func(mr.get(), broken_name)});
    fault.arm = false;

    // THE CATALOG IS ASSERTED FIRST — it is the damage: an unchecked round leaves a pg_proc
    // row for `broken_name` whose oid column holds 0, a function with no identity that
    // survives restart.
    const auto broken_oids = test.pg_proc_oids(broken_name);
    INFO("pg_proc rows for the refused function: " << broken_oids.size()
                                                   << (broken_oids.empty() ? "" : " (first oid: 0 means INVALID)"));
    REQUIRE(broken_oids.empty());

    // ...and the caller is told; unchecked, the registration reports success with no error.
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::io_error);
    REQUIRE(mentions(err, "register_udf"));
    REQUIRE(mentions(err, "OID allocation round"));

    // The refusal really travelled through the injected round.
    REQUIRE(fault.rounds_seen == 2);
    REQUIRE(fault.rounds_failed == 1);

    // The control registration is untouched by all of it.
    {
        const auto oids = test.pg_proc_oids(ok_name);
        REQUIRE(oids.size() == 1);
        REQUIRE(oids.front() != catalog::INVALID_OID);
    }

    components::compute::function_registry_t::reset_default();
}

// ===========================================================================
// pg_cast — CREATE CAST (manager_dispatcher_t::register_cast)
// ===========================================================================
TEST_CASE("services::dispatcher::oid_alloc_operator_refusal::register_cast_refuses_when_the_round_delivers_nothing") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    oid_round_fixture test(mr.get(), oid_alloc_dir("register_cast"));

    constexpr auto source_oid = catalog::well_known_oid::boolean_type;
    constexpr auto target_ok_oid = catalog::well_known_oid::date_type;
    constexpr auto target_broken_oid = catalog::well_known_oid::time_type;

    oid_alloc_fault_scope_t fault;

    // CONTROL — same statement shape as the faulted one below.
    {
        auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_cast,
                                          session_id_t{},
                                          kCastSource,
                                          kCastTargetOk,
                                          make_cast_entry());
        REQUIRE_FALSE(err.contains_error());
    }
    REQUIRE(fault.rounds_seen == 1);
    REQUIRE(fault.rounds_failed == 0);
    {
        const auto oids = test.pg_cast_oids(source_oid, target_ok_oid);
        REQUIRE(oids.size() == 1);
        REQUIRE(oids.front() != catalog::INVALID_OID);
    }

    // FAULT — the round delivers nothing at all.
    fault.arm = true;
    auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_cast,
                                      session_id_t{},
                                      kCastSource,
                                      kCastTargetBroken,
                                      make_cast_entry());
    fault.arm = false;

    // THE CATALOG FIRST: unchecked, this read answers one row whose identity column holds 0 —
    // a pg_cast row that find_cast_oid reads back as "there is no such cast", so it can
    // neither be used nor dropped.
    const auto broken_oids = test.pg_cast_oids(source_oid, target_broken_oid);
    INFO("pg_cast rows for the refused (BOOLEAN, TIME) pair: " << broken_oids.size());
    REQUIRE(broken_oids.empty());
    // Read through the production accessor as well: both readings have to agree that there is
    // nothing there; unchecked they disagree (a row exists, find_cast_oid says it does not).
    {
        auto found = test.disk_invoke(&manager_disk_t::find_cast_oid, test.read_ctx(), source_oid, target_broken_oid);
        REQUIRE_FALSE(found.has_error());
        REQUIRE(found.value() == catalog::INVALID_OID);
    }

    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::io_error);
    REQUIRE(mentions(err, "register_cast"));
    REQUIRE(mentions(err, "OID allocation round"));

    REQUIRE(fault.rounds_seen == 2);
    REQUIRE(fault.rounds_failed == 1);

    // The control cast is untouched.
    {
        const auto oids = test.pg_cast_oids(source_oid, target_ok_oid);
        REQUIRE(oids.size() == 1);
        REQUIRE(oids.front() != catalog::INVALID_OID);
    }

    components::compute::function_registry_t::reset_default();
}

// ===========================================================================
// pg_attribute — ALTER TABLE ... ADD COLUMN
//
// The one of the three that is reachable from plain SQL, so the user consequence is literal:
// the statement reports success and the column it added has no identity.
// ===========================================================================
TEST_CASE("services::dispatcher::oid_alloc_operator_refusal::alter_add_column_refuses_when_the_round_delivers_nothing") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    oid_round_fixture test(mr.get(), oid_alloc_dir("alter_add_column"));

    // CREATE DATABASE answers an empty cursor rather than a success one, so it is asserted
    // for the absence of an error; CREATE TABLE does report success.
    REQUIRE_FALSE(test.run_sql("CREATE DATABASE oidround;")->is_error());
    REQUIRE(test.run_sql("CREATE TABLE oidround.items(id int, val int);")->is_success());

    // Installed AFTER the setup: CREATE DATABASE / CREATE TABLE run the PLANNER's allocation
    // round, and counting those here would say nothing about this operator's own round.
    // ALTER TABLE consumes no planner OIDs at all (compute_oid_demand answers 0 for it), so
    // every round the seam sees from here on is the one under test.
    oid_alloc_fault_scope_t fault;

    // CONTROL — same statement shape as the faulted one below.
    REQUIRE(test.run_sql("ALTER TABLE oidround.items ADD COLUMN extra_ok bigint;")->is_success());
    REQUIRE(fault.rounds_seen == 1);
    REQUIRE(fault.rounds_failed == 0);
    {
        auto t = test.table("oidround", "items");
        REQUIRE(t.found);
        REQUIRE(has_column(t, "extra_ok"));
        REQUIRE(column_attoid(t, "extra_ok") != catalog::INVALID_OID);
    }

    // FAULT — the round delivers nothing at all.
    fault.arm = true;
    auto refused = test.run_sql("ALTER TABLE oidround.items ADD COLUMN extra_broken bigint;");
    fault.arm = false;

    // THE CATALOG FIRST: unchecked, `extra_broken` is there with attoid = 0 — a column whose
    // identity is what the ADD COLUMN backfill hands to the storage that materialises it and
    // what a later DROP COLUMN keys its tombstone on.
    {
        auto t = test.table("oidround", "items");
        REQUIRE(t.found);
        INFO("columns after the refused ALTER: " << t.columns.size());
        REQUIRE_FALSE(has_column(t, "extra_broken"));
        // No surviving column carries a non-identity, whatever its name.
        for (const auto& c : t.columns) {
            INFO("column " << c.attname << " attoid " << c.attoid);
            REQUIRE(c.attoid != catalog::INVALID_OID);
        }
        // The control column is untouched by all of it.
        REQUIRE(has_column(t, "extra_ok"));
        REQUIRE(column_attoid(t, "extra_ok") != catalog::INVALID_OID);
    }

    // ...and the statement says so; unchecked it answers is_success().
    INFO("an ALTER TABLE ADD COLUMN whose OID round delivered nothing must FAIL, not add a "
         "column stamped with an identity nothing allocated");
    REQUIRE(refused->is_error());
    REQUIRE(refused->get_error().type == core::error_code_t::io_error);
    REQUIRE(mentions(refused->get_error(), "alter_column_add"));

    REQUIRE(fault.rounds_seen == 2);
    REQUIRE(fault.rounds_failed == 1);

    components::compute::function_registry_t::reset_default();
}
