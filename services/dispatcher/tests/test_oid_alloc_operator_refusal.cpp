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
#include <components/context/context.hpp>
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

// operator_register_udf_t (pg_proc), operator_register_cast_t (pg_cast) and
// operator_alter_column_add_t (pg_attribute) each run their own one-OID round at execute time.
// allocate_oids_batch has no error channel — a failed round returns empty and allocate() answers
// INVALID_OID (0), so spending it unchecked reports success with a row stamped 0. The round is
// in-memory, reachable only through its own DEV_MODE seam; each test runs a CONTROL statement and
// the faulted one through it, asserting both the round counters and the catalog's content.

using namespace services;
using namespace services::dispatcher;
using namespace services::disk;
using namespace services::wal;
using components::session::session_id_t;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    namespace catalog = components::catalog;

    std::string oid_alloc_dir(const char* leaf) {
        return "/tmp/test_oid_alloc_operator_refusal_" + std::to_string(::getpid()) + "/" + leaf;
    }

    // Clears on the way in too, so a run that died mid-test doesn't leave its directory for the next to boot from.
    const std::string& scrubbed(const std::string& path) {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
        return path;
    }

    // The OID-allocation fault seam, armed per statement: a plain virtual (std::function is banned), DEV_MODE-only.
    class oid_alloc_fault_scope_t final : public services::collection::executor::oid_alloc_interposer_t {
    public:
        oid_alloc_fault_scope_t() { services::collection::executor::dev_set_oid_alloc_interposer(this); }
        ~oid_alloc_fault_scope_t() override { services::collection::executor::dev_set_oid_alloc_interposer(nullptr); }

        oid_alloc_fault_scope_t(const oid_alloc_fault_scope_t&) = delete;
        oid_alloc_fault_scope_t& operator=(const oid_alloc_fault_scope_t&) = delete;

        bool arm = false;

        std::size_t rounds_seen = 0;
        std::size_t rounds_failed = 0;

        std::vector<catalog::oid_t> substitute(std::size_t /*requested*/,
                                               std::vector<catalog::oid_t> allocated) override {
            ++rounds_seen;
            if (!arm) {
                return allocated;
            }
            ++rounds_failed;
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

    // BOOLEAN's only default casts are to numerics and string, so these pairs never collide with each other.
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

struct oid_round_fixture : actor_zeta::actor::actor_mixin<oid_round_fixture> {
    oid_round_fixture(std::pmr::memory_resource* resource, const std::string& disk_path)
        : actor_zeta::actor::actor_mixin<oid_round_fixture>()
        , resource_(resource)
        , disk_path_(scrubbed(disk_path))
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
        , disk_config_(disk_path)
        , manager_disk_(actor_zeta::spawn<manager_disk_t>(resource, scheduler_, scheduler_, disk_config_, log_))
        , wal_config_(disk_path)
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
        // probe_see_all_txn, not transaction_data{0, 0}: a 0 start_time would hide every ALTER-added column.
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
        pending_future_ =
            std::make_unique<actor_zeta::unique_future<components::cursor::cursor_t_ptr>>(std::move(future));
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
        step();
        return result;
    }

    components::cursor::cursor_t_ptr run_sql(const std::string& query) {
        execute_sql(query);
        return take_result();
    }

    std::vector<catalog::oid_t> pg_proc_oids(const std::string& fname) {
        auto matches = disk_invoke(&manager_disk_t::resolve_function_by_name, read_ctx(), fname);
        REQUIRE_FALSE(matches.has_error());
        std::vector<catalog::oid_t> out;
        out.reserve(matches.value().size());
        for (const auto& m : matches.value()) {
            out.push_back(m.oid);
        }
        return out;
    }

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
        auto r = disk_invoke(&manager_disk_t::resolve_namespace, read_ctx(), name);
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
    configuration::config_disk disk_config_;
    std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager_disk_;
    configuration::config_wal wal_config_;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_wal_;
    // Declared after the managers: the dispatcher is spawned with their addresses.
    std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t> manager_dispatcher_;
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

TEST_CASE("services::dispatcher::oid_alloc_operator_refusal::register_udf_refuses_when_the_round_delivers_nothing") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    oid_round_fixture test(mr.get(), oid_alloc_dir("register_udf"));

    oid_alloc_fault_scope_t fault;

    // CONTROL: registration must succeed with a real identity, so the failure below is attributable to the injection.
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

    // The catalog is checked first: unchecked, this leaves a pg_proc row stamped oid=0.
    const auto broken_oids = test.pg_proc_oids(broken_name);
    INFO("pg_proc rows for the refused function: " << broken_oids.size()
                                                   << (broken_oids.empty() ? "" : " (first oid: 0 means INVALID)"));
    REQUIRE(broken_oids.empty());

    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::io_error);
    REQUIRE(mentions(err, "register_udf"));
    REQUIRE(mentions(err, "OID allocation round"));

    REQUIRE(fault.rounds_seen == 2);
    REQUIRE(fault.rounds_failed == 1);

    {
        const auto oids = test.pg_proc_oids(ok_name);
        REQUIRE(oids.size() == 1);
        REQUIRE(oids.front() != catalog::INVALID_OID);
    }

    components::compute::function_registry_t::reset_default();
}

TEST_CASE("services::dispatcher::oid_alloc_operator_refusal::register_cast_refuses_when_the_round_delivers_nothing") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    oid_round_fixture test(mr.get(), oid_alloc_dir("register_cast"));

    constexpr auto source_oid = catalog::well_known_oid::boolean_type;
    constexpr auto target_ok_oid = catalog::well_known_oid::date_type;
    constexpr auto target_broken_oid = catalog::well_known_oid::time_type;

    oid_alloc_fault_scope_t fault;

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

    fault.arm = true;
    auto err = test.dispatcher_invoke(&manager_dispatcher_t::register_cast,
                                      session_id_t{},
                                      kCastSource,
                                      kCastTargetBroken,
                                      make_cast_entry());
    fault.arm = false;

    // The catalog first: unchecked, this pg_cast row is stamped 0, which find_cast_oid reads as "no such cast".
    const auto broken_oids = test.pg_cast_oids(source_oid, target_broken_oid);
    INFO("pg_cast rows for the refused (BOOLEAN, TIME) pair: " << broken_oids.size());
    REQUIRE(broken_oids.empty());
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

    {
        const auto oids = test.pg_cast_oids(source_oid, target_ok_oid);
        REQUIRE(oids.size() == 1);
        REQUIRE(oids.front() != catalog::INVALID_OID);
    }

    components::compute::function_registry_t::reset_default();
}

// ALTER TABLE ADD COLUMN is the one of the three reachable from plain SQL: success, with no identity.
TEST_CASE(
    "services::dispatcher::oid_alloc_operator_refusal::alter_add_column_refuses_when_the_round_delivers_nothing") {
    components::compute::function_registry_t::reset_default();
    auto mr = std::make_unique<core::pmr::otterbrix_resource>();
    oid_round_fixture test(mr.get(), oid_alloc_dir("alter_add_column"));

    // CREATE DATABASE answers an empty cursor, not success, so it's checked for the absence of an error.
    REQUIRE_FALSE(test.run_sql("CREATE DATABASE oidround;")->is_error());
    REQUIRE(test.run_sql("CREATE TABLE oidround.items(id int, val int);")->is_success());

    // Installed after setup — ALTER TABLE consumes no planner OIDs, so every round from here is the one under test.
    oid_alloc_fault_scope_t fault;

    REQUIRE(test.run_sql("ALTER TABLE oidround.items ADD COLUMN extra_ok bigint;")->is_success());
    REQUIRE(fault.rounds_seen == 1);
    REQUIRE(fault.rounds_failed == 0);
    {
        auto t = test.table("oidround", "items");
        REQUIRE(t.found);
        REQUIRE(has_column(t, "extra_ok"));
        REQUIRE(column_attoid(t, "extra_ok") != catalog::INVALID_OID);
    }

    fault.arm = true;
    auto refused = test.run_sql("ALTER TABLE oidround.items ADD COLUMN extra_broken bigint;");
    fault.arm = false;

    // The catalog first: unchecked, `extra_broken` carries attoid=0, the identity storage and DROP COLUMN key on.
    {
        auto t = test.table("oidround", "items");
        REQUIRE(t.found);
        INFO("columns after the refused ALTER: " << t.columns.size());
        REQUIRE_FALSE(has_column(t, "extra_broken"));
        for (const auto& c : t.columns) {
            INFO("column " << c.attname << " attoid " << c.attoid);
            REQUIRE(c.attoid != catalog::INVALID_OID);
        }
        REQUIRE(has_column(t, "extra_ok"));
        REQUIRE(column_attoid(t, "extra_ok") != catalog::INVALID_OID);
    }

    INFO("an ALTER TABLE ADD COLUMN whose OID round delivered nothing must FAIL, not add a "
         "column stamped with an identity nothing allocated");
    REQUIRE(refused->is_error());
    REQUIRE(refused->get_error().type == core::error_code_t::io_error);
    REQUIRE(mentions(refused->get_error(), "alter_column_add"));

    REQUIRE(fault.rounds_seen == 2);
    REQUIRE(fault.rounds_failed == 1);

    components::compute::function_registry_t::reset_default();
}
