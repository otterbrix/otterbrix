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

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::catalog;
using namespace components::cursor;
using namespace components::types;

// V4 dispatcher integration test. Catalog assertions go through manager_disk_t's
// resolve_namespace / resolve_table directly — catalog_snapshot_t is gone.

struct test_dispatcher : actor_zeta::actor::actor_mixin<test_dispatcher> {
    test_dispatcher(std::pmr::memory_resource* resource, const std::string& disk_path)
        : actor_zeta::actor::actor_mixin<test_dispatcher>()
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
        return std::move(fut).take_ready();
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
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
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
    auto mr = std::make_unique<std::pmr::synchronized_pool_resource>();
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
