#include <catch2/catch.hpp>

#include "../dispatcher.hpp"

#include <actor-zeta/spawn.hpp>
#include <components/session/session.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>
#include <components/types/types.hpp>
#include <core/executor.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::catalog;
using namespace components::cursor;
using namespace components::types;

struct test_dispatcher : actor_zeta::actor::actor_mixin<test_dispatcher> {
    test_dispatcher(std::pmr::memory_resource* resource)
        : actor_zeta::actor::actor_mixin<test_dispatcher>()
        , resource_(resource)
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
        , manager_dispatcher_(actor_zeta::spawn<manager_dispatcher_t>(resource, scheduler_, log_))
        , disk_config_("/tmp/test_dispatcher_disk")
        , manager_disk_(actor_zeta::spawn<manager_disk_t>(resource, scheduler_, scheduler_, disk_config_, log_))
        , manager_wal_(actor_zeta::spawn<manager_wal_replicate_empty_t>(resource, scheduler_, log_))
        , transformer_(resource) {
        manager_dispatcher_->sync(std::make_tuple(manager_wal_->address(),
                                                   manager_disk_->address()));
        manager_wal_->sync(std::make_tuple(actor_zeta::address_t(manager_disk_->address()),
                                           manager_dispatcher_->address()));
        manager_disk_->sync(std::make_tuple(manager_dispatcher_->address()));

        manager_dispatcher_->set_run_fn([this]{ scheduler_->run(100); });
        manager_disk_->set_run_fn([this]{ scheduler_->run(100); });
    }

    ~test_dispatcher() {
        scheduler_->stop();
        std::filesystem::remove_all("/tmp/test_dispatcher_disk");
        delete scheduler_;
    }

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

    void step() { scheduler_->run(); }

    void step_with_assertion(std::function<void(cursor_t_ptr, catalog&)> assertion) {
        step();
        if (pending_future_ && pending_future_->valid() && pending_future_->available()) {
            auto result = std::move(*pending_future_).get();
            pending_future_.reset();
            assertion(result, const_cast<catalog&>(manager_dispatcher_->current_catalog()));
        }
    }

    void execute_sql(const std::string& query) {
        std::pmr::monotonic_buffer_resource parser_arena(resource_);
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));
        auto view = std::get<components::sql::transform::result_view>(
            transformer_.transform(components::sql::transform::pg_cell_to_node_cast(parse_result)).finalize());

        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                  &manager_dispatcher_t::execute_plan,
                                                  session_id_t{},
                                                  std::move(view.node),
                                                  std::move(view.params));
        pending_future_ = std::make_unique<actor_zeta::unique_future<cursor_t_ptr>>(std::move(future));
    }

private:
    std::pmr::memory_resource* resource_;
    log_t log_;
    core::non_thread_scheduler::scheduler_test_t* scheduler_{nullptr};
    std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t> manager_dispatcher_;
    configuration::config_disk disk_config_;
    std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager_disk_;
    std::unique_ptr<manager_wal_replicate_empty_t, actor_zeta::pmr::deleter_t> manager_wal_;
    components::sql::transform::transformer transformer_;
    std::unique_ptr<actor_zeta::unique_future<cursor_t_ptr>> pending_future_;
};

TEST_CASE("services::dispatcher::schemeful_operations") {
    auto mr = std::pmr::synchronized_pool_resource();
    test_dispatcher test(&mr);

    test.execute_sql("CREATE DATABASE test;");
    test.step();

    table_id id(&mr, {"test"}, "test");
    test.execute_sql("CREATE TABLE test.test(fld1 int, fld2 string);");
    test.step_with_assertion([&id](cursor_t_ptr cur, const catalog& catalog) {
        REQUIRE(catalog.table_exists(id));
        auto sch = catalog.get_table_schema(id);
        REQUIRE(sch.find_field("fld1")->type_data().front().type() == logical_type::INTEGER);
        REQUIRE(sch.find_field("fld2")->type_data().front().type() == logical_type::STRING_LITERAL);

        REQUIRE(cur->is_success());
    });

    test.execute_sql("INSERT INTO test.test (fld1, fld2) VALUES (1, '1'), (2, '2');");
    test.step_with_assertion([&id](cursor_t_ptr cur, const catalog& catalog) {
        REQUIRE(catalog.table_exists(id));
        REQUIRE(cur->is_success());
    });

    // todo: add typed insert assertions with type tree introduction

    SECTION("in-order") {
        test.execute_sql("DROP TABLE test.test;");
        test.step_with_assertion([&id](cursor_t_ptr cur, const catalog& catalog) {
            REQUIRE(!catalog.table_exists(id));
            REQUIRE(cur->is_success());
        });

        test.execute_sql("DROP DATABASE test;");
        test.step_with_assertion([&id](cursor_t_ptr cur, const catalog& catalog) {
            REQUIRE(!catalog.namespace_exists(id.get_namespace()));
            REQUIRE(cur->is_success());
        });
    }

    SECTION("drop_database") {
        test.execute_sql("DROP DATABASE test;");
        test.step_with_assertion([&id](cursor_t_ptr cur, const catalog& catalog) {
            REQUIRE(!catalog.namespace_exists(id.get_namespace()));
            REQUIRE(cur->is_success());
        });
    }
}

TEST_CASE("services::dispatcher::computed_operations") {
    auto mr = std::pmr::synchronized_pool_resource();
    test_dispatcher test(&mr);

    test.execute_sql("CREATE DATABASE test;");
    test.step();

    table_id id(&mr, {"test"}, "test");
    test.execute_sql("CREATE TABLE test.test();");
    test.step_with_assertion([&id](cursor_t_ptr cur, catalog& catalog) {
        REQUIRE(cur->is_success());
        REQUIRE(catalog.table_computes(id));

        auto sch = catalog.get_computing_table_schema(id);
        REQUIRE(sch.latest_types_struct().size() == 0);
    });

    std::stringstream query;
    query << "INSERT INTO test.test (_id, name, count) VALUES ";
    for (int num = 0; num < 100; ++num) {
        query << "('" << gen_id(num + 1, &mr) << "', "
              << "'Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
    }

    test.execute_sql(query.str());
    test.step_with_assertion([&id](cursor_t_ptr cur, catalog& catalog) {
        auto name = catalog.get_computing_table_schema(id).find_field_versions("name");
        auto count = catalog.get_computing_table_schema(id).find_field_versions("count");

        REQUIRE(cur->is_success());

        REQUIRE(name.size() == 1);
        REQUIRE(name.back().type() == logical_type::STRING_LITERAL);

        REQUIRE(count.size() == 1);
        REQUIRE(count.back().type() == logical_type::BIGINT);
    });

    test.execute_sql("INSERT INTO test.test (_id, name, count) VALUES ('" + gen_id(100) + "', 10, 'test');");
    test.step_with_assertion([&id](cursor_t_ptr cur, catalog& catalog) {
        auto name = catalog.get_computing_table_schema(id).find_field_versions("name");
        auto count = catalog.get_computing_table_schema(id).find_field_versions("count");

        REQUIRE(cur->is_success());

        REQUIRE(name.size() == 2);
        REQUIRE(name.back().type() == logical_type::BIGINT);

        REQUIRE(count.size() == 2);
        REQUIRE(count.back().type() == logical_type::STRING_LITERAL);
    });

    test.execute_sql("DELETE FROM test.test where count < 100;");
    test.step_with_assertion([&id](cursor_t_ptr cur, catalog& catalog) {
        auto name = catalog.get_computing_table_schema(id).find_field_versions("name");
        auto count = catalog.get_computing_table_schema(id).find_field_versions("count");

        REQUIRE(cur->is_success());

        // other versions were deleted
        REQUIRE(name.size() == 1);
        REQUIRE(name.back().type() == logical_type::BIGINT);

        REQUIRE(count.size() == 1);
        REQUIRE(count.back().type() == logical_type::STRING_LITERAL);
    });

    test.execute_sql("DELETE FROM test.test");
    test.step_with_assertion([&id](cursor_t_ptr cur, catalog& catalog) {
        REQUIRE(cur->is_success());

        auto sch = catalog.get_computing_table_schema(id);
        REQUIRE(sch.latest_types_struct().size() == 0);
    });
}
