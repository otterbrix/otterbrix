#include <catch2/catch.hpp>

#include "../dispatcher.hpp"

#include <components/session/session.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/system_command.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/memory_storage.hpp>
#include <services/wal/manager_wal_replicate.hpp>

using namespace services;
using namespace services::wal;
using namespace services::disk;
using namespace services::dispatcher;
using namespace components::catalog;
using namespace components::cursor;
using namespace components::types;

struct test_dispatcher : actor_zeta::cooperative_supervisor<test_dispatcher> {
    test_dispatcher(std::pmr::memory_resource* resource)
        : actor_zeta::cooperative_supervisor<test_dispatcher>(resource)
        , log_(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
        , manager_dispatcher_(actor_zeta::spawn_supervisor<manager_dispatcher_t>(resource, scheduler_, log_))
        , manager_disk_(actor_zeta::spawn_supervisor<manager_disk_empty_t>(resource, scheduler_))
        , manager_wal_(actor_zeta::spawn_supervisor<manager_wal_replicate_empty_t>(resource, scheduler_, log_))
        , memory_storage_(actor_zeta::spawn_supervisor<memory_storage_t>(resource, scheduler_, log_))
        , transformer_(resource)
        , execute_plan_finish_(actor_zeta::make_behavior(resource,
                                                         handler_id(dispatcher::route::execute_plan),
                                                         this,
                                                         &test_dispatcher::execute_plan_finish))

    {
        actor_zeta::send(manager_dispatcher_->address(),
                         actor_zeta::address_t::empty_address(),
                         core::handler_id(core::route::sync),
                         std::make_tuple(memory_storage_->address(),
                                         actor_zeta::address_t(manager_wal_->address()),
                                         actor_zeta::address_t(manager_disk_->address())));

        actor_zeta::send(
            manager_wal_->address(),
            actor_zeta::address_t::empty_address(),
            core::handler_id(core::route::sync),
            std::make_tuple(actor_zeta::address_t(manager_disk_->address()), manager_dispatcher_->address()));

        actor_zeta::send(manager_disk_->address(),
                         actor_zeta::address_t::empty_address(),
                         core::handler_id(core::route::sync),
                         std::make_tuple(manager_dispatcher_->address()));

        actor_zeta::send(memory_storage_,
                         actor_zeta::address_t::empty_address(),
                         core::handler_id(core::route::sync),
                         std::make_tuple(manager_dispatcher_->address(), manager_disk_->address()));

        actor_zeta::send(manager_wal_->address(),
                         actor_zeta::address_t::empty_address(),
                         wal::handler_id(wal::route::create));

        actor_zeta::send(manager_disk_->address(),
                         actor_zeta::address_t::empty_address(),
                         disk::handler_id(disk::route::create_agent));

        manager_dispatcher_->create_dispatcher();
    }

    ~test_dispatcher() { delete scheduler_; }

    auto make_type() const noexcept -> const char* const { return "test_dispatcher"; }

    auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* { return nullptr; }

    auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override {
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    actor_zeta::behavior_t behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(dispatcher::route::execute_plan_finish):
                    execute_plan_finish_(msg);
                    break;
            }
        });
    }

    void execute_plan_finish(components::session::session_id_t& session, cursor_t_ptr result) {
        if (static_cast<bool>(assertion_)) {
            assertion_(result, const_cast<catalog&>(manager_dispatcher_->current_catalog()));
        }
    }

    void step() { scheduler_->run(); }

    void step_with_assertion(std::function<void(cursor_t_ptr, catalog&)> assertion) {
        assertion_ = std::move(assertion);
        step();
    }

    void execute_sql(const std::string& query) {
        auto params = components::logical_plan::make_parameter_node(resource());
        auto parse_result = linitial(raw_parser(query.c_str()));

        auto node =
            transformer_.transform(components::sql::transform::pg_cell_to_node_cast(parse_result), params.get());

        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_plan),
                         session_id_t{},
                         std::move(node),
                         std::move(params));
    }

private:
    log_t log_;
    core::non_thread_scheduler::scheduler_test_t* scheduler_{nullptr};
    std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t> manager_dispatcher_;
    std::unique_ptr<manager_disk_empty_t, actor_zeta::pmr::deleter_t> manager_disk_;
    std::unique_ptr<manager_wal_replicate_empty_t, actor_zeta::pmr::deleter_t> manager_wal_;
    std::unique_ptr<memory_storage_t, actor_zeta::pmr::deleter_t> memory_storage_;
    components::sql::transform::transformer transformer_;
    actor_zeta::behavior_t execute_plan_finish_;
    std::function<void(cursor_t_ptr, catalog&)> assertion_;
};

TEST_CASE("dispatcher::schemeful_operations") {
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
        // Since we have a schema, it will be treated as table, but parser only makes a document raw_data nodes
        REQUIRE_FALSE(cur->is_success()); // should succeed, however, without type tree result as follows:
                                          // schema failure: INTEGER in catalog, BIGINT from json
                                          // no catalog type assertions for now
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

TEST_CASE("dispatcher::computed_operations") {
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
