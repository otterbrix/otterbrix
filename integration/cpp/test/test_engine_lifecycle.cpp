#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <integration/cpp/otterbrix.hpp>

#include <algorithm>
#include <array>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

static const database_name_t lifecycle_database_name = "lifecycledb";
static const collection_name_t lifecycle_collection_one = "lifecycle_col_one";
static const collection_name_t lifecycle_collection_two = "lifecycle_col_two";

namespace {

    std::vector<components::table::column_definition_t> lifecycle_columns(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
        types.emplace_back(components::types::logical_type::BIGINT, "count");
        std::vector<components::table::column_definition_t> columns;
        columns.reserve(types.size());
        for (const auto& type : types) {
            columns.emplace_back(type.alias(), type);
        }
        return columns;
    }

    class lifecycle_wrapper_t final {
    public:
        explicit lifecycle_wrapper_t(otterbrix::otterbrix_ptr engine)
            : engine_(std::move(engine)) {}

        components::cursor::cursor_t_ptr execute_sql(const std::string& query) {
            return engine_->dispatcher()->execute_sql(otterbrix::session_id_t(), query);
        }

        components::cursor::cursor_t_ptr create_collection(const database_name_t& database,
                                                           const collection_name_t& collection,
                                                           components::catalog::oid_t& out_oid) {
            out_oid = components::catalog::INVALID_OID;
            auto* resource = engine_->dispatcher()->resource();
            auto create = components::logical_plan::make_node_create_collection(resource,
                                                                                core::relname_t{collection},
                                                                                lifecycle_columns(resource),
                                                                                {});
            components::logical_plan::node_ptr node =
                components::sql::transform::name_catalog_target(database, {}, create);
            auto cursor = engine_->dispatcher()->execute_plan(
                otterbrix::session_id_t(),
                components::logical_plan::execution_plan_t{resource,
                                                           node,
                                                           components::logical_plan::make_parameter_node(resource)});
            if (cursor && !cursor->is_error()) {
                out_oid = create->table_oid();
            }
            return cursor;
        }

        unsigned int engine_use_count() const { return engine_->use_count(); }

    private:
        otterbrix::otterbrix_ptr engine_;
    };

} // namespace

TEST_CASE("integration::cpp::test_engine_lifecycle::two_owner_refcount", "[engine-lifecycle]") {
    auto config = test_create_config(integration_fixture_path("test_engine_lifecycle/refcount"));
    test_clear_directory(config);
    components::compute::function_registry_t::reset_default();

    auto inst = otterbrix::make_otterbrix(config);
    REQUIRE(inst->use_count() == 1u);
    otterbrix::otterbrix_ptr copy = inst;
    REQUIRE(inst->use_count() == 2u);

    auto* dispatcher = inst->dispatcher();
    auto* resource = dispatcher->resource();

    INFO("create database via execute_sql");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE DATABASE " + lifecycle_database_name + ";");
        REQUIRE(cur->is_success());
        REQUIRE(inst->use_count() == 2u);
    }

    INFO("create collection via execute_plan (catalog wrap idiom)");
    {
        auto session = otterbrix::session_id_t();
        auto cur = test_create_collection(dispatcher,
                                          session,
                                          lifecycle_database_name,
                                          lifecycle_collection_one,
                                          lifecycle_columns(resource));
        REQUIRE(cur->is_success());
        REQUIRE(inst->use_count() == 2u);
    }

    INFO("create collection via raw node, reading the planner-stamped oid");
    {
        // Keep the create node: execute_plan stamps table_oid() onto it.
        auto create = components::logical_plan::make_node_create_collection(resource,
                                                                            core::relname_t{lifecycle_collection_two},
                                                                            lifecycle_columns(resource),
                                                                            {});
        components::logical_plan::node_ptr node =
            components::sql::transform::name_catalog_target(lifecycle_database_name, {}, create);
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(
            session,
            components::logical_plan::execution_plan_t{resource,
                                                       node,
                                                       components::logical_plan::make_parameter_node(resource)});
        REQUIRE(cur->is_success());
        REQUIRE(create->table_oid() != components::catalog::INVALID_OID);
        REQUIRE(inst->use_count() == 2u);
    }

    INFO("insert via execute_sql");
    {
        std::stringstream query;
        query << "INSERT INTO " << lifecycle_database_name << "." << lifecycle_collection_one
              << " (name, count) VALUES ";
        for (int num = 0; num < 10; ++num) {
            query << "('name_" << num << "', " << num << ")" << (num == 9 ? ";" : ", ");
        }
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        REQUIRE(inst->use_count() == 2u);
    }

    INFO("select via execute_sql");
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "SELECT * FROM " + lifecycle_database_name + "." + lifecycle_collection_one + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        REQUIRE(inst->use_count() == 2u);
    }

    INFO("dropping one owner leaves one reference");
    {
        copy.reset();
        REQUIRE(inst->use_count() == 1u);
    }
}

TEST_CASE("integration::cpp::test_engine_lifecycle::two_owner_refcount_client_thread", "[engine-lifecycle]") {
    // Catch2 REQUIRE is unsafe off the main thread, so results are snapshotted and checked after join.
    auto config = test_create_config(integration_fixture_path("test_engine_lifecycle/refcount_thread"));
    test_clear_directory(config);
    components::compute::function_registry_t::reset_default();

    auto inst = otterbrix::make_otterbrix(config);
    otterbrix::otterbrix_ptr copy = inst;
    REQUIRE(inst->use_count() == 2u);

    constexpr size_t op_count = 5;
    std::array<unsigned int, op_count> counts{};
    std::array<bool, op_count> ok{};

    std::thread client([&]() {
        auto* dispatcher = inst->dispatcher();
        auto* resource = dispatcher->resource();
        size_t op = 0;

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE DATABASE " + lifecycle_database_name + ";");
            ok[op] = cur->is_success();
            counts[op] = inst->use_count();
            ++op;
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = test_create_collection(dispatcher,
                                              session,
                                              lifecycle_database_name,
                                              lifecycle_collection_one,
                                              lifecycle_columns(resource));
            ok[op] = cur->is_success();
            counts[op] = inst->use_count();
            ++op;
        }
        {
            auto create =
                components::logical_plan::make_node_create_collection(resource,
                                                                      core::relname_t{lifecycle_collection_two},
                                                                      lifecycle_columns(resource),
                                                                      {});
            components::logical_plan::node_ptr node =
                components::sql::transform::name_catalog_target(lifecycle_database_name, {}, create);
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{resource,
                                                           node,
                                                           components::logical_plan::make_parameter_node(resource)});
            ok[op] = cur->is_success() && create->table_oid() != components::catalog::INVALID_OID;
            counts[op] = inst->use_count();
            ++op;
        }
        {
            std::stringstream query;
            query << "INSERT INTO " << lifecycle_database_name << "." << lifecycle_collection_two
                  << " (name, count) VALUES ";
            for (int num = 0; num < 10; ++num) {
                query << "('name_" << num << "', " << num << ")" << (num == 9 ? ";" : ", ");
            }
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, query.str());
            ok[op] = cur->is_success() && cur->size() == 10;
            counts[op] = inst->use_count();
            ++op;
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM " + lifecycle_database_name + "." +
                                                   lifecycle_collection_two + ";");
            ok[op] = cur->is_success() && cur->size() == 10;
            counts[op] = inst->use_count();
            ++op;
        }
    });
    client.join();

    for (size_t op = 0; op < op_count; ++op) {
        REQUIRE(ok[op]);
        REQUIRE(counts[op] == 2u);
    }
    REQUIRE(inst->use_count() == 2u);
}

TEST_CASE("integration::cpp::test_engine_lifecycle::two_owner_refcount_wrapper_style", "[engine-lifecycle]") {
    auto config = test_create_config(integration_fixture_path("test_engine_lifecycle/refcount_wrapper"));
    test_clear_directory(config);
    components::compute::function_registry_t::reset_default();

    auto inst = otterbrix::make_otterbrix(config);
    otterbrix::otterbrix_ptr copy = inst;
    REQUIRE(inst->use_count() == 2u);

    {
        lifecycle_wrapper_t wrapper(inst);
        REQUIRE(inst->use_count() == 3u);

        constexpr size_t op_count = 7;
        std::array<unsigned int, op_count> counts{};
        std::array<bool, op_count> ok{};

        std::thread client([&]() {
            size_t op = 0;
            {
                auto cur = wrapper.execute_sql("CREATE DATABASE " + lifecycle_database_name + ";");
                ok[op] = cur->is_success();
                counts[op] = wrapper.engine_use_count();
                ++op;
            }
            {
                components::catalog::oid_t oid = components::catalog::INVALID_OID;
                auto cur = wrapper.create_collection(lifecycle_database_name, lifecycle_collection_one, oid);
                ok[op] = cur->is_success() && oid != components::catalog::INVALID_OID;
                counts[op] = wrapper.engine_use_count();
                ++op;
            }
            {
                components::catalog::oid_t oid = components::catalog::INVALID_OID;
                auto cur = wrapper.create_collection(lifecycle_database_name, lifecycle_collection_two, oid);
                ok[op] = cur->is_success() && oid != components::catalog::INVALID_OID;
                counts[op] = wrapper.engine_use_count();
                ++op;
            }
            {
                auto cur = wrapper.execute_sql("SELECT * FROM " + lifecycle_database_name + "." +
                                               lifecycle_collection_one + " LIMIT 0;");
                ok[op] = cur->is_success();
                counts[op] = wrapper.engine_use_count();
                ++op;
            }
            {
                auto cur = wrapper.execute_sql("SELECT * FROM " + lifecycle_database_name + "." +
                                               lifecycle_collection_two + " LIMIT 0;");
                ok[op] = cur->is_success();
                counts[op] = wrapper.engine_use_count();
                ++op;
            }
            {
                std::stringstream query;
                query << "INSERT INTO " << lifecycle_database_name << "." << lifecycle_collection_one
                      << " (name, count) VALUES ";
                for (int num = 0; num < 10; ++num) {
                    query << "('name_" << num << "', " << num << ")" << (num == 9 ? ";" : ", ");
                }
                auto cur = wrapper.execute_sql(query.str());
                ok[op] = cur->is_success() && cur->size() == 10;
                counts[op] = wrapper.engine_use_count();
                ++op;
            }
            {
                auto cur = wrapper.execute_sql("SELECT * FROM " + lifecycle_database_name + "." +
                                               lifecycle_collection_one + ";");
                ok[op] = cur->is_success() && cur->size() == 10;
                counts[op] = wrapper.engine_use_count();
                ++op;
            }
        });
        client.join();

        for (size_t op = 0; op < op_count; ++op) {
            REQUIRE(ok[op]);
            REQUIRE(counts[op] == 3u);
        }
        REQUIRE(inst->use_count() == 3u);
    }

    REQUIRE(inst->use_count() == 2u);
    copy.reset();
    REQUIRE(inst->use_count() == 1u);
}

TEST_CASE("integration::cpp::test_engine_lifecycle::concurrent_insert_scan_eviction", "[engine-lifecycle]") {
    // Under TSAN, drives eviction_queue_t::add_to_eviction_queue against try_dequeue_with_lock/purge across threads.
    auto config = test_create_config(integration_fixture_path("test_engine_lifecycle/eviction"));
    test_clear_directory(config);
    config.wal.auto_checkpoint_threshold_bytes = 1024;
    // agent=3 (not the default 2) splits tables by oid parity across agents, forcing concurrent unpin.
    config.disk.agent = 3;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr size_t num_collections = 4;
    constexpr size_t num_threads = 8;
    constexpr int num_iterations = 25;
    constexpr int preload_batches = 20;
    constexpr int batch_size = 100;
    static const database_name_t eviction_database_name = "evictiondb";

    // Serializes only session construction: duplicate ids would collide because begin_transaction is
    // idempotent per session.
    std::mutex session_mutex;
    auto make_session = [&session_mutex]() {
        std::lock_guard<std::mutex> guard(session_mutex);
        return otterbrix::session_id_t();
    };

    auto describe_failure = [](const components::cursor::cursor_t_ptr& cursor, size_t expected_size) -> std::string {
        if (!cursor) {
            return "null cursor";
        }
        if (cursor->is_error()) {
            // Some failures (e.g. table_not_exists) arrive with an empty what, so the code is spelled out too.
            const auto error = cursor->get_error();
            return "error cursor: code " + std::to_string(static_cast<int>(error.type)) + ", what: '" +
                   std::string(error.what.begin(), error.what.end()) + "'";
        }
        if (cursor->size() < expected_size) {
            return "short result: got " + std::to_string(cursor->size()) + ", expected at least " +
                   std::to_string(expected_size);
        }
        return {};
    };

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE DATABASE " + eviction_database_name + ";");
            REQUIRE(cur->is_success());
        }
        for (size_t id = 0; id < num_collections; ++id) {
            // Extra columns add a segment per row group, so a scan bursts pin/unpin calls per agent.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE " + eviction_database_name + ".eviction_col_" +
                                                   std::to_string(id) +
                                                   " (name string, count bigint, c0 bigint, c1 bigint, c2 bigint,"
                                                   " c3 bigint, c4 bigint, c5 bigint);");
            REQUIRE(cur->is_success());
        }
    }

    // Returns an empty string on success, the failure description otherwise.
    auto insert_batch = [&](size_t collection, int iter) -> std::string {
        std::stringstream query;
        query << "INSERT INTO " << eviction_database_name << ".eviction_col_" << collection
              << " (name, count, c0, c1, c2, c3, c4, c5) VALUES ";
        for (int row = 0; row < batch_size; ++row) {
            int num = iter * batch_size + row;
            query << "('name_" << num << "', " << num << ", " << num << ", " << num << ", " << num << ", " << num
                  << ", " << num << ", " << num << ")" << (row == batch_size - 1 ? ";" : ", ");
        }
        auto session = make_session();
        auto cur = dispatcher->execute_sql(session, query.str());
        auto failure = describe_failure(cur, static_cast<size_t>(batch_size));
        if (failure.empty() && cur->size() != static_cast<size_t>(batch_size)) {
            failure = "insert size mismatch: got " + std::to_string(cur->size());
        }
        return failure;
    };

    INFO("preload: several row groups per collection, checkpointed");
    {
        // Row group = 1024 rows, so several groups per table make each scan a long pin/unpin burst.
        std::array<std::string, num_collections> failures{};
        std::vector<std::thread> threads;
        threads.reserve(num_collections);
        for (size_t id = 0; id < num_collections; ++id) {
            threads.emplace_back(
                [&](size_t collection) {
                    for (int iter = 0; iter < preload_batches; ++iter) {
                        auto failure = insert_batch(collection, iter);
                        if (!failure.empty()) {
                            failures[collection] = "batch " + std::to_string(iter) + ": " + failure;
                            break;
                        }
                    }
                },
                id);
        }
        for (size_t id = 0; id < num_collections; ++id) {
            threads[id].join();
        }
        for (size_t id = 0; id < num_collections; ++id) {
            INFO("collection " << id << ": " << failures[id]);
            REQUIRE(failures[id].empty());
        }
    }

    INFO("scan storm: 8 client threads, both disk agents busy");
    {
        std::array<std::string, num_threads> failures{};

        auto work = [&](size_t id) {
            const size_t collection = id % num_collections;
            const std::string table = eviction_database_name + ".eviction_col_" + std::to_string(collection);
            for (int iter = 0; iter < num_iterations; ++iter) {
                if (id < num_collections && iter % 10 == 0) {
                    auto failure = insert_batch(collection, preload_batches + iter / 10);
                    if (!failure.empty()) {
                        failures[id] = "iter " + std::to_string(iter) + " insert: " + failure;
                        return;
                    }
                }
                auto session = make_session();
                auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + table + ";");
                auto failure = describe_failure(cur, static_cast<size_t>(preload_batches * batch_size));
                if (!failure.empty()) {
                    failures[id] = "iter " + std::to_string(iter) + " scan: " + failure;
                    return;
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (size_t id = 0; id < num_threads; ++id) {
            threads.emplace_back(work, id);
        }
        for (size_t id = 0; id < num_threads; ++id) {
            threads[id].join();
        }
        for (size_t id = 0; id < num_threads; ++id) {
            INFO("thread " << id << ": " << failures[id]);
            REQUIRE(failures[id].empty());
        }
    }

    INFO("verify final row counts");
    {
        constexpr size_t expected = static_cast<size_t>(preload_batches * batch_size) +
                                    static_cast<size_t>((num_iterations + 9) / 10) * batch_size;
        for (size_t id = 0; id < num_collections; ++id) {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM " + eviction_database_name + ".eviction_col_" +
                                                   std::to_string(id) + ";");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == expected);
        }
    }
}

// Linux ASAN+LSan (CI) must show zero leaked freelist nodes after teardown; on macOS LSan
// doesn't run, so this also smoke-tests the scheduler_disk_ teardown ordering under ASan.
TEST_CASE("integration::cpp::test_engine_lifecycle::construct_destroy_clean_teardown",
          "[engine-lifecycle][leak-repro]") {
    auto config = test_create_config(integration_fixture_path("test_engine_lifecycle/teardown_leak"));
    test_clear_directory(config);
    components::compute::function_registry_t::reset_default();

    {
        auto inst = otterbrix::make_otterbrix(config);
        auto* dispatcher = inst->dispatcher();

        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE leakreprodb;")->is_success());
        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE TABLE leakreprodb.t (g bigint, v bigint);")
                    ->is_success());
        REQUIRE(dispatcher
                    ->execute_sql(otterbrix::session_id_t(),
                                  "INSERT INTO leakreprodb.t (g, v) VALUES "
                                  "(1, 10), (1, 20), (2, 30), (2, 40), (2, 50);")
                    ->is_success());
        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "SELECT g, v FROM leakreprodb.t;")->is_success());
    }

    SUCCEED("engine constructed, populated, and destroyed without an ASan/LSan error");
}

TEST_CASE("integration::cpp::test_engine_lifecycle::repeated_construct_destroy_no_leak",
          "[engine-lifecycle][leak-repro]") {
    constexpr int kCycles = 12;
    for (int i = 0; i < kCycles; ++i) {
        auto config = test_create_config(integration_fixture_path("test_engine_lifecycle/stress_" + std::to_string(i)));
        test_clear_directory(config);
        components::compute::function_registry_t::reset_default();

        auto inst = otterbrix::make_otterbrix(config);
        auto* dispatcher = inst->dispatcher();
        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE stressdb;")->is_success());
        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE TABLE stressdb.t (g bigint, v bigint);")
                    ->is_success());
        REQUIRE(
            dispatcher->execute_sql(otterbrix::session_id_t(), "INSERT INTO stressdb.t (g, v) VALUES (1, 10), (2, 20);")
                ->is_success());
    }
    SUCCEED("engine survived repeated construct/destroy cycles without an ASan/LSan error");
}

namespace {

    struct destruction_order_recorder_t {
        std::vector<std::string>* log;
        std::string name;
        ~destruction_order_recorder_t() { log->push_back(name); }
    };

    // Mirrors base_otterbrix_t's member order; keep in lockstep with integration/cpp/base_spaces.hpp.
    struct base_spaces_layout_model_t {
        explicit base_spaces_layout_model_t(std::vector<std::string>* log)
            : scheduler_{log, "scheduler"}
            , scheduler_dispatcher_{log, "scheduler_dispatcher"}
            , scheduler_disk_{log, "scheduler_disk"}
            , manager_dispatcher_{log, "manager_dispatcher"}
            , manager_disk_{log, "manager_disk"}
            , manager_wal_{log, "manager_wal"}
            , manager_index_{log, "manager_index"}
            , wrapper_dispatcher_{log, "wrapper_dispatcher"} {}

        destruction_order_recorder_t scheduler_;
        destruction_order_recorder_t scheduler_dispatcher_;
        destruction_order_recorder_t scheduler_disk_;
        destruction_order_recorder_t manager_dispatcher_;
        destruction_order_recorder_t manager_disk_;
        destruction_order_recorder_t manager_wal_;
        destruction_order_recorder_t manager_index_;
        destruction_order_recorder_t wrapper_dispatcher_;
    };

} // namespace

// Proves reverse-declaration destruction alone keeps schedulers outliving managers, no ordered reset needed.
TEST_CASE("integration::cpp::test_engine_lifecycle::teardown_order_schedulers_outlive_managers",
          "[engine-lifecycle][leak-repro]") {
    std::vector<std::string> order;
    { base_spaces_layout_model_t model(&order); }

    REQUIRE(order.size() == 8);
    const auto pos = [&](const std::string& n) { return std::find(order.begin(), order.end(), n) - order.begin(); };

    const std::array<const char*, 3> schedulers{"scheduler", "scheduler_dispatcher", "scheduler_disk"};
    const std::array<const char*, 5> managers{"manager_dispatcher",
                                              "manager_disk",
                                              "manager_wal",
                                              "manager_index",
                                              "wrapper_dispatcher"};

    // Every scheduler is destroyed after every manager (schedulers outlive actors).
    for (const char* s : schedulers) {
        for (const char* m : managers) {
            INFO(s << " must destruct after " << m);
            CHECK(pos(s) > pos(m));
        }
    }

    // The dispatcher is destroyed last among the managers (cyclic-graph sink).
    for (const char* m : {"manager_disk", "manager_wal", "manager_index", "wrapper_dispatcher"}) {
        INFO("manager_dispatcher must destruct after " << m);
        CHECK(pos("manager_dispatcher") > pos(m));
    }

    // scheduler_disk_ specifically outlives manager_disk_ (the raw-pointer holder).
    CHECK(pos("scheduler_disk") > pos("manager_disk"));
}
