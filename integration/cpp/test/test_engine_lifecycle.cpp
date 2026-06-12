#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <integration/cpp/otterbrix.hpp>

#include <array>
#include <sstream>
#include <thread>

// Reproduction tests for two downstream (otterstax) findings:
//
// 1. TSAN showed intrusive_ptr<otterbrix_t> use_count() hitting 0 while TWO
//    owners were still alive -> the whole engine was deleted prematurely (UAF).
//    The factory (make_otterbrix) and the downstream wrapper hold one reference
//    each and were verified correct, so the suspicion is an operation on the
//    execute path losing one reference. These cases pin use_count() == 2 after
//    every operation the downstream wrapper performs.
//
// 2. A data race at buffer_pool.cpp:33: eviction_queue_t::add_to_eviction_queue
//    pushes into std::queue without taking purge_lock_, racing against
//    try_dequeue_with_lock/purge and against concurrent unpin calls from other
//    threads. The concurrent case drives storage_append_inner +
//    storage_scan_batched + unpin paths from 4 client threads with disk on.

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

    // Mirrors the downstream wrapper (OtterbrixDataManager): holds the engine
    // by value as a second/third owner and funnels every operation through
    // dispatcher()->execute_sql / execute_plan with a fresh session per call.
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
                components::sql::transform::maybe_wrap_with_catalog_resolve_namespace(resource, database, create);
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
    auto config = test_create_config("/tmp/test_engine_lifecycle/refcount");
    test_clear_directory(config);
    components::compute::function_registry_t::reset_default();

    auto inst = otterbrix::make_otterbrix(config);
    REQUIRE(inst->use_count() == 1u);
    otterbrix::otterbrix_ptr copy = inst;
    REQUIRE(inst->use_count() == 2u);

    auto* dispatcher = inst->dispatcher();
    auto* resource = dispatcher->resource();

    INFO("create database via execute_sql") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "CREATE DATABASE " + lifecycle_database_name + ";");
        REQUIRE(cur->is_success());
        REQUIRE(inst->use_count() == 2u);
    }

    INFO("create collection via execute_plan (catalog wrap idiom)") {
        auto session = otterbrix::session_id_t();
        auto cur = test_create_collection(dispatcher,
                                          session,
                                          lifecycle_database_name,
                                          lifecycle_collection_one,
                                          lifecycle_columns(resource));
        REQUIRE(cur->is_success());
        REQUIRE(inst->use_count() == 2u);
    }

    INFO("create collection via raw node, reading the planner-stamped oid") {
        // Same idiom the downstream wrapper uses: keep the create node and read
        // table_oid() off it after execute_plan stamped it.
        auto create = components::logical_plan::make_node_create_collection(resource,
                                                                            core::relname_t{lifecycle_collection_two},
                                                                            lifecycle_columns(resource),
                                                                            {});
        components::logical_plan::node_ptr node =
            components::sql::transform::maybe_wrap_with_catalog_resolve_namespace(resource,
                                                                                  lifecycle_database_name,
                                                                                  create);
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

    INFO("insert via execute_sql") {
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

    INFO("select via execute_sql") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT * FROM " + lifecycle_database_name + "." +
                                               lifecycle_collection_one + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        REQUIRE(inst->use_count() == 2u);
    }

    INFO("dropping one owner leaves one reference") {
        copy.reset();
        REQUIRE(inst->use_count() == 1u);
    }
}

TEST_CASE("integration::cpp::test_engine_lifecycle::two_owner_refcount_client_thread", "[engine-lifecycle]") {
    // Downstream calls into the engine from non-actor client threads; replicate
    // the same operation sequence from a second std::thread. REQUIRE behaves
    // badly off the main thread, so snapshots are collected and checked after
    // join (same pattern as test_connections).
    auto config = test_create_config("/tmp/test_engine_lifecycle/refcount_thread");
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
                components::sql::transform::maybe_wrap_with_catalog_resolve_namespace(resource,
                                                                                      lifecycle_database_name,
                                                                                      create);
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
    // Closest replica of the downstream UAF scenario: a wrapper object owns a
    // by-value copy of the engine (third owner while alive), all SQL goes
    // through it from a non-actor client thread, including the LIMIT 0 schema
    // probes downstream get_schema issues per table.
    auto config = test_create_config("/tmp/test_engine_lifecycle/refcount_wrapper");
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
                // get_schema idiom: LIMIT 0 probe per table.
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
    // Functional smoke under a plain build; under TSAN this drives concurrent
    // unpin -> eviction_queue_t::add_to_eviction_queue (buffer_pool.cpp:33,
    // unsynchronized std::queue push) from client/scan threads vs the disk
    // manager threads. disk.on stays true so appends/scans run through
    // manager_disk's standard_buffer_manager_t.
    auto config = test_create_config("/tmp/test_engine_lifecycle/eviction");
    test_clear_directory(config);
    // Aggressive auto-checkpointing keeps checkpoint_all running on the disk
    // threads while the other agent's scans pin/unpin checkpointed (persistent)
    // blocks — the exact unpin-vs-purge interleaving of the downstream race.
    config.wal.auto_checkpoint_threshold_bytes = 1024;
    // pool_idx_for_oid reserves agent 0 for catalog oids and maps user tables
    // to 1 + (oid % (agent - 1)); with the default agent = 2 every user table
    // lands on one agent and all unpins serialize. 3 agents split the user
    // tables by oid parity across two agents -> concurrent unpin.
    config.disk.agent = 3;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr size_t num_collections = 4;
    constexpr size_t num_threads = 8;
    constexpr int num_iterations = 25;
    constexpr int preload_batches = 20;
    constexpr int batch_size = 100;
    static const database_name_t eviction_database_name = "evictiondb";

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE DATABASE " + eviction_database_name + ";");
            REQUIRE(cur->is_success());
        }
        for (size_t id = 0; id < num_collections; ++id) {
            // Wide tables: every extra column adds a segment per row group, so a
            // single scan produces a burst of back-to-back pin/unpin (and thus
            // eviction-queue push) calls on the owning disk agent.
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE " + eviction_database_name + ".eviction_col_" +
                                                   std::to_string(id) +
                                                   " (name string, count bigint, c0 bigint, c1 bigint, c2 bigint,"
                                                   " c3 bigint, c4 bigint, c5 bigint);");
            REQUIRE(cur->is_success());
        }
    }

    auto insert_batch = [&](size_t collection, int iter) {
        std::stringstream query;
        query << "INSERT INTO " << eviction_database_name << ".eviction_col_" << collection
              << " (name, count, c0, c1, c2, c3, c4, c5) VALUES ";
        for (int row = 0; row < batch_size; ++row) {
            int num = iter * batch_size + row;
            query << "('name_" << num << "', " << num << ", " << num << ", " << num << ", " << num << ", " << num
                  << ", " << num << ", " << num << ")" << (row == batch_size - 1 ? ";" : ", ");
        }
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, query.str());
        return cur->is_success() && cur->size() == static_cast<size_t>(batch_size);
    };

    INFO("preload: several row groups per collection, checkpointed") {
        // Multiple row groups (row group = 1024 rows) per table make each later
        // full scan a long burst of segment pin/unpin calls.
        std::array<bool, num_collections> results{};
        std::vector<std::thread> threads;
        threads.reserve(num_collections);
        for (size_t id = 0; id < num_collections; ++id) {
            threads.emplace_back(
                [&](size_t collection) {
                    bool thread_ok = true;
                    for (int iter = 0; iter < preload_batches && thread_ok; ++iter) {
                        thread_ok = insert_batch(collection, iter);
                    }
                    results[collection] = thread_ok;
                },
                id);
        }
        for (size_t id = 0; id < num_collections; ++id) {
            threads[id].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }
    }

    INFO("scan storm: 8 client threads, both disk agents busy") {
        std::array<bool, num_threads> results{};

        auto work = [&](size_t id) {
            const size_t collection = id % num_collections;
            const std::string table = eviction_database_name + ".eviction_col_" + std::to_string(collection);
            bool thread_ok = true;
            for (int iter = 0; iter < num_iterations && thread_ok; ++iter) {
                if (id < num_collections && iter % 10 == 0) {
                    // One writer per collection keeps WAL auto-checkpoints
                    // running on the disk threads during the storm.
                    thread_ok = insert_batch(collection, preload_batches + iter / 10);
                    if (!thread_ok) {
                        break;
                    }
                }
                auto session = otterbrix::session_id_t();
                auto cur = dispatcher->execute_sql(session, "SELECT * FROM " + table + ";");
                thread_ok = cur->is_success() && cur->size() >= static_cast<size_t>(preload_batches * batch_size);
            }
            results[id] = thread_ok;
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (size_t id = 0; id < num_threads; ++id) {
            threads.emplace_back(work, id);
        }
        for (size_t id = 0; id < num_threads; ++id) {
            threads[id].join();
        }
        for (bool res : results) {
            REQUIRE(res);
        }
    }

    INFO("verify final row counts") {
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
