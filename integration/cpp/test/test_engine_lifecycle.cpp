#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <integration/cpp/otterbrix.hpp>

#include <array>
#include <mutex>
#include <sstream>
#include <thread>

// Engine lifecycle invariants:
//
// 1. With multiple owners of an intrusive_ptr<otterbrix_t>, no operation on
//    the execute path may lose a reference — every owner must keep the engine
//    alive on its own. The refcount cases pin use_count() after each operation.
//
// 2. The buffer pool eviction queue must survive concurrent unpin pressure:
//    add_to_eviction_queue from client/scan threads racing against
//    try_dequeue_with_lock/purge on the disk manager threads.

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

    // Embedder-style wrapper: holds the engine by value as an extra owner and
    // funnels every operation through the dispatcher with a fresh session per
    // call.
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
        // Keep the create node: execute_plan stamps table_oid() onto it.
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
    // Same sequence, but driven from a non-actor client thread. Catch2 REQUIRE
    // is unsafe off the main thread, so results are snapshotted and checked
    // after join.
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
    // A wrapper owning a by-value copy of the engine (third owner while alive)
    // must keep it alive while a non-actor client thread issues SQL through it,
    // including per-table LIMIT 0 schema probes.
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
                // Schema probe: LIMIT 0 per table.
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
    // Functional smoke under a plain build; under TSAN it drives concurrent
    // unpin -> eviction_queue_t::add_to_eviction_queue from client/scan threads
    // against try_dequeue_with_lock/purge on the disk manager threads. disk.on
    // must stay true so appends/scans run through standard_buffer_manager_t.
    auto config = test_create_config("/tmp/test_engine_lifecycle/eviction");
    test_clear_directory(config);
    // Aggressive auto-checkpointing keeps checkpoint_all running on the disk
    // threads while scans pin/unpin checkpointed (persistent) blocks —
    // exercising the unpin-vs-purge interleaving.
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

    // Duplicate session ids would be fatal here: begin_transaction is
    // idempotent per session, so two operations sharing an id share one
    // transaction and the first commit erases it under the other. Serialize
    // only session CONSTRUCTION — dispatch and engine-side execution stay
    // fully concurrent, which is the pressure this case exists to apply.
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
            // Spell out the error code: some failures (e.g. table_not_exists)
            // arrive with an empty what.
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

    INFO("preload: several row groups per collection, checkpointed") {
        // Multiple row groups (row group = 1024 rows) per table make each later
        // full scan a long burst of segment pin/unpin calls.
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

    INFO("scan storm: 8 client threads, both disk agents busy") {
        std::array<std::string, num_threads> failures{};

        auto work = [&](size_t id) {
            const size_t collection = id % num_collections;
            const std::string table = eviction_database_name + ".eviction_col_" + std::to_string(collection);
            for (int iter = 0; iter < num_iterations; ++iter) {
                if (id < num_collections && iter % 10 == 0) {
                    // One writer per collection keeps WAL auto-checkpoints
                    // running on the disk threads during the storm.
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
