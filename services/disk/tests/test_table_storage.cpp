#include <catch2/catch.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <services/disk/manager_disk.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>
#include <components/log/log.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/table/row_group.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <chrono>
#include <filesystem>
#include <thread>
#include <unistd.h>

using namespace services::disk;
using namespace components::table;
using namespace components::types;
using namespace components::vector;

namespace {
    struct scheduler_guard_t {
        actor_zeta::scheduler::sharing_scheduler& scheduler;

        ~scheduler_guard_t() { scheduler.stop(); }
    };

    std::string test_dir() {
        static std::string path = "/tmp/test_otterbrix_table_storage_" + std::to_string(::getpid());
        return path;
    }
    void cleanup_test_dir() { std::filesystem::remove_all(test_dir()); }

    std::vector<storage_index_t> make_column_indices(uint64_t count) {
        std::vector<storage_index_t> indices;
        indices.reserve(count);
        for (uint64_t i = 0; i < count; i++) {
            indices.emplace_back(static_cast<int64_t>(i));
        }
        return indices;
    }

    void append_int64_data(data_table_t& table, std::pmr::memory_resource* resource, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, logical_value_t{resource, static_cast<int64_t>(offset + i)});
            }
            table_append_state state(resource);
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    void append_two_int64_columns(data_table_t& table, std::pmr::memory_resource* resource, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const auto value = static_cast<int64_t>(offset + i);
                chunk.set_value(0, i, logical_value_t{resource, value});
                chunk.set_value(1, i, logical_value_t{resource, value});
            }
            table_append_state state(resource);
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    void append_string_int64_columns(data_table_t& table, std::pmr::memory_resource* resource, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                const auto value = static_cast<int64_t>(offset + i);
                chunk.set_value(0,
                                i,
                                logical_value_t{resource, std::string("R") + std::to_string(offset + i)});
                chunk.set_value(1, i, logical_value_t{resource, value});
            }
            table_append_state state(resource);
            table.append_lock(state);
            table.initialize_append(state);
            table.append(chunk, state);
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    void require_ordered_int64_batches(std::pmr::vector<data_chunk_t>& batches, uint64_t expected_rows) {
        uint64_t seen = 0;
        for (auto& batch : batches) {
            batch.data[0].flatten(batch.size());
            auto* values = batch.data[0].data<int64_t>();
            for (uint64_t i = 0; i < batch.size(); ++i) {
                REQUIRE(values[i] == static_cast<int64_t>(seen));
                ++seen;
            }
        }
        REQUIRE(seen == expected_rows);
    }

    void require_ordered_int64_batches_in_column(std::pmr::vector<data_chunk_t>& batches,
                                                 size_t column_index,
                                                 uint64_t expected_start,
                                                 uint64_t expected_rows) {
        uint64_t seen = expected_start;
        for (auto& batch : batches) {
            batch.data[column_index].flatten(batch.size());
            auto* values = batch.data[column_index].data<int64_t>();
            for (uint64_t i = 0; i < batch.size(); ++i) {
                REQUIRE(values[i] == static_cast<int64_t>(seen));
                ++seen;
            }
        }
        REQUIRE(seen == expected_start + expected_rows);
    }

    struct disk_manager_fixture_t {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t scheduler;
        configuration::config_disk config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit disk_manager_fixture_t(std::string path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(1, 1)
            , config([&path]() {
                configuration::config_disk c;
                c.path = std::filesystem::path(std::move(path));
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, &scheduler, &scheduler, config, log)) {
            std::filesystem::remove_all(config.path);
            std::filesystem::create_directories(config.path);
        }

        ~disk_manager_fixture_t() {
            scheduler.stop();
            std::filesystem::remove_all(config.path);
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            // The merged manager processes messages on its own loop thread and
            // delegates batched scans to disk agents enqueued on this manually
            // pumped scheduler. A single run() races that async delegation: the
            // agent can be enqueued only after run() has already drained an empty
            // queue, so its work — and the response future — would never run.
            // Pump until the manager actually resolves the future.
            while (!future.is_ready()) {
                scheduler.run(10000);
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            return std::move(future).get();
        }
    };
} // namespace

TEST_CASE("services::disk::table_storage::in_memory") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    REQUIRE(ts.mode() == storage_mode_t::IN_MEMORY);

    // Insert data
    append_int64_data(ts.table(), &resource, 100);
    REQUIRE(ts.table().calculate_size() == 100);

    // Scan and verify
    auto types = ts.table().copy_types();
    data_chunk_t result(&resource, types, DEFAULT_VECTOR_CAPACITY);
    table_scan_state scan_state(&resource);
    auto column_indices = make_column_indices(ts.table().column_count());
    ts.table().initialize_scan(scan_state, column_indices);
    ts.table().scan(result, scan_state);
    REQUIRE(result.size() == 100);

    for (uint64_t i = 0; i < result.size(); i++) {
        auto val = result.data[0].value(i);
        REQUIRE(val.value<int64_t>() == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::disk::table_storage::disk_checkpoint_and_load") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "test_table.otbx";
    constexpr uint64_t NUM_ROWS = 500;

    // Create, insert, checkpoint
    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        REQUIRE(ts.mode() == storage_mode_t::DISK);

        append_int64_data(ts.table(), &resource, NUM_ROWS);
        REQUIRE(ts.table().calculate_size() == NUM_ROWS);

        ts.checkpoint();
    }

    // Load and verify
    {
        table_storage_t ts(&resource, otbx_path);
        REQUIRE(ts.mode() == storage_mode_t::DISK);

        auto& table = ts.table();
        REQUIRE(table.calculate_size() == NUM_ROWS);

        auto types = table.copy_types();
        data_chunk_t result(&resource, types, DEFAULT_VECTOR_CAPACITY);
        table_scan_state scan_state(&resource);
        auto column_indices = make_column_indices(table.column_count());
        table.initialize_scan(scan_state, column_indices);
        table.scan(result, scan_state);
        REQUIRE(result.size() == static_cast<uint64_t>(std::min(NUM_ROWS, uint64_t(DEFAULT_VECTOR_CAPACITY))));

        for (uint64_t i = 0; i < result.size(); i++) {
            auto val = result.data[0].value(i);
            REQUIRE(val.value<int64_t>() == static_cast<int64_t>(i));
        }
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::mode_query") {
    std::pmr::synchronized_pool_resource resource;

    // In-memory (schema-less)
    {
        table_storage_t ts(&resource);
        REQUIRE(ts.mode() == storage_mode_t::IN_MEMORY);
    }

    // In-memory (with columns)
    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("x", logical_type::DOUBLE);
        table_storage_t ts(&resource, std::move(columns));
        REQUIRE(ts.mode() == storage_mode_t::IN_MEMORY);
    }

    // Disk (new)
    {
        cleanup_test_dir();
        std::filesystem::create_directories(test_dir());
        auto otbx_path = std::filesystem::path(test_dir()) / "mode_test.otbx";
        std::vector<column_definition_t> columns;
        columns.emplace_back("x", logical_type::DOUBLE);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        REQUIRE(ts.mode() == storage_mode_t::DISK);
        cleanup_test_dir();
    }
}

TEST_CASE("services::disk::table_storage::checkpoint_preserves_multi_column") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "multi_col.otbx";
    constexpr uint64_t NUM_ROWS = 200;

    // Create multi-column disk table, insert, checkpoint
    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("score", logical_type::DOUBLE);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        REQUIRE(ts.mode() == storage_mode_t::DISK);

        auto types = ts.table().copy_types();
        uint64_t offset = 0;
        while (offset < NUM_ROWS) {
            uint64_t batch = std::min(NUM_ROWS - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, logical_value_t{&resource, static_cast<int64_t>(offset + i)});
                chunk.set_value(1, i, logical_value_t{&resource, static_cast<double>(offset + i) * 1.5});
            }
            table_append_state state(&resource);
            ts.table().append_lock(state);
            ts.table().initialize_append(state);
            ts.table().append(chunk, state);
            ts.table().finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
        REQUIRE(ts.table().calculate_size() == NUM_ROWS);
        ts.checkpoint();
    }

    // Load and verify both columns
    {
        table_storage_t ts(&resource, otbx_path);
        REQUIRE(ts.mode() == storage_mode_t::DISK);
        REQUIRE(ts.table().calculate_size() == NUM_ROWS);
        REQUIRE(ts.table().column_count() == 2);

        auto types = ts.table().copy_types();
        data_chunk_t result(&resource, types, DEFAULT_VECTOR_CAPACITY);
        table_scan_state scan_state(&resource);
        auto column_indices = make_column_indices(ts.table().column_count());
        ts.table().initialize_scan(scan_state, column_indices);
        ts.table().scan(result, scan_state);
        REQUIRE(result.size() == NUM_ROWS);

        for (uint64_t i = 0; i < result.size(); i++) {
            auto id_val = result.data[0].value(i);
            auto score_val = result.data[1].value(i);
            REQUIRE(id_val.value<int64_t>() == static_cast<int64_t>(i));
            REQUIRE(score_val.value<double>() == Approx(static_cast<double>(i) * 1.5));
        }
    }

    cleanup_test_dir();
}

// Physical column compaction primitive. table_storage_t::drop_column removes
// the named column from the IN_MEMORY data_table_t via the rebuild
// constructor (data_table_t(parent, removed_column) backed by
// collection_t::remove_column per row_group segment). DISK-mode is out of scope.
TEST_CASE("services::disk::table_storage::drop_column_in_memory") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("a", logical_type::BIGINT);
    columns.emplace_back("b", logical_type::BIGINT);
    columns.emplace_back("c", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));
    REQUIRE(ts.mode() == storage_mode_t::IN_MEMORY);
    REQUIRE(ts.table().column_count() == 3);

    // Append 32 rows: a=i, b=i*10, c=i*100.
    constexpr uint64_t NUM_ROWS = 32;
    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, NUM_ROWS);
        chunk.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; ++i) {
            chunk.set_value(0, i, logical_value_t{&resource, static_cast<int64_t>(i)});
            chunk.set_value(1, i, logical_value_t{&resource, static_cast<int64_t>(i * 10)});
            chunk.set_value(2, i, logical_value_t{&resource, static_cast<int64_t>(i * 100)});
        }
        table_append_state state(&resource);
        ts.table().append_lock(state);
        ts.table().initialize_append(state);
        ts.table().append(chunk, state);
        ts.table().finalize_append(state, transaction_data{0, 0});
    }
    REQUIRE(ts.table().calculate_size() == NUM_ROWS);

    // Drop the middle column "b". Rebuild constructor must produce {a, c} with
    // physical data preserved for the remaining columns.
    REQUIRE(ts.drop_column("b"));
    REQUIRE(ts.table().column_count() == 2);
    REQUIRE(ts.table().columns()[0].name() == "a");
    REQUIRE(ts.table().columns()[1].name() == "c");
    REQUIRE(ts.table().calculate_size() == NUM_ROWS);

    // Scan and verify that a/c data is intact.
    {
        auto types = ts.table().copy_types();
        data_chunk_t result(&resource, types, DEFAULT_VECTOR_CAPACITY);
        table_scan_state scan_state(&resource);
        auto column_indices = make_column_indices(ts.table().column_count());
        ts.table().initialize_scan(scan_state, column_indices);
        ts.table().scan(result, scan_state);
        REQUIRE(result.size() == NUM_ROWS);
        for (uint64_t i = 0; i < result.size(); ++i) {
            REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(i));
            REQUIRE(result.data[1].value(i).value<int64_t>() == static_cast<int64_t>(i * 100));
        }
    }

    // Dropping a non-existent column is a no-op (false).
    REQUIRE(!ts.drop_column("missing"));
    REQUIRE(ts.table().column_count() == 2);
}

TEST_CASE("services::disk::table_storage::drop_column_disk_is_noop") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "test_drop_disk.otbx";
    std::vector<column_definition_t> columns;
    columns.emplace_back("a", logical_type::BIGINT);
    columns.emplace_back("b", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns), otbx_path);
    REQUIRE(ts.mode() == storage_mode_t::DISK);
    REQUIRE(ts.table().column_count() == 2);

    // DISK-mode: drop_column returns false (out of scope).
    REQUIRE(!ts.drop_column("b"));
    REQUIRE(ts.table().column_count() == 2);

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::parallel_scan_via_storage_adapter") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    // Insert 4 row groups worth of data (4 * DEFAULT_VECTOR_CAPACITY)
    append_int64_data(ts.table(), &resource, 4 * DEFAULT_VECTOR_CAPACITY);
    REQUIRE(ts.table().calculate_size() == 4 * DEFAULT_VECTOR_CAPACITY);

    // Use storage adapter's parallel_scan
    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);

    uint64_t chunks_seen = 0;
    uint64_t total = adapter.parallel_scan([&](data_chunk_t& /*chunk*/) { chunks_seen++; });

    REQUIRE(total == 4 * DEFAULT_VECTOR_CAPACITY);
    REQUIRE(chunks_seen == 4);
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_via_storage_adapter_threads") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    append_int64_data(ts.table(), &resource, 4 * DEFAULT_VECTOR_CAPACITY);
    REQUIRE(ts.table().calculate_size() == 4 * DEFAULT_VECTOR_CAPACITY);

    actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
    scheduler.start();
    scheduler_guard_t guard{scheduler};
    {
        components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
        std::pmr::vector<data_chunk_t> batches{&resource};

        adapter.scan_batched(batches, nullptr, -1, nullptr, transaction_data{0, 0});

        REQUIRE(adapter.parallel_worker_count() > 0);
        REQUIRE(batches.size() == 4);
        require_ordered_int64_batches(batches, 4 * DEFAULT_VECTOR_CAPACITY);
    }
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_via_storage_adapter_pumped_scheduler") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    append_int64_data(ts.table(), &resource, 4 * DEFAULT_VECTOR_CAPACITY);
    REQUIRE(ts.table().calculate_size() == 4 * DEFAULT_VECTOR_CAPACITY);

    core::non_thread_scheduler::scheduler_test_t scheduler(1, 1000);
    std::function<void()> pump = [&scheduler] { scheduler.run(10000); };

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler, &pump);
    std::pmr::vector<data_chunk_t> batches{&resource};

    adapter.scan_batched(batches, nullptr, -1, nullptr, transaction_data{0, 0});

    REQUIRE(adapter.parallel_worker_count() > 0);
    REQUIRE(batches.size() == 4);
    require_ordered_int64_batches(batches, 4 * DEFAULT_VECTOR_CAPACITY);
    scheduler.stop();
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_filter_projected_across_row_groups") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::BIGINT);
    columns.emplace_back("count", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 1;
    append_two_int64_columns(ts.table(), &resource, total_rows);
    REQUIRE(ts.table().calculate_size() == total_rows);

    core::non_thread_scheduler::scheduler_test_t scheduler(1, 1000);
    std::function<void()> pump = [&scheduler] { scheduler.run(10000); };

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler, &pump);
    std::pmr::vector<data_chunk_t> batches{&resource};
    std::pmr::vector<uint64_t> table_indices{&resource};
    table_indices.push_back(1);
    constant_filter_t filter(components::expressions::compare_type::gte,
                             logical_value_t{&resource, static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY)},
                             std::move(table_indices));
    const std::vector<size_t> projected_cols{0};

    adapter.scan_batched(batches, &filter, -1, &projected_cols, transaction_data{0, 0});

    REQUIRE(adapter.parallel_worker_count() > 0);
    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0].size() == 1);
    REQUIRE(batches[0].data[0].value(0).value<int64_t>() == static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY));
    scheduler.stop();
}

TEST_CASE("services::disk::table_storage::scan_batched_filter_string_bigint_single_row_group") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY - 1;
    append_string_int64_columns(ts.table(), &resource, total_rows);
    REQUIRE(ts.table().calculate_size() == total_rows);

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);
    std::pmr::vector<data_chunk_t> batches{&resource};
    std::pmr::vector<uint64_t> table_indices{&resource};
    table_indices.push_back(1);
    constant_filter_t filter(components::expressions::compare_type::gte,
                             logical_value_t{&resource, static_cast<int64_t>(0)},
                             std::move(table_indices));

    adapter.scan_batched(batches, &filter, -1, nullptr, transaction_data{0, 0});

    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0].size() == total_rows);
    batches[0].data[0].flatten(batches[0].size());
    batches[0].data[1].flatten(batches[0].size());
    for (uint64_t i = 0; i < total_rows; ++i) {
        REQUIRE(batches[0].data[0].value(i).value<std::string_view>() == "R" + std::to_string(i));
        REQUIRE(batches[0].data[1].value(i).value<int64_t>() == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::disk::table_storage::scan_batched_filter_string_bigint_single_row_group_committed_txn") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY - 1;
    auto types = ts.table().copy_types();
    data_chunk_t chunk(&resource, types, total_rows);
    chunk.set_cardinality(total_rows);
    for (uint64_t i = 0; i < total_rows; ++i) {
        chunk.set_value(0, i, logical_value_t{&resource, std::string("R") + std::to_string(i)});
        chunk.set_value(1, i, logical_value_t{&resource, static_cast<int64_t>(i)});
    }

    const auto writer_txn = transaction_data{TRANSACTION_ID_START + 1, TRANSACTION_ID_START + 1};
    const uint64_t writer_commit_id = 1;
    table_append_state state(&resource);
    ts.table().append_lock(state);
    ts.table().initialize_append(state);
    const auto row_start = static_cast<uint64_t>(state.current_row);
    ts.table().append(chunk, state);
    ts.table().finalize_append(state, writer_txn);
    ts.table().commit_append(writer_commit_id, static_cast<int64_t>(row_start), total_rows);

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);
    std::pmr::vector<data_chunk_t> batches{&resource};
    std::pmr::vector<uint64_t> table_indices{&resource};
    table_indices.push_back(1);
    constant_filter_t filter(components::expressions::compare_type::gte,
                             logical_value_t{&resource, static_cast<int64_t>(0)},
                             std::move(table_indices));
    const auto reader_txn = transaction_data{TRANSACTION_ID_START + 2, TRANSACTION_ID_START + 2};

    adapter.scan_batched(batches, &filter, -1, nullptr, reader_txn);

    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0].size() == total_rows);
    batches[0].data[0].flatten(batches[0].size());
    batches[0].data[1].flatten(batches[0].size());
    for (uint64_t i = 0; i < total_rows; ++i) {
        REQUIRE(batches[0].data[0].value(i).value<std::string_view>() == "R" + std::to_string(i));
        REQUIRE(batches[0].data[1].value(i).value<int64_t>() == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::disk::table_storage::scan_batched_filter_string_bigint_multi_row_group_committed_txn") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 476;
    auto types = ts.table().copy_types();
    uint64_t offset = 0;
    const auto writer_txn = transaction_data{TRANSACTION_ID_START + 11, TRANSACTION_ID_START + 11};
    const uint64_t writer_commit_id = 11;
    while (offset < total_rows) {
        const uint64_t batch = std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, total_rows - offset);
        data_chunk_t chunk(&resource, types, batch);
        chunk.set_cardinality(batch);
        for (uint64_t i = 0; i < batch; ++i) {
            const auto row = offset + i;
            chunk.set_value(0, i, logical_value_t{&resource, std::string("R") + std::to_string(row)});
            chunk.set_value(1, i, logical_value_t{&resource, static_cast<int64_t>(row)});
        }

        table_append_state state(&resource);
        ts.table().append_lock(state);
        ts.table().initialize_append(state);
        const auto row_start = static_cast<uint64_t>(state.current_row);
        ts.table().append(chunk, state);
        ts.table().finalize_append(state, writer_txn);
        ts.table().commit_append(writer_commit_id, static_cast<int64_t>(row_start), batch);
        offset += batch;
    }

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);
    std::pmr::vector<data_chunk_t> batches{&resource};
    std::pmr::vector<uint64_t> table_indices{&resource};
    table_indices.push_back(1);
    constant_filter_t filter(components::expressions::compare_type::gte,
                             logical_value_t{&resource, static_cast<int64_t>(0)},
                             std::move(table_indices));
    const auto reader_txn = transaction_data{TRANSACTION_ID_START + 12, TRANSACTION_ID_START + 12};

    adapter.scan_batched(batches, &filter, -1, nullptr, reader_txn);

    REQUIRE(batches.size() == 2);
    uint64_t seen = 0;
    for (auto& batch : batches) {
        batch.data[0].flatten(batch.size());
        batch.data[1].flatten(batch.size());
        for (uint64_t i = 0; i < batch.size(); ++i) {
            REQUIRE(batch.data[0].value(i).value<std::string_view>() == "R" + std::to_string(seen));
            REQUIRE(batch.data[1].value(i).value<int64_t>() == static_cast<int64_t>(seen));
            ++seen;
        }
    }
    REQUIRE(seen == total_rows);
}

TEST_CASE("services::disk::table_storage::scan_batched_filter_string_bigint_boundary_cutoff") {
    const auto total_rows = GENERATE(1023u, 1024u, 1025u, 1500u);
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    append_string_int64_columns(ts.table(), &resource, total_rows);
    REQUIRE(ts.table().calculate_size() == total_rows);

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);
    std::pmr::vector<data_chunk_t> batches{&resource};
    std::pmr::vector<uint64_t> table_indices{&resource};
    table_indices.push_back(1);
    constant_filter_t filter(components::expressions::compare_type::gte,
                             logical_value_t{&resource, int64_t{1000}},
                             std::move(table_indices));

    adapter.scan_batched(batches, &filter, -1, nullptr, transaction_data{0, 0});

    uint64_t seen = 0;
    for (auto& batch : batches) {
        batch.data[0].flatten(batch.size());
        batch.data[1].flatten(batch.size());
        for (uint64_t i = 0; i < batch.size(); ++i) {
            const auto expected = 1000u + seen;
            REQUIRE(batch.data[0].value(i).value<std::string_view>() == "R" + std::to_string(expected));
            REQUIRE(batch.data[1].value(i).value<int64_t>() == static_cast<int64_t>(expected));
            ++seen;
        }
    }
    REQUIRE(seen == (total_rows > 1000 ? total_rows - 1000 : 0));
}

TEST_CASE("services::disk::table_storage::manager_disk_actor_scan_batched_filter_string_bigint") {
    auto path = test_dir() + "_manager_actor_" + std::to_string(::getpid());
    disk_manager_fixture_t fixture(path);

    constexpr auto database_oid = components::catalog::oid_t{1};
    constexpr auto table_oid = components::catalog::oid_t{777};
    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    fixture.manager->create_storage_with_columns_sync(table_oid, database_oid, std::move(columns));

    std::pmr::vector<complex_logical_type> types{&fixture.resource};
    types.emplace_back(logical_type::STRING_LITERAL);
    types.emplace_back(logical_type::BIGINT);
    data_chunk_t chunk(&fixture.resource, types, DEFAULT_VECTOR_CAPACITY);
    chunk.set_cardinality(DEFAULT_VECTOR_CAPACITY);
    for (uint64_t i = 0; i < DEFAULT_VECTOR_CAPACITY; ++i) {
        chunk.set_value(0, i, logical_value_t{&fixture.resource, std::string("R") + std::to_string(i)});
        chunk.set_value(1, i, logical_value_t{&fixture.resource, static_cast<int64_t>(i)});
    }
    fixture.manager->direct_append_sync(table_oid, chunk, core::date::timezone_offset_t{0});

    std::pmr::vector<uint64_t> table_indices{&fixture.resource};
    table_indices.push_back(1);
    auto filter = std::make_unique<constant_filter_t>(components::expressions::compare_type::gte,
                                                      logical_value_t{&fixture.resource, int64_t{0}},
                                                      std::move(table_indices));
    auto batches = fixture.invoke(&manager_disk_t::storage_scan_batched,
                                  session_id_t{},
                                  table_oid,
                                  std::move(filter),
                                  int64_t{-1},
                                  std::vector<size_t>{},
                                  false,
                                  transaction_data{0, 0});

    REQUIRE(batches != nullptr);
    REQUIRE(batches->size() == 1);
    REQUIRE((*batches)[0].size() == DEFAULT_VECTOR_CAPACITY);
    (*batches)[0].data[0].flatten((*batches)[0].size());
    (*batches)[0].data[1].flatten((*batches)[0].size());
    for (uint64_t i = 0; i < DEFAULT_VECTOR_CAPACITY; ++i) {
        REQUIRE((*batches)[0].data[0].value(i).value<std::string_view>() == "R" + std::to_string(i));
        REQUIRE((*batches)[0].data[1].value(i).value<int64_t>() == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::disk::table_storage::manager_disk_actor_scan_batched_filter_string_bigint_cutoff_1023") {
    auto path = test_dir() + "_manager_actor_cutoff_" + std::to_string(::getpid());
    disk_manager_fixture_t fixture(path);

    constexpr auto database_oid = components::catalog::oid_t{1};
    constexpr auto table_oid = components::catalog::oid_t{778};
    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    fixture.manager->create_storage_with_columns_sync(table_oid, database_oid, std::move(columns));

    std::pmr::vector<complex_logical_type> types{&fixture.resource};
    types.emplace_back(logical_type::STRING_LITERAL);
    types.emplace_back(logical_type::BIGINT);
    data_chunk_t chunk(&fixture.resource, types, 1023);
    chunk.set_cardinality(1023);
    for (uint64_t i = 0; i < 1023; ++i) {
        chunk.set_value(0, i, logical_value_t{&fixture.resource, std::string("R") + std::to_string(i)});
        chunk.set_value(1, i, logical_value_t{&fixture.resource, static_cast<int64_t>(i)});
    }
    fixture.manager->direct_append_sync(table_oid, chunk, core::date::timezone_offset_t{0});

    std::pmr::vector<uint64_t> table_indices{std::pmr::new_delete_resource()};
    table_indices.push_back(1);
    auto filter = std::make_unique<constant_filter_t>(components::expressions::compare_type::gte,
                                                      logical_value_t{std::pmr::new_delete_resource(), int64_t{1000}},
                                                      std::move(table_indices));
    auto batches = fixture.invoke(&manager_disk_t::storage_scan_batched,
                                  session_id_t{},
                                  table_oid,
                                  std::move(filter),
                                  int64_t{-1},
                                  std::vector<size_t>{},
                                  false,
                                  transaction_data{0, 0});

    REQUIRE(batches != nullptr);
    REQUIRE(batches->size() == 1);
    REQUIRE((*batches)[0].size() == 23);
    (*batches)[0].data[0].flatten((*batches)[0].size());
    (*batches)[0].data[1].flatten((*batches)[0].size());
    for (uint64_t i = 0; i < 23; ++i) {
        const auto expected = 1000u + i;
        REQUIRE((*batches)[0].data[0].value(i).value<std::string_view>() == "R" + std::to_string(expected));
        REQUIRE((*batches)[0].data[1].value(i).value<int64_t>() == static_cast<int64_t>(expected));
    }
}

TEST_CASE("services::disk::table_storage::scan_batched_filter_string_bigint_cutoff_1023") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    constexpr uint64_t total_rows = 1023;
    append_string_int64_columns(ts.table(), &resource, total_rows);

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);
    std::pmr::vector<data_chunk_t> batches{&resource};
    std::pmr::vector<uint64_t> table_indices{&resource};
    table_indices.push_back(1);
    constant_filter_t filter(components::expressions::compare_type::gte,
                             logical_value_t{&resource, int64_t{1000}},
                             std::move(table_indices));

    adapter.scan_batched(batches, &filter, -1, nullptr, transaction_data{0, 0});

    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0].size() == 23);
    batches[0].data[0].flatten(batches[0].size());
    batches[0].data[1].flatten(batches[0].size());
    for (uint64_t i = 0; i < 23; ++i) {
        const auto expected = 1000u + i;
        REQUIRE(batches[0].data[0].value(i).value<std::string_view>() == "R" + std::to_string(expected));
        REQUIRE(batches[0].data[1].value(i).value<int64_t>() == static_cast<int64_t>(expected));
    }
}

TEST_CASE("services::disk::table_storage::cursor_from_multi_batch_string_scan") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 1;
    append_string_int64_columns(ts.table(), &resource, total_rows);

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);
    std::pmr::vector<data_chunk_t> batches{&resource};
    std::pmr::vector<uint64_t> table_indices{&resource};
    table_indices.push_back(1);
    constant_filter_t filter(components::expressions::compare_type::gte,
                             logical_value_t{&resource, int64_t{0}},
                             std::move(table_indices));

    adapter.scan_batched(batches, &filter, -1, nullptr, transaction_data{0, 0});
    REQUIRE(batches.size() == 2);

    auto cursor = components::cursor::make_cursor(&resource, std::move(batches));
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == total_rows);

    auto& chunk = cursor->chunk_data();
    REQUIRE(chunk.size() == total_rows);
    chunk.data[0].flatten(chunk.size());
    chunk.data[1].flatten(chunk.size());
    for (uint64_t i = 0; i < total_rows; ++i) {
        REQUIRE(chunk.data[0].value(i).value<std::string_view>() == "R" + std::to_string(i));
        REQUIRE(chunk.data[1].value(i).value<int64_t>() == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::disk::table_storage::sequential_filter_scans_survive_cursor_projection_combine") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("name", logical_type::STRING_LITERAL);
    columns.emplace_back("count", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 1;
    append_string_int64_columns(ts.table(), &resource, total_rows);

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource);

    {
        std::pmr::vector<data_chunk_t> first_batches{&resource};
        std::pmr::vector<uint64_t> first_indices{&resource};
        first_indices.push_back(1);
        constant_filter_t first_filter(components::expressions::compare_type::gte,
                                       logical_value_t{&resource, int64_t{0}},
                                       std::move(first_indices));

        adapter.scan_batched(first_batches, &first_filter, -1, nullptr, transaction_data{0, 0});
        REQUIRE(first_batches.size() == 2);

        std::pmr::vector<data_chunk_t> projected_batches{&resource};
        projected_batches.reserve(first_batches.size());
        for (auto& batch : first_batches) {
            std::pmr::vector<complex_logical_type> projected_types{&resource};
            projected_types.emplace_back(logical_type::BIGINT);
            data_chunk_t projected(&resource, projected_types, batch.size());
            projected.set_cardinality(batch.size());
            for (uint64_t row = 0; row < batch.size(); ++row) {
                projected.set_value(0, row, batch.data[1].value(row));
            }
            projected_batches.push_back(std::move(projected));
        }

        auto cursor = components::cursor::make_cursor(&resource, std::move(projected_batches));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == total_rows);
        auto& projected_chunk = cursor->chunk_data();
        REQUIRE(projected_chunk.size() == total_rows);
        projected_chunk.data[0].flatten(projected_chunk.size());
        for (uint64_t i = 0; i < total_rows; ++i) {
            REQUIRE(projected_chunk.data[0].value(i).value<int64_t>() == static_cast<int64_t>(i));
        }
    }

    std::pmr::vector<data_chunk_t> second_batches{&resource};
    std::pmr::vector<uint64_t> second_indices{&resource};
    second_indices.push_back(1);
    constant_filter_t second_filter(components::expressions::compare_type::gte,
                                    logical_value_t{&resource, int64_t{1000}},
                                    std::move(second_indices));

    adapter.scan_batched(second_batches, &second_filter, -1, nullptr, transaction_data{0, 0});

    REQUIRE(second_batches.size() == 2);
    uint64_t seen = 1000;
    for (auto& batch : second_batches) {
        batch.data[0].flatten(batch.size());
        batch.data[1].flatten(batch.size());
        for (uint64_t i = 0; i < batch.size(); ++i) {
            REQUIRE(batch.data[0].value(i).value<std::string_view>() == "R" + std::to_string(seen));
            REQUIRE(batch.data[1].value(i).value<int64_t>() == static_cast<int64_t>(seen));
            ++seen;
        }
    }
    REQUIRE(seen == total_rows);
}

TEST_CASE("services::disk::table_storage::cursor_from_split_large_output_batches") {
    std::pmr::synchronized_pool_resource resource;

    std::pmr::vector<complex_logical_type> types{&resource};
    types.emplace_back(logical_type::BIGINT);
    types.emplace_back(logical_type::BIGINT);

    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 1;
    data_chunk_t chunk(&resource, types, total_rows);
    chunk.set_cardinality(total_rows);
    for (uint64_t i = 0; i < total_rows; ++i) {
        chunk.set_value(0, i, logical_value_t{&resource, static_cast<int64_t>(i)});
        chunk.set_value(1, i, logical_value_t{&resource, static_cast<int64_t>(i * 2)});
    }

    auto first_batch = chunk.partial_copy(&resource, 0, DEFAULT_VECTOR_CAPACITY);
    REQUIRE(first_batch.size() == DEFAULT_VECTOR_CAPACITY);

    auto second_batch = chunk.partial_copy(&resource, DEFAULT_VECTOR_CAPACITY, 1);
    REQUIRE(second_batch.size() == 1);

    components::operators::chunks_vector_t batches{&resource};
    batches.push_back(std::move(first_batch));
    batches.push_back(std::move(second_batch));

    auto cursor = components::cursor::make_cursor(&resource, std::move(batches));
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == total_rows);

    auto& combined = cursor->chunk_data();
    REQUIRE(combined.size() == total_rows);
    combined.data[0].flatten(combined.size());
    combined.data[1].flatten(combined.size());
    for (uint64_t i = 0; i < total_rows; ++i) {
        REQUIRE(combined.data[0].value(i).value<int64_t>() == static_cast<int64_t>(i));
        REQUIRE(combined.data[1].value(i).value<int64_t>() == static_cast<int64_t>(i * 2));
    }
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_projected_via_storage_adapter_pumped_scheduler") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("id", logical_type::BIGINT);
    columns.emplace_back("value", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    append_two_int64_columns(ts.table(), &resource, 4 * DEFAULT_VECTOR_CAPACITY);
    REQUIRE(ts.table().calculate_size() == 4 * DEFAULT_VECTOR_CAPACITY);

    core::non_thread_scheduler::scheduler_test_t scheduler(1, 1000);
    std::function<void()> pump = [&scheduler] { scheduler.run(10000); };

    components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler, &pump);
    std::pmr::vector<data_chunk_t> batches{&resource};
    const std::vector<size_t> projected_cols{1};

    adapter.scan_batched(batches, nullptr, -1, &projected_cols, transaction_data{0, 0});

    REQUIRE(adapter.parallel_worker_count() > 0);
    REQUIRE(batches.size() == 4);
    require_ordered_int64_batches_in_column(batches, 1, 0, 4 * DEFAULT_VECTOR_CAPACITY);
    scheduler.stop();
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_with_committed_deletes_uses_threads") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", logical_type::BIGINT);
    table_storage_t ts(&resource, std::move(columns));

    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 1;
    append_int64_data(ts.table(), &resource, total_rows);
    REQUIRE(ts.table().calculate_size() == total_rows);

    std::pmr::vector<complex_logical_type> row_id_types(&resource);
    row_id_types.emplace_back(logical_type::BIGINT);
    data_chunk_t row_ids_chunk(&resource, row_id_types, 1);
    row_ids_chunk.data[0].set_value(0, logical_value_t{&resource, static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY)});
    row_ids_chunk.set_cardinality(1);

    components::storage::table_storage_adapter_t mutating_adapter(ts.table(), &resource);
    const auto deleted = mutating_adapter.delete_rows(row_ids_chunk.data[0], 1, TRANSACTION_ID_START);
    REQUIRE(deleted == 1);
    mutating_adapter.commit_all_deletes(TRANSACTION_ID_START, 1);

    actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
    scheduler.start();
    scheduler_guard_t guard{scheduler};
    {
        components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
        std::pmr::vector<data_chunk_t> batches{&resource};

        adapter.scan_batched(batches, nullptr, -1, nullptr, transaction_data{0, 0});

        REQUIRE(adapter.parallel_worker_count() > 0);
        REQUIRE(batches.size() == 1);
        REQUIRE(batches[0].size() == DEFAULT_VECTOR_CAPACITY);
        require_ordered_int64_batches(batches, DEFAULT_VECTOR_CAPACITY);
    }
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_disk_load_uses_threads") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "parallel_disk_scan.otbx";
    constexpr uint64_t total_rows = 2 * DEFAULT_VECTOR_CAPACITY;

    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        append_int64_data(ts.table(), &resource, total_rows);
        ts.checkpoint();
    }

    {
        table_storage_t ts(&resource, otbx_path);
        actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
        scheduler.start();
        scheduler_guard_t guard{scheduler};
        {
            components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
            std::pmr::vector<data_chunk_t> batches{&resource};

            adapter.scan_batched(batches, nullptr, -1, nullptr, transaction_data{0, 0});

            REQUIRE(adapter.parallel_worker_count() > 0);
            REQUIRE(batches.size() == 2);
            require_ordered_int64_batches(batches, total_rows);
        }
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_row_ids_only_keeps_empty_projection") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "parallel_row_ids_only_scan.otbx";
    constexpr uint64_t total_rows = 2 * DEFAULT_VECTOR_CAPACITY;

    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        append_two_int64_columns(ts.table(), &resource, total_rows);
        ts.checkpoint();
    }

    {
        table_storage_t ts(&resource, otbx_path);
        actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
        scheduler.start();
        scheduler_guard_t guard{scheduler};
        {
            components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
            std::pmr::vector<data_chunk_t> batches{&resource};
            std::vector<size_t> row_ids_only_projection;

            adapter.scan_batched(batches, nullptr, -1, &row_ids_only_projection, transaction_data{0, 0});

            REQUIRE(adapter.parallel_worker_count() > 0);
            REQUIRE(batches.size() == 2);

            uint64_t seen = 0;
            for (const auto& batch : batches) {
                REQUIRE(batch.column_count() == 2);
                for (const auto& column : batch.data) {
                    REQUIRE(column.data() == nullptr);
                    REQUIRE(column.auxiliary() == nullptr);
                }
                const auto* row_ids = batch.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < batch.size(); ++i) {
                    REQUIRE(row_ids[i] == static_cast<int64_t>(seen));
                    ++seen;
                }
            }
            REQUIRE(seen == total_rows);
        }
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_disk_load_with_committed_deletes_uses_threads") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "parallel_disk_scan_delete.otbx";
    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 1;

    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        append_int64_data(ts.table(), &resource, total_rows);
        ts.checkpoint();
    }

    {
        table_storage_t ts(&resource, otbx_path);

        std::pmr::vector<complex_logical_type> row_id_types(&resource);
        row_id_types.emplace_back(logical_type::BIGINT);
        data_chunk_t row_ids_chunk(&resource, row_id_types, 1);
        row_ids_chunk.data[0].set_value(0, logical_value_t{&resource, static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY)});
        row_ids_chunk.set_cardinality(1);

        components::storage::table_storage_adapter_t mutating_adapter(ts.table(), &resource);
        const auto deleted = mutating_adapter.delete_rows(row_ids_chunk.data[0], 1, TRANSACTION_ID_START);
        REQUIRE(deleted == 1);
        mutating_adapter.commit_all_deletes(TRANSACTION_ID_START, 1);

#if defined(DEV_MODE)
        auto* first_row_group = ts.table().row_group()->row_group_tree()->root_segment();
        REQUIRE(first_row_group != nullptr);
        first_row_group->debug_reset_scan_path_counts_for_test();
#endif

        actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
        scheduler.start();
        scheduler_guard_t guard{scheduler};
        {
            components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
            std::pmr::vector<data_chunk_t> batches{&resource};

            adapter.scan_batched(batches, nullptr, -1, nullptr, transaction_data{0, 0});

            REQUIRE(adapter.parallel_worker_count() > 0);
            uint64_t seen = 0;
            for (auto& batch : batches) {
                batch.data[0].flatten(batch.size());
                for (uint64_t i = 0; i < batch.size(); ++i) {
                    REQUIRE(batch.data[0].value(i).value<int64_t>() == static_cast<int64_t>(seen));
                    ++seen;
                }
            }
            REQUIRE(seen == DEFAULT_VECTOR_CAPACITY);
        }

#if defined(DEV_MODE)
        const auto counts = first_row_group->debug_scan_path_counts_for_test();
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.regular == 0);
#endif
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_disk_load_with_reader_txn_falls_back") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "parallel_disk_scan_reader_txn.otbx";
    constexpr uint64_t total_rows = 2 * DEFAULT_VECTOR_CAPACITY;

    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        append_int64_data(ts.table(), &resource, total_rows);
        ts.checkpoint();
    }

    {
        table_storage_t ts(&resource, otbx_path);
        actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
        scheduler.start();
        scheduler_guard_t guard{scheduler};
        {
            components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
            std::pmr::vector<data_chunk_t> batches{&resource};

            adapter.scan_batched(
                batches,
                nullptr,
                -1,
                nullptr,
                transaction_data{TRANSACTION_ID_START + 1, TRANSACTION_ID_START + 1});

            REQUIRE(adapter.parallel_worker_count() == 0);
            require_ordered_int64_batches(batches, total_rows);
        }
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_disk_load_with_committed_update_overlay_uses_threads") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "parallel_disk_scan_update.otbx";
    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 1;

    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        append_int64_data(ts.table(), &resource, total_rows);
        ts.checkpoint();
    }

    {
        table_storage_t ts(&resource, otbx_path);

        std::pmr::vector<complex_logical_type> row_id_types(&resource);
        row_id_types.emplace_back(logical_type::BIGINT);
        data_chunk_t row_ids_chunk(&resource, row_id_types, 1);
        row_ids_chunk.data[0].set_value(0, logical_value_t{&resource, int64_t{0}});
        row_ids_chunk.set_cardinality(1);

        auto update_types = ts.table().copy_types();
        data_chunk_t update_chunk(&resource, update_types, 1);
        update_chunk.data[0].set_value(0, logical_value_t{&resource, int64_t{9999}});
        update_chunk.set_cardinality(1);

        components::storage::table_storage_adapter_t mutating_adapter(ts.table(), &resource);
        mutating_adapter.update(row_ids_chunk.data[0], update_chunk);

#if defined(DEV_MODE)
        auto* first_row_group = ts.table().row_group()->row_group_tree()->root_segment();
        REQUIRE(first_row_group != nullptr);
        first_row_group->debug_reset_scan_path_counts_for_test();
#endif

        actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
        scheduler.start();
        scheduler_guard_t guard{scheduler};
        {
            components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
            std::pmr::vector<data_chunk_t> batches{&resource};

            adapter.scan_batched(batches, nullptr, -1, nullptr, transaction_data{0, 0});

            REQUIRE(adapter.parallel_worker_count() > 0);
            REQUIRE_FALSE(batches.empty());
            uint64_t seen = 0;
            for (auto& batch : batches) {
                batch.data[0].flatten(batch.size());
                for (uint64_t i = 0; i < batch.size(); ++i) {
                    if (seen == 0) {
                        REQUIRE(batch.data[0].value(i).value<int64_t>() == 9999);
                    } else {
                        REQUIRE(batch.data[0].value(i).value<int64_t>() == static_cast<int64_t>(seen));
                    }
                    ++seen;
                }
            }
            REQUIRE(seen == total_rows);
        }

#if defined(DEV_MODE)
        const auto counts = first_row_group->debug_scan_path_counts_for_test();
        REQUIRE(counts.pax_fixed_projected > 0);
        REQUIRE(counts.regular == 0);
#endif
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_disk_load_with_uncommitted_versions_falls_back") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "parallel_disk_scan_uncommitted_append.otbx";
    constexpr uint64_t total_rows = 2 * DEFAULT_VECTOR_CAPACITY;

    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        append_int64_data(ts.table(), &resource, total_rows);
        ts.checkpoint();
    }

    {
        table_storage_t ts(&resource, otbx_path);

        auto types = ts.table().copy_types();
        data_chunk_t append_chunk(&resource, types, 1);
        append_chunk.data[0].set_value(0, logical_value_t{&resource, int64_t{9999}});
        append_chunk.set_cardinality(1);

        components::storage::table_storage_adapter_t mutating_adapter(ts.table(), &resource);
        const auto txn = transaction_data{TRANSACTION_ID_START + 21, TRANSACTION_ID_START + 21};
        mutating_adapter.append(append_chunk, txn);

        actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
        scheduler.start();
        scheduler_guard_t guard{scheduler};
        {
            components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
            std::pmr::vector<data_chunk_t> batches{&resource};

            adapter.scan_batched(batches, nullptr, -1, nullptr, transaction_data{0, 0});

            REQUIRE(adapter.parallel_worker_count() == 0);
            REQUIRE(batches.size() == 3);
            uint64_t seen = 0;
            for (size_t batch_index = 0; batch_index + 1 < batches.size(); ++batch_index) {
                auto& batch = batches[batch_index];
                batch.data[0].flatten(batch.size());
                for (uint64_t i = 0; i < batch.size(); ++i) {
                    REQUIRE(batch.data[0].value(i).value<int64_t>() == static_cast<int64_t>(seen));
                    ++seen;
                }
            }
            REQUIRE(seen == total_rows);
            auto& tail = batches.back();
            tail.data[0].flatten(tail.size());
            REQUIRE(tail.size() == 1);
            REQUIRE(tail.data[0].value(0).value<int64_t>() == 9999);
        }
    }

    cleanup_test_dir();
}

TEST_CASE("services::disk::table_storage::parallel_scan_batched_disk_load_with_unloaded_deletes_uses_threads") {
    cleanup_test_dir();
    std::filesystem::create_directories(test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx_path = std::filesystem::path(test_dir()) / "parallel_disk_scan_unloaded_deletes.otbx";
    constexpr uint64_t total_rows = DEFAULT_VECTOR_CAPACITY + 1;

    {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(columns), otbx_path);
        append_int64_data(ts.table(), &resource, total_rows);

        std::pmr::vector<complex_logical_type> row_id_types(&resource);
        row_id_types.emplace_back(logical_type::BIGINT);
        data_chunk_t row_ids_chunk(&resource, row_id_types, 1);
        row_ids_chunk.data[0].set_value(0, logical_value_t{&resource, static_cast<int64_t>(DEFAULT_VECTOR_CAPACITY)});
        row_ids_chunk.set_cardinality(1);

        components::storage::table_storage_adapter_t mutating_adapter(ts.table(), &resource);
        const auto deleted = mutating_adapter.delete_rows(row_ids_chunk.data[0], 1, TRANSACTION_ID_START);
        REQUIRE(deleted == 1);
        mutating_adapter.commit_all_deletes(TRANSACTION_ID_START, 1);
        ts.checkpoint();
    }

    {
        table_storage_t ts(&resource, otbx_path);
        auto* root_row_group = ts.table().row_group()->row_group_tree()->root_segment();
        REQUIRE(root_row_group != nullptr);
        root_row_group->debug_set_unloaded_deletes_for_test(true);
        actor_zeta::scheduler::sharing_scheduler scheduler(2, 1000);
        scheduler.start();
        scheduler_guard_t guard{scheduler};
        {
            components::storage::table_storage_adapter_t adapter(ts.table(), &resource, &scheduler);
            std::pmr::vector<data_chunk_t> batches{&resource};

            adapter.scan_batched(batches, nullptr, -1, nullptr, transaction_data{0, 0});

            REQUIRE(adapter.parallel_worker_count() > 0);
            uint64_t seen = 0;
            for (auto& batch : batches) {
                batch.data[0].flatten(batch.size());
                for (uint64_t i = 0; i < batch.size(); ++i) {
                    REQUIRE(batch.data[0].value(i).value<int64_t>() == static_cast<int64_t>(seen));
                    ++seen;
                }
            }
            REQUIRE(seen == DEFAULT_VECTOR_CAPACITY);
        }
    }

    cleanup_test_dir();
}
