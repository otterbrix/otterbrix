#include <core/pmr.hpp>

#include <catch2/catch.hpp>

#include <absl/crc/crc32c.h>
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <components/log/log.hpp>
#include <string>
#include <thread>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/tests/generaty.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal.hpp>

using namespace std::chrono_literals;

using namespace services::wal;
using namespace components::logical_plan;
using namespace components::expressions;

constexpr auto database_name = "test_database";
constexpr auto collection_name = "test_collection";

struct test_wal {
    test_wal(const std::filesystem::path& path, std::pmr::memory_resource* resource)
        : log(initialization_logger("python", "/tmp/docker_logs/"))
        , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
        , config([path, this]() {
            configuration::config_wal config_wal;
            log.set_level(log_t::level::trace);
            std::filesystem::remove_all(path);
            std::filesystem::create_directories(path);
            config_wal.path = path;
            return config_wal;
        }())
        , manager(actor_zeta::spawn<manager_wal_replicate_t>(resource, scheduler, config, log))
        , wal(actor_zeta::spawn<wal_replicate_t>(resource, manager.get(), log, config)) {
        log.set_level(log_t::level::trace);
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
        config.path = path;
    }

    ~test_wal() { delete scheduler; }

    log_t log;
    core::non_thread_scheduler::scheduler_test_t* scheduler{nullptr};
    configuration::config_wal config;
    std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager;
    std::unique_ptr<wal_replicate_t, actor_zeta::pmr::deleter_t> wal;
};

test_wal create_test_wal(const std::filesystem::path& path, std::pmr::memory_resource* resource) {
    return {path, resource};
}

TEST_CASE("services::wal::transaction_id_round_trip") {
    auto resource = std::pmr::synchronized_pool_resource();

    // Test 1: default txn_id = 0
    {
        auto chunk = gen_data_chunk(1, 0, &resource);
        auto data = make_node_insert(&resource, {database_name, collection_name}, {std::move(chunk)});
        auto params = make_parameter_node(&resource);
        buffer_t storage;
        pack(storage, crc32_t(0), services::wal::id_t(1), data, params);

        auto payload_size = read_size_impl(storage, 0);
        buffer_t payload(storage.begin() + static_cast<std::ptrdiff_t>(sizeof(size_tt)),
                         storage.begin() + static_cast<std::ptrdiff_t>(sizeof(size_tt) + payload_size));

        wal_entry_t entry;
        unpack(payload, entry);
        REQUIRE(entry.id_ == 1);
        REQUIRE(entry.transaction_id_ == 0);
    }

    // Test 2: non-zero txn_id
    {
        auto chunk = gen_data_chunk(1, 0, &resource);
        auto data = make_node_insert(&resource, {database_name, collection_name}, {std::move(chunk)});
        auto params = make_parameter_node(&resource);
        buffer_t storage;
        uint64_t txn_id = 4611686018427387904ULL; // 2^62 = TRANSACTION_ID_START
        pack(storage, crc32_t(0), services::wal::id_t(2), data, params, txn_id);

        auto payload_size = read_size_impl(storage, 0);
        buffer_t payload(storage.begin() + static_cast<std::ptrdiff_t>(sizeof(size_tt)),
                         storage.begin() + static_cast<std::ptrdiff_t>(sizeof(size_tt) + payload_size));

        wal_entry_t entry;
        unpack(payload, entry);
        REQUIRE(entry.id_ == 2);
        REQUIRE(entry.transaction_id_ == txn_id);
        REQUIRE(entry.entry_->type() == node_type::insert_t);
    }
}

TEST_CASE("services::wal::physical_insert_write_and_read") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/physical_insert", &resource);

    auto chunk = gen_data_chunk(5, 0, &resource);
    auto session = components::session::session_id_t();
    auto data_chunk_ptr = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
    test_wal.wal->write_physical_insert(session, database_name, collection_name,
                                         std::move(data_chunk_ptr), 0, 5, 0);

    auto record = test_wal.wal->test_read_record(0);
    REQUIRE(record.is_physical());
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_INSERT);
    REQUIRE(record.collection_name.database == database_name);
    REQUIRE(record.collection_name.collection == collection_name);
    REQUIRE(record.physical_data != nullptr);
    REQUIRE(record.physical_data->size() == 5);
    REQUIRE(record.physical_row_start == 0);
    REQUIRE(record.physical_row_count == 5);
}

TEST_CASE("services::wal::physical_delete_write_and_read") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/physical_delete", &resource);

    std::pmr::vector<int64_t> row_ids(&resource);
    row_ids.push_back(0);
    row_ids.push_back(2);
    row_ids.push_back(4);

    auto session = components::session::session_id_t();
    test_wal.wal->write_physical_delete(session, database_name, collection_name,
                                         std::move(row_ids), 3, 0);

    auto record = test_wal.wal->test_read_record(0);
    REQUIRE(record.is_physical());
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_DELETE);
    REQUIRE(record.collection_name.database == database_name);
    REQUIRE(record.collection_name.collection == collection_name);
    REQUIRE(record.physical_row_ids.size() == 3);
    REQUIRE(record.physical_row_ids[0] == 0);
    REQUIRE(record.physical_row_ids[1] == 2);
    REQUIRE(record.physical_row_ids[2] == 4);
    REQUIRE(record.physical_row_count == 3);
}

TEST_CASE("services::wal::physical_update_write_and_read") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/physical_update", &resource);

    std::pmr::vector<int64_t> row_ids(&resource);
    row_ids.push_back(1);
    row_ids.push_back(3);

    auto chunk = gen_data_chunk(2, 0, &resource);
    auto data_chunk_ptr = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));

    auto session = components::session::session_id_t();
    test_wal.wal->write_physical_update(session, database_name, collection_name,
                                         std::move(row_ids), std::move(data_chunk_ptr), 2, 0);

    auto record = test_wal.wal->test_read_record(0);
    REQUIRE(record.is_physical());
    REQUIRE(record.record_type == wal_record_type::PHYSICAL_UPDATE);
    REQUIRE(record.collection_name.database == database_name);
    REQUIRE(record.collection_name.collection == collection_name);
    REQUIRE(record.physical_row_ids.size() == 2);
    REQUIRE(record.physical_row_ids[0] == 1);
    REQUIRE(record.physical_row_ids[1] == 3);
    REQUIRE(record.physical_data != nullptr);
    REQUIRE(record.physical_data->size() == 2);
    REQUIRE(record.physical_row_count == 2);
}

TEST_CASE("services::wal::commit_marker_write_and_read") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/commit_marker", &resource);

    auto session = components::session::session_id_t();
    uint64_t txn_id = 4611686018427387904ULL;
    test_wal.wal->commit_txn(session, txn_id);

    auto record = test_wal.wal->test_read_record(0);
    REQUIRE(record.is_commit_marker());
    REQUIRE(record.transaction_id == txn_id);
}
