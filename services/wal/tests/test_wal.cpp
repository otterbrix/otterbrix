#include <core/pmr.hpp>

#include <catch2/catch.hpp>

#include <absl/crc/crc32c.h>
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <components/log/log.hpp>
#include <fstream>
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
#include <services/wal/wal_reader.hpp>

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

TEST_CASE("services::wal::physical_insert_write_and_read") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/physical_insert", &resource);

    auto chunk = gen_data_chunk(5, 0, &resource);
    auto session = components::session::session_id_t();
    auto data_chunk_ptr = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
    test_wal.wal->write_physical_insert(session, database_name, collection_name, std::move(data_chunk_ptr), 0, 5, 0);

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
    test_wal.wal->write_physical_delete(session, database_name, collection_name, std::move(row_ids), 3, 0);

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
    test_wal.wal->write_physical_update(session,
                                        database_name,
                                        collection_name,
                                        std::move(row_ids),
                                        std::move(data_chunk_ptr),
                                        2,
                                        0);

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

TEST_CASE("services::wal::corrupted_record_detected") {
    auto resource = std::pmr::synchronized_pool_resource();
    const std::filesystem::path wal_path("/tmp/wal/corrupt_single");
    std::filesystem::remove_all(wal_path);
    std::filesystem::create_directories(wal_path);

    // Phase 1: write a valid INSERT record via WAL actor, keeping it alive to ensure file write
    {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        log.set_level(log_t::level::trace);
        auto scheduler = new core::non_thread_scheduler::scheduler_test_t(1, 1);
        configuration::config_wal config;
        config.path = wal_path;

        auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler, config, log);
        auto wal = actor_zeta::spawn<wal_replicate_t>(&resource, manager.get(), log, config);

        auto chunk = gen_data_chunk(5, 0, &resource);
        auto session = components::session::session_id_t();
        auto data_chunk_ptr = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
        wal->write_physical_insert(session, database_name, collection_name, std::move(data_chunk_ptr), 0, 5, 0);

        // wal/manager destroyed here, file handle closed, data flushed to disk
        delete scheduler;
    }

    // Phase 2: corrupt the WAL file — flip bytes in the payload
    auto wal_file = wal_path / ".wal_0_000000";
    REQUIRE(std::filesystem::exists(wal_file));
    {
        std::fstream fs(wal_file.string(), std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(fs.is_open());
        // Skip the 4-byte size header, then corrupt bytes 10-20 in the payload
        for (int offset = 10; offset < 20; ++offset) {
            fs.seekp(offset);
            char c;
            fs.read(&c, 1);
            c = static_cast<char>(~c); // flip all bits
            fs.seekp(offset);
            fs.write(&c, 1);
        }
        fs.flush();
    }

    // Phase 3: read via wal_reader_t — should detect corruption
    {
        configuration::config_wal config;
        config.path = wal_path;
        config.agent = 1;
        auto log = initialization_logger("wal_test_corrupt", "/tmp/docker_logs/");
        wal_reader_t reader(config, &resource, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        // Corrupted record should not appear in committed records
        REQUIRE(records.empty());
    }
}

TEST_CASE("services::wal::mixed_valid_corrupt_records") {
    auto resource = std::pmr::synchronized_pool_resource();
    const std::filesystem::path wal_path("/tmp/wal/corrupt_mixed");
    std::filesystem::remove_all(wal_path);
    std::filesystem::create_directories(wal_path);

    // Phase 1: write 3 INSERT records + COMMIT via WAL actor
    {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        log.set_level(log_t::level::trace);
        auto scheduler = new core::non_thread_scheduler::scheduler_test_t(1, 1);
        configuration::config_wal config;
        config.path = wal_path;

        auto manager = actor_zeta::spawn<manager_wal_replicate_t>(&resource, scheduler, config, log);
        auto wal = actor_zeta::spawn<wal_replicate_t>(&resource, manager.get(), log, config);

        auto session = components::session::session_id_t();
        uint64_t txn_id = 4611686018427387904ULL;

        for (int i = 0; i < 3; ++i) {
            auto chunk = gen_data_chunk(5, 0, &resource);
            auto data_chunk_ptr = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
            wal->write_physical_insert(session,
                                       database_name,
                                       collection_name,
                                       std::move(data_chunk_ptr),
                                       uint64_t(i * 5),
                                       5,
                                       txn_id);
        }
        wal->commit_txn(session, txn_id);

        delete scheduler;
    }

    // Phase 2: read the WAL file to find record boundaries, then corrupt the 2nd record
    auto wal_file = wal_path / ".wal_0_000000";
    REQUIRE(std::filesystem::exists(wal_file));
    {
        // Read the size of the first record to find the start of the second
        std::ifstream ifs(wal_file.string(), std::ios::binary);
        REQUIRE(ifs.is_open());

        // Read size_tt (4 bytes big-endian)
        uint8_t size_buf[4];
        ifs.read(reinterpret_cast<char*>(size_buf), 4);
        uint32_t first_size = (uint32_t(size_buf[0]) << 24) | (uint32_t(size_buf[1]) << 16) |
                              (uint32_t(size_buf[2]) << 8) | uint32_t(size_buf[3]);
        ifs.close();

        // Second record starts at: sizeof(size_tt) + first_size + sizeof(crc32_t)
        std::size_t second_record_offset = 4 + first_size + 4;

        // Corrupt the payload of the second record (offset into payload)
        std::fstream fs(wal_file.string(), std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(fs.is_open());
        // Skip size header of 2nd record (4 bytes), then corrupt 10 bytes in payload
        for (int i = 0; i < 10; ++i) {
            auto pos = static_cast<std::streamoff>(second_record_offset + 4 + 5 + static_cast<std::size_t>(i));
            fs.seekp(pos);
            char c;
            fs.seekg(pos);
            fs.read(&c, 1);
            c = static_cast<char>(~c);
            fs.seekp(pos);
            fs.write(&c, 1);
        }
        fs.flush();
    }

    // Phase 3: read via wal_reader_t — should get first record only, stop at corruption
    {
        configuration::config_wal config;
        config.path = wal_path;
        config.agent = 1;
        auto log = initialization_logger("wal_test_mixed", "/tmp/docker_logs/");
        wal_reader_t reader(config, &resource, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        // The corrupt 2nd record stops iteration.
        // The COMMIT marker comes AFTER the corrupt record, so never reached.
        // First record is valid but its txn was never committed (commit marker lost).
        // So we expect 0 committed records.
        REQUIRE(records.empty());
    }
}
