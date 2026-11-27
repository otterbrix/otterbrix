#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>
#include <core/pmr.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_page_reader.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

// A transaction that never committed leaves its records in the journal without a COMMIT marker. A later incarnation
// of the engine must not hand its id to a transaction that commits, or replay would take that marker for theirs.

using namespace test_helpers;

namespace {

    constexpr int64_t kRolledBackStatements = 4;
    constexpr int64_t kRowsPerStatement = 300; // a record over a page, so the earlier ones reach the segment file
    constexpr int64_t kCommittedId = 900'000;

    std::string fixture_root() { return integration_fixture_path("test_txn_id_reuse_replay").string(); }

    configuration::config crash_config(const std::filesystem::path& path) {
        auto config = test_create_config(path);
        config.log.level = log_t::level::off;
        // The journal has to stay the recovery source; an auto checkpoint would absorb it.
        config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;
        return config;
    }

    void copy_dir_as_crash(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::filesystem::remove_all(to);
        std::filesystem::create_directories(to.parent_path());
        std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
    }

    std::string rows_sql(int64_t from, int64_t count) {
        std::string sql = "INSERT INTO tr.t (id) VALUES ";
        for (int64_t row = 0; row < count; ++row) {
            if (row != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(from + row) + ")";
        }
        sql += ";";
        return sql;
    }

    std::vector<services::wal::record_t> journal_of(std::pmr::memory_resource* resource,
                                                    const std::filesystem::path& wal_dir) {
        std::vector<services::wal::record_t> records;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(wal_dir)) {
            if (!entry.is_regular_file() || entry.path().filename().string().rfind("wal_", 0) != 0) {
                continue;
            }
            services::wal::wal_page_reader_t reader(resource, entry.path());
            auto read = reader.read_all_records(0);
            REQUIRE_FALSE(read.has_error());
            for (auto& record : read.value()) {
                records.push_back(std::move(record));
            }
        }
        return records;
    }

    std::vector<int64_t> ids_of(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto cur = exec(dispatcher, "SELECT id FROM tr.t;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        std::vector<int64_t> ids;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto cell = cur->get<int64_t>(0, row);
            REQUIRE_FALSE(cell.has_error());
            ids.push_back(cell.value());
        }
        return ids;
    }

} // namespace

TEST_CASE("integration::cpp::txn_id_reuse_replay::a_rolled_back_transaction_stays_out_after_its_id_is_reused") {
    const std::filesystem::path first = fixture_root() + "/first";
    const std::filesystem::path second = fixture_root() + "/second";
    const std::filesystem::path third = fixture_root() + "/third";
    auto first_config = crash_config(first);
    test_clear_directory(first_config);

    {
        test_spaces space(first_config);
        auto* dispatcher = space.dispatcher();
        const auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE SCHEMA tr;")->is_success());
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE tr.t (id bigint);")->is_success());
        REQUIRE(dispatcher->execute_sql(session, "BEGIN;")->is_success());
        for (int64_t statement = 0; statement < kRolledBackStatements; ++statement) {
            REQUIRE(dispatcher->execute_sql(session, rows_sql(1 + statement * kRowsPerStatement, kRowsPerStatement))
                        ->is_success());
        }
        REQUIRE(dispatcher->execute_sql(session, "ROLLBACK;")->is_success());
        copy_dir_as_crash(first_config.main_path, second);
    }

    auto second_config = crash_config(second);
    {
        test_spaces space(second_config);
        auto* dispatcher = space.dispatcher();
        // The first statement after the restart: no read takes the reused id before it.
        REQUIRE(exec(dispatcher, "INSERT INTO tr.t (id) VALUES (" + std::to_string(kCommittedId) + ");")->is_success());
        copy_dir_as_crash(second_config.main_path, third);
    }

    // The rolled-back records are in the journal, and the committing transaction holds an id of its own.
    core::pmr::otterbrix_resource journal_resource;
    const auto journal = journal_of(&journal_resource, third / "wal");
    uint64_t committed_txn = 0;
    services::wal::id_t committed_marker = 0;
    for (const auto& record : journal) {
        if (record.is_commit_marker() && record.id > committed_marker) {
            committed_marker = record.id;
            committed_txn = record.transaction_id;
        }
    }
    REQUIRE(committed_txn != 0);
    int64_t rolled_back_records = 0;
    for (const auto& record : journal) {
        if (record.is_physical() && record.physical_row_count == static_cast<uint64_t>(kRowsPerStatement)) {
            ++rolled_back_records;
            CHECK(record.transaction_id != committed_txn);
        }
    }
    {
        INFO("NOT VACUOUS: the rolled-back transaction's records have to reach the journal");
        REQUIRE(rolled_back_records > 1);
    }

    auto third_config = crash_config(third);
    {
        test_spaces space(third_config);
        const auto ids = ids_of(space.dispatcher());
        CHECK(ids.size() == 1);
        CHECK(std::find(ids.begin(), ids.end(), kCommittedId) != ids.end());
    }
}
