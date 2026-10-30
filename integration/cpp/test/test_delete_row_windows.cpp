// A DELETE of otterbrix's own table names its rows to the disk in windows of at most DEFAULT_VECTOR_CAPACITY numbers;
// the windows are one message and one journal record, and RETURNING gives every row in the order of the scan.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/wal/wal_reader.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <unistd.h>
#include <vector>

using namespace test_helpers;

TEST_CASE("integration::cpp::delete_row_windows::one_journal_record_for_many_windows") {
    auto config = make_test_config(integration_fixture_path("test_delete_row_windows") / std::to_string(::getpid()));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr int64_t n = 1500;
    REQUIRE(exec(dispatcher, "CREATE SCHEMA windows;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE windows.t (id bigint);")->is_success());
    for (int64_t first = 1; first <= n; first += 500) {
        std::string insert = "INSERT INTO windows.t (id) VALUES ";
        for (int64_t id = first; id < first + 500; ++id) {
            insert += (id == first ? "(" : ", (") + std::to_string(id) + ")";
        }
        REQUIRE(exec(dispatcher, insert + ";")->is_success());
    }

    auto deleted = exec(dispatcher, "DELETE FROM windows.t RETURNING id;");
    REQUIRE(deleted->is_success());
    std::vector<int64_t> returned;
    for (const auto& chunk : deleted->chunks()) {
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            returned.push_back(chunk.get_value<int64_t>(0, row));
        }
    }
    std::vector<int64_t> scan_order;
    for (int64_t id = 1; id <= n; ++id) {
        scan_order.push_back(id);
    }
    CHECK(returned == scan_order);

    std::pmr::synchronized_pool_resource pool;
    log_t quiet;
    services::wal::wal_reader_t reader(&pool, config.wal, quiet);
    auto records = reader.read_committed_records();
    REQUIRE_FALSE(records.has_error());
    std::vector<std::size_t> delete_records;
    for (const auto& r : records.value()) {
        if (r.record_type == services::wal::wal_record_type::PHYSICAL_DELETE &&
            r.table_oid >= components::catalog::FIRST_USER_OID) {
            delete_records.push_back(r.physical_row_ids.size());
        }
    }
    CHECK(delete_records == std::vector<std::size_t>{static_cast<std::size_t>(n)});
}
