// A marker written before the dispatcher drain allocates commit_id can only carry commit_id 0; since replay
// keys committed transactions off that id (wal_reader.cpp), a refusal after that point still leaves a durable
// marker on disk, and a restart resurrects the rejected commit.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/wal/wal_reader.hpp>

#include <cstddef>
#include <filesystem>
#include <memory_resource>
#include <string>
#include <unistd.h>

using namespace test_helpers;

TEST_CASE("integration::cpp::ddl_commit_marker::carries_the_drained_commit_id") {
    auto config =
        make_test_config(integration_fixture_path("test_ddl_commit_marker_after_drain") / std::to_string(::getpid()));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE markers;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE markers.t (id bigint, v bigint);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO markers.t (id, v) VALUES (1, 10), (2, 20);")->is_success());

    std::pmr::synchronized_pool_resource pool;
    log_t quiet; // null logger: every log macro null-checks through should_log
    services::wal::wal_reader_t reader(&pool, config.wal, quiet);
    auto records = reader.read_committed_records(services::wal::id_t{0});
    REQUIRE_FALSE(records.has_error());

    std::size_t markers_total = 0;
    std::size_t markers_without_commit_id = 0;
    for (const auto& r : records.value()) {
        if (r.is_commit_marker() && r.transaction_id != 0) {
            ++markers_total;
            if (r.commit_id == 0) {
                ++markers_without_commit_id;
            }
        }
    }

    REQUIRE(markers_total >= 3);

    INFO("commit markers found: " << markers_total
                                  << ", of them carrying commit_id == 0: " << markers_without_commit_id);
    INFO("a marker with commit_id == 0 was written BEFORE the drain that allocates the id — i.e. "
         "before the first step that may still refuse the commit");
    CHECK(markers_without_commit_id == 0);
}
