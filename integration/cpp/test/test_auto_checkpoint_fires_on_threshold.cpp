#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <services/wal/manager_wal_replicate.hpp>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

// The byte threshold is checked only inside commit_txn (manager_wal_replicate.cpp), and with the
// WAL switched off commit_txn short-circuits before the trigger -- so the 16 MB default, and any
// value a user sets, never fires. The single checkpoint left is the one in base_spaces's
// destructor, which logs its refusal instead of returning it.
//
// The flag is gone now; what stays is the assertion it was written for: crossing
// auto_checkpoint_threshold_bytes starts a round.

namespace {
    constexpr int kRowsPerStatement = 64;
    constexpr int kPayloadChars = 512;
    constexpr int kMaxStatements = 400;
    constexpr std::uintmax_t kThresholdBytes = 256 * 1024;

    void insert_a_batch(otterbrix::wrapper_dispatcher_t* d, int64_t& next_id) {
        const std::string payload(kPayloadChars, 'x');
        std::string sql = "INSERT INTO adb.pad (id, payload) VALUES ";
        for (int i = 0; i < kRowsPerStatement; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(next_id) + ", '" + payload + "')";
            ++next_id;
        }
        sql += ";";
        REQUIRE(test_helpers::exec(d, sql)->is_success());
    }
} // namespace

TEST_CASE("integration::cpp::auto_checkpoint::the byte threshold starts a round") {
    auto config = test_create_config(integration_fixture_path("test_auto_checkpoint_threshold/fires"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    config.wal.auto_checkpoint_threshold_bytes = kThresholdBytes;

    services::wal::reset_auto_checkpoint_rounds();

    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(test_helpers::exec(d, "CREATE DATABASE adb;")->is_success());
    REQUIRE(test_helpers::exec(d, "CREATE TABLE adb.pad (id BIGINT, payload TEXT);")->is_success());

    int64_t next_id = 1;
    for (int i = 0; i < kMaxStatements && services::wal::auto_checkpoint_rounds() == 0; ++i) {
        insert_a_batch(d, next_id);
    }

    // The round is started by a self-send, so it completes after the committing statement returns.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (services::wal::auto_checkpoint_rounds() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    INFO("rows written: " << (next_id - 1) << ", threshold: " << kThresholdBytes << " bytes");
    CHECK(services::wal::auto_checkpoint_rounds() > 0);
}
