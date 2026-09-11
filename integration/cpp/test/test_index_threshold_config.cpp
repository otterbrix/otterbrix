#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>

// config.disk.bitcask_segment_record_limit must hold for an index created at runtime, not only one restored
// at bootstrap. Observed via bitcask's CURRENT file (segment ids grow from 2 on rotation), read only while
// the engine is up: a shutdown CHECKPOINT bulk-reloads every index with rotation suppressed.

namespace {

    std::filesystem::path find_bitcask_index_dir(const std::filesystem::path& disk_root) {
        for (const auto& e : std::filesystem::recursive_directory_iterator(disk_root)) {
            if (e.is_directory() && std::filesystem::exists(e.path() / "CURRENT")) {
                return e.path();
            }
        }
        return {};
    }

    uint64_t current_segment_id(const std::filesystem::path& index_dir) {
        std::ifstream input(index_dir / "CURRENT");
        uint64_t id = 0;
        input >> id;
        return input.fail() ? 0 : id;
    }

    constexpr uint64_t kFirstRegularSegmentId = 2;
    constexpr uint64_t kSegmentRecordLimit = 2;
    constexpr unsigned kRows = 12;
    constexpr uint64_t kMinRotations = kRows / kSegmentRecordLimit - 1;

} // namespace

TEST_CASE("integration::cpp::test_index_threshold_config::every_road_honours_the_configured_segment_limit") {
    auto config = test_create_config(integration_fixture_path("test_index_threshold_config/segments"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    config.disk.bitcask_segment_record_limit = kSegmentRecordLimit;

    std::filesystem::path index_dir;
    uint64_t runtime_rotations = 0;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX k_idx ON b.t USING hash (k);")->is_success());
        for (unsigned i = 0; i < kRows; ++i) {
            REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (" + std::to_string(i) + ", " + std::to_string(i) + ");")
                        ->is_success());
        }

        index_dir = find_bitcask_index_dir(config.disk.path);
        INFO("a USING hash index must own a bitcask directory");
        REQUIRE_FALSE(index_dir.empty());
        REQUIRE(current_segment_id(index_dir) >= kFirstRegularSegmentId);
        runtime_rotations = current_segment_id(index_dir) - kFirstRegularSegmentId;

        INFO("with bitcask_segment_record_limit=2 configured, a dozen committed inserts must "
             "have rotated the active segment; a CURRENT still naming the first segment means "
             "the runtime CREATE INDEX built its store from the backend's static default instead");
        CHECK(runtime_rotations >= kMinRotations);
    }

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        const auto before = current_segment_id(index_dir);
        for (unsigned i = 0; i < kRows; ++i) {
            REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (" + std::to_string(kRows + i) + ", " +
                         std::to_string(kRows + i) + ");")
                        ->is_success());
        }
        const auto bootstrap_rotations = current_segment_id(index_dir) - before;

        INFO("the same index, written through the same configuration, must rotate at the same "
             "rate whichever road raised its agent -- to within the one segment the two runs "
             "can differ by, because this run starts part-way through a segment the shutdown "
             "checkpoint's bulk reload left behind");
        CHECK(bootstrap_rotations >= kMinRotations);
        // Written as two additions rather than one subtraction: these are unsigned.
        CHECK(runtime_rotations + 1 >= bootstrap_rotations);
        CHECK(bootstrap_rotations + 1 >= runtime_rotations);
    }
}
