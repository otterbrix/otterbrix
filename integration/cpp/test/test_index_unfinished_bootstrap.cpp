#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

// bootstrap_indexes_sync folds skip reasons into a PHASE 4 count; this stages the reachable one
// (unopenable storage). The other (ready_since == 0, unfinished backfill) can't be staged here:
// a CHECKPOINT over an open CREATE INDEX transaction leaves pg_index EMPTY in the crash image
// (probed empirically), since replay filters whole transactions by their COMMIT marker.

using namespace test_helpers;

namespace {

    bool log_contains(const std::filesystem::path& log_dir, const std::string& needle) {
        if (!std::filesystem::exists(log_dir)) {
            return false;
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(log_dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            std::ifstream in(entry.path());
            std::string line;
            while (std::getline(in, line)) {
                if (line.find(needle) != std::string::npos) {
                    return true;
                }
            }
        }
        return false;
    }

} // namespace

TEST_CASE("integration::cpp::index_unfinished_bootstrap::an_unopenable_index_is_counted_as_its_own_event") {
    const auto base = integration_fixture_path("test_index_unfinished_bootstrap") / std::to_string(::getpid());
    auto config = test_create_config(base);
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    std::filesystem::path index_dir;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE b;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE b.t (id BIGINT, k BIGINT);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX k_idx ON b.t USING hash (k);")->is_success());
        REQUIRE(exec(d, "INSERT INTO b.t (id, k) VALUES (1, 10), (2, 20), (3, 30);")->is_success());

        // The on-disk layout is oid-keyed with no index name, so find the directory by content.
        for (const auto& entry : std::filesystem::recursive_directory_iterator(config.disk.path)) {
            if (entry.is_directory() && std::filesystem::exists(entry.path() / "hash_index.bin")) {
                index_dir = entry.path();
                break;
            }
        }
        REQUIRE_FALSE(index_dir.empty());
    }

    const auto storage_file = index_dir / "hash_index.bin";
    std::filesystem::remove_all(storage_file);
    std::filesystem::create_directories(storage_file);
    REQUIRE(std::filesystem::is_directory(storage_file));

    // Trace level, so the PHASE 4 accounting line the assertion checks for gets logged.
    config.log.level = log_t::level::trace;
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto cur = exec(d, "SELECT id FROM b.t WHERE k = 20;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("an unopenable index storage is its own event and carries its own count");
    REQUIRE(log_contains(config.log.path, "skipped: unopenable storage"));

    std::filesystem::remove_all(base);
}
