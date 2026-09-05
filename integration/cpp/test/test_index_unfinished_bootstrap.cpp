#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

// THE TWO REASONS BOOTSTRAP SKIPS AN INDEX ARE DIFFERENT EVENTS AND MUST NOT SHARE A COUNT.
//
// bootstrap_indexes_sync used to fold "the backfill never committed" (re-issue CREATE INDEX)
// and "the index storage would not open" (may heal on its own — a permission, a bad disk)
// into ONE aggregate number in the PHASE 4 trace line, so the operator reading the log could
// not tell which of the two an affected start had. This case stages the reachable half — an
// index whose on-disk storage is unopenable (the test_index_bootstrap_failure convention: a
// directory where the storage file belongs) — and reads the reopened engine's log back.
//
// The OTHER half, ready_since == 0, is defended by its own loud per-row error in the same
// function, but the state cannot be staged from inside the current tree: CREATE INDEX is one
// transaction (metadata + backfill + flip), replay filters whole transactions by their COMMIT
// marker, and a checkpoint does not flush another transaction's in-flight catalog rows —
// probed empirically: a CHECKPOINT taken over an open CREATE INDEX transaction leaves the
// pg_index storage of the crash image EMPTY.
//
// BEFORE: the PHASE 4 line read "(1 skipped: unfinished build or unopenable storage)".

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
    const auto base = integration_fixture_path("test_index_unfinished_bootstrap") /
                      std::to_string(::getpid());
    auto config = test_create_config(base);
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    std::filesystem::path index_dir;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE b;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE b.t (id BIGINT, k BIGINT);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX k_idx ON b.t USING hash (k);")->is_success());
        REQUIRE(exec(d, "INSERT INTO b.t (id, k) VALUES (1, 10), (2, 20), (3, 30);")->is_success());

        // The on-disk layout is oid-keyed and carries no index name; find the index
        // directory by content (it owns the hash storage file).
        for (const auto& entry : std::filesystem::recursive_directory_iterator(config.disk.path)) {
            if (entry.is_directory() && std::filesystem::exists(entry.path() / "hash_index.bin")) {
                index_dir = entry.path();
                break;
            }
        }
        REQUIRE_FALSE(index_dir.empty());
    }

    // The engine is down; make the per-index storage unopenable for the next start — a
    // directory where a regular file is expected.
    const auto storage_file = index_dir / "hash_index.bin";
    std::filesystem::remove_all(storage_file);
    std::filesystem::create_directories(storage_file);
    REQUIRE(std::filesystem::is_directory(storage_file));

    // Reopen at trace level: the PHASE 4 accounting line is the assertion target.
    config.log.level = log_t::level::trace;
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        // The skip must not cost the start or the table.
        auto cur = exec(d, "SELECT id FROM b.t WHERE k = 20;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    INFO("an unopenable index storage is its own event and carries its own count");
    REQUIRE(log_contains(config.log.path, "skipped: unopenable storage"));

    std::filesystem::remove_all(base);
}
