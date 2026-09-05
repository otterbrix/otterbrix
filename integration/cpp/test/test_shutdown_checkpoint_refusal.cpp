#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/wal/wal_page.hpp>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unistd.h>

// THE SHUTDOWN CHECKPOINT'S ANSWER MUST NOT VANISH.
//
// ~base_otterbrix_t runs one last CHECKPOINT statement so the journal is folded into storage
// before the engine goes down. That statement HAS an error channel — execute_plan returns the
// cursor every refused step of the round fails into — and the destructor used to drop the
// cursor on the floor and wrap the call in a catch (...) that swallowed the rest. A failed
// final checkpoint is the difference between "the next start replays a journal" and "the next
// start replays nothing", and the operator deserves to read WHICH of the two happened.
//
// A destructor has no caller to answer, so the loudest honest channel it has is the error
// log. The case below makes the final CHECKPOINT fail deterministically — the WAL truncation
// step meets segments that will not open, and operator_checkpoint fails the statement on a
// refused truncate — and then reads the engine's log back.
//
// BEFORE: the log said "checkpoint complete"-nothing; no line reported the refusal.

using namespace test_helpers;

namespace {

    // Same seam shape as test_create_index_catchup_refusal.cpp: segment files refuse to open
    // while armed. Armed only around the destruction.
    class wal_open_refusal_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_open_refusal_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_open_refusal_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_open_refusal_t(const wal_open_refusal_t&) = delete;
        wal_open_refusal_t& operator=(const wal_open_refusal_t&) = delete;

        bool armed{false};
        uint64_t refusals{0};

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (armed && path.filename().string().compare(0, 4, "wal_") == 0) {
                ++refusals;
                return nullptr;
            }
            return inner;
        }
    };

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

TEST_CASE("integration::cpp::shutdown_checkpoint::a_refused_final_checkpoint_is_reported") {
    const auto dir = integration_fixture_path("test_shutdown_checkpoint_refusal") /
                     std::to_string(::getpid());
    auto config = test_create_config(dir);
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::err;
    // Small segments so the load below rolls the journal over: truncate_before never opens
    // the writer's CURRENT segment, so a single-segment journal would give the armed refusal
    // nothing to refuse.
    config.wal.max_segment_size = 16 * 1024;

    wal_open_refusal_t fault;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();

        REQUIRE(exec(d, "CREATE DATABASE db;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE db.t (id BIGINT, v BIGINT);")->is_success());
        auto load_batches = [&](int from, int upto) {
            for (int batch = from; batch < upto; ++batch) {
                std::string sql = "INSERT INTO db.t (id, v) VALUES ";
                for (int i = 0; i < 100; ++i) {
                    const int n = batch * 100 + i;
                    sql += "(" + std::to_string(n) + ", " + std::to_string(n * 2) + ")";
                    sql += (i == 99) ? ";" : ", ";
                }
                REQUIRE(exec(d, sql)->is_success());
            }
        };
        load_batches(0, 8);

        // A COMPLETED round first: the truncation floor is W-TORN — min over each table's
        // PREVIOUS checkpoint id — so the very first round answers 0 and skips the truncate
        // step entirely. The shutdown checkpoint below is then the SECOND round, whose floor
        // is this round's id, and its truncation actually opens the closed segments.
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        // Fresh journal traffic AFTER the completed round, enough to roll the journal over:
        // truncate_before never opens the writer's CURRENT segment, so at least one segment
        // must be closed again by the time the shutdown checkpoint runs.
        load_batches(8, 16);

        // The journal must have rolled over, otherwise the fault below meets nothing.
        std::size_t wal_segments = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(config.wal.path)) {
            if (entry.is_regular_file() && entry.path().filename().string().compare(0, 4, "wal_") == 0) {
                ++wal_segments;
            }
        }
        REQUIRE(wal_segments >= 2);

        // Arm for the destructor's final CHECKPOINT and tear the engine down.
        fault.armed = true;
    }
    fault.armed = false;

    INFO("the armed seam must actually have been met by the shutdown checkpoint's truncation");
    REQUIRE(fault.refusals > 0);

    INFO("a failed final checkpoint must leave an error-level line naming the shutdown checkpoint");
    REQUIRE(log_contains(config.log.path, "shutdown checkpoint"));
}
