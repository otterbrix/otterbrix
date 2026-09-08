#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <stdexcept>
#include <string>

// Three consecutive opens of the same directory must show the same rows.
//
// Written RED against config_wal::on: a middle run with the WAL switched off stamped a zero
// checkpoint floor into the sidecar (with no WAL there is no current_wal_id to stage, and
// agent_disk reads a zero as "floor known, and it is zero"), so the run after it replayed every
// segment above the previous truncation on top of already-durable rows. The symptom was not
// duplicated rows but a duplicated CATALOG: the third run answered "path: 'id' is ambiguous" and
// no scan of the table worked at all.
//
// The flag is gone, so the recipe cannot be written any more. What stays is the assertion it was
// built on -- reopening must not replay a tail the checkpoint already absorbed.

namespace {
    constexpr int kRows = 200;

    // The row count of a plain scan, not COUNT(*): a duplicated replay shows up as extra rows,
    // and size() is what the rest of these tests read.
    std::size_t count_rows(otterbrix::wrapper_dispatcher_t* d) {
        auto cur = test_helpers::exec(d, "SELECT id FROM adb.t;");
        if (!cur->is_success()) {
            INFO("scan refused: " << cur->get_error().what);
            REQUIRE(cur->is_success());
        }
        return cur->size();
    }
} // namespace

TEST_CASE("integration::cpp::replay_floor::reopening twice must not replay an absorbed tail") {
    const auto fixture = integration_fixture_path("test_replay_floor/db");

    INFO("run A: seed rows, clean shutdown");
    {
        auto config = test_create_config(fixture);
        test_clear_directory(config);
        config.log.level = log_t::level::off;

        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(test_helpers::exec(d, "CREATE DATABASE adb;")->is_success());
        REQUIRE(test_helpers::exec(d, "CREATE TABLE adb.t (id BIGINT);")->is_success());
        for (int i = 1; i <= kRows; ++i) {
            REQUIRE(test_helpers::exec(d, "INSERT INTO adb.t (id) VALUES (" + std::to_string(i) + ");")->is_success());
        }
        REQUIRE(count_rows(d) == static_cast<std::size_t>(kRows));
    }

    INFO("run B: reopen the same directory, read only");
    {
        auto config = test_create_config(fixture);
        config.log.level = log_t::level::off;

        test_spaces space(config);
        auto* d = space.dispatcher();
        CHECK(count_rows(d) == static_cast<std::size_t>(kRows));
    }

    INFO("run C: reopen again -- the tail must not be replayed a second time");
    {
        auto config = test_create_config(fixture);
        config.log.level = log_t::level::off;

        test_spaces space(config);
        auto* d = space.dispatcher();
        CHECK(count_rows(d) == static_cast<std::size_t>(kRows));
    }
}
