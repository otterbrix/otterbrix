// Write-through puts data blocks into table.otbx from the very first filled segment, before any
// CHECKPOINT wrote a root. A crash image taken then is a file that is neither "young" (exactly
// BLOCK_START bytes) nor rooted; the reopen must still bring the table up and replay the journal.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

namespace {
    void copy_dir_as_crash(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::filesystem::remove_all(to);
        std::filesystem::create_directories(to.parent_path());
        std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
    }
} // namespace

TEST_CASE("integration::cpp::write_through_before_first_checkpoint::a_crash_image_reopens_and_replays") {
    auto config = test_create_config(integration_fixture_path("test_wt_first_ckpt/src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    const std::filesystem::path crash_dir = integration_fixture_path("test_wt_first_ckpt/crash");

    constexpr int kRows = 3000; // three row groups: filled segments AND closed row groups reach the file
    const std::string value(64, 'x');
    std::uintmax_t otbx_bytes = 0;
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE w;")->is_success());
        REQUIRE(exec("CREATE TABLE w.t (id bigint, payload text);")->is_success());
        for (int base = 0; base < kRows; base += 50) {
            std::string sql = "INSERT INTO w.t (id, payload) VALUES ";
            for (int i = 0; i < 50; ++i) {
                sql += (i ? ", (" : "(") + std::to_string(base + i) + ", '" + value + "')";
            }
            REQUIRE(exec(sql + ";")->is_success());
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(config.main_path)) {
            if (entry.path().filename() == "table.otbx") {
                otbx_bytes = std::max(otbx_bytes, std::filesystem::file_size(entry.path()));
            }
        }
        copy_dir_as_crash(config.main_path, crash_dir);
    }
    WARN("largest table.otbx in the crash image: " << otbx_bytes << " bytes (BLOCK_START is 12288)");
    REQUIRE(otbx_bytes > 12288); // the write-through did reach the file before any root

    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        auto cur = exec("SELECT COUNT(*) FROM w.t;");
        if (cur->is_error()) {
            WARN("reopen of the crash image: " << cur->get_error().what.c_str());
        }
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == kRows);
        REQUIRE(exec("INSERT INTO w.t (id, payload) VALUES (" + std::to_string(kRows) + ", 'tail');")->is_success());
        REQUIRE(exec("CHECKPOINT;")->is_success());
        auto after = exec("SELECT COUNT(*) FROM w.t;");
        REQUIRE(after->is_success());
        CHECK(after->value(0, 0).value<int64_t>() == kRows + 1);
    }
}
