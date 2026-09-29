// Crash-point matrix around the FIRST checkpoint of a table: a crash image (recursive copy of the
// live directory) is reopened and must come up with every committed row, at every point.
// OTTERBRIX_CRASH_IMAGE_KEEP=<dir> also copies each image there for offline inspection.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <components/table/storage/single_file_block_manager.hpp>

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

namespace {
    constexpr std::uintmax_t kBlockStart = components::table::storage::BLOCK_START;

    struct sql_t {
        otterbrix::wrapper_dispatcher_t* d;
        auto operator()(const std::string& sql) const {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        }
    };

    void copy_dir(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::filesystem::remove_all(to);
        std::filesystem::create_directories(to.parent_path());
        std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
    }

    void take_crash_image(const configuration::config& config, const std::filesystem::path& crash_dir, const char* tag) {
        copy_dir(config.main_path, crash_dir);
        if (const char* keep = std::getenv("OTTERBRIX_CRASH_IMAGE_KEEP")) {
            copy_dir(config.main_path, std::filesystem::path(keep) / tag);
        }
    }

    std::vector<std::filesystem::path> otbx_files(const std::filesystem::path& root) {
        std::vector<std::filesystem::path> out;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
            if (entry.path().filename() == "table.otbx") {
                out.push_back(entry.path());
            }
        }
        return out;
    }

    // User tables live under <root>/<db oid>/<table oid>/table.otbx with db oid >= 16384; system tables (db dir
    // "4") are megabytes right after bootstrap and must not be mistaken for the table under test.
    bool is_user_otbx(const std::filesystem::path& p) {
        const auto db_dir = p.parent_path().parent_path().filename().string();
        return std::stoull(db_dir) >= 16384;
    }

    std::uintmax_t largest_otbx(const std::filesystem::path& root) {
        std::uintmax_t best = 0;
        for (const auto& p : otbx_files(root)) {
            if (is_user_otbx(p)) {
                best = std::max(best, std::filesystem::file_size(p));
            }
        }
        return best;
    }

    std::filesystem::path user_otbx(const std::filesystem::path& root) {
        for (const auto& p : otbx_files(root)) {
            if (is_user_otbx(p) && std::filesystem::file_size(p) > kBlockStart) {
                return p;
            }
        }
        return {};
    }

    void insert_rows(const sql_t& exec, const std::string& table, int from, int to, const std::string& payload) {
        for (int base = from; base < to; base += 50) {
            std::string sql = "INSERT INTO " + table + " (id, payload) VALUES ";
            const int end = std::min(to, base + 50);
            for (int i = base; i < end; ++i) {
                sql += (i != base ? ", (" : "(") + std::to_string(i) + ", '" + payload + "')";
            }
            REQUIRE(exec(sql + ";")->is_success());
        }
    }

    int64_t count_rows(const sql_t& exec, const std::string& table) {
        auto cur = exec("SELECT COUNT(*) FROM " + table + ";");
        if (cur->is_error()) {
            WARN("count over " << table << ": " << cur->get_error().what.c_str());
        }
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        return cur->value(0, 0).value<int64_t>();
    }

    void overwrite_slot(const std::filesystem::path& otbx, uint64_t slot_offset, const std::vector<char>& bytes) {
        std::fstream f(otbx, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        f.seekp(static_cast<std::streamoff>(slot_offset));
        f.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        REQUIRE(f.good());
    }

    const std::string kPayload(64, 'x');
} // namespace

// P1: crash right after DDL (no DML) — the classic "young" image, size == BLOCK_START.
TEST_CASE("integration::cpp::crash_first_ckpt::P1_after_create_only") {
    auto config = test_create_config(integration_fixture_path("crash_first_ckpt/p1_src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    const auto crash_dir = integration_fixture_path("crash_first_ckpt/p1_crash");
    {
        test_spaces space(config);
        sql_t exec{space.dispatcher()};
        REQUIRE(exec("CREATE DATABASE w;")->is_success());
        REQUIRE(exec("CREATE TABLE w.t (id bigint, payload text);")->is_success());
        take_crash_image(config, crash_dir, "p1_after_create_only");
    }
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "w.t") == 0);
        insert_rows(exec, "w.t", 0, 10, kPayload);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        CHECK(count_rows(exec, "w.t") == 10);
    }
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "w.t") == 10);
    }
}

// P2: crash after a small DML (no filled segment, so no write-through yet) — rows only in the WAL.
TEST_CASE("integration::cpp::crash_first_ckpt::P2_after_small_dml") {
    auto config = test_create_config(integration_fixture_path("crash_first_ckpt/p2_src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    const auto crash_dir = integration_fixture_path("crash_first_ckpt/p2_crash");
    std::uintmax_t otbx_bytes = 0;
    {
        test_spaces space(config);
        sql_t exec{space.dispatcher()};
        REQUIRE(exec("CREATE DATABASE w;")->is_success());
        REQUIRE(exec("CREATE TABLE w.t (id bigint, payload text);")->is_success());
        insert_rows(exec, "w.t", 0, 20, kPayload);
        take_crash_image(config, crash_dir, "p2_after_small_dml");
        otbx_bytes = largest_otbx(config.main_path);
    }
    WARN("P2 largest table.otbx: " << otbx_bytes);
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "w.t") == 20);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        CHECK(count_rows(exec, "w.t") == 20);
    }
}

// P3: crash after write-through (filled segments reached the file), two databases, DELETE and UPDATE
// in the journal too, plus an index created before the DML.
TEST_CASE("integration::cpp::crash_first_ckpt::P3_after_write_through_multi_table_index") {
    auto config = test_create_config(integration_fixture_path("crash_first_ckpt/p3_src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    const auto crash_dir = integration_fixture_path("crash_first_ckpt/p3_crash");
    constexpr int kRows = 3000;
    std::uintmax_t image_bytes = 0;
    {
        test_spaces space(config);
        sql_t exec{space.dispatcher()};
        REQUIRE(exec("CREATE DATABASE a;")->is_success());
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE a.t (id bigint, payload text);")->is_success());
        REQUIRE(exec("CREATE TABLE b.u (id bigint, payload text);")->is_success());
        REQUIRE(exec("CREATE INDEX a_t_id ON a.t (id);")->is_success());
        insert_rows(exec, "a.t", 0, kRows, kPayload);
        insert_rows(exec, "b.u", 0, kRows, kPayload);
        REQUIRE(exec("DELETE FROM a.t WHERE id < 100;")->is_success());
        REQUIRE(exec("UPDATE b.u SET payload = 'updated' WHERE id = 7;")->is_success());
        take_crash_image(config, crash_dir, "p3_after_write_through");
        image_bytes = largest_otbx(config.main_path);
    }
    WARN("P3 largest table.otbx in the crash image: " << image_bytes);
    REQUIRE(image_bytes > kBlockStart);
    std::uintmax_t after_bytes = 0;
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "a.t") == kRows - 100);
        CHECK(count_rows(exec, "b.u") == kRows);
        {
            auto cur = exec("SELECT id FROM a.t WHERE id = 1234;");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 1);
        }
        {
            auto cur = exec("SELECT id FROM a.t WHERE id = 5;");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 0);
        }
        {
            auto cur = exec("SELECT payload FROM b.u WHERE id = 7;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell = cur->value(0, 0);
            CHECK(cell.value<std::string_view>() == "updated");
        }
        insert_rows(exec, "a.t", kRows, kRows + 10, kPayload);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        CHECK(count_rows(exec, "a.t") == kRows - 100 + 10);
        after_bytes = largest_otbx(crash_config.main_path);
    }
    // Orphaned write-through blocks must be reused, not stacked: the file may not double.
    WARN("P3 largest table.otbx after reopen + checkpoint: " << after_bytes << " (image " << image_bytes << ")");
    CHECK(after_bytes < 2 * image_bytes);
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "a.t") == kRows - 100 + 10);
        CHECK(count_rows(exec, "b.u") == kRows);
        auto cur = exec("SELECT id FROM a.t WHERE id = 3005;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }
}

// P4/P5: crash after the first CHECKPOINT, and after further write-through on top of a root.
TEST_CASE("integration::cpp::crash_first_ckpt::P4_P5_after_first_checkpoint_then_more_dml") {
    auto config = test_create_config(integration_fixture_path("crash_first_ckpt/p4_src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    const auto crash4 = integration_fixture_path("crash_first_ckpt/p4_crash");
    const auto crash5 = integration_fixture_path("crash_first_ckpt/p5_crash");
    constexpr int kRows = 3000;
    {
        test_spaces space(config);
        sql_t exec{space.dispatcher()};
        REQUIRE(exec("CREATE DATABASE w;")->is_success());
        REQUIRE(exec("CREATE TABLE w.t (id bigint, payload text);")->is_success());
        insert_rows(exec, "w.t", 0, kRows, kPayload);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        take_crash_image(config, crash4, "p4_after_first_checkpoint");
        insert_rows(exec, "w.t", kRows, 2 * kRows, kPayload);
        take_crash_image(config, crash5, "p5_write_through_after_root");
    }
    {
        auto crash_config = test_create_config(crash4);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "w.t") == kRows);
    }
    {
        auto crash_config = test_create_config(crash5);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "w.t") == 2 * kRows);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        CHECK(count_rows(exec, "w.t") == 2 * kRows);
    }
}

// P6: crash DURING the first checkpoint's header commit: a write-through image whose slot 1 holds a torn
// (checksum-invalid) header. Every row is still in the journal; the table must come up with all of them.
TEST_CASE("integration::cpp::crash_first_ckpt::P6_torn_first_header") {
    auto config = test_create_config(integration_fixture_path("crash_first_ckpt/p6_src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    const auto crash_dir = integration_fixture_path("crash_first_ckpt/p6_crash");
    constexpr int kRows = 3000;
    {
        test_spaces space(config);
        sql_t exec{space.dispatcher()};
        REQUIRE(exec("CREATE DATABASE w;")->is_success());
        REQUIRE(exec("CREATE TABLE w.t (id bigint, payload text);")->is_success());
        insert_rows(exec, "w.t", 0, kRows, kPayload);
        take_crash_image(config, crash_dir, "p6_torn_first_header");
    }
    const auto otbx = user_otbx(crash_dir);
    REQUIRE_FALSE(otbx.empty());
    std::vector<char> garbage(components::table::storage::SECTOR_SIZE);
    std::mt19937_64 rng(0x5EEDULL);
    for (auto& b : garbage) {
        b = static_cast<char>(rng());
    }
    overwrite_slot(otbx, components::table::storage::SECTOR_SIZE, garbage);
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "w.t") == kRows);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        CHECK(count_rows(exec, "w.t") == kRows);
    }
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "w.t") == kRows);
    }
}

// P7: the first CHECKPOINT committed, then its slot rots. With the mirror the table comes back from root 1; without
// it the file falls back to the CREATE-time header and the sidecar contradiction refuses the open (no data loss,
// no availability either).
TEST_CASE("integration::cpp::crash_first_ckpt::P7_slot_rot_after_first_checkpoint") {
    auto config = test_create_config(integration_fixture_path("crash_first_ckpt/p7_src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    const auto crash_dir = integration_fixture_path("crash_first_ckpt/p7_crash");
    constexpr int kRows = 3000;
    {
        test_spaces space(config);
        sql_t exec{space.dispatcher()};
        REQUIRE(exec("CREATE DATABASE w;")->is_success());
        REQUIRE(exec("CREATE TABLE w.t (id bigint, payload text);")->is_success());
        insert_rows(exec, "w.t", 0, kRows, kPayload);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        take_crash_image(config, crash_dir, "p7_slot_rot_after_first_checkpoint");
    }
    const auto otbx = user_otbx(crash_dir);
    REQUIRE_FALSE(otbx.empty());
    std::vector<char> garbage(components::table::storage::SECTOR_SIZE);
    std::mt19937_64 rng(0x7EEDULL);
    for (auto& b : garbage) {
        b = static_cast<char>(rng());
    }
    overwrite_slot(otbx, components::table::storage::SECTOR_SIZE, garbage);
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::warn;
        test_spaces space(crash_config);
        sql_t exec{space.dispatcher()};
        CHECK(count_rows(exec, "w.t") == kRows);
        REQUIRE(exec("CHECKPOINT;")->is_success());
        CHECK(count_rows(exec, "w.t") == kRows);
    }
}
