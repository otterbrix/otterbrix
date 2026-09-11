#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <string>
#include <vector>

// The index TYPE (btree vs hash) must survive a restart, or a `USING hash` index could come back as a btree
// pointed at the bitcask directory; post-restart writes must grow the bitcask segments, not create metadata.

namespace {

    bool has_bitcask_artefacts(const std::filesystem::path& dir) { return std::filesystem::exists(dir / "CURRENT"); }

    std::uintmax_t bitcask_segment_bytes(const std::filesystem::path& dir) {
        std::uintmax_t total = 0;
        for (const auto& e : std::filesystem::directory_iterator(dir)) {
            if (!e.is_regular_file())
                continue;
            const auto name = e.path().filename().string();
            if (name.rfind("bitcask.", 0) == 0 && e.path().extension() == ".data") {
                total += e.file_size();
            }
        }
        return total;
    }

    std::filesystem::path find_index_dir(const std::filesystem::path& disk_root) {
        for (const auto& e : std::filesystem::recursive_directory_iterator(disk_root)) {
            if (e.is_directory() && has_bitcask_artefacts(e.path())) {
                return e.path();
            }
        }
        return {};
    }

} // namespace

TEST_CASE("integration::cpp::test_index_type_persistence::hash_index_type_survives_restart") {
    auto config = test_create_config(integration_fixture_path("test_index_type_persistence/hash_restart"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    std::filesystem::path index_dir;
    std::uintmax_t bytes_before = 0;

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
        REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (1, 10), (2, 20), (3, 30);")->is_success());
    }

    index_dir = find_index_dir(config.disk.path);
    INFO("a USING hash index must own a bitcask directory before the restart");
    REQUIRE_FALSE(index_dir.empty());
    bytes_before = bitcask_segment_bytes(index_dir);
    REQUIRE(bytes_before > 0);
    REQUIRE_FALSE(std::filesystem::exists(index_dir / "metadata"));

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (4, 40), (5, 50), (6, 60);")->is_success());
        auto cur = exec("SELECT id FROM b.t WHERE k = 40;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }

    INFO("post-restart writes must land in the bitcask segments the hash index owns");
    CHECK(bitcask_segment_bytes(index_dir) > bytes_before);

    INFO("no b+tree may take over the hash index directory across a restart");
    CHECK_FALSE(std::filesystem::exists(index_dir / "metadata"));
}

// DATE/TIME are physically INT32/INT64 counters whose logical type crosses a restart via pg_attribute; losing
// it doesn't announce itself, since raw counters still order among themselves under the wrong tag.
namespace {

    std::string type_persistence_plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto cell = cur->value(0, r);
            out += std::string(cell.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

} // namespace

TEST_CASE("integration::cpp::test_index_type_persistence::temporal_key_type_survives_restart") {
    auto config = test_create_config(integration_fixture_path("test_index_type_persistence/temporal_restart"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    const char* before[] = {
        "(1, DATE '2024-03-15', TIME '12:30:00')",
        "(2, DATE '2024-01-01', TIME '08:00:00')",
        "(3, DATE '2024-12-31', TIME '23:59:00')",
    };
    const char* after[] = {
        "(4, DATE '2024-02-01', TIME '09:15:00')",
        "(5, DATE '2024-06-30', TIME '18:45:00')",
    };

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE t;")->is_success());
        REQUIRE(exec("CREATE TABLE t.ti (id BIGINT, d DATE, tm TIME);")->is_success());
        // Unindexed oracle: a wrong answer can't pass by both sides being wrong the same way.
        REQUIRE(exec("CREATE TABLE t.tp (id BIGINT, d DATE, tm TIME);")->is_success());
        REQUIRE(exec("CREATE INDEX i_d ON t.ti (d);")->is_success());
        REQUIRE(exec("CREATE INDEX i_tm ON t.ti (tm);")->is_success());

        for (const char* row : before) {
            for (const char* table : {"t.ti", "t.tp"}) {
                const std::string sql = std::string{"INSERT INTO "} + table + " (id, d, tm) VALUES " + row + ";";
                INFO(sql);
                REQUIRE(exec(sql)->is_success());
            }
        }
    }

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        for (const char* row : after) {
            for (const char* table : {"t.ti", "t.tp"}) {
                const std::string sql = std::string{"INSERT INTO "} + table + " (id, d, tm) VALUES " + row + ";";
                INFO(sql);
                REQUIRE(exec(sql)->is_success());
            }
        }

        const auto probe = [&](const std::string& predicate, std::size_t expected) {
            {
                auto plan = exec("EXPLAIN SELECT id FROM t.ti WHERE " + predicate + ";");
                REQUIRE(plan->is_success());
                const auto text = type_persistence_plan_text(plan);
                INFO("predicate: " << predicate << "\nplan:\n" << text);
                INFO("a Seq Scan answers out of the heap and would pass with a broken index");
                REQUIRE(text.find("Index Scan") != std::string::npos);
            }
            auto indexed = exec("SELECT id FROM t.ti WHERE " + predicate + ";");
            REQUIRE(indexed->is_success());
            auto heap = exec("SELECT id FROM t.tp WHERE " + predicate + ";");
            REQUIRE(heap->is_success());
            INFO("predicate: " << predicate << " -- index " << indexed->size() << ", unindexed twin " << heap->size()
                               << ", expected " << expected);
            CHECK(heap->size() == expected);
            CHECK(indexed->size() == expected);
        };

        probe("d = DATE '2024-03-15'", 1);
        probe("tm = TIME '12:30:00'", 1);
        probe("d = DATE '2024-06-30'", 1);
        probe("tm = TIME '18:45:00'", 1);
        probe("d = DATE '2020-05-05'", 0);

        probe("d < DATE '2024-03-15'", 2);
        probe("d >= DATE '2024-03-15'", 3);
        probe("tm > TIME '09:15:00'", 3);
        probe("tm <= TIME '09:15:00'", 2);
    }
}
