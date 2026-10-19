#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

// Each test pins a restart/crash defect fixed on this branch (written RED before the fix).
//
// 1a/1b. The .otbx checkpoint used to serialize each column type as ONE byte, losing DECIMAL
//    scale and LIST child types on reload (data_table.cpp checkpoint(), now
//    types::encode_type_spec). The LIST case additionally crashed WAL replay at startup; split
//    into two TEST_CASEs so that SIGSEGV can't mask the decimal symptom.
// 2. Column DEFAULTs used to be re-applied from a storage-layer column list rebuilt from
//    pg_attribute WITHOUT defaults on restart, so a default silently became NULL after the
//    first session. Fixed by stamping the value onto node_insert above the journal
//    (build_insert_fill_list, reading pg_attribute.attdefspec) before operator_insert appends.
// 3. A freshly created, never-checkpointed .otbx (meta_block == INVALID_INDEX) must still load
//    and expose an empty table after a simulated crash.

using components::types::logical_type;

TEST_CASE("integration::cpp::test_persistence_gaps::type_spec_lost_across_restart_decimal") {
    auto config = test_create_config(integration_fixture_path("test_persistence_gaps/type_spec_decimal"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    INFO("phase 1: DECIMAL(10,2) disk table, verified values, CHECKPOINT");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.dec (id BIGINT, d DECIMAL(10,2)) ;")->is_success());
        REQUIRE(exec("INSERT INTO b.dec (id, d) VALUES (1, 12.34), (2, 56.78);")->is_success());

        // Record what the engine returns BEFORE the restart: DECIMAL(10,2) is INT64-backed
        // and the cursor exposes the scaled payload (value * 100).
        {
            auto cur = exec("SELECT d FROM b.dec ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->type_data()[0].type() == logical_type::DECIMAL);
            auto cell0 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell0.has_error());
            REQUIRE(cell0.value() == 1234);
            auto cell1 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell1.has_error());
            REQUIRE(cell1.value() == 5678);
        }
        {
            auto cur = exec("SELECT id FROM b.dec WHERE d = 12.34;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell2 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell2.has_error());
            REQUIRE(cell2.value() == 1);
        }

        REQUIRE(exec("CHECKPOINT;")->is_success());
    }

    INFO("phase 2: restart — the same values must round-trip exactly");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        {
            auto cur = exec("SELECT d FROM b.dec ORDER BY id;");
            INFO("DECIMAL(10,2) column read back after restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->type_data()[0].type() == logical_type::DECIMAL);
            auto cell3 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell3.has_error());
            CHECK(cell3.value() == 1234);
            auto cell4 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell4.has_error());
            CHECK(cell4.value() == 5678);
        }
        {
            auto cur = exec("SELECT id FROM b.dec WHERE d = 12.34;");
            INFO("DECIMAL predicate must still match the stored value after restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell5 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell5.has_error());
            CHECK(cell5.value() == 1);
        }
    }
}

TEST_CASE("integration::cpp::test_persistence_gaps::type_spec_lost_across_restart_list") {
    auto config = test_create_config(integration_fixture_path("test_persistence_gaps/type_spec_list"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    INFO("phase 1: BIGINT[] disk table, verified values, CHECKPOINT");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.lst (id BIGINT, l BIGINT[]) ;")->is_success());
        REQUIRE(exec("INSERT INTO b.lst (id, l) VALUES (1, ARRAY[1, 2, 3]);")->is_success());

        {
            auto cur = exec("SELECT l FROM b.lst WHERE id = 1;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell16 = cur->list_size(0, 0);
            REQUIRE_FALSE(cell16.has_error());
            REQUIRE(cell16.value() == 3);
            auto cell17 = cur->get<int64_t>(0, 0, {0});
            REQUIRE_FALSE(cell17.has_error());
            REQUIRE(cell17.value() == 1);
            auto cell18 = cur->get<int64_t>(0, 0, {1});
            REQUIRE_FALSE(cell18.has_error());
            REQUIRE(cell18.value() == 2);
            auto cell19 = cur->get<int64_t>(0, 0, {2});
            REQUIRE_FALSE(cell19.has_error());
            REQUIRE(cell19.value() == 3);
        }

        REQUIRE(exec("CHECKPOINT;")->is_success());
    }

    INFO("phase 2: restart — the list must round-trip exactly");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        {
            auto cur = exec("SELECT l FROM b.lst WHERE id = 1;");
            INFO("BIGINT[] column read back after restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell20 = cur->list_size(0, 0);
            REQUIRE_FALSE(cell20.has_error());
            REQUIRE(cell20.value() == 3);
            auto cell21 = cur->get<int64_t>(0, 0, {0});
            REQUIRE_FALSE(cell21.has_error());
            CHECK(cell21.value() == 1);
            auto cell22 = cur->get<int64_t>(0, 0, {1});
            REQUIRE_FALSE(cell22.has_error());
            CHECK(cell22.value() == 2);
            auto cell23 = cur->get<int64_t>(0, 0, {2});
            REQUIRE_FALSE(cell23.has_error());
            CHECK(cell23.value() == 3);
        }
    }
}

TEST_CASE("integration::cpp::test_persistence_gaps::default_lost_across_restart") {
    auto config = test_create_config(integration_fixture_path("test_persistence_gaps/default_lost"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    INFO("phase 1: tables with DEFAULT / NOT NULL DEFAULT, defaults verified, CHECKPOINT");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.t (id BIGINT, c BIGINT DEFAULT 5);")->is_success());
        REQUIRE(exec("CREATE TABLE b.t2 (id BIGINT, c BIGINT NOT NULL DEFAULT 7);")->is_success());
        REQUIRE(exec("INSERT INTO b.t (id) VALUES (1);")->is_success());

        // The default applies within the creating session.
        {
            auto cur = exec("SELECT c FROM b.t WHERE id = 1;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell6 = cur->is_null(0, 0);
            REQUIRE_FALSE(cell6.has_error());
            REQUIRE_FALSE(cell6.value());
            auto cell7 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell7.has_error());
            REQUIRE(cell7.value() == 5);
        }

        REQUIRE(exec("CHECKPOINT;")->is_success());
    }

    INFO("phase 2: restart — DEFAULTs must still apply to new INSERTs");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        {
            auto ins = exec("INSERT INTO b.t (id) VALUES (2);");
            REQUIRE(ins->is_success());
            REQUIRE(ins->affected_rows() == 1);
        }
        {
            auto cur = exec("SELECT c FROM b.t WHERE id = 2;");
            INFO("DEFAULT 5 must survive the restart: c must be 5, not NULL");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell8 = cur->is_null(0, 0);
            REQUIRE_FALSE(cell8.has_error());
            CHECK_FALSE(cell8.value());
            auto cell9 = cur->is_null(0, 0);
            REQUIRE_FALSE(cell9.has_error());
            if (!cell9.value()) {
                auto cell10 = cur->get<int64_t>(0, 0);
                REQUIRE_FALSE(cell10.has_error());
                CHECK(cell10.value() == 5);
            }
        }

        // A lost default made the agent reject the row while the cursor still reported
        // success ("success, 0 rows"), so the row count is checked separately from the verdict.
        {
            auto ins = exec("INSERT INTO b.t2 (id) VALUES (1);");
            INFO("INSERT omitting a NOT NULL DEFAULT column after restart");
            REQUIRE(ins->is_success());
            CHECK(ins->affected_rows() == 1);
        }
        {
            auto cur = exec("SELECT id FROM b.t2;");
            INFO("the INSERT above must have actually landed one row");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 1);
        }
        {
            auto cur = exec("SELECT c FROM b.t2 WHERE id = 1;");
            INFO("NOT NULL DEFAULT 7 must survive the restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            auto cell11 = cur->is_null(0, 0);
            REQUIRE_FALSE(cell11.has_error());
            CHECK_FALSE(cell11.value());
            auto cell12 = cur->is_null(0, 0);
            REQUIRE_FALSE(cell12.has_error());
            if (!cell12.value()) {
                auto cell13 = cur->get<int64_t>(0, 0);
                REQUIRE_FALSE(cell13.has_error());
                CHECK(cell13.value() == 7);
            }
        }
    }
}

TEST_CASE("integration::cpp::test_persistence_gaps::create_then_kill_before_checkpoint") {
    auto config = test_create_config(integration_fixture_path("test_persistence_gaps/crash_src"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    // kill -9 simulation: copy the LIVE directory while the engine is up, so the copy is
    // exactly what a crash right after the DDL would have left on disk.
    const std::filesystem::path crash_dir = integration_fixture_path("test_persistence_gaps/crash_copy");

    INFO("phase 1: DDL only, no checkpoint; copy the live directory (crash image)");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        // Default-storage, explicit disk-storage (never checkpointed), and computed
        // (dynamic-schema) tables must all survive the crash.
        REQUIRE(exec("CREATE TABLE b.t (id BIGINT);")->is_success());
        REQUIRE(exec("CREATE TABLE b.td (id BIGINT) ;")->is_success());
        REQUIRE(exec("CREATE TABLE b.g ();")->is_success());

        std::filesystem::remove_all(crash_dir);
        std::filesystem::create_directories(crash_dir.parent_path());
        std::filesystem::copy(config.main_path, crash_dir, std::filesystem::copy_options::recursive);
    }

    // Never-checkpointed .otbx files are exactly BLOCK_START (12288) bytes, header only,
    // meta_block INVALID -- recorded before the reopen so the checks below can tell a
    // genuinely loaded empty DISK table (grows on CHECKPOINT) from one left unloaded and
    // answered by the storage-less record branch. b.td took no DML, so it must be here.
    constexpr std::uintmax_t kNeverCheckpointedBytes = 12288; // storage::BLOCK_START
    std::vector<std::filesystem::path> young_otbx;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(crash_dir)) {
        if (entry.path().filename() == "table.otbx" &&
            std::filesystem::file_size(entry.path()) == kNeverCheckpointedBytes) {
            young_otbx.push_back(entry.path());
        }
    }
    INFO("the never-DML'd b.td (at least) must carry the never-checkpointed signature");
    REQUIRE_FALSE(young_otbx.empty());

    INFO("phase 2: reopen the crash image — the engine must start and every table must be readable (0 rows)");
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::off;

        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        {
            auto cur = exec("SELECT * FROM b.t;");
            INFO("default-storage table after crash-before-checkpoint");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 0);
        }
        {
            auto cur = exec("SELECT * FROM b.td;");
            INFO("disk-storage table (never-checkpointed .otbx) after crash-before-checkpoint");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 0);
        }
        {
            auto cur = exec("SELECT * FROM b.g;");
            INFO("computed (dynamic-schema) table after crash-before-checkpoint");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 0);
        }

        // The young table must be ALIVE, not merely answered-around: only a genuinely
        // loaded DISK storage takes these rows and flushes them into the young .otbx.
        {
            auto cur = exec("INSERT INTO b.td (id) VALUES (1), (2), (3);");
            INFO("INSERT into the never-checkpointed disk table after the crash reopen");
            REQUIRE(cur->is_success());
        }
        {
            auto cur = exec("SELECT * FROM b.td ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
        {
            auto cur = exec("CHECKPOINT;");
            INFO("first-ever checkpoint of the crash image");
            REQUIRE(cur->is_success());
        }
    }

    // An unloaded storage (rows answered by the record-only branch) leaves the .otbx at its
    // header-only size forever; a loaded-as-empty DISK storage writes its first root and grows.
    for (const auto& otbx : young_otbx) {
        INFO("checkpoint must have written the once-young file: " << otbx.string());
        CHECK(std::filesystem::file_size(otbx) > kNeverCheckpointedBytes);
    }

    INFO("phase 3: reopen the crash image again — the checkpointed rows must come back from disk");
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.log.level = log_t::level::off;

        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        {
            auto cur = exec("SELECT * FROM b.td ORDER BY id;");
            INFO("the once-young disk table after write + checkpoint + restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            auto cell14 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell14.has_error());
            CHECK(cell14.value() == 1);
            auto cell15 = cur->get<int64_t>(0, 2);
            REQUIRE_FALSE(cell15.has_error());
            CHECK(cell15.value() == 3);
        }
        {
            auto cur = exec("SELECT * FROM b.t;");
            REQUIRE(cur->is_success());
        }
    }

    // The engine owns the `table.otbx.*` sidecar namespace and writes only `.wal_id`
    // (staged via `.wal_id.tmp`); a stray sidecar here would make the next open refuse the
    // table.
    for (const auto& entry : std::filesystem::recursive_directory_iterator(crash_dir)) {
        const auto name = entry.path().filename().string();
        if (name.rfind("table.otbx.", 0) != 0) {
            continue;
        }
        INFO("unexpected sidecar in the crash image: " << entry.path().string());
        CHECK((name == "table.otbx.wal_id" || name == "table.otbx.wal_id.tmp"));
    }
}
