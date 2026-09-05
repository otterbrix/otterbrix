#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

// Persistence gaps: restart/crash scenarios that the write path must survive but (as of
// the branch point) does not. Each test encodes the DESIRED behavior; on HEAD they are
// expected to fail (red-first proof for the fixes that follow).
//
// 1a/1b. type_spec_lost_across_restart_{decimal,list} — the .otbx checkpoint serializes
//    each column type as ONE byte (data_table.cpp checkpoint():
//    writer.write<uint8_t>(col.type().type())), so DECIMAL precision/scale and LIST
//    child types never reach disk. load_from_disk rebuilds a bare complex_logical_type
//    from that byte; for DECIMAL, to_physical_type() then reinterpret_casts a
//    non-decimal extension as decimal_logical_type_extension (types.cpp) — the reloaded
//    values are garbage/UB-adjacent. The LIST variant additionally exposes the WAL chunk
//    codec (data_chunk_binary.cpp read_type_header has ARRAY and DECIMAL legs but NO
//    LIST leg), so a variadic-list INSERT record crashes WAL replay at startup. The two
//    variants are separate TEST_CASEs on purpose: the list crash is a process-killing
//    SIGSEGV and must not mask the decimal symptom.
// 2. default_lost_across_restart — column DEFAULTs are applied from the storage-layer
//    column list, and the restart rehydration rebuilds that list from pg_attribute
//    WITHOUT defaults (manager_disk_bootstrap.cpp: defs.emplace_back(name, type)).
// 3. create_then_kill_before_checkpoint — a freshly created .otbx that was never
//    checkpointed still has meta_block == INVALID_INDEX in its header; after a crash
//    (simulated by copying the live directory), reopening the copy must still start the
//    engine and expose the (empty) table.

using components::types::logical_type;

TEST_CASE("integration::cpp::test_persistence_gaps::type_spec_lost_across_restart_decimal") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence_gaps/type_spec_decimal");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
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
            REQUIRE(cur->value(0, 0).type().type() == logical_type::DECIMAL);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 1234);
            REQUIRE(cur->value(0, 1).value<int64_t>() == 5678);
        }
        {
            auto cur = exec("SELECT id FROM b.dec WHERE d = 12.34;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
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
            REQUIRE(cur->value(0, 0).type().type() == logical_type::DECIMAL);
            CHECK(cur->value(0, 0).value<int64_t>() == 1234);
            CHECK(cur->value(0, 1).value<int64_t>() == 5678);
        }
        {
            auto cur = exec("SELECT id FROM b.dec WHERE d = 12.34;");
            INFO("DECIMAL predicate must still match the stored value after restart");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == 1);
        }
    }
}

TEST_CASE("integration::cpp::test_persistence_gaps::type_spec_lost_across_restart_list") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence_gaps/type_spec_list");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
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
            auto v = cur->value(0, 0);
            REQUIRE(v.children().size() == 3);
            REQUIRE(v.children()[0].value<int64_t>() == 1);
            REQUIRE(v.children()[1].value<int64_t>() == 2);
            REQUIRE(v.children()[2].value<int64_t>() == 3);
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
            auto v = cur->value(0, 0);
            REQUIRE(v.children().size() == 3);
            CHECK(v.children()[0].value<int64_t>() == 1);
            CHECK(v.children()[1].value<int64_t>() == 2);
            CHECK(v.children()[2].value<int64_t>() == 3);
        }
    }
}

TEST_CASE("integration::cpp::test_persistence_gaps::default_lost_across_restart") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence_gaps/default_lost");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
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
            REQUIRE_FALSE(cur->value(0, 0).is_null());
            REQUIRE(cur->value(0, 0).value<int64_t>() == 5);
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
            REQUIRE(ins->size() == 1);
        }
        {
            auto cur = exec("SELECT c FROM b.t WHERE id = 2;");
            INFO("DEFAULT 5 must survive the restart: c must be 5, not NULL");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK_FALSE(cur->value(0, 0).is_null());
            if (!cur->value(0, 0).is_null()) {
                CHECK(cur->value(0, 0).value<int64_t>() == 5);
            }
        }

        // NOT NULL DEFAULT: after the restart the INSERT must still succeed AND land the
        // row (on HEAD the lost default makes the agent reject the row while the cursor
        // still reports success — "success, 0 rows").
        {
            auto ins = exec("INSERT INTO b.t2 (id) VALUES (1);");
            INFO("INSERT omitting a NOT NULL DEFAULT column after restart");
            REQUIRE(ins->is_success());
            CHECK(ins->size() == 1);
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
            CHECK_FALSE(cur->value(0, 0).is_null());
            if (!cur->value(0, 0).is_null()) {
                CHECK(cur->value(0, 0).value<int64_t>() == 7);
            }
        }
    }
}

TEST_CASE("integration::cpp::test_persistence_gaps::create_then_kill_before_checkpoint") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_persistence_gaps/crash_src");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
    config.log.level = log_t::level::off;

    // kill -9 simulation: copy the LIVE data directory while the engine is up; the scope
    // destructor's checkpoint then touches only the ORIGINAL, so the copy is exactly what
    // a crash right after the DDL would have left on disk.
    const std::filesystem::path crash_dir = "/tmp/otterbrix/integration/test_persistence_gaps/crash_copy";

    INFO("phase 1: DDL only, no checkpoint; copy the live directory (crash image)");
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        // Default-storage table, an explicit disk-storage table (its .otbx has never been
        // checkpointed => meta_block == INVALID_INDEX), and a computed (dynamic-schema)
        // table — all three must survive the crash.
        REQUIRE(exec("CREATE TABLE b.t (id BIGINT);")->is_success());
        REQUIRE(exec("CREATE TABLE b.td (id BIGINT) ;")->is_success());
        REQUIRE(exec("CREATE TABLE b.g ();")->is_success());

        std::filesystem::remove_all(crash_dir);
        std::filesystem::create_directories(crash_dir.parent_path());
        std::filesystem::copy(config.main_path, crash_dir, std::filesystem::copy_options::recursive);
    }

    // A7.6: record which of the crash image's .otbx files are never-checkpointed — headers
    // only, exactly BLOCK_START (12288) bytes, meta_block INVALID — BEFORE the reopen, so
    // the "right reason" assertions below can prove the engine really loaded them as empty
    // DISK tables (a real storage grows its own file on CHECKPOINT) rather than leaving
    // them unloaded and answering through the storage-less record branch (which never
    // touches the file again). Not every file is young: phase 1's row-threshold flushes
    // checkpoint busy system tables — but b.td took no DML, so its file must be in the set.
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
        crash_config.disk.on = true;
        crash_config.wal.on = true;
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

        // A7.6 "right reason" leg 1: the young table is ALIVE, not merely answered-around.
        // Write through it and checkpoint: only a genuinely loaded DISK storage takes the
        // rows and flushes them into the very .otbx that was young.
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

    // A7.6 "right reason" leg 2: the checkpoint physically reached the files that were
    // young. An unloaded storage (the old accident: a warning deep in the metadata reader,
    // rows answered by the record-only branch) leaves the .otbx at its header-only size
    // forever; a loaded-as-empty DISK storage writes its first root and the file grows.
    for (const auto& otbx : young_otbx) {
        INFO("checkpoint must have written the once-young file: " << otbx.string());
        CHECK(std::filesystem::file_size(otbx) > kNeverCheckpointedBytes);
    }

    INFO("phase 3: reopen the crash image again — the checkpointed rows must come back from disk");
    {
        auto crash_config = test_create_config(crash_dir);
        crash_config.disk.on = true;
        crash_config.wal.on = true;
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
            CHECK(cur->value(0, 0).value<int64_t>() == 1);
            CHECK(cur->value(0, 2).value<int64_t>() == 3);
        }
        {
            auto cur = exec("SELECT * FROM b.t;");
            REQUIRE(cur->is_success());
        }
    }

    // Recovery must not litter (A7.5): the engine owns the `table.otbx.*` sidecar namespace
    // and writes exactly the `.wal_id` sidecar (staged via `.wal_id.tmp`). No quarantine,
    // backup, or any other sidecar may appear in the crash image — a stray there would make
    // the next open refuse the table.
    for (const auto& entry : std::filesystem::recursive_directory_iterator(crash_dir)) {
        const auto name = entry.path().filename().string();
        if (name.rfind("table.otbx.", 0) != 0) {
            continue;
        }
        INFO("unexpected sidecar in the crash image: " << entry.path().string());
        CHECK((name == "table.otbx.wal_id" || name == "table.otbx.wal_id.tmp"));
    }
}
