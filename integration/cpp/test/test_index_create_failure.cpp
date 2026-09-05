#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

// A CREATE INDEX that cannot bring up its on-disk storage must fail the statement.
//
// It used to succeed: manager_index_t caught the failure and silently built an IN-MEMORY
// index in place of the disk one, reporting it at trace level only. The caller was told the
// index exists, so the durability the user asked for was gone with no way to notice short of
// reading the log, and every later restart brought the index up empty while the table kept
// claiming to be indexed.
//
// The failure is injected by planting a DIRECTORY where the storage file belongs:
// open(O_RDWR|O_CREAT) on a directory is EISDIR by POSIX, so open_file returns nullptr on
// every platform this builds on — no permission games, no root-dependent behaviour, and
// nothing a rebuild or a different filesystem can quietly turn green.
TEST_CASE("integration::cpp::test_index_create_failure::unopenable_disk_index_is_an_error") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_create_failure/unopenable");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE d;")->is_success());
    REQUIRE(exec("CREATE TABLE d.t (id bigint, k bigint);")->is_success());

    // A healthy hash index first: it both proves the path works and reveals the
    // per-table directory (<disk>/<table_oid>/<indexrelid>) without hardcoding an
    // oid the test cannot know. The layout is oid-keyed and carries no name, so
    // the healthy index dir is found by content: only bitcask leaves a CURRENT
    // marker.
    REQUIRE(exec("CREATE INDEX ok_idx ON d.t USING hash (k);")->is_success());

    std::filesystem::path ok_index_dir;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(config.disk.path)) {
        if (entry.is_directory() && std::filesystem::exists(entry.path() / "CURRENT")) {
            ok_index_dir = entry.path();
            break;
        }
    }
    REQUIRE_FALSE(ok_index_dir.empty());
    const auto oid_dir = ok_index_dir.parent_path();

    // The next CREATE INDEX statement allocates exactly one oid (its indexrelid),
    // and no other DDL runs in between, so the bad index's directory is
    // <table_dir>/<ok_oid + 1>. Plant a DIRECTORY at its hash_index.bin path.
    const auto ok_oid = std::stoull(ok_index_dir.filename().string());
    std::filesystem::create_directories(oid_dir / std::to_string(ok_oid + 1) / "hash_index.bin");

    auto cursor = exec("CREATE INDEX bad_idx ON d.t USING hash (k);");
    INFO("a disk index that cannot open its storage must not silently become an in-memory index");
    CHECK(cursor->is_error());
}
