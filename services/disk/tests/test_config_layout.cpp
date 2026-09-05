#include <catch2/catch_test_macros.hpp>

#include <components/configuration/configuration.hpp>

#include <filesystem>
#include <type_traits>

// ---------------------------------------------------------------------------
// config_disk_path_layout — the storage root is `<base>/wal`, and it has to stay there.
//
// config_disk's constructor is the only way to build one — the struct is not an aggregate, and the
// constructor's default argument covers default-construction — so its `path(path / "wal")` is the
// single reachable initializer. Every database ever written therefore lives under `<base>/wal`,
// sharing the directory with the WAL segments.
//
// That is why "fixing the typo" by writing `path / "disk"` is not a fix but a silent relocation:
// the engine does not look for a database anywhere but `config_disk::path`, so a reopen finds an
// empty directory, bootstraps a fresh pg_catalog and reports success while the tables sit untouched
// next door. The Python binding — the shipped package — hands `Client(path)` straight to
// `config::create_config`, so this is exactly the configuration users' data is under.
//
// The layout works because the two trees interleave without colliding:
//
//     <base>/wal/WAL_ID
//     <base>/wal/<db_oid>/wal_<db_oid>_NNNNNN     WAL segments, regular files
//     <base>/wal/<db_oid>/<tbl_oid>/table.otbx    tables, one directory each
//     <base>/wal/<db_oid>/<tbl_oid>/table.otbx.wal_id
//
// Every WAL scan below `<base>/wal/<db_oid>` takes regular files only, and every disk scan takes
// directories with numeric names only, so neither sees the other's entries.
// ---------------------------------------------------------------------------

TEST_CASE("config_disk_path_layout") {
    const std::filesystem::path base{"/otterbrix-config-layout-probe"};
    const auto config = configuration::config::create_config(base);

    // The shipped layout. Moving `disk.path` orphans every existing database.
    REQUIRE(config.disk.path == base / "wal");
    REQUIRE(config.wal.path == base / "wal");
    REQUIRE(config.disk.path == config.wal.path);
    REQUIRE(config.log.path == base / "log");
    REQUIRE(config.main_path == base);

    // One initializer per path, and it is the constructor: a default-constructed
    // config_disk has to land where the CWD-rooted one does. A default member
    // initializer that came back and disagreed would fail here.
    REQUIRE_FALSE(std::is_aggregate_v<configuration::config_disk>);
    const configuration::config_disk defaulted;
    REQUIRE(defaulted.path == std::filesystem::current_path() / "wal");
    REQUIRE(defaulted.path == configuration::config_disk{std::filesystem::current_path()}.path);

    // config_wal::on stays: the WAL can genuinely be switched off and tests do it. There is no
    // config_disk::on, because it would select nothing.
    REQUIRE(config.wal.on);
}
