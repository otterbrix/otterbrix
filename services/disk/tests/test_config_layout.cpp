#include <catch2/catch_test_macros.hpp>

#include <components/configuration/configuration.hpp>

#include <filesystem>
#include <type_traits>

// config_disk and config_wal deliberately share <base>/wal; renaming it would orphan every
// existing database instead of fixing anything.

TEST_CASE("config_disk_path_layout") {
    const std::filesystem::path base{"/otterbrix-config-layout-probe"};
    const auto config = configuration::config::create_config(base);

    REQUIRE(config.disk.path == base / "wal");
    REQUIRE(config.wal.path == base / "wal");
    REQUIRE(config.disk.path == config.wal.path);
    REQUIRE(config.log.path == base / "log");
    REQUIRE(config.main_path == base);

    REQUIRE_FALSE(std::is_aggregate_v<configuration::config_disk>);
    const configuration::config_disk defaulted;
    REQUIRE(defaulted.path == std::filesystem::current_path() / "wal");
    REQUIRE(defaulted.path == configuration::config_disk{std::filesystem::current_path()}.path);

    // WAL can genuinely be switched off; disk storage has no equivalent switch, since it would
    // select nothing.
    REQUIRE(config.wal.on);
}
