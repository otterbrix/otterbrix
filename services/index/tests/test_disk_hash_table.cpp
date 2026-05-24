#include <catch2/catch.hpp>
#include <services/index/disk_hash_table.hpp>

using services::index::disk_hash_table_t;

TEST_CASE("services::index::disk_hash_table::put_get_erase_roundtrip") {
    std::filesystem::path path{"/tmp/index_disk/disk_hash_table_roundtrip.data"};
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 64);
    REQUIRE(table.put("alpha", 10, 1, 100));
    REQUIRE(table.put("beta", 20, 1, 200));

    auto alpha = table.get("alpha");
    REQUIRE(alpha.has_value());
    REQUIRE(alpha->value == 10);
    REQUIRE(alpha->log_file_id == 1);
    REQUIRE(alpha->log_offset == 100);

    auto beta = table.get("beta");
    REQUIRE(beta.has_value());
    REQUIRE(beta->value == 20);

    REQUIRE(table.erase("alpha"));
    REQUIRE_FALSE(table.get("alpha").has_value());
    REQUIRE(table.get("beta").has_value());
}

TEST_CASE("services::index::disk_hash_table::persist_reopen") {
    std::filesystem::path path{"/tmp/index_disk/disk_hash_table_persist.data"};
    std::filesystem::remove(path);

    {
        disk_hash_table_t table(path, 32);
        REQUIRE(table.put("k1", 111, 2, 1234));
        REQUIRE(table.put("k2", 222, 2, 5678));
        table.sync();
    }

    {
        disk_hash_table_t reopened(path, 32);
        auto v1 = reopened.get("k1");
        REQUIRE(v1.has_value());
        REQUIRE(v1->value == 111);
        REQUIRE(v1->log_file_id == 2);
        REQUIRE(v1->log_offset == 1234);
        auto v2 = reopened.get("k2");
        REQUIRE(v2.has_value());
        REQUIRE(v2->value == 222);
    }
}

TEST_CASE("services::index::disk_hash_table::long_key_prefix_and_loader") {
    std::filesystem::path path{"/tmp/index_disk/disk_hash_table_long_key.data"};
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    const std::string other_key = long_key + "y";

    disk_hash_table_t table(path, 8);
    REQUIRE(table.put(long_key, 777, 7, 700));

    auto with_loader = table.get(long_key, [&](uint32_t file_id, uint64_t offset, std::string& out) {
        REQUIRE(file_id == 7);
        REQUIRE(offset == 700);
        out = long_key;
        return true;
    });
    REQUIRE(with_loader.has_value());
    REQUIRE(with_loader->value == 777);

    auto mismatch = table.get(other_key, [&](uint32_t, uint64_t, std::string& out) {
        out = long_key;
        return true;
    });
    REQUIRE_FALSE(mismatch.has_value());
}
