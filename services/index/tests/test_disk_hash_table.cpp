#include <catch2/catch.hpp>
#include <services/index/disk_hash_table.hpp>

using services::index::disk_hash_table_t;

namespace {
    std::filesystem::path mk_path(const std::string& name) {
        const auto dir = std::filesystem::path("/tmp/index_disk");
        std::filesystem::create_directories(dir);
        return dir / name;
    }
} // namespace

TEST_CASE("services::index::disk_hash_table::put_get_erase_roundtrip") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_roundtrip.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 64, true, &resource);
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
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_persist.data");
    std::filesystem::remove(path);

    {
        disk_hash_table_t table(path, 32, true, &resource);
        REQUIRE(table.put("k1", 111, 2, 1234));
        REQUIRE(table.put("k2", 222, 2, 5678));
        table.sync();
    }

    {
        disk_hash_table_t reopened(path, 32, true, &resource);
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

TEST_CASE("services::index::disk_hash_table::multiple_values_per_key") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_multi_values.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 32, true, &resource);
    REQUIRE(table.put("dup", 10, 1, 100));
    REQUIRE(table.put("dup", 20, 2, 200));
    REQUIRE(table.put("dup", 10, 3, 300));

    const auto values = table.get_all("dup");
    REQUIRE(values.size() == 3);
}

TEST_CASE("services::index::disk_hash_table::long_key_prefix_and_loader") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_long_key.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    const std::string other_key = long_key + "y";

    disk_hash_table_t table(path, 8, true, &resource);
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

TEST_CASE("services::index::disk_hash_table::rehash_preserves_entries") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_rehash.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 4, true, &resource);
    REQUIRE(table.bucket_count() == 4);

    for (int i = 0; i < 300; ++i) {
        const auto key = "k." + std::to_string(i);
        REQUIRE(table.put(key, static_cast<int64_t>(i), 1, static_cast<uint64_t>(1000 + i)));
    }

    REQUIRE(table.rehash(128));
    REQUIRE(table.bucket_count() == 128);

    for (int i = 0; i < 300; ++i) {
        const auto key = "k." + std::to_string(i);
        auto v = table.get(key);
        REQUIRE(v.has_value());
        REQUIRE(v->value == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::index::disk_hash_table::rehash_truncated_keys_requires_loader") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_rehash_truncated.data");
    std::filesystem::remove(path);

    const std::string key1(200, 'a');
    const std::string key2 = std::string(199, 'a') + "b";

    disk_hash_table_t table(path, 4, true, &resource);
    REQUIRE(table.put(key1, 11, 5, 500));
    REQUIRE(table.put(key2, 22, 6, 600));

    REQUIRE_THROWS(table.rehash(64));

    REQUIRE(table.rehash(64, [&](uint32_t file_id, uint64_t offset, std::string& out) {
        if (file_id == 5 && offset == 500) {
            out = key1;
            return true;
        }
        if (file_id == 6 && offset == 600) {
            out = key2;
            return true;
        }
        return false;
    }));
}

TEST_CASE("services::index::disk_hash_table::auto_rehash_by_load_factor") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_auto_rehash.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 4, true, &resource);
    const auto initial_buckets = table.bucket_count();
    REQUIRE(initial_buckets == 4);

    for (int i = 0; i < 20; ++i) {
        const auto key = "auto.k." + std::to_string(i);
        REQUIRE(table.put(key, static_cast<int64_t>(i), 10, static_cast<uint64_t>(i + 1)));
    }

    REQUIRE(table.bucket_count() > initial_buckets);
    REQUIRE(table.load_factor() <= 0.85);
}
