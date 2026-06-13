#include <catch2/catch.hpp>
#include <services/index/disk_hash_table.hpp>

#include <cstdlib>
#include <unordered_map>
#include <vector>

using services::index::disk_hash_table_t;

namespace {
    std::filesystem::path mk_path(const std::string& name) {
        const auto dir = std::filesystem::path("/tmp/index_disk");
        std::filesystem::create_directories(dir);
        return dir / name;
    }

    struct env_var_guard_t {
        std::string name;
        bool had_value{false};
        std::string prev;

        env_var_guard_t(std::string env_name, const std::string& value)
            : name(std::move(env_name)) {
            if (const char* current = std::getenv(name.c_str()); current != nullptr) {
                had_value = true;
                prev = current;
            }
            setenv(name.c_str(), value.c_str(), 1);
        }

        ~env_var_guard_t() {
            if (had_value) {
                setenv(name.c_str(), prev.c_str(), 1);
            } else {
                unsetenv(name.c_str());
            }
        }
    };
} // namespace

TEST_CASE("services::index::disk_hash_table::put_get_erase_roundtrip") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_roundtrip.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 64, &resource);
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
        disk_hash_table_t table(path, 32, &resource);
        REQUIRE(table.put("k1", 111, 2, 1234));
        REQUIRE(table.put("k2", 222, 2, 5678));
        table.sync();
    }

    {
        disk_hash_table_t reopened(path, 32, &resource);
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

    disk_hash_table_t table(path, 32, &resource);
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

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE(table.put(long_key, 777, 7, 700));

    table.set_full_key_loader([&](uint32_t file_id, uint64_t offset, std::string& out, bool /*lock_bitcask*/) {
        REQUIRE(file_id == 7);
        REQUIRE(offset == 700);
        out = long_key;
        return true;
    });
    auto with_loader = table.get(long_key);
    REQUIRE(with_loader.has_value());
    REQUIRE(with_loader->value == 777);

    table.set_full_key_loader([&](uint32_t, uint64_t, std::string& out, bool /*lock_bitcask*/) {
        out = long_key;
        return true;
    });
    auto mismatch = table.get(other_key);
    REQUIRE_FALSE(mismatch.has_value());
}

TEST_CASE("services::index::disk_hash_table::truncated_collision_requires_loader") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_truncated_collision.data");
    std::filesystem::remove(path);

    // FNV-1a collision pair for truncated entries (same 32-byte prefix and encoded length).
    static const unsigned char enc_a_bytes[] = {
        35, 200, 0, 0, 0, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 9, 116, 135, 155, 250, 116, 9, 140, 227, 29, 188, 54, 49, 139, 96, 164, 244, 19, 249, 118, 220, 255, 148, 220, 154, 28, 241, 216, 101, 91, 42, 168, 242, 57, 62, 204, 83, 169, 47, 172, 148, 146, 211, 44, 178, 68, 202, 191, 171, 5, 69, 71, 120, 74, 61, 120, 148, 11, 199, 187, 225, 101, 225, 164, 182, 68, 140, 150, 33, 215, 9, 12, 5, 73, 92, 160, 147, 212, 150, 60, 92, 23, 165, 246, 199, 204, 52, 81, 209, 3, 39, 193, 82, 8, 115, 21, 138, 68, 42, 7, 109, 19, 18, 220, 242, 193, 163, 118, 20, 9, 178, 204, 190, 70, 178, 36, 177, 154, 201, 137, 158, 10, 92, 58, 81, 117, 170, 175, 9, 255, 203, 33, 205, 21, 157, 219, 3, 208, 151, 119, 135, 125, 83, 141, 108, 68, 110, 205, 129, 211, 216, 70, 31, 86, 165, 11, 140, 244, 78, 89, 216, 175, 81, 98, 151, 17, 46, 66, 24, 207, 219, 64, 203, 205};
    static const unsigned char enc_b_bytes[] = {
        35, 200, 0, 0, 0, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 126, 48, 173, 224, 150, 190, 22, 109, 132, 141, 117, 4, 146, 254, 102, 53, 239, 54, 221, 225, 64, 61, 41, 164, 185, 142, 115, 85, 203, 158, 211, 52, 221, 90, 4, 72, 254, 69, 71, 181, 204, 241, 230, 254, 1, 180, 253, 16, 49, 196, 230, 70, 99, 29, 138, 164, 35, 206, 53, 53, 22, 52, 229, 141, 252, 108, 171, 189, 178, 58, 29, 44, 201, 235, 88, 137, 102, 149, 69, 191, 51, 71, 158, 49, 119, 244, 227, 199, 41, 65, 233, 111, 253, 53, 252, 85, 231, 211, 32, 172, 122, 99, 61, 32, 207, 24, 56, 209, 250, 208, 195, 54, 33, 212, 87, 54, 203, 127, 180, 209, 40, 118, 118, 124, 112, 214, 35, 120, 149, 130, 214, 169, 59, 182, 224, 47, 208, 12, 168, 49, 95, 174, 2, 225, 33, 5, 230, 190, 75, 223, 159, 194, 122, 246, 192, 57, 180, 202, 72, 69, 22, 67, 149, 49, 195, 91, 7, 21, 177, 73, 137, 228, 127, 205};
    static const std::string enc_a(reinterpret_cast<const char*>(enc_a_bytes), sizeof(enc_a_bytes));
    static const std::string enc_b(reinterpret_cast<const char*>(enc_b_bytes), sizeof(enc_b_bytes));

    disk_hash_table_t table(path, 32, &resource);
    REQUIRE(table.put(enc_a, 777, 1, 100));

    size_t loader_calls = 0;
    table.set_full_key_loader([&](uint32_t, uint64_t, std::string& out, bool /*lock_bitcask*/) {
        ++loader_calls;
        out = enc_a;
        return true;
    });

    REQUIRE(table.get_all(enc_b).empty());
    REQUIRE(loader_calls >= 1);
}

TEST_CASE("services::index::disk_hash_table::get_invokes_key_loader_for_truncated_entry") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_loader_invoked_on_get.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    disk_hash_table_t table(path, 8, &resource);
    REQUIRE(table.put(long_key, 777, 7, 700));

    size_t loader_calls = 0;
    table.set_full_key_loader([&](uint32_t file_id, uint64_t offset, std::string& out, bool /*lock_bitcask*/) {
        ++loader_calls;
        REQUIRE(file_id == 7);
        REQUIRE(offset == 700);
        out = long_key;
        return true;
    });

    const auto value = table.get(long_key);
    REQUIRE(value.has_value());
    REQUIRE(value->value == 777);
    REQUIRE(loader_calls == 1);
}

TEST_CASE("services::index::disk_hash_table::get_skips_key_loader_for_inline_entry") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_loader_skipped_inline.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE(table.put("short-key", 5, 1, 100));

    size_t loader_calls = 0;
    table.set_full_key_loader([&](uint32_t, uint64_t, std::string& out, bool /*lock_bitcask*/) {
        ++loader_calls;
        out = "short-key";
        return true;
    });

    const auto value = table.get("short-key");
    REQUIRE(value.has_value());
    REQUIRE(value->value == 5);
    REQUIRE(loader_calls == 0);
}

TEST_CASE("services::index::disk_hash_table::erase_invokes_key_loader_for_truncated_entry") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_loader_invoked_on_erase.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'y');
    disk_hash_table_t table(path, 8, &resource);
    REQUIRE(table.put(long_key, 909, 9, 900));

    size_t loader_calls = 0;
    table.set_full_key_loader([&](uint32_t file_id, uint64_t offset, std::string& out, bool /*lock_bitcask*/) {
        ++loader_calls;
        REQUIRE(file_id == 9);
        REQUIRE(offset == 900);
        out = long_key;
        return true;
    });

    REQUIRE(table.erase(long_key));
    REQUIRE(loader_calls >= 1);
    REQUIRE_FALSE(table.get(long_key).has_value());
}

TEST_CASE("services::index::disk_hash_table::rehash_preserves_entries") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_rehash.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 4, &resource);
    table.set_auto_rehash_suppressed(true);
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

TEST_CASE("services::index::disk_hash_table::rehash_truncated_keys_without_loader") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_rehash_truncated.data");
    std::filesystem::remove(path);

    const std::string key1(200, 'a');
    const std::string key2 = std::string(199, 'a') + "b";

    disk_hash_table_t table(path, 4, &resource);
    REQUIRE(table.put(key1, 11, 5, 500));
    REQUIRE(table.put(key2, 22, 6, 600));

    REQUIRE(table.rehash(64));

    table.set_full_key_loader([&](uint32_t file_id, uint64_t offset, std::string& out, bool /*lock_bitcask*/) {
        if (file_id == 5 && offset == 500) {
            out = key1;
            return true;
        }
        if (file_id == 6 && offset == 600) {
            out = key2;
            return true;
        }
        return false;
    });
    auto v1 = table.get(key1);
    REQUIRE(v1.has_value());
    REQUIRE(v1->value == 11);

    auto v2 = table.get(key2);
    REQUIRE(v2.has_value());
    REQUIRE(v2->value == 22);
}

TEST_CASE("services::index::disk_hash_table::linear_hashing_progression") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_linear_progression.data");
    std::filesystem::remove(path);

    std::vector<std::string> keys;
    keys.reserve(120);
    for (int i = 0; i < 100; ++i) {
        keys.emplace_back("progress.k." + std::to_string(i));
    }
    for (int i = 0; i < 20; ++i) {
        keys.emplace_back(std::string(120, static_cast<char>('a' + (i % 10))) + ".long." + std::to_string(i));
    }

    std::unordered_map<uint64_t, std::string> full_key_by_offset;
    {
        disk_hash_table_t table(path, 4, &resource);
        table.set_auto_rehash_suppressed(true);
        REQUIRE(table.bucket_count() == 4);

        for (size_t i = 0; i < keys.size(); ++i) {
            const auto offset = static_cast<uint64_t>(10'000 + i);
            full_key_by_offset.emplace(offset, keys[i]);
            REQUIRE(table.put(keys[i], static_cast<int64_t>(i), 42, offset));
        }

        table.set_full_key_loader([&](uint32_t file_id, uint64_t offset, std::string& out, bool /*lock_bitcask*/) {
            if (file_id != 42) {
                return false;
            }
            const auto it = full_key_by_offset.find(offset);
            if (it == full_key_by_offset.end()) {
                return false;
            }
            out = it->second;
            return true;
        });

        for (uint32_t target = 5; target <= 9; ++target) {
            REQUIRE(table.rehash(target));
            REQUIRE(table.bucket_count() == target);
            for (size_t i = 0; i < keys.size(); ++i) {
                auto v = table.get(keys[i]);
                REQUIRE(v.has_value());
                REQUIRE(v->value == static_cast<int64_t>(i));
            }
        }
        table.sync();
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 9);

        reopened.set_full_key_loader([&](uint32_t file_id, uint64_t offset, std::string& out, bool /*lock_bitcask*/) {
            if (file_id != 42) {
                return false;
            }
            const auto it = full_key_by_offset.find(offset);
            if (it == full_key_by_offset.end()) {
                return false;
            }
            out = it->second;
            return true;
        });

        for (uint32_t target = 10; target <= 12; ++target) {
            REQUIRE(reopened.rehash(target));
            REQUIRE(reopened.bucket_count() == target);
            for (size_t i = 0; i < keys.size(); ++i) {
                auto v = reopened.get(keys[i]);
                REQUIRE(v.has_value());
                REQUIRE(v->value == static_cast<int64_t>(i));
            }
        }
    }
}

TEST_CASE("services::index::disk_hash_table::auto_rehash_by_load_factor") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_auto_rehash.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 4, &resource);
    const auto initial_buckets = table.bucket_count();
    REQUIRE(initial_buckets == 4);

    for (int i = 0; i < 20; ++i) {
        const auto key = "auto.k." + std::to_string(i);
        REQUIRE(table.put(key, static_cast<int64_t>(i), 10, static_cast<uint64_t>(i + 1)));
    }

    REQUIRE(table.bucket_count() > initial_buckets);
    REQUIRE(table.load_factor() <= 0.75);
}

TEST_CASE("services::index::disk_hash_table::split_crash_after_copy_sync") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_split_crash_after_copy.data");
    std::filesystem::remove(path);

    std::vector<std::string> keys;
    keys.reserve(300);
    {
        disk_hash_table_t table(path, 4, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int i = 0; i < 300; ++i) {
            keys.emplace_back("crash.copy.k." + std::to_string(i));
            REQUIRE(table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(1000 + i)));
        }
        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_copy_sync");
        REQUIRE_THROWS(table.rehash(5));
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 4);
        for (int i = 0; i < 300; ++i) {
            auto v = reopened.get(keys[static_cast<size_t>(i)]);
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }
}

TEST_CASE("services::index::disk_hash_table::split_crash_after_header_sync") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_split_crash_after_header.data");
    std::filesystem::remove(path);

    std::vector<std::string> keys;
    keys.reserve(300);
    {
        disk_hash_table_t table(path, 4, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int i = 0; i < 300; ++i) {
            keys.emplace_back("crash.header.k." + std::to_string(i));
            REQUIRE(table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(2000 + i)));
        }
        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_header_sync");
        REQUIRE_THROWS(table.rehash(5));
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 5);
        for (int i = 0; i < 300; ++i) {
            auto v = reopened.get(keys[static_cast<size_t>(i)]);
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }
}

TEST_CASE("services::index::disk_hash_table::split_crash_recovery_continues_progression") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto path = mk_path("disk_hash_table_split_crash_recovery_progression.data");
    std::filesystem::remove(path);

    std::vector<std::string> keys;
    keys.reserve(400);
    {
        disk_hash_table_t table(path, 4, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int i = 0; i < 400; ++i) {
            keys.emplace_back("crash.recover.k." + std::to_string(i));
            REQUIRE(table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(5000 + i)));
        }

        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_header_sync");
        REQUIRE_THROWS(table.rehash(5));
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 5);

        REQUIRE(reopened.rehash(6));
        REQUIRE(reopened.bucket_count() == 6);

        for (int i = 0; i < 400; ++i) {
            auto v = reopened.get(keys[static_cast<size_t>(i)]);
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }
}
