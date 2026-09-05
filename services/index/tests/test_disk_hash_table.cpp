#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>
#include <services/index/disk_hash_table.hpp>

#include <cstdlib>
#include <memory_resource>
#include <string>
#include <string_view>
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    // enc_a/enc_b collide only for seed=0 (plain FNV-1a); table uses random seed by default.
    env_var_guard_t seed_guard("OTTERBRIX_DISK_HASH_SEED", "0");
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_truncated_collision.data");
    std::filesystem::remove(path);

    // FNV-1a collision pair for truncated entries (same 32-byte prefix and encoded length).
    static const unsigned char enc_a_bytes[] = {
        35,  200, 0,   0,   0,   97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,
        97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  9,   116, 135, 155, 250, 116, 9,   140, 227, 29,
        188, 54,  49,  139, 96,  164, 244, 19,  249, 118, 220, 255, 148, 220, 154, 28,  241, 216, 101, 91,  42,
        168, 242, 57,  62,  204, 83,  169, 47,  172, 148, 146, 211, 44,  178, 68,  202, 191, 171, 5,   69,  71,
        120, 74,  61,  120, 148, 11,  199, 187, 225, 101, 225, 164, 182, 68,  140, 150, 33,  215, 9,   12,  5,
        73,  92,  160, 147, 212, 150, 60,  92,  23,  165, 246, 199, 204, 52,  81,  209, 3,   39,  193, 82,  8,
        115, 21,  138, 68,  42,  7,   109, 19,  18,  220, 242, 193, 163, 118, 20,  9,   178, 204, 190, 70,  178,
        36,  177, 154, 201, 137, 158, 10,  92,  58,  81,  117, 170, 175, 9,   255, 203, 33,  205, 21,  157, 219,
        3,   208, 151, 119, 135, 125, 83,  141, 108, 68,  110, 205, 129, 211, 216, 70,  31,  86,  165, 11,  140,
        244, 78,  89,  216, 175, 81,  98,  151, 17,  46,  66,  24,  207, 219, 64,  203, 205};
    static const unsigned char enc_b_bytes[] = {
        35,  200, 0,   0,   0,   97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,
        97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  126, 48,  173, 224, 150, 190, 22,  109, 132, 141,
        117, 4,   146, 254, 102, 53,  239, 54,  221, 225, 64,  61,  41,  164, 185, 142, 115, 85,  203, 158, 211,
        52,  221, 90,  4,   72,  254, 69,  71,  181, 204, 241, 230, 254, 1,   180, 253, 16,  49,  196, 230, 70,
        99,  29,  138, 164, 35,  206, 53,  53,  22,  52,  229, 141, 252, 108, 171, 189, 178, 58,  29,  44,  201,
        235, 88,  137, 102, 149, 69,  191, 51,  71,  158, 49,  119, 244, 227, 199, 41,  65,  233, 111, 253, 53,
        252, 85,  231, 211, 32,  172, 122, 99,  61,  32,  207, 24,  56,  209, 250, 208, 195, 54,  33,  212, 87,
        54,  203, 127, 180, 209, 40,  118, 118, 124, 112, 214, 35,  120, 149, 130, 214, 169, 59,  182, 224, 47,
        208, 12,  168, 49,  95,  174, 2,   225, 33,  5,   230, 190, 75,  223, 159, 194, 122, 246, 192, 57,  180,
        202, 72,  69,  22,  67,  149, 49,  195, 91,  7,   21,  177, 73,  137, 228, 127, 205};
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
        // The failpoint aborts the split; failure arrives by value, like the rest of this API
        // (put/erase/rehash return bool). The property under test is unchanged: the rehash does
        // not complete, and the reopened table below is still consistent.
        REQUIRE_FALSE(table.rehash(5));
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
    auto resource = core::pmr::otterbrix_resource();
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
        REQUIRE_FALSE(table.rehash(5));
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
    auto resource = core::pmr::otterbrix_resource();
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
        REQUIRE_FALSE(table.rehash(5));
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

// create() is the production entry point: an unusable file must come back as a
// value, not as an exception and not as a half-open table. The direct ctor keeps
// aborting on the same input (same split as bitcask_index_disk_t), which is why
// callers that can report a failure — manager_index_t — must use create().
TEST_CASE("services::index::disk_hash_table::create_reports_unopenable_storage") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("create_unopenable.bin");
    std::filesystem::remove_all(path);
    // A directory where the file belongs: open(O_RDWR|O_CREAT) is EISDIR.
    std::filesystem::create_directories(path);

    auto result = disk_hash_table_t::create(path, disk_hash_table_t::default_bucket_count, &resource);
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::index_create_fail);

    std::filesystem::remove_all(path);
}

TEST_CASE("services::index::disk_hash_table::create_returns_a_usable_table") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("create_usable.bin");
    std::filesystem::remove_all(path);
    std::filesystem::remove_all(std::filesystem::path(path).concat(".ovf"));

    auto result = disk_hash_table_t::create(path, 8, &resource);
    REQUIRE_FALSE(result.has_error());
    auto table = result.value();
    REQUIRE(table);
    REQUIRE(table->put("k", 42, 0, 0));
    auto found = table->get("k");
    REQUIRE(found.has_value());
    REQUIRE(found->value == 42);
}

// --- for_each: behavioural gate on walk order and capture lifetime -----------
// for_each's ORDER is observable, not an implementation detail: both production
// callers (bitcask_index_disk_t::load_entries and ::merge_immutable_segments)
// accumulate through a by-reference capture, so the sequence for_each hands out is
// the sequence they build -- load_entries' duplicate entries and the merge's ref
// list come out in exactly this order. The cases below pin that walk (buckets
// ascending, primary page before its overflow chain, slots 0..n-1 inside a page)
// so that a table which is merely "still complete" cannot pass.

TEST_CASE("services::index::disk_hash_table::for_each_walks_duplicates_in_insertion_order") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_duplicate_order.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    // ONE bucket: every entry lands on the same chain, so the whole for_each output
    // is the whole insertion order and no hash seed can perturb it. put() never
    // reuses a freed slot, it appends at slot index count(), so "insertion order"
    // is the ground truth the walk has to reproduce.
    disk_hash_table_t table(path, 1, &resource);
    REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

    constexpr int64_t duplicates = 300;
    for (int64_t i = 0; i < duplicates; ++i) {
        REQUIRE(table.put("dup", i, static_cast<uint32_t>(i + 1), static_cast<uint64_t>(1000 + i)));
    }
    table.sync();

    // ~104 entries of this shape fit one 4096-byte page, so 300 duplicates PROVE the
    // walk crossed primary -> overflow instead of stopping at the first page. Asserted,
    // not assumed: without a real chain the order below would be a single-page order.
    REQUIRE(std::filesystem::exists(overflow_path));
    REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

    std::pmr::vector<disk_hash_table_t::value_ref_t> seen(&resource);
    table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { seen.push_back(ref); });

    REQUIRE(seen.size() == static_cast<size_t>(duplicates));
    for (int64_t i = 0; i < duplicates; ++i) {
        const auto& ref = seen[static_cast<size_t>(i)];
        REQUIRE(ref.value == i);
        // The whole value_ref_t rides along in step: a reordering that regenerated
        // values but shuffled the log coordinates would still be caught here.
        REQUIRE(ref.log_file_id == static_cast<uint32_t>(i + 1));
        REQUIRE(ref.log_offset == static_cast<uint64_t>(1000 + i));
        REQUIRE_FALSE(ref.key_truncated);
    }

    // Independent oracle: get_all walks the same chain by the same rule and is NOT
    // touched by this task, so the two orders must agree.
    auto all = table.get_all("dup");
    REQUIRE(all.size() == seen.size());
    for (size_t i = 0; i < all.size(); ++i) {
        REQUIRE(all[i].value == seen[i].value);
        REQUIRE(all[i].log_offset == seen[i].log_offset);
    }
}

TEST_CASE("services::index::disk_hash_table::for_each_keeps_interleaved_keys_in_chain_order") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_interleaved_order.data");
    std::filesystem::remove(path);
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    // One bucket again, but now with three keys interleaved: this pins that the walk
    // reports slot order and does NOT group or sort by key on the way out.
    disk_hash_table_t table(path, 1, &resource);
    REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

    struct put_t {
        std::string_view key;
        int64_t value;
    };
    const std::pmr::vector<put_t> script(
        {put_t{"alpha", 1},
         put_t{"beta", 2},
         put_t{"alpha", 3},
         put_t{"gamma", 4},
         put_t{"beta", 5},
         put_t{"alpha", 6},
         put_t{"gamma", 7},
         put_t{"beta", 8},
         put_t{"alpha", 9}},
        &resource);
    for (const auto& step : script) {
        REQUIRE(table.put(step.key, step.value, 7, static_cast<uint64_t>(step.value) * 10));
    }

    std::pmr::vector<int64_t> seen(&resource);
    table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { seen.push_back(ref.value); });

    REQUIRE(seen.size() == script.size());
    for (size_t i = 0; i < script.size(); ++i) {
        REQUIRE(seen[i] == script[i].value);
    }
}

TEST_CASE("services::index::disk_hash_table::for_each_multi_bucket_order_is_stable_and_per_key_ordered") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_multi_bucket_order.data");
    std::filesystem::remove(path);
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    constexpr int key_count = 5;
    constexpr int64_t per_key = 20;
    std::pmr::vector<int64_t> first_pass(&resource);
    std::pmr::vector<int64_t> second_pass(&resource);

    {
        // Pin the hash seed so the bucket layout -- and therefore this case -- is the
        // same on every run instead of being reseeded per file.
        env_var_guard_t seed_guard("OTTERBRIX_DISK_HASH_SEED", "0x5eed1234");
        disk_hash_table_t table(path, 64, &resource);
        REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

        // Interleave the keys so that insertion order and key order disagree; the value
        // encodes which key produced it (key*1000 + n) so each key's subsequence is
        // recoverable from the value_ref_t alone.
        for (int64_t n = 0; n < per_key; ++n) {
            for (int k = 0; k < key_count; ++k) {
                const std::string key = "key_" + std::to_string(k);
                REQUIRE(table.put(key, static_cast<int64_t>(k) * 1000 + n, 1, static_cast<uint64_t>(n)));
            }
        }

        table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { first_pass.push_back(ref.value); });
        table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { second_pass.push_back(ref.value); });
    }

    REQUIRE(first_pass.size() == static_cast<size_t>(key_count) * static_cast<size_t>(per_key));
    // Two consecutive walks over an unchanged table must produce the SAME order.
    REQUIRE(second_pass.size() == first_pass.size());
    for (size_t i = 0; i < first_pass.size(); ++i) {
        REQUIRE(second_pass[i] == first_pass[i]);
    }

    // The bucket loop runs ASCENDING, and with the seed pinned above every key lands
    // in a bucket of its own, so the whole sequence is fixed: fnv1a-32 seeded with
    // 0x5eed1234 mod 64 puts key_4 in bucket 3, key_0 in 15, key_3 in 34, key_2 in 53
    // and key_1 in 60. Walking the buckets the other way, or in any other order, moves
    // these five groups and is caught here -- the per-key check below cannot see it,
    // because reversing the bucket loop leaves every chain internally intact.
    const std::pmr::vector<int> expected_key_order({4, 0, 3, 2, 1}, &resource);
    for (size_t group = 0; group < expected_key_order.size(); ++group) {
        for (int64_t n = 0; n < per_key; ++n) {
            const auto position = group * static_cast<size_t>(per_key) + static_cast<size_t>(n);
            REQUIRE(first_pass[position] == static_cast<int64_t>(expected_key_order[group]) * 1000 + n);
        }
    }

    // Entries of one key share a bucket and a chain, so their relative order in the
    // output is their insertion order -- ascending n. This holds whatever bucket the
    // seed sends them to, and breaks the moment a chain is walked backwards or an
    // overflow page is visited before its primary.
    for (int k = 0; k < key_count; ++k) {
        int64_t expected_n = 0;
        for (auto value : first_pass) {
            if (value / 1000 != static_cast<int64_t>(k)) {
                continue;
            }
            REQUIRE(value % 1000 == expected_n);
            ++expected_n;
        }
        REQUIRE(expected_n == per_key);
    }
}

TEST_CASE("services::index::disk_hash_table::for_each_delivers_every_entry_before_it_returns") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_capture_lifetime.data");
    std::filesystem::remove(path);
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    disk_hash_table_t table(path, 8, &resource);

    // An empty table must not invoke the callable at all -- no phantom entry, and
    // nothing queued to run later.
    std::pmr::vector<int64_t> collected(&resource);
    size_t calls = 0;
    table.for_each([&](const disk_hash_table_t::value_ref_t& ref) {
        collected.push_back(ref.value);
        ++calls;
    });
    REQUIRE(calls == 0);
    REQUIRE(collected.empty());

    constexpr int64_t total = 40;
    for (int64_t i = 0; i < total; ++i) {
        REQUIRE(table.put("k" + std::to_string(i), i, 1, static_cast<uint64_t>(i)));
    }

    table.for_each([&](const disk_hash_table_t::value_ref_t& ref) {
        collected.push_back(ref.value);
        ++calls;
    });

    // Everything the callable did through its by-reference capture is visible to the
    // caller the instant for_each returns: the walk is synchronous, not deferred.
    const auto size_on_return = collected.size();
    REQUIRE(calls == size_on_return);
    REQUIRE(size_on_return == static_cast<size_t>(total));

    // ... and nothing keeps calling it afterwards: later work on the same table must
    // not append one more entry through the capture.
    REQUIRE(table.get("k0").has_value());
    table.sync();
    REQUIRE(collected.size() == size_on_return);
    REQUIRE(calls == size_on_return);
}
