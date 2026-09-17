#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>
#include <services/index/disk_hash_table.hpp>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "index_fixture_path.hpp"

using services::index::tests::index_fixture_path;
using services::index::tests::index_fixture_root;

using services::index::disk_hash_table_t;

namespace {
    std::filesystem::path mk_path(const std::string& name) {
        const auto dir = std::filesystem::path(index_fixture_root());
        std::filesystem::create_directories(dir);
        return dir / name;
    }

    auto loader_must_not_be_consulted(std::pmr::memory_resource* resource) {
        return [resource](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"the loader must not be consulted: every key in this case is inline", resource});
        };
    }

    std::pmr::string as_loader_key(std::pmr::memory_resource* resource, std::string_view key) {
        return std::pmr::string(key.data(), key.size(), resource);
    }

    template<typename result_t>
    auto must_read(result_t&& walked) {
        REQUIRE_FALSE(walked.has_error());
        return std::move(walked.value());
    }

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
    REQUIRE_FALSE(table.put("alpha", 10, 1, 100).contains_error());
    REQUIRE_FALSE(table.put("beta", 20, 1, 200).contains_error());

    auto alpha = must_read(table.get("alpha", loader_must_not_be_consulted(&resource)));
    REQUIRE(alpha.has_value());
    REQUIRE(alpha->value == 10);
    REQUIRE(alpha->log_file_id == 1);
    REQUIRE(alpha->log_offset == 100);

    auto beta = must_read(table.get("beta", loader_must_not_be_consulted(&resource)));
    REQUIRE(beta.has_value());
    REQUIRE(beta->value == 20);

    REQUIRE(must_read(table.erase("alpha", loader_must_not_be_consulted(&resource))));
    REQUIRE_FALSE(must_read(table.get("alpha", loader_must_not_be_consulted(&resource))).has_value());
    REQUIRE(must_read(table.get("beta", loader_must_not_be_consulted(&resource))).has_value());
}

TEST_CASE("services::index::disk_hash_table::persist_reopen") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_persist.data");
    std::filesystem::remove(path);

    {
        disk_hash_table_t table(path, 32, &resource);
        REQUIRE_FALSE(table.put("k1", 111, 2, 1234).contains_error());
        REQUIRE_FALSE(table.put("k2", 222, 2, 5678).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    {
        disk_hash_table_t reopened(path, 32, &resource);
        auto v1 = must_read(reopened.get("k1", loader_must_not_be_consulted(&resource)));
        REQUIRE(v1.has_value());
        REQUIRE(v1->value == 111);
        REQUIRE(v1->log_file_id == 2);
        REQUIRE(v1->log_offset == 1234);
        auto v2 = must_read(reopened.get("k2", loader_must_not_be_consulted(&resource)));
        REQUIRE(v2.has_value());
        REQUIRE(v2->value == 222);
    }
}

TEST_CASE("services::index::disk_hash_table::multiple_values_per_key") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_multi_values.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 32, &resource);
    REQUIRE_FALSE(table.put("dup", 10, 1, 100).contains_error());
    REQUIRE_FALSE(table.put("dup", 20, 2, 200).contains_error());
    REQUIRE_FALSE(table.put("dup", 10, 3, 300).contains_error());

    const auto values = must_read(table.get_all("dup", loader_must_not_be_consulted(&resource)));
    REQUIRE(values.size() == 3);
}

TEST_CASE("services::index::disk_hash_table::long_key_prefix_and_loader") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_long_key.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    const std::string other_key = long_key + "y";

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 777, 7, 700).contains_error());

    const auto source_1 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        REQUIRE(file_id == 7);
        REQUIRE(offset == 700);
        return as_loader_key(&resource, long_key);
    };
    auto with_loader = must_read(table.get(long_key, source_1));
    REQUIRE(with_loader.has_value());
    REQUIRE(with_loader->value == 777);

    const auto source_2 = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        return as_loader_key(&resource, long_key);
    };
    auto mismatch = must_read(table.get(other_key, source_2));
    REQUIRE_FALSE(mismatch.has_value());
}

TEST_CASE("services::index::disk_hash_table::truncated_collision_requires_loader") {
    // enc_a/enc_b collide only for seed=0 (plain FNV-1a); table uses random seed by default.
    env_var_guard_t seed_guard("OTTERBRIX_DISK_HASH_SEED", "0");
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_truncated_collision.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 32, &resource);
    REQUIRE_FALSE(table.put(enc_a, 777, 1, 100).contains_error());

    size_t loader_calls = 0;
    const auto source_3 = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        return as_loader_key(&resource, enc_a);
    };

    REQUIRE(must_read(table.get_all(enc_b, source_3)).empty());
    REQUIRE(loader_calls >= 1);
}

TEST_CASE("services::index::disk_hash_table::a_colliding_stranger_that_cannot_be_read_refuses_the_whole_walk") {
    env_var_guard_t seed_guard("OTTERBRIX_DISK_HASH_SEED", "0");
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_unreadable_collision_stranger.data");
    std::filesystem::remove(path);
    // The table opens `path` and `path + ".ovf"` as one pair; removing only the first leaks a previous run's chain.
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    disk_hash_table_t table(path, 32, &resource);
    REQUIRE_FALSE(table.put(enc_b, 555, 2, 200).contains_error());
    REQUIRE_FALSE(table.put(enc_a, 777, 1, 100).contains_error());

    // A walk's refusal copies through VALUE_OR_RETURN and doesn't propagate the allocator.
    const auto produced_key = as_loader_key(&resource, enc_a);
    REQUIRE(produced_key.get_allocator().resource() == &resource);

    const auto both_readable = [&](uint32_t, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        return as_loader_key(&resource, offset == 200 ? enc_b : enc_a);
    };
    const auto complete = must_read(table.get_all(enc_b, both_readable));
    REQUIRE(complete.size() == 1);
    REQUIRE(complete.front().value == 555);

    std::vector<uint64_t> consulted;
    const auto stranger_unreadable = [&](uint32_t, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        consulted.push_back(offset);
        if (offset == 200) {
            return as_loader_key(&resource, enc_b);
        }
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{"the stranger's record is unreadable", &resource});
    };

    auto walked = table.get_all(enc_b, stranger_unreadable);
    REQUIRE(walked.has_error());
    REQUIRE(walked.error().type == core::error_code_t::io_error);
    REQUIRE(consulted.size() == 2);
    REQUIRE(consulted[0] == 200);
    REQUIRE(consulted[1] == 100);
}

TEST_CASE("services::index::disk_hash_table::get_invokes_key_loader_for_truncated_entry") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_invoked_on_get.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 777, 7, 700).contains_error());

    size_t loader_calls = 0;
    const auto source_4 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        REQUIRE(file_id == 7);
        REQUIRE(offset == 700);
        return as_loader_key(&resource, long_key);
    };

    const auto value = must_read(table.get(long_key, source_4));
    REQUIRE(value.has_value());
    REQUIRE(value->value == 777);
    REQUIRE(loader_calls == 1);
}

TEST_CASE("services::index::disk_hash_table::get_skips_key_loader_for_inline_entry") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_skipped_inline.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put("short-key", 5, 1, 100).contains_error());

    size_t loader_calls = 0;
    const auto source_5 = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        return as_loader_key(&resource, "short-key");
    };

    const auto value = must_read(table.get("short-key", source_5));
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
    REQUIRE_FALSE(table.put(long_key, 909, 9, 900).contains_error());

    size_t loader_calls = 0;
    const auto source_6 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        REQUIRE(file_id == 9);
        REQUIRE(offset == 900);
        return as_loader_key(&resource, long_key);
    };

    REQUIRE(must_read(table.erase(long_key, source_6)));
    REQUIRE(loader_calls >= 1);
    REQUIRE_FALSE(must_read(table.get(long_key, source_6)).has_value());
}

// test_bitcask_index_disk.cpp::find_refuses_when_a_long_keys_record_cannot_be_read can't catch this alone.
// Measured: substituting `return true` for keys_equal's VALUE_OR_RETURN passed 19 assertions there, but failed here.
TEST_CASE("services::index::disk_hash_table::truncated_entry_refuses_when_the_record_cannot_be_read") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_refuses.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 777, 7, 700).contains_error());

    uint64_t truncated_entries = 0;
    REQUIRE_FALSE(table
                      .for_each([&](const disk_hash_table_t::value_ref_t& ref) {
                          if (ref.key_truncated) {
                              ++truncated_entries;
                          }
                      })
                      .contains_error());
    REQUIRE(truncated_entries == 1);

    const auto refuses = [&resource](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        return core::error_t(core::error_code_t::io_error, std::pmr::string{"record unreadable", &resource});
    };

    auto read = table.get_all(long_key, refuses);
    REQUIRE(read.has_error());
    REQUIRE(read.error().type == core::error_code_t::io_error);

    auto erased = table.erase(long_key, refuses);
    REQUIRE(erased.has_error());
    REQUIRE(erased.error().type == core::error_code_t::io_error);

    // try_erase_in_page mutates the page only right before reporting success -- no half-done removal.
    const auto source = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        return as_loader_key(&resource, long_key);
    };
    const auto still_there = must_read(table.get(long_key, source));
    REQUIRE(still_there.has_value());
    REQUIRE(still_there->value == 777);
}

TEST_CASE("services::index::disk_hash_table::truncated_entry_answers_no_when_the_full_key_differs") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_says_different.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    const std::string different_key = std::string(199, 'x') + "y";

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 777, 7, 700).contains_error());

    size_t loader_calls = 0;
    const auto answers = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        return as_loader_key(&resource, different_key);
    };

    const auto missing = must_read(table.get(long_key, answers));
    REQUIRE_FALSE(missing.has_value());
    REQUIRE(loader_calls == 1);
}

TEST_CASE("services::index::disk_hash_table::inline_entry_never_reaches_a_refusing_loader") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_inline_skips_refusing_loader.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put("short-key", 5, 1, 100).contains_error());

    const auto value = must_read(table.get("short-key", loader_must_not_be_consulted(&resource)));
    REQUIRE(value.has_value());
    REQUIRE(value->value == 5);

    REQUIRE(must_read(table.erase("short-key", loader_must_not_be_consulted(&resource))));
    REQUIRE_FALSE(must_read(table.get("short-key", loader_must_not_be_consulted(&resource))).has_value());

    auto refusal = loader_must_not_be_consulted(&resource)(0, 0);
    REQUIRE(refusal.has_error());
    REQUIRE(refusal.error().what.get_allocator().resource() == &resource);
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
        REQUIRE_FALSE(table.put(key, static_cast<int64_t>(i), 1, static_cast<uint64_t>(1000 + i)).contains_error());
    }

    REQUIRE_FALSE(table.rehash(128).contains_error());
    REQUIRE(table.bucket_count() == 128);

    for (int i = 0; i < 300; ++i) {
        const auto key = "k." + std::to_string(i);
        auto v = must_read(table.get(key, loader_must_not_be_consulted(&resource)));
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
    REQUIRE_FALSE(table.put(key1, 11, 5, 500).contains_error());
    REQUIRE_FALSE(table.put(key2, 22, 6, 600).contains_error());

    REQUIRE_FALSE(table.rehash(64).contains_error());

    // The unknown-location leg refuses rather than answering "not your key" (a dangling keydir entry is corruption).
    const auto source_7 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        if (file_id == 5 && offset == 500) {
            return as_loader_key(&resource, key1);
        }
        if (file_id == 6 && offset == 600) {
            return as_loader_key(&resource, key2);
        }
        return core::error_t(core::error_code_t::io_error, std::pmr::string{"no record at this location", &resource});
    };
    auto v1 = must_read(table.get(key1, source_7));
    REQUIRE(v1.has_value());
    REQUIRE(v1->value == 11);

    auto v2 = must_read(table.get(key2, source_7));
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
            REQUIRE_FALSE(table.put(keys[i], static_cast<int64_t>(i), 42, offset).contains_error());
        }

        const auto source_8 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
            if (file_id != 42) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"no record at this location", &resource});
            }
            const auto it = full_key_by_offset.find(offset);
            if (it == full_key_by_offset.end()) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"no record at this location", &resource});
            }
            return as_loader_key(&resource, it->second);
        };

        for (uint32_t target = 5; target <= 9; ++target) {
            REQUIRE_FALSE(table.rehash(target).contains_error());
            REQUIRE(table.bucket_count() == target);
            for (size_t i = 0; i < keys.size(); ++i) {
                auto v = must_read(table.get(keys[i], source_8));
                REQUIRE(v.has_value());
                REQUIRE(v->value == static_cast<int64_t>(i));
            }
        }
        REQUIRE_FALSE(table.sync().contains_error());
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 9);

        const auto source_9 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
            if (file_id != 42) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"no record at this location", &resource});
            }
            const auto it = full_key_by_offset.find(offset);
            if (it == full_key_by_offset.end()) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"no record at this location", &resource});
            }
            return as_loader_key(&resource, it->second);
        };

        for (uint32_t target = 10; target <= 12; ++target) {
            REQUIRE_FALSE(reopened.rehash(target).contains_error());
            REQUIRE(reopened.bucket_count() == target);
            for (size_t i = 0; i < keys.size(); ++i) {
                auto v = must_read(reopened.get(keys[i], source_9));
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
        REQUIRE_FALSE(table.put(key, static_cast<int64_t>(i), 10, static_cast<uint64_t>(i + 1)).contains_error());
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
            REQUIRE_FALSE(
                table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(1000 + i)).contains_error());
        }
        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_copy_sync");
        REQUIRE(table.rehash(5).contains_error());
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 4);
        for (int i = 0; i < 300; ++i) {
            auto v = must_read(reopened.get(keys[static_cast<size_t>(i)], loader_must_not_be_consulted(&resource)));
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
            REQUIRE_FALSE(
                table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(2000 + i)).contains_error());
        }
        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_header_sync");
        REQUIRE(table.rehash(5).contains_error());
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 5);
        for (int i = 0; i < 300; ++i) {
            auto v = must_read(reopened.get(keys[static_cast<size_t>(i)], loader_must_not_be_consulted(&resource)));
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
            REQUIRE_FALSE(
                table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(5000 + i)).contains_error());
        }

        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_header_sync");
        REQUIRE(table.rehash(5).contains_error());
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 5);

        REQUIRE_FALSE(reopened.rehash(6).contains_error());
        REQUIRE(reopened.bucket_count() == 6);

        for (int i = 0; i < 400; ++i) {
            auto v = must_read(reopened.get(keys[static_cast<size_t>(i)], loader_must_not_be_consulted(&resource)));
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }
}

// create() is the production entry point: an unusable file must come back as a value, not an exception.
TEST_CASE("services::index::disk_hash_table::create_reports_unopenable_storage") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("create_unopenable.bin");
    std::filesystem::remove_all(path);
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
    auto table = std::move(result.value());
    REQUIRE(table);
    REQUIRE_FALSE(table->put("k", 42, 0, 0).contains_error());
    auto found = must_read(table->get("k", loader_must_not_be_consulted(&resource)));
    REQUIRE(found.has_value());
    REQUIRE(found->value == 42);
}

// for_each's order is observable: both production callers accumulate it through a by-reference capture.

TEST_CASE("services::index::disk_hash_table::for_each_walks_duplicates_in_insertion_order") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_duplicate_order.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    disk_hash_table_t table(path, 1, &resource);
    REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

    constexpr int64_t duplicates = 300;
    for (int64_t i = 0; i < duplicates; ++i) {
        REQUIRE_FALSE(
            table.put("dup", i, static_cast<uint32_t>(i + 1), static_cast<uint64_t>(1000 + i)).contains_error());
    }
    REQUIRE_FALSE(table.sync().contains_error());

    // ~104 entries fit one 4096-byte page; 300 duplicates prove the walk crossed primary -> overflow.
    REQUIRE(std::filesystem::exists(overflow_path));
    REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

    std::pmr::vector<disk_hash_table_t::value_ref_t> seen(&resource);
    REQUIRE_FALSE(
        table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { seen.push_back(ref); }).contains_error());

    REQUIRE(seen.size() == static_cast<size_t>(duplicates));
    for (int64_t i = 0; i < duplicates; ++i) {
        const auto& ref = seen[static_cast<size_t>(i)];
        REQUIRE(ref.value == i);
        REQUIRE(ref.log_file_id == static_cast<uint32_t>(i + 1));
        REQUIRE(ref.log_offset == static_cast<uint64_t>(1000 + i));
        REQUIRE_FALSE(ref.key_truncated);
    }

    auto all = must_read(table.get_all("dup", loader_must_not_be_consulted(&resource)));
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

    disk_hash_table_t table(path, 1, &resource);
    REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

    struct put_t {
        std::string_view key;
        int64_t value;
    };
    const std::pmr::vector<put_t> script({put_t{"alpha", 1},
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
        REQUIRE_FALSE(table.put(step.key, step.value, 7, static_cast<uint64_t>(step.value) * 10).contains_error());
    }

    std::pmr::vector<int64_t> seen(&resource);
    REQUIRE_FALSE(
        table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { seen.push_back(ref.value); }).contains_error());

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
        env_var_guard_t seed_guard("OTTERBRIX_DISK_HASH_SEED", "0x5eed1234");
        disk_hash_table_t table(path, 64, &resource);
        REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

        // The value encodes which key produced it (key*1000 + n), recoverable from value_ref_t alone.
        for (int64_t n = 0; n < per_key; ++n) {
            for (int k = 0; k < key_count; ++k) {
                const std::string key = "key_" + std::to_string(k);
                REQUIRE_FALSE(
                    table.put(key, static_cast<int64_t>(k) * 1000 + n, 1, static_cast<uint64_t>(n)).contains_error());
            }
        }

        REQUIRE_FALSE(
            table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { first_pass.push_back(ref.value); })
                .contains_error());
        REQUIRE_FALSE(
            table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { second_pass.push_back(ref.value); })
                .contains_error());
    }

    REQUIRE(first_pass.size() == static_cast<size_t>(key_count) * static_cast<size_t>(per_key));
    REQUIRE(second_pass.size() == first_pass.size());
    for (size_t i = 0; i < first_pass.size(); ++i) {
        REQUIRE(second_pass[i] == first_pass[i]);
    }

    // The bucket loop runs ascending; with the seed pinned above, fnv1a-32(0x5eed1234) mod 64 puts
    // key_4 in bucket 3, key_0 in 15, key_3 in 34, key_2 in 53, key_1 in 60 -- fixing this order.
    const std::pmr::vector<int> expected_key_order({4, 0, 3, 2, 1}, &resource);
    for (size_t group = 0; group < expected_key_order.size(); ++group) {
        for (int64_t n = 0; n < per_key; ++n) {
            const auto position = group * static_cast<size_t>(per_key) + static_cast<size_t>(n);
            REQUIRE(first_pass[position] == static_cast<int64_t>(expected_key_order[group]) * 1000 + n);
        }
    }

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

    // An empty table must not invoke the callable at all -- no phantom entry, nothing queued for later.
    std::pmr::vector<int64_t> collected(&resource);
    size_t calls = 0;
    auto empty_walk = table.for_each([&](const disk_hash_table_t::value_ref_t& ref) {
        collected.push_back(ref.value);
        ++calls;
    });
    REQUIRE_FALSE(empty_walk.contains_error());
    REQUIRE(calls == 0);
    REQUIRE(collected.empty());

    constexpr int64_t total = 40;
    for (int64_t i = 0; i < total; ++i) {
        REQUIRE_FALSE(table.put("k" + std::to_string(i), i, 1, static_cast<uint64_t>(i)).contains_error());
    }

    auto full_walk = table.for_each([&](const disk_hash_table_t::value_ref_t& ref) {
        collected.push_back(ref.value);
        ++calls;
    });
    REQUIRE_FALSE(full_walk.contains_error());

    const auto size_on_return = collected.size();
    REQUIRE(calls == size_on_return);
    REQUIRE(size_on_return == static_cast<size_t>(total));

    REQUIRE(must_read(table.get("k0", loader_must_not_be_consulted(&resource))).has_value());
    REQUIRE_FALSE(table.sync().contains_error());
    REQUIRE(collected.size() == size_on_return);
    REQUIRE(calls == size_on_return);
}

// A `break` on an unreadable page would silently return a subset dressed as the whole answer.
namespace {
    std::string read_file_bytes(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        REQUIRE(in.good());
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }

    // std::ios::in keeps the stream from truncating the file and keeps the inode the live table has open.
    void restore_file_bytes(const std::filesystem::path& path, const std::string& bytes) {
        std::filesystem::resize_file(path, bytes.size());
        std::fstream out(path, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE(out.good());
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.flush();
        REQUIRE(out.good());
    }

    // inline_key_limit (64B) fixes the per-entry cost at 91 payload bytes + a 9-byte slot -- roughly 40 per page.
    std::string chain_key() { return std::string(disk_hash_table_t::inline_key_limit, 'k'); }

    std::string chain_key(int index) {
        const auto suffix = "." + std::to_string(index);
        return std::string(disk_hash_table_t::inline_key_limit - suffix.size(), 'k') + suffix;
    }
} // namespace

TEST_CASE("services::index::disk_hash_table::reads_refuse_when_an_overflow_page_cannot_be_read") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_overflow_read_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    disk_hash_table_t table(path, 1, &resource);
    table.set_auto_rehash_suppressed(true);

    const auto key = chain_key();
    constexpr int64_t entry_count = 200;
    // MEASURED, not assumed: the entry that first grows the overflow file is where the primary page ran out of room.
    size_t entries_that_fit_the_primary_page = 0;
    for (int64_t i = 0; i < entry_count; ++i) {
        REQUIRE_FALSE(table.put(key, i, 1, static_cast<uint64_t>(1000 + i)).contains_error());
        if (entries_that_fit_the_primary_page == 0 && std::filesystem::exists(overflow_path) &&
            std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size) {
            entries_that_fit_the_primary_page = static_cast<size_t>(i);
        }
    }
    REQUIRE(entries_that_fit_the_primary_page > 0);
    REQUIRE(entries_that_fit_the_primary_page < static_cast<size_t>(entry_count));
    REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

    REQUIRE_FALSE(table.sync().contains_error());
    const auto whole = must_read(table.get_all(key, loader_must_not_be_consulted(&resource)));
    REQUIRE(whole.size() == static_cast<size_t>(entry_count));

    const auto overflow_bytes = read_file_bytes(overflow_path);
    std::filesystem::resize_file(overflow_path, 0);

    // THE PROPERTY: before the fix, this line silently came back with 40 of the 200 rows and no way to say so.
    auto after = table.get_all(key, loader_must_not_be_consulted(&resource));
    INFO("get_all met an unreadable overflow page and must REFUSE, not answer with the primary page alone");
    REQUIRE(after.has_error());
    REQUIRE(after.error().type == core::error_code_t::io_error);

    auto single = table.get(key, loader_must_not_be_consulted(&resource));
    REQUIRE(single.has_error());

    size_t seen = 0;
    auto walk = table.for_each([&](const disk_hash_table_t::value_ref_t&) { ++seen; });
    INFO("for_each met the same unreadable page and must refuse the same way");
    REQUIRE(walk.contains_error());
    REQUIRE(seen < static_cast<size_t>(entry_count));

    // erase's bare false would conflate "no such key" with "the chain ran out from under me".
    auto erased = table.erase("absent-" + key, loader_must_not_be_consulted(&resource));
    REQUIRE(erased.has_error());

    restore_file_bytes(overflow_path, overflow_bytes);
    const auto restored = must_read(table.get_all(key, loader_must_not_be_consulted(&resource)));
    REQUIRE(restored.size() == static_cast<size_t>(entry_count));
}

// Guards against split_one_bucket_unlocked advancing addressing state even when an entry's copy failed.
TEST_CASE("services::index::disk_hash_table::split_refuses_when_an_entry_cannot_be_copied") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_split_copy_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    std::vector<std::string> keys;
    keys.reserve(400);
    {
        disk_hash_table_t table(path, 1, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int i = 0; i < 400; ++i) {
            auto key = chain_key(i);
            REQUIRE_FALSE(table.put(key, static_cast<int64_t>(i), 1, static_cast<uint64_t>(3000 + i)).contains_error());
            keys.emplace_back(std::move(key));
        }
        REQUIRE(table.bucket_count() == 1);
        REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

        {
            env_var_guard_t deny_overflow("OTTERBRIX_DISK_HASH_OVERFLOW_ALLOC_FAILPOINT", "1");
            bool put_refused = false;
            for (int i = 0; i < 200 && !put_refused; ++i) {
                put_refused = table.put("sensitivity." + std::to_string(i), 0, 1, 0).contains_error();
            }
            REQUIRE(put_refused);

            INFO("a split that could not copy every entry must refuse, not publish a half-copied bucket");
            REQUIRE(table.rehash(2).contains_error());
        }
        REQUIRE(table.bucket_count() == 1);

        for (size_t i = 0; i < keys.size(); ++i) {
            auto v = must_read(table.get(keys[i], loader_must_not_be_consulted(&resource)));
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }

    disk_hash_table_t reopened(path, 1, &resource);
    reopened.set_auto_rehash_suppressed(true);
    REQUIRE(reopened.bucket_count() == 1);
    REQUIRE_FALSE(reopened.rehash(2).contains_error());
    REQUIRE(reopened.bucket_count() == 2);
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto rows = must_read(reopened.get_all(keys[i], loader_must_not_be_consulted(&resource)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front().value == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::index::disk_hash_table::split_refuses_when_a_source_page_cannot_be_read") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_split_source_read_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    disk_hash_table_t table(path, 1, &resource);
    table.set_auto_rehash_suppressed(true);

    std::vector<std::string> keys;
    keys.reserve(400);
    for (int i = 0; i < 400; ++i) {
        auto key = chain_key(i);
        REQUIRE_FALSE(table.put(key, static_cast<int64_t>(i), 1, static_cast<uint64_t>(4000 + i)).contains_error());
        keys.emplace_back(std::move(key));
    }
    REQUIRE_FALSE(table.sync().contains_error());
    REQUIRE(table.bucket_count() == 1);
    REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

    const auto overflow_bytes = read_file_bytes(overflow_path);
    std::filesystem::resize_file(overflow_path, 0);

    INFO("a split whose source chain could not be walked to the end must refuse, not publish");
    REQUIRE(table.rehash(2).contains_error());
    REQUIRE(table.bucket_count() == 1);

    restore_file_bytes(overflow_path, overflow_bytes);
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto rows = must_read(table.get_all(keys[i], loader_must_not_be_consulted(&resource)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front().value == static_cast<int64_t>(i));
    }
    REQUIRE_FALSE(table.rehash(2).contains_error());
    REQUIRE(table.bucket_count() == 2);
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto rows = must_read(table.get_all(keys[i], loader_must_not_be_consulted(&resource)));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front().value == static_cast<int64_t>(i));
    }
}

// Header layout: 8-byte magic at [0,8), CRC32C over [12,40) stored at [8,12).

#include "absl/crc/crc32c.h"

namespace {
    constexpr char hash_header_magic[8] = {'o', 't', 'b', 'x', 'h', 'a', 's', 'h'};

    void write_le32_at(std::string& bytes, size_t offset, uint32_t v) {
        for (unsigned i = 0; i < 4; ++i) {
            bytes[offset + i] = static_cast<char>((v >> (8U * i)) & 0xFFU);
        }
    }

    void write_le64_at(std::string& bytes, size_t offset, uint64_t v) {
        for (unsigned i = 0; i < 8; ++i) {
            bytes[offset + i] = static_cast<char>((v >> (8U * i)) & 0xFFU);
        }
    }

    void reseal_hash_header(std::string& bytes) {
        std::memcpy(bytes.data(), hash_header_magic, sizeof(hash_header_magic));
        const auto crc = static_cast<uint32_t>(absl::ComputeCrc32c(absl::string_view(bytes.data() + 12, 28)));
        write_le32_at(bytes, 8, crc);
    }
} // namespace

// count_entries_unlocked must not `break` out of an unreadable chain and hand open_or_create a partial count.
TEST_CASE("services::index::disk_hash_table::open_refuses_when_the_entry_count_cannot_be_counted") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_count_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 8, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int64_t i = 0; i < 64; ++i) {
            REQUIRE_FALSE(
                table.put("count-key-" + std::to_string(i), i, 1, 100 + static_cast<uint64_t>(i)).contains_error());
        }
        REQUIRE_FALSE(table.sync().contains_error());
    }

    // Cuts the file mid-table: header + first four bucket pages survive, last four don't.
    std::filesystem::resize_file(path, static_cast<uintmax_t>(5) * disk_hash_table_t::page_size);

    auto reopened = disk_hash_table_t::create(path, 8, &resource);
    INFO("an open that could not count its entries must refuse, not open over a partial count");
    REQUIRE(reopened.has_error());
    REQUIRE(reopened.error().type == core::error_code_t::io_error);
}

// An INCONSISTENT level/split_bucket pair (2^level + split_bucket != bucket_count) must refuse, not silently rewrite.
TEST_CASE("services::index::disk_hash_table::open_refuses_a_corrupt_linear_hash_state_instead_of_repairing_it") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_linear_state_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 32, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    auto bytes = read_file_bytes(path);
    write_le32_at(bytes, 28, 3);
    reseal_hash_header(bytes);
    restore_file_bytes(path, bytes);

    auto reopened = disk_hash_table_t::create(path, 32, &resource);
    INFO("a header whose linear-hash state does not describe its bucket count is corruption, not input");
    REQUIRE(reopened.has_error());
}

// The second silent auto-repair: a next_overflow_page BELOW the overflow id base must refuse, not clamp.
TEST_CASE("services::index::disk_hash_table::open_refuses_a_corrupt_overflow_cursor_instead_of_clamping_it") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_overflow_cursor_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 16, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    auto bytes = read_file_bytes(path);
    // 5 is far below the 2^40 overflow id base -- no persist_header ever wrote it, so it's damaged, not clampable.
    write_le64_at(bytes, 20, 5);
    reseal_hash_header(bytes);
    restore_file_bytes(path, bytes);

    auto reopened = disk_hash_table_t::create(path, 16, &resource);
    INFO("an overflow cursor below the overflow page id base is corruption, not a value to clamp");
    REQUIRE(reopened.has_error());
}

// The checksum arm: without it, a flipped bit that still looks structurally plausible silently re-addresses EVERY key.
TEST_CASE("services::index::disk_hash_table::open_refuses_a_header_whose_checksum_does_not_match") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_checksum_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 16, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    auto bytes = read_file_bytes(path);
    write_le32_at(bytes, 16, 17); // one flipped count, structurally plausible
    restore_file_bytes(path, bytes);

    auto reopened = disk_hash_table_t::create(path, 16, &resource);
    INFO("a header the checksum disowns must not be loaded, however plausible its fields look");
    REQUIRE(reopened.has_error());
}

// The magic arm: a file that isn't a hash index must be refused by name before any field is interpreted.
TEST_CASE("services::index::disk_hash_table::open_refuses_a_file_without_the_magic") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_magic_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 16, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    auto bytes = read_file_bytes(path);
    reseal_hash_header(bytes);
    std::memset(bytes.data(), 0, 8); // valid checksum, no name
    restore_file_bytes(path, bytes);

    auto reopened = disk_hash_table_t::create(path, 16, &resource);
    INFO("a header page that does not carry this table's magic is not this table's header");
    REQUIRE(reopened.has_error());
}

// An erased slot must not stay dead: if every insert appended a NEW slot, put/erase on one key would exhaust the page.
TEST_CASE("services::index::disk_hash_table::an_erased_slot_is_reused_by_the_next_insert") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_slot_reuse.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    disk_hash_table_t table(path, 1, &resource);
    table.set_auto_rehash_suppressed(true);

    // Live size never exceeds 1 across all put/erase rounds.
    for (int64_t i = 0; i < 500; ++i) {
        REQUIRE_FALSE(table.put("steady-key", i, 1, static_cast<uint64_t>(1000 + i)).contains_error());
        auto erased = table.erase("steady-key", loader_must_not_be_consulted(&resource));
        REQUIRE_FALSE(erased.has_error());
        REQUIRE(erased.value());
    }

    const bool overflow_grew = std::filesystem::exists(overflow_path) && std::filesystem::file_size(overflow_path) > 0;
    INFO("a page cycling ONE live entry must reuse its freed slot, not grow an overflow chain");
    REQUIRE_FALSE(overflow_grew);

    REQUIRE_FALSE(table.put("steady-key", 42, 3, 4242).contains_error());
    auto found = must_read(table.get("steady-key", loader_must_not_be_consulted(&resource)));
    REQUIRE(found.has_value());
    REQUIRE(found->value == 42);
    REQUIRE(found->log_file_id == 3);
    REQUIRE(found->log_offset == 4242);
}

// assert(false) alone compiles out under -DNDEBUG; the destructor must report failure on stderr in every build mode.

#include <fcntl.h>
#include <unistd.h>

namespace {
    template<typename fn_t>
    std::string capture_stderr_of(const std::filesystem::path& capture_file, fn_t&& fn) {
        std::fflush(stderr);
        const int saved_stderr = ::dup(2);
        REQUIRE(saved_stderr >= 0);
        const int capture_fd = ::open(capture_file.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
        REQUIRE(capture_fd >= 0);
        REQUIRE(::dup2(capture_fd, 2) == 2);
        ::close(capture_fd);
        fn();
        std::fflush(stderr);
        REQUIRE(::dup2(saved_stderr, 2) == 2);
        ::close(saved_stderr);
        return read_file_bytes(capture_file);
    }
} // namespace

TEST_CASE("services::index::disk_hash_table::a_failed_closing_flush_is_reported_loudly") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_close_flush_report.data");
    const auto capture_file = mk_path("hash_close_flush_report.stderr");
    std::filesystem::remove(path);
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    const auto stderr_text = capture_stderr_of(capture_file, [&] {
        env_var_guard_t failpoint("OTTERBRIX_DISK_HASH_CLOSE_FAILPOINT", "1");
        disk_hash_table_t table(path, 8, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
    });

    INFO("a closing flush that failed must be named on stderr, not dropped in silence");
    REQUIRE(stderr_text.find("disk_hash_table") != std::string::npos);
    REQUIRE(stderr_text.find(path.string()) != std::string::npos);
}
