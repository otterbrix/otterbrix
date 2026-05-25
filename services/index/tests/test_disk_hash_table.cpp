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
    const auto path = mk_path("disk_hash_table_roundtrip.data");
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
    const auto path = mk_path("disk_hash_table_persist.data");
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

TEST_CASE("services::index::disk_hash_table::multiple_values_per_key") {
    const auto path = mk_path("disk_hash_table_multi_values.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 32);
    REQUIRE(table.put("dup", 10, 1, 100));
    REQUIRE(table.put("dup", 20, 2, 200));
    REQUIRE(table.put("dup", 10, 3, 300));

    const auto values = table.get_all("dup");
    REQUIRE(values.size() == 3);

    size_t count10 = 0;
    size_t count20 = 0;
    for (const auto& v : values) {
        if (v.value == 10) {
            ++count10;
        } else if (v.value == 20) {
            ++count20;
        }
    }
    REQUIRE(count10 == 2);
    REQUIRE(count20 == 1);
}

TEST_CASE("services::index::disk_hash_table::long_key_prefix_and_loader") {
    const auto path = mk_path("disk_hash_table_long_key.data");
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

TEST_CASE("services::index::disk_hash_table::pending_insert_finalize_and_checkpoint") {
    const auto path = mk_path("disk_hash_table_pending_insert.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    disk_hash_table_t table(path, 32);
    table.append_pending_insert(101, "k.pending", 42);

    REQUIRE_FALSE(table.get("k.pending").has_value());
    REQUIRE(table.checkpoint_txn_id() == 0);

    table.finalize_txn(101, true, false);

    auto v = table.get("k.pending");
    REQUIRE(v.has_value());
    REQUIRE(v->value == 42);
    REQUIRE(table.checkpoint_txn_id() == 101);
}

TEST_CASE("services::index::disk_hash_table::pending_delete_finalize") {
    const auto path = mk_path("disk_hash_table_pending_delete.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    disk_hash_table_t table(path, 32);
    REQUIRE(table.put("k.delete", 77, 0, 0));
    REQUIRE(table.get("k.delete").has_value());

    table.append_pending_delete(202, "k.delete", 77);
    REQUIRE(table.get("k.delete").has_value());

    table.finalize_txn(202, false, true);
    REQUIRE_FALSE(table.get("k.delete").has_value());
    REQUIRE(table.checkpoint_txn_id() == 202);
}

TEST_CASE("services::index::disk_hash_table::pending_revert_does_not_apply") {
    const auto path = mk_path("disk_hash_table_pending_revert.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    disk_hash_table_t table(path, 32);
    table.append_pending_insert(303, "k.revert", 1);
    table.append_pending_delete(303, "k.absent", 0);

    table.finalize_txn(303, false, false);

    REQUIRE_FALSE(table.get("k.revert").has_value());
    REQUIRE_FALSE(table.get("k.absent").has_value());
    REQUIRE(table.checkpoint_txn_id() == 303);
}

TEST_CASE("services::index::disk_hash_table::pending_survives_reopen_until_finalize") {
    const auto path = mk_path("disk_hash_table_pending_reopen.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    {
        disk_hash_table_t table(path, 32);
        table.append_pending_insert(404, "k.reopen", 404);
        REQUIRE_FALSE(table.get("k.reopen").has_value());
    }

    {
        disk_hash_table_t reopened(path, 32);
        REQUIRE_FALSE(reopened.get("k.reopen").has_value());
        REQUIRE(reopened.checkpoint_txn_id() == 0);
        reopened.finalize_txn(404, true, false);
        auto v = reopened.get("k.reopen");
        REQUIRE(v.has_value());
        REQUIRE(v->value == 404);
        REQUIRE(reopened.checkpoint_txn_id() == 404);
    }
}

TEST_CASE("services::index::disk_hash_table::rehash_preserves_entries") {
    const auto path = mk_path("disk_hash_table_rehash.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 4);
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
    const auto path = mk_path("disk_hash_table_rehash_truncated.data");
    std::filesystem::remove(path);

    const std::string key1(200, 'a');
    const std::string key2 = std::string(199, 'a') + "b";

    disk_hash_table_t table(path, 4);
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

    auto v1 = table.get(key1, [&](uint32_t, uint64_t, std::string& out) {
        out = key1;
        return true;
    });
    auto v2 = table.get(key2, [&](uint32_t, uint64_t, std::string& out) {
        out = key2;
        return true;
    });
    REQUIRE(v1.has_value());
    REQUIRE(v1->value == 11);
    REQUIRE(v2.has_value());
    REQUIRE(v2->value == 22);
}
