#include <catch2/catch.hpp>
#include <services/index/disk_hash_table.hpp>
#include <fstream>

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

TEST_CASE("services::index::disk_hash_table::auto_rehash_by_load_factor") {
    const auto path = mk_path("disk_hash_table_auto_rehash.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 4);
    const auto initial_buckets = table.bucket_count();
    REQUIRE(initial_buckets == 4);

    for (int i = 0; i < 20; ++i) {
        const auto key = "auto.k." + std::to_string(i);
        REQUIRE(table.put(key, static_cast<int64_t>(i), 10, static_cast<uint64_t>(i + 1)));
    }

    REQUIRE(table.bucket_count() > initial_buckets);
    REQUIRE(table.load_factor() <= 0.85);
}

TEST_CASE("services::index::disk_hash_table::finalize_txn_is_idempotent") {
    const auto path = mk_path("disk_hash_table_finalize_idempotent.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    disk_hash_table_t table(path, 32);
    table.append_pending_insert(901, "k.idempotent", 91);

    table.finalize_txn(901, true, false);
    table.finalize_txn(901, true, false);

    const auto values = table.get_all("k.idempotent");
    REQUIRE(values.size() == 1);
    REQUIRE(values[0].value == 91);
    REQUIRE(table.checkpoint_txn_id() == 901);
}

TEST_CASE("services::index::disk_hash_table::finalize_out_of_order_txns") {
    const auto path = mk_path("disk_hash_table_finalize_out_of_order.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    disk_hash_table_t table(path, 32);
    table.append_pending_insert(1001, "k.txn1", 11);
    table.append_pending_insert(1002, "k.txn2", 22);

    table.finalize_txn(1002, true, false);
    auto v2 = table.get("k.txn2");
    auto v1_before = table.get("k.txn1");
    REQUIRE(v2.has_value());
    REQUIRE(v2->value == 22);
    REQUIRE_FALSE(v1_before.has_value());
    REQUIRE(table.checkpoint_txn_id() == 1002);

    table.finalize_txn(1001, true, false);
    auto v1_after = table.get("k.txn1");
    REQUIRE(v1_after.has_value());
    REQUIRE(v1_after->value == 11);
    REQUIRE(table.checkpoint_txn_id() == 1001);
}

TEST_CASE("services::index::disk_hash_table::mixed_ops_same_txn_same_key_row") {
    const auto path = mk_path("disk_hash_table_mixed_same_txn.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    SECTION("insert_then_delete_in_same_txn") {
        disk_hash_table_t table(path, 32);
        table.append_pending_insert(1101, "k.mix", 11);
        table.append_pending_delete(1101, "k.mix", 11);
        table.finalize_txn(1101, true, true);
        REQUIRE_FALSE(table.get("k.mix").has_value());
    }

    SECTION("delete_then_insert_in_same_txn") {
        disk_hash_table_t table(path, 32);
        table.append_pending_delete(1102, "k.mix", 22);
        table.append_pending_insert(1102, "k.mix", 22);
        table.finalize_txn(1102, true, true);
        auto v = table.get("k.mix");
        REQUIRE(v.has_value());
        REQUIRE(v->value == 22);
    }

    SECTION("duplicate_pending_insert_same_row") {
        disk_hash_table_t table(path, 32);
        table.append_pending_insert(1103, "k.mix", 33);
        table.append_pending_insert(1103, "k.mix", 33);
        table.finalize_txn(1103, true, false);
        auto values = table.get_all("k.mix");
        REQUIRE(values.size() == 2);
        REQUIRE(values[0].value == 33);
        REQUIRE(values[1].value == 33);
    }
}

TEST_CASE("services::index::disk_hash_table::conflicts_same_key_different_rows_different_txns") {
    const auto path = mk_path("disk_hash_table_conflicts_multi_txn.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    disk_hash_table_t table(path, 32);
    table.append_pending_insert(1201, "k.conflict", 101);
    table.append_pending_insert(1202, "k.conflict", 202);
    table.append_pending_delete(1203, "k.conflict", 101);
    table.append_pending_insert(1204, "k.conflict", 303);
    table.append_pending_delete(1205, "k.conflict", 202);

    // Interleaved finalization order.
    table.finalize_txn(1202, true, false);
    table.finalize_txn(1203, false, true);
    table.finalize_txn(1201, true, false);
    table.finalize_txn(1205, false, true);
    table.finalize_txn(1204, true, false);

    auto values = table.get_all("k.conflict");
    REQUIRE(values.size() == 2);
    bool has101 = false;
    bool has303 = false;
    for (const auto& v : values) {
        if (v.value == 101) {
            has101 = true;
        }
        if (v.value == 303) {
            has303 = true;
        }
    }
    REQUIRE(has101);
    REQUIRE(has303);
}

TEST_CASE("services::index::disk_hash_table::pending_log_partial_record_recovery") {
    const auto path = mk_path("disk_hash_table_pending_partial_recovery.data");
    std::filesystem::remove(path);
    const auto pending = path.parent_path() / "pending.log";
    const auto checkpoint = path.parent_path() / "pending.checkpoint";
    std::filesystem::remove(pending);
    std::filesystem::remove(checkpoint);

    disk_hash_table_t table(path, 32);
    table.append_pending_insert(1301, "k.good", 1301);

    {
        std::ofstream out(pending, std::ios::binary | std::ios::app);
        const char op = 'I';
        const uint64_t bad_txn = 9999;
        out.write(&op, sizeof(op));
        out.write(reinterpret_cast<const char*>(&bad_txn), sizeof(bad_txn));
        // Intentionally incomplete tail (no row_id/key_len/key bytes).
    }

    REQUIRE_NOTHROW(table.finalize_txn(1301, true, false));
    auto v = table.get("k.good");
    REQUIRE(v.has_value());
    REQUIRE(v->value == 1301);
}

TEST_CASE("services::index::disk_hash_table::truncated_key_pending_finalize_rehash") {
    const auto path = mk_path("disk_hash_table_truncated_pending_finalize_rehash.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    const std::string long_key(200, 'z');
    disk_hash_table_t table(path, 4);
    table.append_pending_insert(1401, long_key, 1401);
    table.finalize_txn(1401, true, false);

    REQUIRE_THROWS(table.rehash(64));
    REQUIRE(table.rehash(64, [&](uint32_t, uint64_t, std::string& out) {
        out = long_key;
        return true;
    }));

    auto v = table.get(long_key, [&](uint32_t, uint64_t, std::string& out) {
        out = long_key;
        return true;
    });
    REQUIRE(v.has_value());
    REQUIRE(v->value == 1401);
}

TEST_CASE("services::index::disk_hash_table::many_txns_many_pending_periodic_finalize_checkpoint") {
    const auto path = mk_path("disk_hash_table_mass_txn_stability.data");
    std::filesystem::remove(path);
    std::filesystem::remove(path.parent_path() / "pending.log");
    std::filesystem::remove(path.parent_path() / "pending.checkpoint");

    disk_hash_table_t table(path, 32);
    int64_t expected_total = 0;

    for (uint64_t txn = 1501; txn < 1701; ++txn) {
        for (int i = 0; i < 5; ++i) {
            const auto key = "k.mass." + std::to_string(txn) + "." + std::to_string(i);
            const int64_t row = static_cast<int64_t>(txn * 10 + i);
            table.append_pending_insert(txn, key, row);
        }

        if (txn % 3 == 0) {
            table.finalize_txn(txn, true, false);
            expected_total += 5;
        } else if (txn % 5 == 0) {
            table.finalize_txn(txn, false, false); // simulate rollback
        } else {
            table.finalize_txn(txn, true, false);
            expected_total += 5;
        }

        if (txn % 25 == 0) {
            table.sync();
            REQUIRE(table.checkpoint_txn_id() == txn);
        }
    }

    int64_t actual_total = 0;
    table.for_each([&](const disk_hash_table_t::value_ref_t&) {
        ++actual_total;
    });
    REQUIRE(actual_total == expected_total);
    REQUIRE(table.checkpoint_txn_id() == 1700);
}
