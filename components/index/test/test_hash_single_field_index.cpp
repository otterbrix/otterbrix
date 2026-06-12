#include <algorithm>
#include <filesystem>
#include <memory>
#include <catch2/catch.hpp>
#include <filesystem>

#include "components/index/disk_hash_single_field_index.hpp"
#include "components/index/hash_single_field_index.hpp"
#include "components/index/index_engine.hpp"
#include "components/index/logical_value_binary_codec.hpp"
#include "components/index/single_field_index.hpp"
#include "components/tests/generaty.hpp"
#include "services/index/bitcask_hash_key_loader.hpp"
#include "services/index/bitcask_index_disk.hpp"
#include "services/index/disk_hash_table.hpp"

using namespace components::index;
using key = components::expressions::key_t;

namespace {
    enum class hash_index_mode
    {
        in_memory,
        on_disk
    };

    std::unique_ptr<index_t>
    make_hash_index(std::pmr::memory_resource* resource, const std::string& name, hash_index_mode mode) {
        if (mode == hash_index_mode::in_memory) {
            return std::make_unique<hash_single_field_index_t>(resource,
                                                               name,
                                                               keys_base_storage_t{key(resource, "count")});
        }
        const auto base = std::filesystem::path("/tmp/index_disk/components_hash_tests");
        std::filesystem::create_directories(base);
        const auto file = base / (name + ".bin");
        std::filesystem::remove(file);
        return std::make_unique<disk_hash_single_field_index_t>(
            resource,
            name,
            keys_base_storage_t{key(resource, "count")},
            boost::intrusive_ptr(new services::index::disk_hash_table_t(file,
                                                                        services::index::disk_hash_table_t::default_bucket_count,
                                                                        resource)));
    }

    void run_base_contract(hash_index_mode mode) {
        auto resource = std::pmr::synchronized_pool_resource();
        auto index =
            make_hash_index(&resource, mode == hash_index_mode::in_memory ? "hash_count_ram" : "hash_count_disk", mode);
        std::vector<std::pair<int64_t, int64_t>> data =
            {{0, 0}, {1, 1}, {10, 2}, {5, 3}, {6, 4}, {2, 5}, {8, 6}, {13, 7}};

        for (const auto& [value, row_idx] : data) {
            components::types::logical_value_t val(&resource, value);
            index->insert(val, row_idx, {});
        }

        components::types::logical_value_t value(&resource, static_cast<int64_t>(10));
        auto find_range = index->find(value, {});
        if (mode == hash_index_mode::in_memory) {
            REQUIRE(find_range.first != find_range.second);
            REQUIRE(std::distance(find_range.first, find_range.second) == 1);
            REQUIRE(find_range.first->row_index == 2);
        } else {
            // reads from bitcash disk agent
            REQUIRE(find_range.first == find_range.second);
        }

        components::types::logical_value_t missing(&resource, static_cast<int64_t>(11));
        find_range = index->find(missing, {});
        REQUIRE(find_range.first == find_range.second);

        for (const auto& [data_value, row_idx] : data) {
            components::types::logical_value_t val(&resource, data_value);
            index->insert(val, row_idx + 100, {});
        }
        find_range = index->find(value, {});
        if (mode == hash_index_mode::in_memory) {
            REQUIRE(find_range.first != find_range.second);
            REQUIRE(std::distance(find_range.first, find_range.second) == 2);
        } else {
            // reads from bitcash disk agent
            REQUIRE(find_range.first == find_range.second);
        }

        std::vector<int64_t> rows;
        for (auto it = find_range.first; it != find_range.second; ++it) {
            rows.push_back(it->row_index);
        }
        if (mode == hash_index_mode::in_memory) {
            REQUIRE(std::find(rows.begin(), rows.end(), static_cast<int64_t>(2)) != rows.end());
            REQUIRE(std::find(rows.begin(), rows.end(), static_cast<int64_t>(102)) != rows.end());
        } else {
            // reads from bitcash disk agent
            REQUIRE(rows.empty());
        }
    }

    void run_engine_contract(hash_index_mode mode) {
        auto resource = std::pmr::synchronized_pool_resource();
        auto index_engine = make_index_engine(&resource);
        uint32_t id = INDEX_ID_UNDEFINED;
        if (mode == hash_index_mode::in_memory) {
            id = make_index<hash_single_field_index_t>(index_engine, "hash_count", {key(&resource, "count")});
        } else {
            const auto base = std::filesystem::path("/tmp/index_disk/components_hash_engine_tests");
            std::filesystem::create_directories(base);
            const auto file = base / "hash_count_disk.bin";
            std::filesystem::remove(file);
            id = make_index<disk_hash_single_field_index_t>(index_engine,
                                                            "hash_count",
                                                            {key(&resource, "count")},
                                                            boost::intrusive_ptr(new services::index::disk_hash_table_t(
                                                                file,
                                                                services::index::disk_hash_table_t::default_bucket_count,
                                                                &resource)));
        }

        auto* idx = search_index(index_engine, id);
        REQUIRE(idx != nullptr);

        idx->insert(components::types::logical_value_t(&resource, 0), int64_t(0), {});
        for (int i = 10; i >= 1; --i) {
            idx->insert(components::types::logical_value_t(&resource, i), int64_t(11 - i), {});
        }

        components::types::logical_value_t value(&resource, 5);
        auto find_range = idx->find(value, {});
        if (mode == hash_index_mode::in_memory) {
            REQUIRE(find_range.first != find_range.second);
            REQUIRE(find_range.first->row_index == 6);
        } else {
            // reads from bitcash disk agent
            REQUIRE(find_range.first == find_range.second);
        }
    }
} // namespace

TEST_CASE("hash_single_field_index:base") { run_base_contract(hash_index_mode::in_memory); }

TEST_CASE("disk_single_field_index:base") { run_base_contract(hash_index_mode::on_disk); }

TEST_CASE("hash_single_field_index:engine") { run_engine_contract(hash_index_mode::in_memory); }

TEST_CASE("disk_single_field_index:engine") { run_engine_contract(hash_index_mode::on_disk); }

TEST_CASE("disk_single_field_index:read_only_facade_direct_ops_do_not_materialize_committed_state") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto index = make_hash_index(&resource, "hash_count_disk_read_only", hash_index_mode::on_disk);
    components::types::logical_value_t value(&resource, int64_t(42));

    index->insert(value, int64_t(1), {});
    auto range = index->find(value, {});
    REQUIRE(range.first == range.second);

    const uint64_t txn_insert = components::table::TRANSACTION_ID_START + 1001;
    const uint64_t txn_other = components::table::TRANSACTION_ID_START + 1002;
    index->insert(value, int64_t(2), txn_insert, {});

    auto own_before_commit =
        index->search(components::expressions::compare_type::eq, value, txn_insert - 1, txn_insert, {});
    REQUIRE(own_before_commit.size() == 1);
    REQUIRE(own_before_commit[0] == 2);

    index->commit_insert(txn_insert, 77);
    auto other_after_commit = index->search(components::expressions::compare_type::eq, value, 78, txn_other, {});
    REQUIRE(other_after_commit.empty());

    index->remove(value, {});
    range = index->find(value, {});
    REQUIRE(range.first == range.second);
}

TEST_CASE("disk_single_field_index:pending_insert_delete_and_txn_state") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto index = make_hash_index(&resource, "hash_count_disk_txn_state", hash_index_mode::on_disk);

    const uint64_t txn_insert = components::table::TRANSACTION_ID_START + 201;
    const uint64_t txn_delete = components::table::TRANSACTION_ID_START + 202;
    components::types::logical_value_t key(&resource, int64_t(50));

    index->insert(key, int64_t(700), txn_insert, {});

    std::vector<int64_t> pending_rows;
    index->for_each_pending_insert(txn_insert,
                                   [&](const components::types::logical_value_t& pending_key, int64_t row_id) {
                                       REQUIRE(pending_key == key);
                                       pending_rows.push_back(row_id);
                                   });
    REQUIRE(pending_rows.size() == 1);
    REQUIRE(pending_rows.front() == 700);

    auto visible_own_txn =
        index->search(components::expressions::compare_type::eq, key, txn_insert - 1, txn_insert, {});
    REQUIRE(visible_own_txn.size() == 1);
    REQUIRE(visible_own_txn.front() == 700);

    index->mark_delete(key, 700, txn_delete, {});
    std::vector<int64_t> pending_delete_rows;
    index->for_each_pending_delete(txn_delete,
                                   [&](const components::types::logical_value_t& pending_key, int64_t row_id) {
                                       REQUIRE(pending_key == key);
                                       pending_delete_rows.push_back(row_id);
                                   });
    REQUIRE(pending_delete_rows.size() == 1);
    REQUIRE(pending_delete_rows.front() == 700);

    // Own delete transaction should not see row anymore.
    auto hidden_for_delete_txn =
        index->search(components::expressions::compare_type::eq, key, txn_delete - 1, txn_delete, {});
    REQUIRE(hidden_for_delete_txn.empty());

    // Commit paths clear pending maps.
    index->commit_insert(txn_insert, 1001);
    index->commit_delete(txn_delete, 1002);

    bool seen_after_commit_insert = false;
    index->for_each_pending_insert(txn_insert, [&](const components::types::logical_value_t&, int64_t) {
        seen_after_commit_insert = true;
    });
    REQUIRE_FALSE(seen_after_commit_insert);

    bool seen_after_commit_delete = false;
    index->for_each_pending_delete(txn_delete, [&](const components::types::logical_value_t&, int64_t) {
        seen_after_commit_delete = true;
    });
    REQUIRE_FALSE(seen_after_commit_delete);
}

TEST_CASE("disk_single_field_index:revert_cleanup_and_clear_memory") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto index = make_hash_index(&resource, "hash_count_disk_cleanup", hash_index_mode::on_disk);

    const uint64_t txn_insert = components::table::TRANSACTION_ID_START + 301;
    const uint64_t txn_delete = components::table::TRANSACTION_ID_START + 302;
    components::types::logical_value_t key(&resource, int64_t(77));

    index->insert(key, int64_t(900), txn_insert, {});
    index->mark_delete(key, 900, txn_delete, {});

    index->cleanup_versions(123456); // no-op branch for disk facade

    index->revert_insert(txn_insert);
    bool seen_after_revert = false;
    index->for_each_pending_insert(txn_insert, [&](const components::types::logical_value_t&, int64_t) {
        seen_after_revert = true;
    });
    REQUIRE_FALSE(seen_after_revert);

    index->clean_memory_to_new_elements(1);

    bool seen_after_clean_delete = false;
    index->for_each_pending_delete(txn_delete, [&](const components::types::logical_value_t&, int64_t) {
        seen_after_clean_delete = true;
    });
    REQUIRE_FALSE(seen_after_clean_delete);
}

TEST_CASE("disk_single_field_index:find_reads_disk_and_normalizes_integer_keys") {
    auto resource = std::pmr::synchronized_pool_resource();
    const auto base = std::filesystem::path("/tmp/index_disk/components_hash_normalize_tests");
    std::filesystem::create_directories(base);
    const auto file = base / "hash_count_disk_normalize.bin";
    std::filesystem::remove(file);

    auto table = boost::intrusive_ptr(new services::index::disk_hash_table_t(file,
                                                                              services::index::disk_hash_table_t::default_bucket_count,
                                                                              &resource));
    auto* table_raw = table.get();
    auto index = std::make_unique<disk_hash_single_field_index_t>(&resource,
                                                                  "hash_count_disk_normalize",
                                                                  keys_base_storage_t{key(&resource, "count")},
                                                                  std::move(table));

    // Persist committed row with BIGINT-encoded key.
    components::types::logical_value_t key_bigint(&resource, int64_t(42));
    const auto encoded = codec::encode_disk_hash_key(key_bigint);
    REQUIRE(table_raw->put(encoded, 4242, 0, 0));

    // Query with SMALLINT; normalize_key should cast integer family to BIGINT and match stored key.
    components::types::logical_value_t key_smallint(&resource, int16_t(42));
    auto eq_rows = index->search(components::expressions::compare_type::eq, key_smallint, 0, 0, {});
    REQUIRE(eq_rows.size() == 1);
    REQUIRE(eq_rows.front() == 4242);
}

TEST_CASE("disk_single_field_index:lower_upper_bound_not_supported") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto index = make_hash_index(&resource, "hash_count_disk_bounds", hash_index_mode::on_disk);
    components::types::logical_value_t key(&resource, int64_t(1));

    REQUIRE_THROWS(index->lower_bound(key, {}));
    REQUIRE_THROWS(index->upper_bound(key, {}));
}
