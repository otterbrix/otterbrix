#include <algorithm>
#include <catch2/catch.hpp>
#include <filesystem>
#include <memory>

#include "components/index/disk_hash_single_field_index.hpp"
#include "components/index/hash_single_field_index.hpp"
#include "components/index/index_engine.hpp"
#include "components/index/logical_value_binary_codec.hpp"
#include "components/index/single_field_index.hpp"
#include "services/index/disk_hash_table.hpp"
#include <components/table/row_version_manager.hpp>

using namespace components::index;
using namespace components::table;
using namespace components::expressions;
using key = components::expressions::key_t;

namespace {
    enum class hash_index_mode
    {
        in_memory,
        on_disk
    };

    std::unique_ptr<index_t> make_hash_mvcc_index(std::pmr::memory_resource* resource,
                                                  const std::string& name,
                                                  const std::string& file_name,
                                                  hash_index_mode mode) {
        if (mode == hash_index_mode::in_memory) {
            return std::make_unique<hash_single_field_index_t>(resource,
                                                               name,
                                                               keys_base_storage_t{key(resource, "val")});
        }
        const auto base = std::filesystem::path("/tmp/index_disk/components_hash_mvcc_tests");
        std::filesystem::create_directories(base);
        const auto file = base / file_name;
        std::filesystem::remove(file);
        return std::make_unique<disk_hash_single_field_index_t>(
            resource,
            name,
            keys_base_storage_t{key(resource, "val")},
            boost::intrusive_ptr(new services::index::disk_hash_table_t(file,
                                                                        services::index::disk_hash_table_t::default_bucket_count,
                                                                        resource)));
    }

    void run_txn_insert_search_contract(hash_index_mode mode) {
        auto resource = std::pmr::synchronized_pool_resource();
        uint64_t txn1 = TRANSACTION_ID_START + 1;
        uint64_t txn2 = TRANSACTION_ID_START + 2;
        components::types::logical_value_t val42(&resource, int64_t(42));

        {
            auto index = make_hash_mvcc_index(&resource,
                                              "test_hash_idx",
                                              mode == hash_index_mode::in_memory ? "txn_insert_search_mem_1.bin"
                                                                                 : "txn_insert_search_disk_1.bin",
                                              mode);
            index->insert(val42, int64_t(0), txn1, {});
            auto result = index->search(compare_type::eq, val42, txn1 - 1, txn1, {});
            REQUIRE(result.size() == 1);
            REQUIRE(result[0] == 0);
        }

        {
            auto index = make_hash_mvcc_index(&resource,
                                              "test_hash_idx",
                                              mode == hash_index_mode::in_memory ? "txn_insert_search_mem_2.bin"
                                                                                 : "txn_insert_search_disk_2.bin",
                                              mode);
            index->insert(val42, int64_t(0), txn1, {});
            auto result = index->search(compare_type::eq, val42, txn1 - 1, txn2, {});
            REQUIRE(result.empty());
        }

        {
            auto index = make_hash_mvcc_index(&resource,
                                              "test_hash_idx",
                                              mode == hash_index_mode::in_memory ? "txn_insert_search_mem_3.bin"
                                                                                 : "txn_insert_search_disk_3.bin",
                                              mode);
            index->insert(val42, int64_t(0), txn1, {});
            index->commit_insert(txn1, 10);
            auto result = index->search(compare_type::eq, val42, 15, txn2, {});
            if (mode == hash_index_mode::in_memory) {
                REQUIRE(result.size() == 1);
                REQUIRE(result[0] == 0);
            } else {
                // reads from bitcash disk agent
                REQUIRE(result.empty());
            }
        }

        {
            auto index = make_hash_mvcc_index(&resource,
                                              "test_hash_idx",
                                              mode == hash_index_mode::in_memory ? "txn_insert_search_mem_4.bin"
                                                                                 : "txn_insert_search_disk_4.bin",
                                              mode);
            index->insert(val42, int64_t(0), txn1, {});
            index->revert_insert(txn1);
            auto result = index->search(compare_type::eq, val42, txn1 - 1, txn1, {});
            REQUIRE(result.empty());
        }
    }

    void run_full_lifecycle_contract(hash_index_mode mode) {
        auto resource = std::pmr::synchronized_pool_resource();
        auto index = make_hash_mvcc_index(&resource,
                                          "test_hash_idx",
                                          mode == hash_index_mode::in_memory ? "full_lifecycle_mem.bin"
                                                                             : "full_lifecycle_disk.bin",
                                          mode);

        uint64_t txn1 = TRANSACTION_ID_START + 1;
        uint64_t txn2 = TRANSACTION_ID_START + 2;
        uint64_t commit1 = 10;
        uint64_t commit2 = 20;

        components::types::logical_value_t val42(&resource, int64_t(42));

        index->insert(val42, int64_t(0), txn1, {});
        index->commit_insert(txn1, commit1);

        auto result = index->search(compare_type::eq, val42, commit1 + 1, txn2, {});
        if (mode == hash_index_mode::in_memory) {
            REQUIRE(result.size() == 1);
        } else {
            // reads from bitcash disk agent
            REQUIRE(result.empty());
        }

        index->mark_delete(val42, int64_t(0), txn2, {});
        index->commit_delete(txn2, commit2);

        result = index->search(compare_type::eq, val42, commit2 + 1, TRANSACTION_ID_START + 3, {});
        REQUIRE(result.empty());

        index->cleanup_versions(commit2 + 1);
        result = index->search(compare_type::eq, val42, {});
        REQUIRE(result.empty());
    }

    void run_pending_delete_visibility_contract(hash_index_mode mode) {
        auto resource = std::pmr::synchronized_pool_resource();
        auto index = make_hash_mvcc_index(&resource,
                                          "test_hash_idx_pending_delete",
                                          mode == hash_index_mode::in_memory ? "pending_delete_mem.bin"
                                                                             : "pending_delete_disk.bin",
                                          mode);

        const uint64_t txn_insert = TRANSACTION_ID_START + 11;
        const uint64_t txn_delete = TRANSACTION_ID_START + 22;
        const uint64_t txn_other = TRANSACTION_ID_START + 33;
        const uint64_t commit_insert = 100;
        const uint64_t commit_delete = 200;
        components::types::logical_value_t val42(&resource, int64_t(42));

        index->insert(val42, int64_t(7), txn_insert, {});
        index->commit_insert(txn_insert, commit_insert);

        index->mark_delete(val42, int64_t(7), txn_delete, {});

        auto seen_by_deleter = index->search(compare_type::eq, val42, commit_insert + 1, txn_delete, {});
        REQUIRE(seen_by_deleter.empty());

        auto seen_by_other = index->search(compare_type::eq, val42, commit_insert + 1, txn_other, {});
        if (mode == hash_index_mode::in_memory) {
            REQUIRE(seen_by_other.size() == 1);
            REQUIRE(seen_by_other[0] == 7);
        } else {
            // reads from bitcash disk agent
            REQUIRE(seen_by_other.empty());
        }

        index->commit_delete(txn_delete, commit_delete);
        auto gone_after_commit = index->search(compare_type::eq, val42, commit_delete + 1, txn_other + 1, {});
        REQUIRE(gone_after_commit.empty());
    }

    void run_revert_insert_contract(hash_index_mode mode) {
        auto resource = std::pmr::synchronized_pool_resource();
        auto index = make_hash_mvcc_index(&resource,
                                          "test_hash_idx_revert",
                                          mode == hash_index_mode::in_memory ? "revert_insert_mem.bin"
                                                                             : "revert_insert_disk.bin",
                                          mode);
        const uint64_t txn_insert = TRANSACTION_ID_START + 44;
        const uint64_t txn_other = TRANSACTION_ID_START + 55;
        components::types::logical_value_t val42(&resource, int64_t(42));

        index->insert(val42, int64_t(9), txn_insert, {});
        auto own_before_revert = index->search(compare_type::eq, val42, txn_insert - 1, txn_insert, {});
        REQUIRE(own_before_revert.size() == 1);

        index->revert_insert(txn_insert);

        auto own_after_revert = index->search(compare_type::eq, val42, txn_insert - 1, txn_insert, {});
        REQUIRE(own_after_revert.empty());

        auto other_after_revert = index->search(compare_type::eq, val42, txn_insert - 1, txn_other, {});
        REQUIRE(other_after_revert.empty());
    }
} // namespace

TEST_CASE("index_value_t:backward_compat") {
    SECTION("default constructor") {
        index_value_t val;
        REQUIRE(val.insert_id == 0);
        REQUIRE(val.delete_id == NOT_DELETED_ID);
    }

    SECTION("row_index constructor") {
        index_value_t val(42);
        REQUIRE(val.row_index == 42);
        REQUIRE(val.insert_id == 0);
        REQUIRE(val.delete_id == NOT_DELETED_ID);
    }

    SECTION("full constructor") {
        index_value_t val(10, 100, 200);
        REQUIRE(val.row_index == 10);
        REQUIRE(val.insert_id == 100);
        REQUIRE(val.delete_id == 200);
    }
}

TEST_CASE("index_entry_visible:committed_entries") {
    // Entry with committed insert, no delete
    index_value_t committed(1, 5, NOT_DELETED_ID);

    SECTION("visible to transaction starting after commit") {
        REQUIRE(index_entry_visible(committed, 10, TRANSACTION_ID_START + 1));
    }

    SECTION("not visible to transaction starting before commit") {
        REQUIRE_FALSE(index_entry_visible(committed, 3, TRANSACTION_ID_START + 1));
    }

    SECTION("visible to own transaction") { REQUIRE(index_entry_visible(committed, 3, 5)); }
}

TEST_CASE("index_entry_visible:uncommitted_entries") {
    // Entry with uncommitted insert (txn_id in TRANSACTION_ID_START range)
    uint64_t txn_id = TRANSACTION_ID_START + 100;
    index_value_t uncommitted(1, txn_id, NOT_DELETED_ID);

    SECTION("visible to own transaction") { REQUIRE(index_entry_visible(uncommitted, txn_id - 1, txn_id)); }

    SECTION("not visible to other transactions") {
        uint64_t other_txn = TRANSACTION_ID_START + 200;
        REQUIRE_FALSE(index_entry_visible(uncommitted, txn_id - 1, other_txn));
    }
}

TEST_CASE("index_entry_visible:deleted_entries") {
    // Entry committed at 5, deleted at 10 (committed delete)
    index_value_t deleted_entry(1, 5, 10);

    SECTION("visible before delete committed") {
        REQUIRE(index_entry_visible(deleted_entry, 8, TRANSACTION_ID_START + 1));
    }

    SECTION("not visible after delete committed") {
        REQUIRE_FALSE(index_entry_visible(deleted_entry, 15, TRANSACTION_ID_START + 1));
    }

    SECTION("not visible to deleting transaction") { REQUIRE_FALSE(index_entry_visible(deleted_entry, 8, 10)); }
}

TEST_CASE("index_entry_visible:see_all_committed") {
    // Special case: txn_id==0 && start_time==0

    SECTION("sees committed entry") {
        index_value_t committed(1, 5, NOT_DELETED_ID);
        REQUIRE(index_entry_visible(committed, 0, 0));
    }

    SECTION("does not see uncommitted insert") {
        uint64_t txn_id = TRANSACTION_ID_START + 100;
        index_value_t uncommitted(1, txn_id, NOT_DELETED_ID);
        REQUIRE_FALSE(index_entry_visible(uncommitted, 0, 0));
    }

    SECTION("does not see committed+deleted entry") {
        index_value_t deleted_entry(1, 5, 10);
        REQUIRE_FALSE(index_entry_visible(deleted_entry, 0, 0));
    }

    SECTION("sees entry with uncommitted delete") {
        uint64_t del_txn = TRANSACTION_ID_START + 200;
        index_value_t pending_delete(1, 5, del_txn);
        REQUIRE(index_entry_visible(pending_delete, 0, 0));
    }
}

TEST_CASE("single_field_index:txn_insert_search") {
    auto resource = std::pmr::synchronized_pool_resource();
    single_field_index_t index(&resource, "test_idx", {key(&resource, "val")});

    uint64_t txn1 = TRANSACTION_ID_START + 1;
    uint64_t txn2 = TRANSACTION_ID_START + 2;

    // txn1 inserts value 42 at row 0
    components::types::logical_value_t val42(&resource, int64_t(42));
    index.insert(val42, int64_t(0), txn1, {});

    SECTION("visible to own transaction") {
        auto result = index.search(compare_type::eq, val42, txn1 - 1, txn1, {});
        REQUIRE(result.size() == 1);
        REQUIRE(result[0] == 0);
    }

    SECTION("not visible to other transaction") {
        auto result = index.search(compare_type::eq, val42, txn1 - 1, txn2, {});
        REQUIRE(result.empty());
    }

    SECTION("visible after commit") {
        index.commit_insert(txn1, 10); // commit_id = 10
        auto result = index.search(compare_type::eq, val42, 15, txn2, {});
        REQUIRE(result.size() == 1);
        REQUIRE(result[0] == 0);
    }

    SECTION("gone after revert") {
        index.revert_insert(txn1);
        auto result = index.search(compare_type::eq, val42, txn1 - 1, txn1, {});
        REQUIRE(result.empty());
    }
}

TEST_CASE("single_field_index:full_lifecycle") {
    auto resource = std::pmr::synchronized_pool_resource();
    single_field_index_t index(&resource, "test_idx", {key(&resource, "val")});

    uint64_t txn1 = TRANSACTION_ID_START + 1;
    uint64_t txn2 = TRANSACTION_ID_START + 2;
    uint64_t commit1 = 10;
    uint64_t commit2 = 20;

    components::types::logical_value_t val42(&resource, int64_t(42));

    // insert → commit → visible
    index.insert(val42, int64_t(0), txn1, {});
    index.commit_insert(txn1, commit1);

    auto result = index.search(compare_type::eq, val42, commit1 + 1, txn2, {});
    REQUIRE(result.size() == 1);

    // delete → commit → not visible
    index.mark_delete(val42, int64_t(0), txn2, {});
    index.commit_delete(txn2, commit2);

    result = index.search(compare_type::eq, val42, commit2 + 1, TRANSACTION_ID_START + 3, {});
    REQUIRE(result.empty());

    // cleanup → erased from storage
    index.cleanup_versions(commit2 + 1);
    // After cleanup, even the old non-txn search should not find it
    result = index.search(compare_type::eq, val42, {});
    REQUIRE(result.empty());
}

TEST_CASE("hash_single_field_index:txn_insert_search") { run_txn_insert_search_contract(hash_index_mode::in_memory); }

TEST_CASE("hash_single_field_index:full_lifecycle") { run_full_lifecycle_contract(hash_index_mode::in_memory); }

TEST_CASE("disk_single_field_index:txn_insert_search") { run_txn_insert_search_contract(hash_index_mode::on_disk); }

TEST_CASE("disk_single_field_index:full_lifecycle") { run_full_lifecycle_contract(hash_index_mode::on_disk); }

TEST_CASE("hash_single_field_index:pending_delete_visibility") {
    run_pending_delete_visibility_contract(hash_index_mode::in_memory);
}

TEST_CASE("disk_single_field_index:pending_delete_visibility") {
    run_pending_delete_visibility_contract(hash_index_mode::on_disk);
}

TEST_CASE("hash_single_field_index:revert_insert_contract") { run_revert_insert_contract(hash_index_mode::in_memory); }

TEST_CASE("disk_single_field_index:revert_insert_contract") { run_revert_insert_contract(hash_index_mode::on_disk); }

TEST_CASE("index_engine:txn_methods") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto engine = make_index_engine(&resource);
    make_index<single_field_index_t>(engine, "idx1", {key(&resource, "val")});

    uint64_t txn1 = TRANSACTION_ID_START + 1;
    uint64_t commit1 = 10;

    auto* idx = search_index(engine, std::string("idx1"));
    REQUIRE(idx != nullptr);

    components::types::logical_value_t val(&resource, int64_t(99));
    idx->insert(val, int64_t(0), txn1, {});

    // Commit via engine
    engine->commit_insert(txn1, commit1);

    auto result = idx->search(compare_type::eq, val, commit1 + 1, TRANSACTION_ID_START + 2, {});
    REQUIRE(result.size() == 1);
    REQUIRE(result[0] == 0);
}
