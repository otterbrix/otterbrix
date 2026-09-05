#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <memory>

#include "components/index/disk_hash_single_field_index.hpp"
#include "components/index/disk_ordered_single_field_index.hpp"
#include "components/index/index_engine.hpp"
#include <components/table/row_version_manager.hpp>

using namespace components::index;
using namespace components::table;
using namespace components::expressions;
using key = components::expressions::key_t;

namespace {
    // The two index FAMILIES, which is what this axis has meant since C6a removed the
    // in-memory pair. It used to read {in_memory, on_disk} and ran every contract below
    // over an in-memory hashed index beside the disk-backed one, because a table with no
    // disk catalog got the in-memory one; there is no such table any more (manager_index_t
    // refuses CREATE INDEX without a catalog path), and no such class.
    //
    // Running BOTH disk facades here is not a rename of that axis, it is the replacement
    // for the ordered half that went with it: the ordered MVCC cases this file used to
    // carry (`single_field_index:txn_insert_search`, `single_field_index:full_lifecycle`)
    // were the only unit-level statement of these contracts for an ORDERED index, and the
    // ordered DISK facade is what answers them on the real path now.
    enum class index_family
    {
        ordered_disk,
        hashed_disk
    };

    // Indexes are oid-identified; the facades open no file at all.
    constexpr components::catalog::oid_t mvcc_test_index_oid = 601u;

    // The disk facades open NOTHING (C2c, rule 10): their committed rows live in the store
    // each one's disk agent owns, so there is no file to name and no handle to build.
    std::unique_ptr<index_t> make_mvcc_index(std::pmr::memory_resource* resource, index_family family) {
        if (family == index_family::ordered_disk) {
            return std::make_unique<disk_ordered_single_field_index_t>(resource,
                                                                       mvcc_test_index_oid,
                                                                       keys_base_storage_t{key(resource, "val")});
        }
        return std::make_unique<disk_hash_single_field_index_t>(resource,
                                                                mvcc_test_index_oid,
                                                                keys_base_storage_t{key(resource, "val")});
    }

    // The rows this index answers with, asked through the door a disk facade has.
    //
    // A disk facade keeps only the PENDING half, and since C2c every local door on it is
    // terminal: with the storage handle gone, find() could only answer from that half, i.e.
    // with a SUBSET of the rows the key carries that no caller could tell from the whole
    // answer (rule 6). Its production door is merge_uncommitted_rows, which manager_index_t
    // calls with the committed half the disk agent returned; these cases have no agent, so
    // the half handed in is empty and what comes back is exactly the txn-local half.
    std::vector<int64_t> search_rows(index_t& index,
                                     compare_type compare,
                                     const components::types::logical_value_t& value,
                                     uint64_t txn_id) {
        std::pmr::vector<int64_t> rows(index.resource());
        index.merge_uncommitted_rows(compare, value, txn_id, {}, rows);
        return {rows.begin(), rows.end()};
    }

    void run_txn_insert_search_contract(index_family family) {
        auto resource = core::pmr::otterbrix_resource();
        uint64_t txn1 = TRANSACTION_ID_START + 1;
        uint64_t txn2 = TRANSACTION_ID_START + 2;
        components::types::logical_value_t val42(&resource, int64_t(42));

        {
            auto index = make_mvcc_index(&resource, family);
            index->insert(val42, int64_t(0), txn1, {});
            auto result = search_rows(*index, compare_type::eq, val42, txn1);
            REQUIRE(result.size() == 1);
            REQUIRE(result[0] == 0);
        }

        {
            auto index = make_mvcc_index(&resource, family);
            index->insert(val42, int64_t(0), txn1, {});
            auto result = search_rows(*index, compare_type::eq, val42, txn2);
            REQUIRE(result.empty());
        }

        {
            auto index = make_mvcc_index(&resource, family);
            index->insert(val42, int64_t(0), txn1, {});
            index->commit_insert(txn1, 10);
            // Committing hands the entry to the disk agent and drops the bucket. The
            // txn-local half is then empty by construction and the row is the agent's to
            // answer -- which is what the integration suites read back
            // (index_read_through_agent / index_ordered_read_through_agent).
            auto result = search_rows(*index, compare_type::eq, val42, txn2);
            REQUIRE(result.empty());
        }

        {
            auto index = make_mvcc_index(&resource, family);
            index->insert(val42, int64_t(0), txn1, {});
            index->revert_insert(txn1);
            auto result = search_rows(*index, compare_type::eq, val42, txn1);
            REQUIRE(result.empty());
        }
    }

    void run_full_lifecycle_contract(index_family family) {
        auto resource = core::pmr::otterbrix_resource();
        auto index = make_mvcc_index(&resource, family);

        uint64_t txn1 = TRANSACTION_ID_START + 1;
        uint64_t txn2 = TRANSACTION_ID_START + 2;
        uint64_t commit1 = 10;
        uint64_t commit2 = 20;

        components::types::logical_value_t val42(&resource, int64_t(42));

        index->insert(val42, int64_t(0), txn1, {});
        // Before the commit the inserting transaction sees its own write, so the empty
        // answers below are the commit's doing and not an index that never took the row.
        REQUIRE(search_rows(*index, compare_type::eq, val42, txn1).size() == 1);
        index->commit_insert(txn1, commit1);

        auto result = search_rows(*index, compare_type::eq, val42, txn2);
        REQUIRE(result.empty());

        index->mark_delete(val42, int64_t(0), txn2, {});
        index->commit_delete(txn2, commit2);

        result = search_rows(*index, compare_type::eq, val42, TRANSACTION_ID_START + 3);
        REQUIRE(result.empty());

        index->cleanup_versions(commit2 + 1);
        // The see-all-committed probe: txn_id 0, i.e. bucket 0 only.
        result = search_rows(*index, compare_type::eq, val42, 0);
        REQUIRE(result.empty());
    }

    void run_pending_delete_visibility_contract(index_family family) {
        auto resource = core::pmr::otterbrix_resource();
        auto index = make_mvcc_index(&resource, family);

        const uint64_t txn_insert = TRANSACTION_ID_START + 11;
        const uint64_t txn_delete = TRANSACTION_ID_START + 22;
        const uint64_t txn_other = TRANSACTION_ID_START + 33;
        const uint64_t commit_insert = 100;
        const uint64_t commit_delete = 200;
        components::types::logical_value_t val42(&resource, int64_t(42));

        index->insert(val42, int64_t(7), txn_insert, {});
        index->commit_insert(txn_insert, commit_insert);

        index->mark_delete(val42, int64_t(7), txn_delete, {});

        auto seen_by_deleter = search_rows(*index, compare_type::eq, val42, txn_delete);
        REQUIRE(seen_by_deleter.empty());

        auto seen_by_other = search_rows(*index, compare_type::eq, val42, txn_other);
        REQUIRE(seen_by_other.empty());

        index->commit_delete(txn_delete, commit_delete);
        auto gone_after_commit = search_rows(*index, compare_type::eq, val42, txn_other + 1);
        REQUIRE(gone_after_commit.empty());
    }

    void run_revert_insert_contract(index_family family) {
        auto resource = core::pmr::otterbrix_resource();
        auto index = make_mvcc_index(&resource, family);
        const uint64_t txn_insert = TRANSACTION_ID_START + 44;
        const uint64_t txn_other = TRANSACTION_ID_START + 55;
        components::types::logical_value_t val42(&resource, int64_t(42));

        index->insert(val42, int64_t(9), txn_insert, {});
        auto own_before_revert = search_rows(*index, compare_type::eq, val42, txn_insert);
        REQUIRE(own_before_revert.size() == 1);

        index->revert_insert(txn_insert);

        auto own_after_revert = search_rows(*index, compare_type::eq, val42, txn_insert);
        REQUIRE(own_after_revert.empty());

        auto other_after_revert = search_rows(*index, compare_type::eq, val42, txn_other);
        REQUIRE(other_after_revert.empty());
    }
} // namespace

// The three constructors of index_value_t and the stamps each one defaults.
//
// It was called `index_value_t:backward_compat`, which named a compatibility promise this
// project does not make (rule 6) and did not describe what the case checks: nothing here
// reads an older layout or an older stamp convention -- the three SECTIONs are the three
// constructors and the values they leave behind.
TEST_CASE("index_value_t:constructors_default_the_mvcc_stamps") {
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

TEST_CASE("ordered_disk_index:txn_insert_search") { run_txn_insert_search_contract(index_family::ordered_disk); }

TEST_CASE("hashed_disk_index:txn_insert_search") { run_txn_insert_search_contract(index_family::hashed_disk); }

TEST_CASE("ordered_disk_index:full_lifecycle") { run_full_lifecycle_contract(index_family::ordered_disk); }

TEST_CASE("hashed_disk_index:full_lifecycle") { run_full_lifecycle_contract(index_family::hashed_disk); }

TEST_CASE("ordered_disk_index:pending_delete_visibility") {
    run_pending_delete_visibility_contract(index_family::ordered_disk);
}

TEST_CASE("hashed_disk_index:pending_delete_visibility") {
    run_pending_delete_visibility_contract(index_family::hashed_disk);
}

TEST_CASE("ordered_disk_index:revert_insert_contract") { run_revert_insert_contract(index_family::ordered_disk); }

TEST_CASE("hashed_disk_index:revert_insert_contract") { run_revert_insert_contract(index_family::hashed_disk); }

// The ENGINE's txn fan-out: index_engine_t::commit_insert must reach every index it holds.
//
// The subject is the engine, not the index -- the index is the witness. It used to be an
// in-memory ordered index, read back with index_t::search; with C6a the witness is the
// ordered DISK facade, and the door that shows the fan-out arrived is the pending bucket:
// commit_insert hands the bucket to the disk agent and clears it, so a bucket that is
// still populated afterwards means the engine never called the index at all.
TEST_CASE("index_engine:txn_methods") {
    auto resource = core::pmr::otterbrix_resource();
    auto engine = make_index_engine(&resource);
    constexpr components::catalog::oid_t idx1_oid = 301u;
    make_index<disk_ordered_single_field_index_t>(engine, idx1_oid, {key(&resource, "val")});

    uint64_t txn1 = TRANSACTION_ID_START + 1;
    uint64_t commit1 = 10;

    auto* idx = engine->matching_relid(idx1_oid);
    REQUIRE(idx != nullptr);

    components::types::logical_value_t val(&resource, int64_t(99));
    idx->insert(val, int64_t(0), txn1, {});

    // The control: the entry IS in the bucket before the engine is asked to commit, so an
    // empty bucket afterwards cannot be an index that never took the write.
    REQUIRE(idx->pending_inserts(txn1).size() == 1);
    REQUIRE(idx->pending_inserts(txn1).front().row_index == 0);
    REQUIRE(search_rows(*idx, compare_type::eq, val, txn1).size() == 1);

    // Commit via engine
    engine->commit_insert(txn1, commit1);

    REQUIRE(idx->pending_inserts(txn1).empty());
    REQUIRE(search_rows(*idx, compare_type::eq, val, txn1).empty());
}
