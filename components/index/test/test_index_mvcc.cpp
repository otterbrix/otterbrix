#include <catch2/catch.hpp>

#include "components/index/index_engine.hpp"
#include "components/index/single_field_index.hpp"
#include <components/table/row_version_manager.hpp>

using namespace components::index;
using namespace components::table;
using namespace components::expressions;
using key = components::expressions::key_t;

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

    SECTION("visible to own transaction") {
        REQUIRE(index_entry_visible(committed, 3, 5));
    }
}

TEST_CASE("index_entry_visible:uncommitted_entries") {
    // Entry with uncommitted insert (txn_id in TRANSACTION_ID_START range)
    uint64_t txn_id = TRANSACTION_ID_START + 100;
    index_value_t uncommitted(1, txn_id, NOT_DELETED_ID);

    SECTION("visible to own transaction") {
        REQUIRE(index_entry_visible(uncommitted, txn_id - 1, txn_id));
    }

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

    SECTION("not visible to deleting transaction") {
        REQUIRE_FALSE(index_entry_visible(deleted_entry, 8, 10));
    }
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
    index.insert(val42, int64_t(0), txn1);

    SECTION("visible to own transaction") {
        auto result = index.search(compare_type::eq, val42, txn1 - 1, txn1);
        REQUIRE(result.size() == 1);
        REQUIRE(result[0] == 0);
    }

    SECTION("not visible to other transaction") {
        auto result = index.search(compare_type::eq, val42, txn1 - 1, txn2);
        REQUIRE(result.empty());
    }

    SECTION("visible after commit") {
        index.commit_insert(txn1, 10); // commit_id = 10
        auto result = index.search(compare_type::eq, val42, 15, txn2);
        REQUIRE(result.size() == 1);
        REQUIRE(result[0] == 0);
    }

    SECTION("gone after revert") {
        index.revert_insert(txn1);
        auto result = index.search(compare_type::eq, val42, txn1 - 1, txn1);
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
    index.insert(val42, int64_t(0), txn1);
    index.commit_insert(txn1, commit1);

    auto result = index.search(compare_type::eq, val42, commit1 + 1, txn2);
    REQUIRE(result.size() == 1);

    // delete → commit → not visible
    index.mark_delete(val42, int64_t(0), txn2);
    index.commit_delete(txn2, commit2);

    result = index.search(compare_type::eq, val42, commit2 + 1, TRANSACTION_ID_START + 3);
    REQUIRE(result.empty());

    // cleanup → erased from storage
    index.cleanup_versions(commit2 + 1);
    // After cleanup, even the old non-txn search should not find it
    result = index.search(compare_type::eq, val42);
    REQUIRE(result.empty());
}

TEST_CASE("index_engine:txn_methods") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto engine = make_index_engine(&resource);
    make_index<single_field_index_t>(engine, "idx1", {key(&resource, "val")});

    uint64_t txn1 = TRANSACTION_ID_START + 1;
    uint64_t commit1 = 10;

    auto* idx = search_index(engine, std::string("idx1"));
    REQUIRE(idx != nullptr);

    components::types::logical_value_t val(&resource, int64_t(99));
    idx->insert(val, int64_t(0), txn1);

    // Commit via engine
    engine->commit_insert(txn1, commit1);

    auto result = idx->search(compare_type::eq, val, commit1 + 1, TRANSACTION_ID_START + 2);
    REQUIRE(result.size() == 1);
    REQUIRE(result[0] == 0);
}
