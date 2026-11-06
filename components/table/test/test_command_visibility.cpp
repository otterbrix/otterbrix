// A transaction's own writes carry the command id of the statement that made them, and a reader with
// sees_commands_before = N sees those of the commands before N only, as PostgreSQL 18's HeapTupleSatisfiesMVCC compares
// cmin / cmax with the snapshot's curcid: an insert of command N is seen from N + 1 on, a delete of command N hides its
// row from N + 1 on. sees_commands_before = 0 sees every own write, as every statement does.

#include <catch2/catch_test_macros.hpp>

#include <components/table/data_table.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/pmr.hpp>

using namespace components::table;
using components::vector::DEFAULT_VECTOR_CAPACITY;
using components::vector::indexing_vector_t;

namespace {
    constexpr uint64_t kTxn = TRANSACTION_ID_START + 7;

    transaction_data writer(uint64_t command_id) {
        transaction_data txn{kTxn, 1};
        txn.command_id = command_id;
        return txn;
    }

    transaction_data reader(uint64_t sees_commands_before) {
        transaction_data txn{kTxn, 1};
        txn.sees_commands_before = sees_commands_before;
        return txn;
    }
} // namespace

TEST_CASE("components::table::command_visibility::a_write_of_command_n_is_seen_from_n_plus_1") {
    row_version_manager_t versions(0);
    // Rows 0..9 inserted by command 2 (a part of a vector: per-row stamps), row 0 deleted by command 3.
    versions.append_version_info(writer(2), 10, 0, 10);
    int64_t deleted[] = {0};
    REQUIRE(versions.delete_rows(0, kTxn, 0, 3, deleted, 1) == 1);

    CHECK_FALSE(versions.fetch(reader(2), 1));
    CHECK(versions.fetch(reader(3), 1));
    CHECK(versions.fetch(reader(0), 1));

    CHECK(versions.fetch(reader(3), 0));
    CHECK_FALSE(versions.fetch(reader(4), 0));
    CHECK_FALSE(versions.fetch(reader(0), 0));

    core::pmr::otterbrix_resource resource;
    indexing_vector_t rows(&resource, DEFAULT_VECTOR_CAPACITY);
    CHECK(versions.indexing_vector(reader(2), 0, rows, 10) == 0);
    CHECK(versions.indexing_vector(reader(3), 0, rows, 10) == 10);
    CHECK(versions.indexing_vector(reader(4), 0, rows, 10) == 9);
    CHECK(versions.indexing_vector(reader(0), 0, rows, 10) == 9);

    // Rows 10..19 by command 5 in the same vector: one insert id, two commands.
    versions.append_version_info(writer(5), 10, 10, 20);
    CHECK(versions.indexing_vector(reader(4), 0, rows, 20) == 9);
    CHECK(versions.indexing_vector(reader(6), 0, rows, 20) == 19);
    CHECK(versions.indexing_vector(reader(0), 0, rows, 20) == 19);
}

TEST_CASE("components::table::command_visibility::a_whole_vector_of_one_command") {
    row_version_manager_t versions(0);
    versions.append_version_info(writer(5), DEFAULT_VECTOR_CAPACITY, 0, DEFAULT_VECTOR_CAPACITY);
    core::pmr::otterbrix_resource resource;
    indexing_vector_t rows(&resource, DEFAULT_VECTOR_CAPACITY);
    CHECK(versions.indexing_vector(reader(5), 0, rows, DEFAULT_VECTOR_CAPACITY) == 0);
    CHECK(versions.indexing_vector(reader(6), 0, rows, DEFAULT_VECTOR_CAPACITY) == DEFAULT_VECTOR_CAPACITY);
    CHECK_FALSE(versions.fetch(reader(5), 7));
    CHECK(versions.fetch(reader(6), 7));

    // A delete makes the vector per-row: the insert's command stays with every row.
    int64_t deleted[] = {7};
    REQUIRE(versions.delete_rows(0, kTxn, 0, 6, deleted, 1) == 1);
    CHECK(versions.indexing_vector(reader(5), 0, rows, DEFAULT_VECTOR_CAPACITY) == 0);
    CHECK(versions.indexing_vector(reader(6), 0, rows, DEFAULT_VECTOR_CAPACITY) == DEFAULT_VECTOR_CAPACITY);
    CHECK(versions.indexing_vector(reader(7), 0, rows, DEFAULT_VECTOR_CAPACITY) == DEFAULT_VECTOR_CAPACITY - 1);
}

// A committed insert is no longer the transaction's own: its command id goes, and a later delete in the vector makes
// no command ids for it.
TEST_CASE("components::table::command_visibility::a_commit_forgets_the_command") {
    chunk_constant_info info(0);
    info.insert_id = kTxn;
    info.insert_command_id = 5;
    info.commit_append(42, 0, DEFAULT_VECTOR_CAPACITY);
    CHECK(info.insert_id == 42);
    CHECK(info.insert_command_id == 0);
}
