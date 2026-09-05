// ============================================================================
// context_t MUST SURVIVE A MOVE WHOLE.
//
// The hand-written move constructor moved SIX of context_t's ~25 members —
// session, sender, parameters, disk/index addresses, address_ — and silently
// DROPPED the rest: txn, the DML append/delete range lists, the created/
// dropped-oid back-channels, committed_id, the flush flags, the runner seam.
// Those are exactly the members the executor's commit/abort back-channels ride
// on, so a moved context did not carry a smaller context — it carried a context
// that would publish nothing and revert nothing. No caller move-constructs a
// populated context today (which is how the half-copy survived), but a
// constructor that exists is a constructor that will be called; it must not be
// a trap. The fix defaults it so every member — present and future — moves.
// ============================================================================

#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/context/context.hpp>

#include <utility>

using namespace components;

TEST_CASE("integration::cpp::pipeline_context::move_keeps_every_member", "[context_move]") {
    auto resource = core::pmr::otterbrix_resource();
    pipeline::context_t ctx(logical_plan::storage_parameters{&resource});

    ctx.txn = table::transaction_data{7, 9};
    ctx.lowest_active_start_time = 42;
    ctx.committed_id = 77;
    ctx.analyze = true;
    ctx.dml_flush_is_final = false;
    ctx.dml_has_parent_constraint = true;
    ctx.dml_appends.push_back(table::dml_append_range_t{5, 10, 3});
    ctx.dml_deletes.push_back(table::dml_delete_range_t{6, 11});
    ctx.created_storage_oids.push_back(21);
    ctx.dropped_storage_oids.push_back(22);
    ctx.created_indexes.push_back(table::created_index_t{5, 31});
    ctx.pg_catalog_delete_tables.insert(41);

    pipeline::context_t moved(std::move(ctx));

    CHECK(moved.txn.transaction_id == 7);
    CHECK(moved.txn.start_time == 9);
    CHECK(moved.lowest_active_start_time == 42);
    CHECK(moved.committed_id == 77);
    CHECK(moved.analyze);
    CHECK_FALSE(moved.dml_flush_is_final);
    CHECK(moved.dml_has_parent_constraint);

    REQUIRE(moved.dml_appends.size() == 1);
    CHECK(moved.dml_appends.front().table_oid == 5);
    CHECK(moved.dml_appends.front().row_start == 10);
    CHECK(moved.dml_appends.front().row_count == 3);

    REQUIRE(moved.dml_deletes.size() == 1);
    CHECK(moved.dml_deletes.front().table_oid == 6);
    CHECK(moved.dml_deletes.front().txn_id == 11);

    REQUIRE(moved.created_storage_oids.size() == 1);
    CHECK(moved.created_storage_oids.front() == 21);
    REQUIRE(moved.dropped_storage_oids.size() == 1);
    CHECK(moved.dropped_storage_oids.front() == 22);
    REQUIRE(moved.created_indexes.size() == 1);
    CHECK(moved.created_indexes.front().table_oid == 5);
    CHECK(moved.created_indexes.front().index_oid == 31);
    CHECK(moved.pg_catalog_delete_tables.count(41) == 1);
}
