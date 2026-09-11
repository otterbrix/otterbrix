#include <catch2/catch_test_macros.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/transaction_manager.hpp>
#include <memory_resource>

TEST_CASE("components::table::transaction_manager::begin_commit") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto session = session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session, transaction_scope_t::statement);

    REQUIRE(txn.state() == transaction_state_t::active);
    REQUIRE(txn.transaction_id() >= TRANSACTION_ID_START);
    REQUIRE(txn.session() == session);

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    REQUIRE(commit_id > 0);
    REQUIRE(!mgr.has_active_transaction(session));
}

TEST_CASE("components::table::transaction_manager::begin_abort") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto session = session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session, transaction_scope_t::statement);
    REQUIRE(txn.state() == transaction_state_t::active);

    mgr.abort(session);
    REQUIRE(!mgr.has_active_transaction(session));
}

TEST_CASE("components::table::transaction_manager::two_sessions_independent") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto s1 = session_id_t::generate_uid();
    auto s2 = session_id_t::generate_uid();

    auto& txn1 = mgr.begin_transaction(s1, transaction_scope_t::statement);
    auto& txn2 = mgr.begin_transaction(s2, transaction_scope_t::statement);

    REQUIRE(txn1.transaction_id() != txn2.transaction_id());
    REQUIRE(txn1.start_time() != txn2.start_time());
    REQUIRE(mgr.has_active_transactions());

    auto cid1 = mgr.commit(s1);
    mgr.publish(cid1);
    REQUIRE(mgr.has_active_transaction(s2));
    REQUIRE(!mgr.has_active_transaction(s1));

    auto cid2 = mgr.commit(s2);
    mgr.publish(cid2);
    REQUIRE(!mgr.has_active_transactions());
}

TEST_CASE("components::table::transaction_manager::find_transaction") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto session = session_id_t::generate_uid();
    auto missing = session_id_t::generate_uid();

    mgr.begin_transaction(session, transaction_scope_t::statement);
    REQUIRE(mgr.find_transaction(session) != nullptr);
    REQUIRE(mgr.find_transaction(missing) == nullptr);

    auto cid = mgr.commit(session);
    mgr.publish(cid);
    REQUIRE(mgr.find_transaction(session) == nullptr);
}

TEST_CASE("components::table::transaction_manager::lowest_active_start_time") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    [[maybe_unused]] auto baseline = mgr.lowest_active_start_time();

    auto s1 = session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1, transaction_scope_t::statement);
    auto t1 = txn1.start_time();
    REQUIRE(mgr.lowest_active_start_time() == t1);

    auto s2 = session_id_t::generate_uid();
    mgr.begin_transaction(s2, transaction_scope_t::statement);
    REQUIRE(mgr.lowest_active_start_time() == t1);

    auto cid = mgr.commit(s1);
    mgr.publish(cid);
    REQUIRE(mgr.lowest_active_start_time() > t1);
}

TEST_CASE("components::table::transaction_manager::id_monotonicity") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());
    uint64_t prev_id = 0;

    for (int i = 0; i < 10; i++) {
        auto session = session_id_t::generate_uid();
        auto& txn = mgr.begin_transaction(session, transaction_scope_t::statement);
        REQUIRE(txn.transaction_id() > prev_id);
        prev_id = txn.transaction_id();
        auto cid = mgr.commit(session);
        mgr.publish(cid);
    }
}

// Reproduces the MVCC reopen-visibility bug: restoring only published_horizon_ let a post-reopen
// commit reuse an id (SSB symptom: q1-1 returned 0 rows); a commit_id must land strictly above it.
TEST_CASE("components::table::transaction_manager::reopen_post_append_visible") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    // restore_commit_clock raises both published_horizon_ and current_timestamp_ (kept >= horizon + 1).
    constexpr uint64_t kFrontier = 1208;
    mgr.restore_commit_clock(kFrontier);
    REQUIRE(mgr.published_horizon() == kFrontier);

    auto writer = session_id_t::generate_uid();
    auto& wtxn = mgr.begin_transaction(writer, transaction_scope_t::statement);
    chunk_constant_info row(0);
    row.insert_id = wtxn.transaction_id();
    auto commit_id = mgr.commit(writer);
    row.commit_append(commit_id, 0, components::vector::DEFAULT_VECTOR_CAPACITY);

    REQUIRE(commit_id > kFrontier);

    mgr.publish(commit_id);

    auto reader = session_id_t::generate_uid();
    auto& rtxn = mgr.begin_transaction(reader, transaction_scope_t::statement);

    REQUIRE(row.fetch(rtxn.data(), 0));
}

TEST_CASE("components::table::transaction_manager::append_tracking") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto session = session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session, transaction_scope_t::statement);

    txn.add_append(0, 100);
    txn.add_append(100, 50);

    REQUIRE(txn.appends().size() == 2);
    REQUIRE(txn.appends()[0].row_start == 0);
    REQUIRE(txn.appends()[0].count == 100);
    REQUIRE(txn.appends()[1].row_start == 100);
    REQUIRE(txn.appends()[1].count == 50);

    auto cid = mgr.commit(session);
    mgr.publish(cid);
}

// Regression: publish() keeps only the max published id, so c1 can still be in flight after a
// later c2 publishes; broadcasting c2 as a GC floor would erase c1's still-visible rows.
TEST_CASE("components::table::transaction_manager::out_of_order_publish_floor") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto s1 = session_id_t::generate_uid();
    auto s2 = session_id_t::generate_uid();
    mgr.begin_transaction(s1, transaction_scope_t::statement);
    mgr.begin_transaction(s2, transaction_scope_t::statement);

    const auto c1 = mgr.commit(s1);
    const auto c2 = mgr.commit(s2);
    REQUIRE(c2 > c1);

    mgr.publish(c2);

    REQUIRE_FALSE(mgr.has_active_transactions());
    REQUIRE(mgr.lowest_active_snapshot_horizon() < c1);

    auto s3 = session_id_t::generate_uid();
    auto& reader = mgr.begin_transaction(s3, transaction_scope_t::statement);

    // Clamping the reader instead would break read-committed for sessions opened after an acked commit.
    REQUIRE(reader.data().snapshot_horizon == c2);

    REQUIRE(mgr.lowest_active_snapshot_horizon() < c1);

    mgr.abort(s3);
}

// An early exit between commit() and publish() orphans a commit_id in in_flight_commits_ forever,
// pinning compact_watermark()/lowest_active_snapshot_horizon() at c_lost - 1. The cure is an erase
// (discard()), not a second "discarded" set, since the sweeps read one horizon number and would
// still have to subtract a discarded-ids set into it -- the same defect, verbatim.
TEST_CASE("components::table::transaction_manager::orphaned_commit_pins_horizon_forever") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto s_lost = session_id_t::generate_uid();
    auto s_ok = session_id_t::generate_uid();
    mgr.begin_transaction(s_lost, transaction_scope_t::statement);
    mgr.begin_transaction(s_ok, transaction_scope_t::statement);

    const auto c_lost = mgr.commit(s_lost);
    const auto c_ok = mgr.commit(s_ok);
    REQUIRE(c_ok > c_lost);
    mgr.publish(c_ok);

    // Not vacuous: no active transaction, no pending publish anyone could still send.
    REQUIRE_FALSE(mgr.has_active_transactions());

    REQUIRE(mgr.compact_watermark() == c_lost - 1);
    REQUIRE(mgr.lowest_active_snapshot_horizon() == c_lost - 1);

    // discard() raises the floor without touching published_horizon_.
    const auto horizon_before = mgr.published_horizon();
    mgr.discard(c_lost);
    REQUIRE(mgr.published_horizon() == horizon_before);
    REQUIRE(mgr.compact_watermark() == c_ok);
    REQUIRE(mgr.lowest_active_snapshot_horizon() == c_ok);

    // Idempotent: a second discard, or discarding an id never in flight, is a no-op.
    mgr.discard(c_lost);
    REQUIRE(mgr.compact_watermark() == c_ok);

    auto s_read = session_id_t::generate_uid();
    auto& reader = mgr.begin_transaction(s_read, transaction_scope_t::statement);
    REQUIRE(reader.data().in_flight_snapshot.empty());
    REQUIRE(reader.data().snapshot_horizon == c_ok);
    mgr.abort(s_read);
}

TEST_CASE("components::table::transaction_manager::resolve_transaction_contract") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());
    const auto plain = transaction_control_t::none;
    const auto rollback = transaction_control_t::rollback;
    const auto commit = transaction_control_t::commit;

    auto statement_session = session_id_t::generate_uid();
    auto for_statement = mgr.resolve_transaction(statement_session, transaction_scope_t::statement, plain);
    REQUIRE_FALSE(for_statement.has_error());
    REQUIRE(for_statement.value()->scope() == transaction_scope_t::statement);
    REQUIRE(mgr.resolve_transaction(statement_session, transaction_scope_t::statement, plain).has_error());
    REQUIRE(mgr.resolve_transaction(statement_session, transaction_scope_t::until_commit, rollback).has_error());

    auto open_session = session_id_t::generate_uid();
    auto opened = mgr.resolve_transaction(open_session, transaction_scope_t::until_commit, plain);
    REQUIRE_FALSE(opened.has_error());
    auto* open_transaction = opened.value();
    auto joined = mgr.resolve_transaction(open_session, transaction_scope_t::statement, plain);
    REQUIRE_FALSE(joined.has_error());
    REQUIRE(joined.value() == open_transaction);
    REQUIRE(open_transaction->scope() == transaction_scope_t::until_commit);

    mgr.fail(open_session);
    REQUIRE(open_transaction->state() == transaction_state_t::failed);
    REQUIRE(mgr.resolve_transaction(open_session, transaction_scope_t::statement, plain).has_error());

    // A ROLLBACK ends the failed transaction and runs in one of its own.
    const auto failed_id = open_transaction->transaction_id();
    auto rolled_back = mgr.resolve_transaction(open_session, transaction_scope_t::statement, rollback);
    REQUIRE_FALSE(rolled_back.has_error());
    REQUIRE(rolled_back.value()->state() == transaction_state_t::active);
    REQUIRE(rolled_back.value()->transaction_id() != failed_id);
    mgr.abort(open_session);

    // A COMMIT ends it too, reporting that nothing was committed.
    REQUIRE_FALSE(mgr.resolve_transaction(open_session, transaction_scope_t::until_commit, plain).has_error());
    mgr.fail(open_session);
    REQUIRE(mgr.resolve_transaction(open_session, transaction_scope_t::statement, commit).has_error());
    REQUIRE(mgr.find_transaction(open_session) == nullptr);

    mgr.abort(statement_session);
    REQUIRE_FALSE(mgr.has_active_transactions());
}

TEST_CASE("components::table::transaction_manager::failed_transaction_holds_no_horizon") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto failing = session_id_t::generate_uid();
    mgr.begin_transaction(failing, transaction_scope_t::until_commit);

    auto writer = session_id_t::generate_uid();
    mgr.begin_transaction(writer, transaction_scope_t::statement);
    const auto commit_id = mgr.commit(writer);
    mgr.publish(commit_id);

    // Not vacuous: while active, the older snapshot pins both horizons below the commit.
    REQUIRE(mgr.compact_watermark() < commit_id);
    const auto pinned_start_time = mgr.lowest_active_start_time();

    mgr.fail(failing);
    REQUIRE(mgr.compact_watermark() == commit_id);
    REQUIRE(mgr.lowest_active_start_time() > pinned_start_time);
    REQUIRE(mgr.find_transaction(failing) != nullptr);
}
