#include <catch2/catch_test_macros.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/transaction_manager.hpp>
#include <memory_resource>

TEST_CASE("components::table::transaction_manager::begin_commit") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto session = session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    REQUIRE(txn.is_active());
    REQUIRE(!txn.is_committed());
    REQUIRE(!txn.is_aborted());
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
    auto& txn = mgr.begin_transaction(session);
    REQUIRE(txn.is_active());

    mgr.abort(session);
    REQUIRE(!mgr.has_active_transaction(session));
}

TEST_CASE("components::table::transaction_manager::two_sessions_independent") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto s1 = session_id_t::generate_uid();
    auto s2 = session_id_t::generate_uid();

    auto& txn1 = mgr.begin_transaction(s1);
    auto& txn2 = mgr.begin_transaction(s2);

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

    mgr.begin_transaction(session);
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
    auto& txn1 = mgr.begin_transaction(s1);
    auto t1 = txn1.start_time();
    REQUIRE(mgr.lowest_active_start_time() == t1);

    auto s2 = session_id_t::generate_uid();
    mgr.begin_transaction(s2);
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
        auto& txn = mgr.begin_transaction(session);
        REQUIRE(txn.transaction_id() > prev_id);
        prev_id = txn.transaction_id();
        auto cid = mgr.commit(session);
        mgr.publish(cid);
    }
}

// Reproduces the MVCC reopen-visibility bug: on reopen, published_horizon_ was restored to the
// prior session's frontier F, but current_timestamp_ restarted at 1, so a post-reopen commit
// REUSED an already-published id — colliding with an old row, and freezing that reused id as
// still in-flight for any reader mid-commit (the SSB symptom: q1-1 returned 0 rows after reopen).
//
// Invariant under test: after ANY reopen, a transaction committing afterward draws a commit_id
// STRICTLY ABOVE the restored published_horizon_, and its row is visible once published.
TEST_CASE("components::table::transaction_manager::reopen_post_append_visible") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    // --- Restore the durable frontier at reopen -------------------------------
    // restore_commit_clock is the single entry point every reopen restore funnels
    // through (seed_commit_clock_sync calls it with the combined durable frontier).
    // It raises BOTH halves of the commit clock from the frontier: published_horizon_
    // AND current_timestamp_, maintaining current_timestamp_ >= horizon + 1.
    //
    // The PRE-EXISTING bug raised only published_horizon_ — the WAL-replay restore
    // did publish(F) alone, leaving current_timestamp_ at 1. Replacing the call
    // below with `mgr.publish(kFrontier)` (the old behaviour) makes the two REQUIREs
    // below FAIL: the post-reopen commit_id comes back as 2 (<= kFrontier, a reuse)
    // and a reader never sees the row.
    constexpr uint64_t kFrontier = 1208; // SSB-style durable frontier
    mgr.restore_commit_clock(kFrontier);
    REQUIRE(mgr.published_horizon() == kFrontier);

    // --- Post-reopen writer: INSERT a new row, then COMMIT --------------------
    auto writer = session_id_t::generate_uid();
    auto& wtxn = mgr.begin_transaction(writer);
    // The row's pending insert version is the writer's (pending) transaction_id.
    chunk_constant_info row(0);
    row.insert_id = wtxn.transaction_id();
    auto commit_id = mgr.commit(writer);
    // Stamp the row's committed insert version with the freshly-allocated id.
    row.commit_append(commit_id, 0, components::vector::DEFAULT_VECTOR_CAPACITY);

    // INVARIANT 1 (no id reuse): the post-reopen commit_id is STRICTLY above the
    // restored frontier — it never lands back inside the already-published band.
    REQUIRE(commit_id > kFrontier);

    // Writer publishes (advancing the horizon to include the new commit).
    mgr.publish(commit_id);

    // --- Reader snapshot taken AFTER the writer published ----------------------
    auto reader = session_id_t::generate_uid();
    auto& rtxn = mgr.begin_transaction(reader);

    // INVARIANT 2 (post-publish visibility): the post-reopen row is visible to a
    // snapshot taken after its commit published.
    REQUIRE(row.fetch(rtxn.data(), 0));
}

TEST_CASE("components::table::transaction_manager::append_tracking") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto session = session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

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

// Regression for lowest_active_snapshot_horizon()'s in_flight_commits_ gap (see the comment on
// that function in transaction_manager.cpp): publish() keeps only the MAXIMUM published id while
// commits finish in any order, so a smaller commit-id (c1) can still be in flight after a later
// one (c2) publishes. Broadcasting c2 as the DROP-GC/index-sweep floor (its only production
// caller is services/dispatcher/dispatcher.cpp) would let the sweep erase c1's still-visible rows.
//
// Laid out by hand, no threads/sleeps: commit(s1), commit(s2), publish(c2) IS the window.
TEST_CASE("components::table::transaction_manager::out_of_order_publish_floor") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto s1 = session_id_t::generate_uid();
    auto s2 = session_id_t::generate_uid();
    mgr.begin_transaction(s1);
    mgr.begin_transaction(s2);

    const auto c1 = mgr.commit(s1); // in_flight { c1 }
    const auto c2 = mgr.commit(s2); // in_flight { c1, c2 }
    REQUIRE(c2 > c1);

    // The LATER commit finishes its pipeline FIRST. c1 is still in flight.
    mgr.publish(c2);

    // Branch 1: active_ is empty.
    REQUIRE_FALSE(mgr.has_active_transactions());
    // c1 is unpublished; broadcasting >= c1 would license an erase of still-read rows.
    REQUIRE(mgr.lowest_active_snapshot_horizon() < c1);

    // Branch 2: active_ is NOT empty. A reader begun inside the window.
    auto s3 = session_id_t::generate_uid();
    auto& reader = mgr.begin_transaction(s3);

    // Guards against "fix the reader" instead: begin_transaction captures (horizon, in-flight
    // set) atomically, so the reader is already correct at c2. Clamping it down would break
    // read-committed — a session opened after an acknowledged commit would stop seeing it.
    REQUIRE(reader.data().snapshot_horizon == c2);

    // Same floor with a live snapshot present.
    REQUIRE(mgr.lowest_active_snapshot_horizon() < c1);

    mgr.abort(s3);
}

// An orphaned commit_id pins the horizon for the life of the process: an early exit of
// operator_commit_transaction_t between commit() and publish() leaves the id in
// in_flight_commits_ with commit() having already erased the txn from active_ (transaction_manager.cpp,
// commit()), so nothing can reach it — pre-fix, this was a "KNOWN leak ... accepted" by comment.
// visible_to_all_locked() floors compact_watermark()/lowest_active_snapshot_horizon() on
// min(in_flight_commits_) - 1, so the orphan freezes both at c_lost - 1 forever, even with zero
// active transactions and every later commit publishing normally.
//
// The cure is an ERASE, not a second "discarded" set: compact()/the sweeps read a single horizon
// NUMBER and never see a per-reader snapshot, so a discarded-ids set would still have to be
// subtracted into that floor — the same defect, verbatim. discard() is publish() minus the CAS:
// it raises the floor without ever advancing published_horizon_. Safety rests on the operator's
// ordering rule that no step which can fail runs after the step that stamps the commit_id.
//
// Laid out by hand, no threads/sleeps: begin, begin, commit, commit, publish(the second one) IS
// the leak.
TEST_CASE("components::table::transaction_manager::orphaned_commit_pins_horizon_forever") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto s_lost = session_id_t::generate_uid();
    auto s_ok = session_id_t::generate_uid();
    mgr.begin_transaction(s_lost);
    mgr.begin_transaction(s_ok);

    // s_lost reaches an early exit: allocated and out of active_, but publish() never runs for it.
    const auto c_lost = mgr.commit(s_lost);
    // s_ok runs its whole pipeline and publishes.
    const auto c_ok = mgr.commit(s_ok);
    REQUIRE(c_ok > c_lost);
    mgr.publish(c_ok);

    // Not vacuous: no active transaction, no pending publish anyone could still send.
    REQUIRE_FALSE(mgr.has_active_transactions());

    // The defect: both floors are stuck one below the orphan even though c_ok is published.
    REQUIRE(mgr.compact_watermark() == c_lost - 1);
    REQUIRE(mgr.lowest_active_snapshot_horizon() == c_lost - 1);

    // The cure: one erase, no CAS. The floor rises to the highest published id and
    // published_horizon_ is untouched.
    const auto horizon_before = mgr.published_horizon();
    mgr.discard(c_lost);
    REQUIRE(mgr.published_horizon() == horizon_before);
    REQUIRE(mgr.compact_watermark() == c_ok);
    REQUIRE(mgr.lowest_active_snapshot_horizon() == c_ok);

    // Idempotent: a second discard, or discarding an id never in flight, is a no-op.
    mgr.discard(c_lost);
    REQUIRE(mgr.compact_watermark() == c_ok);

    auto s_read = session_id_t::generate_uid();
    auto& reader = mgr.begin_transaction(s_read);
    REQUIRE(reader.data().in_flight_snapshot.empty());
    REQUIRE(reader.data().snapshot_horizon == c_ok);
    mgr.abort(s_read);
}
