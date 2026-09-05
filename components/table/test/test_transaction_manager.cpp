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

// Reproduces the MVCC reopen-visibility bug. On reopen the commit clock's two halves were
// restored from inconsistent sources: published_horizon_ was raised to the prior session's
// durable frontier F (the old WAL-replay restore did publish(F) alone) while current_timestamp_
// — the fetch_add source of every new start_time/commit_id — restarted at 1. A post-reopen txn
// therefore drew a commit_id that REUSED the already-published band <= F.
//
// Two consequences, both correctness defects:
//   1. ID REUSE: the new commit_id collides with an id a PRIOR session already published, so two
//      distinct rows share one commit_id and visibility filters (use_inserted_version) can no
//      longer tell them apart;
//   2. IN-FLIGHT FREEZE: a reader that snapshots the new commit while it is in-flight freezes the
//      colliding id (<= published_horizon_) in in_flight_snapshot; after publish() that reader
//      judges the row invisible forever (the SSB symptom: q1-1 / probe returned 0 rows after
//      reopen).
//
// INVARIANT under test: after ANY reopen restore, a transaction that commits AFTER reopen must
// draw a commit_id STRICTLY ABOVE the restored published_horizon_ (no reuse), keeping
// current_timestamp_ >= published_horizon_ + 1; and its row must be visible to a snapshot taken
// after it publishes.
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

// OUT-OF-ORDER PUBLISH WINDOW. commit() hands out commit-ids in increasing order but publish()
// runs at the END of each commit pipeline (after WAL fsync + storage_publish_*), so the
// pipelines finish in ANY order and publish() keeps only the MAXIMUM ever published. A SMALLER
// commit-id can therefore still sit in in_flight_commits_ while published_horizon_ has moved
// past it.
//
// lowest_active_snapshot_horizon() is the DROP-GC / deferred-index-delete broadcast
// (services/dispatcher/dispatcher.cpp is its only production caller), and both of its branches
// ignored in_flight_commits_ entirely: active_ empty -> published_horizon_ (c2 here), active_
// non-empty -> min over snapshot_horizon only, never the per-txn in_flight_snapshot (c2 again).
// Either way it broadcast a horizon that had ALREADY PASSED the still-unpublished c1. A snapshot
// taken in that window carries c1 in in_flight_snapshot and still SEES c1's rows, while the
// index sweep (services/index/manager_index.cpp, `entry->commit_id <= new_horizon`) was already
// cleared to erase their index entries — the index then answers a strict SUBSET of the table: a
// silent wrong answer, no error raised.
//
// The window is laid out by hand with three public calls — no threads, no sleeps, no timing.
// commit(s1), commit(s2), publish(c2) IS the window.
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
    // Nothing at or above c1 is visible-to-all: c1 is unpublished, so every
    // snapshot from now on carries it in in_flight_snapshot and hides its rows.
    // Broadcasting anything >= c1 licenses an erase of rows that are still read.
    REQUIRE(mgr.lowest_active_snapshot_horizon() < c1);

    // Branch 2: active_ is NOT empty. A reader begun inside the window.
    auto s3 = session_id_t::generate_uid();
    auto& reader = mgr.begin_transaction(s3);

    // GUARD AGAINST "FIX THE READER" (variant (c) of the fork). The SNAPSHOT
    // horizon must stay at the freshly published c2: begin_transaction captures
    // (horizon, in-flight set) atomically and the visibility filter applies the
    // pair honestly, so the reader is already correct. Clamping the SNAPSHOT
    // instead would break read-committed in a user-visible way — a session opened
    // AFTER a commit was acknowledged would stop seeing it. This line goes red the
    // day anyone tries it.
    REQUIRE(reader.data().snapshot_horizon == c2);

    // Same floor with a live snapshot present: the reader itself is entitled to
    // read below c1, so the broadcast must stay below c1 too.
    REQUIRE(mgr.lowest_active_snapshot_horizon() < c1);

    mgr.abort(s3);
}

// AN ORPHANED commit_id PINS THE HORIZON FOR THE LIFE OF THE PROCESS.
//
// commit() allocates the id into in_flight_commits_ and publish() is the only thing that takes
// it out. Every early exit of operator_commit_transaction_t between those two hops leaves the id
// there with nobody left to remove it: commit() has ALREADY erased the txn from active_ (see
// :41-42 there), so find_transaction() answers nullptr and neither a ROLLBACK nor the
// dispatcher's failure-release net can reach it. It was documented on the spot as a "KNOWN leak
// ... accepted".
//
// WHAT THAT COSTS IS ONE TERM: visible_to_all_locked() floors the watermark on
// min(in_flight_commits_) - 1, and that ONE number is what data_table_t::compact(), the DROP-GC
// tombstone sweep and the deferred index-delete sweep all read. The orphan freezes the horizon
// at c_lost - 1 forever, WITH NO TRANSACTION ANYWHERE IN THE SYSTEM — has_active_transactions()
// is false and every later commit publishes normally, and still nothing is ever reclaimed again.
//
// The window is laid out by hand with four public calls — no threads, no sleeps, no timing:
// begin, begin, commit, commit, publish(the second one) IS the leak.
//
// WHY THE CURE IS AN ERASE AND NOT A SECOND SET. A "discarded" set that snapshots UNION into
// their in-flight vector is correct for READERS (an insert stamped with a discarded id stays
// hidden, a delete stamped with one leaves its row alive), but the parties pinned here are not
// readers: compact() and both sweeps take a single horizon NUMBER and never see a snapshot, so
// the floor would have to include min(discarded) - 1 as well — which is this defect, verbatim.
// The id has to leave in_flight_commits_ outright, and what makes that safe is an ORDERING rule
// in the operator: no step that can fail may run after the first step that stamps the commit_id,
// so a discarded id is stamped nowhere. discard() is publish() minus the CAS for exactly that
// reason — it must raise the floor without advancing published_horizon_ by even one.
TEST_CASE("components::table::transaction_manager::orphaned_commit_pins_horizon_forever") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr(std::pmr::new_delete_resource());

    auto s_lost = session_id_t::generate_uid();
    auto s_ok = session_id_t::generate_uid();
    mgr.begin_transaction(s_lost);
    mgr.begin_transaction(s_ok);

    // s_lost reaches an early exit of the commit pipeline: its id is allocated and it
    // is already out of active_, but publish() will never run for it.
    const auto c_lost = mgr.commit(s_lost);
    // s_ok runs its whole pipeline and publishes.
    const auto c_ok = mgr.commit(s_ok);
    REQUIRE(c_ok > c_lost);
    mgr.publish(c_ok);

    // NOT VACUOUS: nothing is left to release the pin. No active transaction, no
    // pending publish anyone could still send.
    REQUIRE_FALSE(mgr.has_active_transactions());

    // THE DEFECT, stated as the state it leaves behind. Both public horizon names
    // answer visible_to_all_locked(), so both are stuck one below the orphan — c_ok is
    // published and reclaimable, and neither number will ever say so.
    REQUIRE(mgr.compact_watermark() == c_lost - 1);
    REQUIRE(mgr.lowest_active_snapshot_horizon() == c_lost - 1);

    // THE CURE. One erase, no CAS: the floor rises to the highest published id and
    // published_horizon_ is untouched, so the discarded transaction is not published
    // by the act of forgetting it.
    const auto horizon_before = mgr.published_horizon();
    mgr.discard(c_lost);
    REQUIRE(mgr.published_horizon() == horizon_before);
    REQUIRE(mgr.compact_watermark() == c_ok);
    REQUIRE(mgr.lowest_active_snapshot_horizon() == c_ok);

    // Idempotent: a second discard of the same id changes nothing, and discarding an
    // id that was never in flight is a no-op rather than a horizon move.
    mgr.discard(c_lost);
    REQUIRE(mgr.compact_watermark() == c_ok);

    // A reader opened after the discard must not see the discarded id in its
    // in-flight set — there is nothing left to hide, because after the operator's
    // ordering rule no row anywhere carries it.
    auto s_read = session_id_t::generate_uid();
    auto& reader = mgr.begin_transaction(s_read);
    REQUIRE(reader.data().in_flight_snapshot.empty());
    REQUIRE(reader.data().snapshot_horizon == c_ok);
    mgr.abort(s_read);
}
