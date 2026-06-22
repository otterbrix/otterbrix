#include <catch2/catch.hpp>
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

// Reproduces the MVCC reopen-visibility bug. On reopen the commit clock's two
// halves were restored from inconsistent sources: published_horizon_ was raised to
// the prior session's durable frontier F (the old WAL-replay restore did publish(F)
// alone) while current_timestamp_ — the fetch_add source of every new
// start_time/commit_id — restarted at 1. A post-reopen txn therefore drew a
// commit_id that REUSED the already-published band <= F.
//
// Two consequences, both correctness defects:
//   1. ID REUSE: the new commit_id collides with an id a PRIOR session already
//      published. Two distinct rows then share one commit_id — visibility filters
//      (use_inserted_version) can no longer tell them apart.
//   2. IN-FLIGHT FREEZE: a reader that snapshots the new commit while it is
//      in-flight freezes the colliding id (<= published_horizon_) in
//      in_flight_snapshot; after publish() that reader judges the row invisible
//      forever, even though the id sits below its horizon (the SSB symptom:
//      q1-1 / probe returned 0 rows after reopen).
//
// INVARIANT under test (what the fix restores): after ANY reopen restore, a
// transaction that commits AFTER reopen must draw a commit_id STRICTLY ABOVE the
// restored published_horizon_ (no reuse), keeping current_timestamp_ >=
// published_horizon_ + 1; and its row must be visible to a snapshot taken after
// it publishes.
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
