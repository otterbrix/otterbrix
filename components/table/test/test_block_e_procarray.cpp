#include <algorithm>
#include <catch2/catch.hpp>
#include <components/logical_plan/node_catalog_resolve_database.hpp>
#include <components/table/transaction_manager.hpp>
#include <memory_resource>

namespace components::table::test_block_e {

    TEST_CASE("Block E committed_version_operator respects in_flight_snapshot", "[block_e][procarray]") {
        using namespace components::table;
        using namespace components::session;

        std::pmr::synchronized_pool_resource resource;
        transaction_manager_t mgr(&resource);

        // T2 begins + commits (allocates commit_id, ADDS to in_flight_commits_).
        // We DO NOT call publish() — keeps the commit_id in_flight.
        auto session2 = session_id_t::generate_uid();
        mgr.begin_transaction(session2);
        auto commit_id_t2 = mgr.commit(session2);
        REQUIRE(commit_id_t2 > 0);

        // T1 begins NOW — takes a snapshot that includes commit_id_t2 in its
        // in_flight_snapshot (since T2 committed but not yet published).
        auto session1 = session_id_t::generate_uid();
        auto& txn1 = mgr.begin_transaction(session1);
        auto data = txn1.data();

        // T1's snapshot MUST list commit_id_t2 as in_flight.
        bool contains_t2 = std::find(data.in_flight_snapshot.begin(), data.in_flight_snapshot.end(), commit_id_t2) !=
                           data.in_flight_snapshot.end();
        REQUIRE(contains_t2);

        // publish() drops commit_id_t2 from in_flight_commits_, but T1's snapshot is
        // an immutable copy taken at begin and must not reflect the change.
        mgr.publish(commit_id_t2);

        auto data_again = txn1.data();
        bool still_contains_t2 =
            std::find(data_again.in_flight_snapshot.begin(), data_again.in_flight_snapshot.end(), commit_id_t2) !=
            data_again.in_flight_snapshot.end();
        REQUIRE(still_contains_t2); // snapshot is frozen at begin — does NOT reflect later publish

        // A FRESH txn (T3) starting now sees the PUBLISHED state — in_flight is empty.
        auto session3 = session_id_t::generate_uid();
        auto& txn3 = mgr.begin_transaction(session3);
        auto data3 = txn3.data();
        bool t3_contains_t2 =
            std::find(data3.in_flight_snapshot.begin(), data3.in_flight_snapshot.end(), commit_id_t2) !=
            data3.in_flight_snapshot.end();
        REQUIRE(!t3_contains_t2); // T3 sees post-publish state, T2 NOT in_flight

        mgr.abort(session1);
        mgr.abort(session3);
    }

    TEST_CASE("Block E transaction_t::data() snapshot caching", "[block_e][procarray]") {
        using namespace components::table;
        using namespace components::session;

        std::pmr::synchronized_pool_resource resource;
        transaction_manager_t mgr(&resource);

        auto session = session_id_t::generate_uid();
        auto& txn = mgr.begin_transaction(session);

        // Snapshot captured at begin_transaction is cached on transaction_t.
        // data() returns by value-copy on each call — same snapshot fields each call.
        auto data1 = txn.data();
        auto data2 = txn.data();
        auto data3 = txn.data();

        REQUIRE(data1.transaction_id == data2.transaction_id);
        REQUIRE(data2.transaction_id == data3.transaction_id);
        REQUIRE(data1.start_time == data2.start_time);
        REQUIRE(data2.start_time == data3.start_time);
        REQUIRE(data1.snapshot_horizon == data2.snapshot_horizon);
        REQUIRE(data2.snapshot_horizon == data3.snapshot_horizon);
        REQUIRE(data1.in_flight_snapshot.size() == data2.in_flight_snapshot.size());
        REQUIRE(data2.in_flight_snapshot.size() == data3.in_flight_snapshot.size());

        mgr.abort(session);
    }

    TEST_CASE("Block E cross-agent atomic visibility (Version B*)", "[block_e][procarray]") {
        // A transaction's writes may land on two agents (catalog→agent_0,
        // user→hash-routed). A reader hitting the partial-publish window must not
        // see one half without the other. The canonical visibility filter
        // (transaction_version_operator::use_inserted_version) lives in
        // row_version_manager.cpp; we inline its rules per the contract documented
        // on transaction_data in row_version_manager.hpp and drive publish() /
        // take_snapshot() directly.
        using namespace components::table;
        using namespace components::session;

        std::pmr::synchronized_pool_resource resource;
        transaction_manager_t mgr(&resource);

        // Inlined copy of the row_version_manager.cpp visibility filter.
        auto visible = [](const transaction_manager_t::snapshot_t& snap, uint64_t id) -> bool {
            if (id >= TRANSACTION_ID_START)
                return false; // other-txn pending
            if (id > snap.snapshot_horizon)
                return false; // post-snapshot
            if (std::binary_search(snap.in_flight_snapshot.begin(), snap.in_flight_snapshot.end(), id))
                return false; // in-flight
            return true;
        };

        // T1 + T2 begin and commit — both commit_ids are allocated and parked in
        // in_flight_commits_ until publish().
        auto session1 = session_id_t::generate_uid();
        auto session2 = session_id_t::generate_uid();
        mgr.begin_transaction(session1);
        mgr.begin_transaction(session2);
        auto commit_id_t1 = mgr.commit(session1);
        auto commit_id_t2 = mgr.commit(session2);
        REQUIRE(commit_id_t1 > 0);
        REQUIRE(commit_id_t2 > 0);
        REQUIRE(commit_id_t1 != commit_id_t2);

        // Before any publish, a reader sees both commits in_flight.
        auto snap_S1 = mgr.take_snapshot(&resource);
        REQUIRE(snap_S1.in_flight_snapshot.size() == 2);
        bool s1_has_t1 =
            std::find(snap_S1.in_flight_snapshot.begin(), snap_S1.in_flight_snapshot.end(), commit_id_t1) !=
            snap_S1.in_flight_snapshot.end();
        bool s1_has_t2 =
            std::find(snap_S1.in_flight_snapshot.begin(), snap_S1.in_flight_snapshot.end(), commit_id_t2) !=
            snap_S1.in_flight_snapshot.end();
        REQUIRE(s1_has_t1);
        REQUIRE(s1_has_t2);
        REQUIRE(!visible(snap_S1, commit_id_t1));
        REQUIRE(!visible(snap_S1, commit_id_t2));

        // Partial-publish window: T2's agent finished and published, T1's has not.
        mgr.publish(commit_id_t2);

        auto snap_S2 = mgr.take_snapshot(&resource);
        REQUIRE(snap_S2.snapshot_horizon == commit_id_t2);
        REQUIRE(snap_S2.in_flight_snapshot.size() == 1);
        REQUIRE(snap_S2.in_flight_snapshot[0] == commit_id_t1);
        REQUIRE(visible(snap_S2, commit_id_t2));
        // Atomicity guarantee: T1 is still in_flight and thus invisible — a reader
        // never sees one agent's half of a partially-published transaction.
        REQUIRE(!visible(snap_S2, commit_id_t1));

        // T1 publishes; a fresh snapshot now sees both.
        mgr.publish(commit_id_t1);
        auto snap_S3 = mgr.take_snapshot(&resource);
        REQUIRE(snap_S3.in_flight_snapshot.empty());
        REQUIRE(snap_S3.snapshot_horizon >= commit_id_t2);
        REQUIRE(visible(snap_S3, commit_id_t1));
        REQUIRE(visible(snap_S3, commit_id_t2));

        // Consistent-view invariant: the old snap_S2 (captured pre-T1-publish) still
        // does not see T1 — snapshots are frozen at capture, no retroactive leak.
        REQUIRE(!visible(snap_S2, commit_id_t1));
        REQUIRE(visible(snap_S2, commit_id_t2));
    }

    TEST_CASE("Block E db_oid resolve via catalog_resolve_database", "[block_e][b14c]") {
        // Exercise node_catalog_resolve_database_t's set/get in isolation;
        // operator_resolve_database stamps the resolved oid via set_database_oid.
        using namespace components::logical_plan;
        using namespace components::catalog;

        std::pmr::synchronized_pool_resource resource;

        // Fresh node defaults to INVALID_OID.
        auto node = make_node_catalog_resolve_database(&resource, core::dbname_t{"main"});
        REQUIRE(node->dbname() == "main");
        REQUIRE(node->database_oid() == INVALID_OID);

        node->set_database_oid(well_known_oid::main_database);
        REQUIRE(node->database_oid() == well_known_oid::main_database);

        // Re-stamping is allowed (last-write-wins) — the planner re-runs resolve
        // after invalidation.
        node->set_database_oid(oid_t{42});
        REQUIRE(node->database_oid() == 42);
    }

} // namespace components::table::test_block_e
