#include <catch2/catch.hpp>
#include <components/table/transaction_manager.hpp>

TEST_CASE("components::table::transaction_manager::begin_commit") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr;

    auto session = session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    REQUIRE(txn.is_active());
    REQUIRE(!txn.is_committed());
    REQUIRE(!txn.is_aborted());
    REQUIRE(txn.transaction_id() >= TRANSACTION_ID_START);
    REQUIRE(txn.session() == session);

    auto commit_id = mgr.commit(session);
    REQUIRE(commit_id > 0);
    REQUIRE(!mgr.has_active_transaction(session));
}

TEST_CASE("components::table::transaction_manager::begin_abort") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr;

    auto session = session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);
    REQUIRE(txn.is_active());

    mgr.abort(session);
    REQUIRE(!mgr.has_active_transaction(session));
}

TEST_CASE("components::table::transaction_manager::two_sessions_independent") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr;

    auto s1 = session_id_t::generate_uid();
    auto s2 = session_id_t::generate_uid();

    auto& txn1 = mgr.begin_transaction(s1);
    auto& txn2 = mgr.begin_transaction(s2);

    REQUIRE(txn1.transaction_id() != txn2.transaction_id());
    REQUIRE(txn1.start_time() != txn2.start_time());
    REQUIRE(mgr.has_active_transactions());

    mgr.commit(s1);
    REQUIRE(mgr.has_active_transaction(s2));
    REQUIRE(!mgr.has_active_transaction(s1));

    mgr.commit(s2);
    REQUIRE(!mgr.has_active_transactions());
}

TEST_CASE("components::table::transaction_manager::find_transaction") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr;

    auto session = session_id_t::generate_uid();
    auto missing = session_id_t::generate_uid();

    mgr.begin_transaction(session);
    REQUIRE(mgr.find_transaction(session) != nullptr);
    REQUIRE(mgr.find_transaction(missing) == nullptr);

    mgr.commit(session);
    REQUIRE(mgr.find_transaction(session) == nullptr);
}

TEST_CASE("components::table::transaction_manager::lowest_active_start_time") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr;

    auto baseline = mgr.lowest_active_start_time();

    auto s1 = session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    auto t1 = txn1.start_time();
    REQUIRE(mgr.lowest_active_start_time() == t1);

    auto s2 = session_id_t::generate_uid();
    mgr.begin_transaction(s2);
    REQUIRE(mgr.lowest_active_start_time() == t1);

    mgr.commit(s1);
    REQUIRE(mgr.lowest_active_start_time() > t1);
}

TEST_CASE("components::table::transaction_manager::id_monotonicity") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr;
    uint64_t prev_id = 0;

    for (int i = 0; i < 10; i++) {
        auto session = session_id_t::generate_uid();
        auto& txn = mgr.begin_transaction(session);
        REQUIRE(txn.transaction_id() > prev_id);
        prev_id = txn.transaction_id();
        mgr.commit(session);
    }
}

TEST_CASE("components::table::transaction_manager::append_tracking") {
    using namespace components::table;
    using namespace components::session;

    transaction_manager_t mgr;

    auto session = session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    txn.add_append(0, 100);
    txn.add_append(100, 50);

    REQUIRE(txn.appends().size() == 2);
    REQUIRE(txn.appends()[0].row_start == 0);
    REQUIRE(txn.appends()[0].count == 100);
    REQUIRE(txn.appends()[1].row_start == 100);
    REQUIRE(txn.appends()[1].count == 50);

    mgr.commit(session);
}
