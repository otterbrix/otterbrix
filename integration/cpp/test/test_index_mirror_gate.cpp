#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <string>

static std::string mirror_gate_plan_text(const components::cursor::cursor_t_ptr& cur) {
    std::string out;
    for (std::size_t r = 0; r < cur->size(); ++r) {
        auto cell = cur->value(0, r);
        out += std::string(cell.value<std::string_view>());
        out += '\n';
    }
    return out;
}

// A table with no indexes must not pay for index maintenance -- and must still answer.
//
// The gate was `ctx->index_address != empty_address()` -- "does an index manager exist" -- which
// is true for every table in every configuration, because register_collection creates an engine
// per table whether or not any index was ever declared. So every INSERT deep-copied its whole
// chunk a second time, shipped it across a mailbox, and the index manager walked the rows
// against an empty index list.
//
// THE PLAN SHAPE IS HALF THE CASE, and it is the half the disk layout made load-bearing. An
// index is a disk-backed engine now (C6a: there is no in-memory one left), so "the table has no
// index on this key" has exactly one correct consequence -- has_index_on says no, the planner
// builds a full scan, and the rows come from the heap. The forbidden outcome is an Index Scan
// over a key nothing indexes: manager_index_t would answer index_not_exists and the statement
// would fail, or worse, answer nothing at all. Counting mirror sends alone cannot tell those
// apart, because a plan that never reaches an index sends nothing either way.
TEST_CASE("integration::cpp::test_index_mirror_gate::table_without_indexes_skips_the_index") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_mirror_gate/plain");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE m;")->is_success());
    REQUIRE(exec("CREATE TABLE m.plain (id bigint, v bigint);")->is_success());

    components::operators::reset_insert_index_mirror_sends();
    for (int i = 0; i < 5; ++i) {
        REQUIRE(exec("INSERT INTO m.plain (id, v) VALUES (" + std::to_string(i) + ", 1);")->is_success());
    }
    const auto sends = components::operators::insert_index_mirror_sends();

    INFO("index-mirror sends for 5 inserts into an unindexed table: " << sends);
    CHECK(sends == 0);

    {
        auto plan = exec("EXPLAIN SELECT id FROM m.plain WHERE id = 3;");
        REQUIRE(plan->is_success());
        const auto text = mirror_gate_plan_text(plan);
        INFO("plan:\n" << text);
        INFO("an Index Scan on a table that declared no index would route the read at an engine "
             "that does not exist");
        CHECK(text.find("Index Scan") == std::string::npos);
        CHECK(text.find("Seq Scan") != std::string::npos);
    }

    auto cur = exec("SELECT id FROM m.plain WHERE id = 3;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 1);
}

// The guard on the other side matters just as much: an indexed table MUST still mirror, or the
// table stays right while the index quietly goes stale.
//
// WHAT THE DISK LAYOUT CHANGED. The index no longer holds the rows this test writes: the mirror
// send hands them to manager_index_t, which buckets them per transaction and passes them to the
// index's own disk agent at commit; a read is a message to that agent and comes back with what
// the agent's store holds. So "the row reached the index" is now a claim about a store in
// another actor, and only a read that actually TRAVELS there can check it.
//
// Which is why the plan shape is asserted before every count below. A Seq Scan would answer
// every query here correctly out of the heap -- and would keep answering correctly if the index
// had received nothing at all, which is exactly the failure this case exists to catch. And an
// Index Scan that comes back short is the other forbidden outcome (rule 6): a REGISTERED engine
// answering fewer rows than the table holds, silently. The unindexed twin is the oracle for
// what "the table holds".
TEST_CASE("integration::cpp::test_index_mirror_gate::indexed_table_still_mirrors") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_mirror_gate/indexed");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE m;")->is_success());
    REQUIRE(exec("CREATE TABLE m.idx (id bigint, k bigint);")->is_success());
    // The unindexed twin: same rows, no index, so every answer below has a heap-only oracle.
    REQUIRE(exec("CREATE TABLE m.twin (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX idx_k ON m.idx (k);")->is_success());

    components::operators::reset_insert_index_mirror_sends();
    for (int i = 0; i < 5; ++i) {
        const std::string values = "(" + std::to_string(i) + ", " + std::to_string(100 + i) + ")";
        REQUIRE(exec("INSERT INTO m.idx (id, k) VALUES " + values + ";")->is_success());
        REQUIRE(exec("INSERT INTO m.twin (id, k) VALUES " + values + ";")->is_success());
    }
    const auto sends = components::operators::insert_index_mirror_sends();

    INFO("index-mirror sends for 5 inserts into an INDEXED table (the twin is unindexed and "
         "must not add any): "
         << sends);
    CHECK(sends == 5);

    // Read back through the index, and prove it IS the index doing the reading.
    const auto probe = [&](const std::string& predicate) {
        {
            auto plan = exec("EXPLAIN SELECT id FROM m.idx WHERE " + predicate + ";");
            REQUIRE(plan->is_success());
            const auto text = mirror_gate_plan_text(plan);
            INFO("predicate: " << predicate << "\nplan:\n" << text);
            INFO("a Seq Scan here would answer out of the heap and pass even with an empty index");
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        auto indexed = exec("SELECT id FROM m.idx WHERE " + predicate + ";");
        REQUIRE(indexed->is_success());
        auto heap = exec("SELECT id FROM m.twin WHERE " + predicate + ";");
        REQUIRE(heap->is_success());
        INFO("predicate: " << predicate << " -- index answered " << indexed->size() << " row(s), the "
             << "unindexed twin holds " << heap->size());
        INFO("a registered index engine answering short is the silent wrong answer rule 6 forbids");
        CHECK(indexed->size() == heap->size());
        return indexed->size();
    };

    CHECK(probe("k = 103") == 1);
    // A key nothing carries: the index must answer EMPTY because the rows are not there, and the
    // twin says so too -- so an index that answered empty for everything could not pass here.
    CHECK(probe("k = 999") == 0);
    // A RANGE: only an ORDERED index answers one at all (manager_index_t refuses a range on an
    // index with no ordering), and it walks the agent's b+tree rather than a single key.
    CHECK(probe("k >= 102") == 3);
    CHECK(probe("k < 102") == 2);

    // DELETE must reach the index too, or the index keeps answering with a row the table no
    // longer has -- the mirror gate's failure in the opposite direction.
    REQUIRE(exec("DELETE FROM m.idx WHERE k = 103;")->is_success());
    REQUIRE(exec("DELETE FROM m.twin WHERE k = 103;")->is_success());
    CHECK(probe("k = 103") == 0);
    CHECK(probe("k >= 102") == 2);
}

// ---------------------------------------------------------------------------
// TWO INDEXES OVER ONE COLUMN, and dropping one of them.
//
// `CREATE INDEX i ON t (k)` and `CREATE INDEX j ON t USING hash (k)` both succeed:
// manager_index_t::create_index rejects a duplicate on the PAIR (keys, type), and the
// planner counts on the pair existing — it only routes a range predicate to an index when
// a NON-hashed index also covers the key.
//
// The index registry used to answer "which key sets are indexed" out of a map holding ONE
// slot per key set, beside the list that actually OWNS the indexes. Registering the second
// index could not write the taken slot, and dropping EITHER index erased it by key — taking
// the SURVIVOR's registration with it. From that moment the table looked unindexed while
// still holding a live index: the planner stopped choosing it (this test), DML stopped
// mirroring into it, and the compact/repopulate gates that count the table's indexes read
// zero.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_index_mirror_gate::dropping_a_twin_index_leaves_the_survivor_live") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_mirror_gate/twin");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE m;")->is_success());
    REQUIRE(exec("CREATE TABLE m.twin (id bigint, k bigint);")->is_success());
    // Both indexes are built over the SAME column, on an empty table, and both must be
    // accepted — the ordered one first, so it is the one whose registration the drop below
    // used to take away.
    REQUIRE(exec("CREATE INDEX twin_k ON m.twin (k);")->is_success());
    REQUIRE(exec("CREATE INDEX twin_k_h ON m.twin USING hash (k);")->is_success());

    REQUIRE(exec("INSERT INTO m.twin (id, k) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);")->is_success());

    REQUIRE(exec("DROP INDEX m.twin.twin_k_h;")->is_success());

    // Rows written AFTER the drop. Whether they reach the surviving index is the whole
    // question: the mirror stamp DML reads is `does this table have any indexed key set`,
    // computed from the very list the drop used to empty.
    REQUIRE(exec("INSERT INTO m.twin (id, k) VALUES (6, 100), (7, 110), (8, 120);")->is_success());

    auto probe = [&](const std::string& predicate, std::size_t expected_rows) {
        {
            auto plan = exec("EXPLAIN SELECT id FROM m.twin WHERE " + predicate + ";");
            REQUIRE(plan->is_success());
            const auto text = mirror_gate_plan_text(plan);
            INFO("predicate: " << predicate << "\nplan:\n" << text);
            INFO("a Seq Scan here means the planner cannot see an index the table still holds");
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        auto cur = exec("SELECT id FROM m.twin WHERE " + predicate + ";");
        REQUIRE(cur->is_success());
        INFO("predicate: " << predicate);
        INFO("the plan above is an Index Scan and an ordered index answers out of its disk "
             "agent, so a row the index never received is simply missing here");
        CHECK(cur->size() == expected_rows);
    };

    // Written before the drop.
    probe("k = 30", 1);
    // Written after it — present only if DML still mirrored into the survivor.
    probe("k = 110", 1);
    // A RANGE: only the ordered survivor can answer one at all (manager_index_t refuses a
    // range on an index with no ordering), so this also pins that the untyped lookup hands
    // back the ordered index and not some other candidate.
    probe("k >= 100", 3);
    probe("k < 100", 5);

    // Deletes must reach it too.
    REQUIRE(exec("DELETE FROM m.twin WHERE k = 110;")->is_success());
    probe("k >= 100", 2);
}
