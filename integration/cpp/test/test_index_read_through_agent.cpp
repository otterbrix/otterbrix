// ============================================================================
// WHAT A HASHED INDEX LOOKUP IS ALLOWED TO ANSWER.
//
// A hashed index is backed by bitcask, and bitcask stores one SNAPSHOT RECORD per
// key holding the whole row-id list. Its in-memory keydir keeps a single entry per
// key pointing at that record, and the entry's payload field carries only
// `rows.back()` (bitcask_index_disk.cpp, append_snapshot). So there are two ways to
// answer "which rows carry this key", and they do not agree:
//
//   * through the KEYDIR (disk_hash_table_t::get_all) -- one value_ref per key,
//     whose `.value` is the LAST row id written. Every earlier duplicate is lost.
//     Nothing above the bitcask store can even ask it that way any more: the handle
//     the index facade used to hold went with C2c.
//   * through the RECORD (bitcask_index_disk_t::find) -- the payload is read back
//     and unrolled, so all duplicates come out.
//
// Only the second is the truth. A SELECT that loses duplicates is not a slow
// answer, it is a WRONG one, and no row-count assertion anywhere else in the suite
// notices, because every OTHER path (full scan, btree index) reads its rows from
// somewhere else entirely.
//
// The second case below is the other half, and it is not about duplicates at all:
// uncommitted index entries never reach the disk (owner decision 16, per-txn
// buckets), so a lookup that reads ONLY the disk answers "no such row" to the very
// transaction that just inserted it. The rows-from-disk half and the
// own-uncommitted half must BOTH be in the answer, and the second half must be
// scoped to the ASKING transaction -- another transaction's uncommitted insert must
// stay invisible.
//
// Both cases go through the SQL front door on purpose: they pin the behaviour a
// user can observe, not the shape of whatever component currently produces it.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace {

    using components::cursor::cursor_t_ptr;

    std::string plan_text(const cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    // Every value of column 0, as int64. The tests below assert on the SET of ids
    // returned, not only on their count: a lookup that answers with the right
    // NUMBER of wrong rows is the failure mode a size() check cannot see.
    std::vector<int64_t> ids_of(const cursor_t_ptr& cur) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t r = 0; r < cur->size(); ++r) {
            out.push_back(cur->value(0, r).value<int64_t>());
        }
        std::sort(out.begin(), out.end());
        return out;
    }

} // namespace

// A hashed index over a column with REPEATED values. `WHERE k = <repeated>` must
// return every row carrying that key.
//
// EXPLAIN is asserted first, and it is load-bearing rather than decoration: if the
// planner ever stopped choosing the index for this predicate the statement would go
// back to a full scan, return all the right rows, and the test would pass while
// testing nothing at all.
TEST_CASE("integration::cpp::index_read_through_agent::hash_lookup_returns_every_duplicate") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_read_through_agent/duplicates");
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    // Scoped: the restart round below needs this instance torn down first (one
    // otterbrix instance per directory).
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE dupdb;")->is_success());
        REQUIRE(exec("CREATE TABLE dupdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX t_k ON dupdb.t USING hash (k);")->is_success());

        // k = 7 three times, k = 9 once, k = 8 twice. The singleton is the control: a
        // reader that keeps only the last row id per key answers it CORRECTLY, so it
        // separates "the lookup is broken" from "the index is empty".
        REQUIRE(
            exec("INSERT INTO dupdb.t (id, k) VALUES (1, 7), (2, 8), (3, 7), (4, 9), (5, 8), (6, 7);")->is_success());

        {
            auto plan = exec("EXPLAIN SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(plan->is_success());
            auto text = plan_text(plan);
            INFO("plan:\n" << text);
            INFO("without an Index Scan this test would be a full-scan test in disguise");
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }

        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(cur->is_success());
            INFO("three rows carry k = 7; a keydir-only read reports only the last one");
            CHECK(ids_of(cur) == std::vector<int64_t>{1, 3, 6});
        }
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 8;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{2, 5});
        }
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 9;");
            REQUIRE(cur->is_success());
            INFO("the singleton control: correct even for a reader that loses duplicates");
            CHECK(ids_of(cur) == std::vector<int64_t>{4});
        }

        // A committed DELETE of ONE of the duplicates must take exactly that row out and
        // leave its twins. This is the same snapshot record rewritten with a shorter row
        // list, so it also proves the read is following the record and not a cached
        // single id.
        REQUIRE(exec("DELETE FROM dupdb.t WHERE id = 3;")->is_success());
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{1, 6});
        }

        // CHECKPOINT rebuilds every index from a full table scan, and that feed replays
        // repeated keys. A rebuild that writes ONE row per key reduces the index to the
        // last row of each -- no crash, no restart needed, and only a lookup that reads
        // the whole row list can see it happen.
        REQUIRE(exec("CHECKPOINT;")->is_success());
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(cur->is_success());
            INFO("the index rebuild must keep every row of a repeated key");
            CHECK(ids_of(cur) == std::vector<int64_t>{1, 6});
        }
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 8;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{2, 5});
        }
    }

    // The duplicates must survive a restart: the index is rebuilt at every start,
    // and a rebuild that re-registers only one row per key would be the same loss
    // arriving by another road.
    {
        test_spaces restarted(config);
        auto* rd = restarted.dispatcher();
        auto rexec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return rd->execute_sql(session, sql);
        };
        auto cur = rexec("SELECT id FROM dupdb.t WHERE k = 7;");
        REQUIRE(cur->is_success());
        INFO("duplicates must still be there after the start-up rebuild");
        CHECK(ids_of(cur) == std::vector<int64_t>{1, 6});
    }
}

// A transaction's own uncommitted insert must be visible to ITSELF through the
// index, and to nobody else.
//
// Uncommitted index entries are deliberately never written to disk, so they live
// only in the index's per-transaction bucket. Any read path that answers from the
// disk alone loses them; any read path that folds in EVERY transaction's bucket
// leaks them. Both failures are silent, and neither is visible to a test written
// over committed data only -- which is precisely why this case exists next to the
// duplicates one above.
TEST_CASE("integration::cpp::index_read_through_agent::own_uncommitted_insert_is_visible_only_to_its_txn") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_read_through_agent/visibility");
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();

    auto writer = otterbrix::session_id_t();
    auto reader = otterbrix::session_id_t();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE visdb;")->is_success());
    REQUIRE(exec("CREATE TABLE visdb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX t_k ON visdb.t USING hash (k);")->is_success());
    // A committed baseline row under a DIFFERENT key: the lookups below must keep
    // answering from disk while an uncommitted entry exists beside them.
    REQUIRE(exec("INSERT INTO visdb.t (id, k) VALUES (1, 10);")->is_success());

    {
        auto plan = exec("EXPLAIN SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(plan->is_success());
        auto text = plan_text(plan);
        INFO("plan:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    REQUIRE(d->execute_sql(writer, "BEGIN;")->is_success());
    REQUIRE(d->execute_sql(writer, "INSERT INTO visdb.t (id, k) VALUES (2, 20);")->is_success());

    {
        auto own = d->execute_sql(writer, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(own->is_success());
        INFO("the inserting transaction must find its own uncommitted row through the index");
        CHECK(ids_of(own) == std::vector<int64_t>{2});
    }
    {
        auto other = d->execute_sql(reader, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(other->is_success());
        INFO("no other transaction may see an uncommitted index entry");
        CHECK(other->size() == 0);
    }
    {
        auto committed = d->execute_sql(reader, "SELECT id FROM visdb.t WHERE k = 10;");
        REQUIRE(committed->is_success());
        INFO("the committed row must stay reachable while an uncommitted entry exists");
        CHECK(ids_of(committed) == std::vector<int64_t>{1});
    }

    REQUIRE(d->execute_sql(writer, "COMMIT;")->is_success());

    {
        auto other = d->execute_sql(reader, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(other->is_success());
        INFO("after the commit the row is everyone's");
        CHECK(ids_of(other) == std::vector<int64_t>{2});
    }

    // The delete side of the same rule. An uncommitted DELETE must disappear from
    // the deleting transaction's own index lookups and from nobody else's.
    auto deleter = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(deleter, "BEGIN;")->is_success());
    REQUIRE(d->execute_sql(deleter, "DELETE FROM visdb.t WHERE k = 20;")->is_success());
    {
        auto own = d->execute_sql(deleter, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(own->is_success());
        INFO("the deleting transaction must not find the row it just removed");
        CHECK(own->size() == 0);
    }
    REQUIRE(d->execute_sql(deleter, "COMMIT;")->is_success());
    {
        auto after = d->execute_sql(reader, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(after->is_success());
        CHECK(after->size() == 0);
    }
}
