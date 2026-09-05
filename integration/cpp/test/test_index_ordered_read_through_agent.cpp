// ============================================================================
// WHAT ANSWERS A PLAIN `CREATE INDEX`.
//
// SQL has exactly one explicit index spelling, `USING hash`. Everything else —
// including no USING clause at all — is index_type::single, so an ordinary
// `CREATE INDEX i ON t (c)` is the default and by far the common case.
//
// Its committed rows have always been written to an on-disk b+tree, but until C2b
// they were also kept in a second, in-memory copy, and every SELECT was answered
// from that copy. The tree was write-only: the manager rebuilt the memory copy from
// it at start-up and never read it again. This file pins the engine change — reads
// now travel a message to the index's own agent, which reads the tree — and it has
// to pin it with something other than row counts, because the two engines return
// the same rows when both are healthy. A facade that registers, gets chosen by the
// planner and answers out of a leftover in-memory structure would satisfy every
// count assertion in the suite while changing nothing.
//
// So each case asserts THREE things at once:
//   * the plan really uses the index (without this the file would be a full-scan
//     test in disguise, and would pass with the index deleted);
//   * the ROWS are exactly right, by id, not merely right in number;
//   * services::index::index_agent_reads() moved, i.e. the answer came out of the
//     disk agent rather than out of memory.
//
// The range predicates carry a second load. Before C2b the agent's read message
// was equality-only: there was no message a `<`, `<=`, `>`, `>=` or `<>` could
// travel on at all, so those queries could not have been answered this way even in
// principle. Every range case below is therefore both a correctness gate and a
// witness that the message now carries the predicate.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <services/index/manager_index.hpp>

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

    // Every value of column 0, as int64, sorted. The cases below assert on the SET of
    // ids: a lookup that answers with the right NUMBER of wrong rows is the failure a
    // size() check cannot see.
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

// The whole predicate set over a plain `CREATE INDEX`, every answer taken from the
// index's disk agent.
//
// The key column carries DUPLICATES on purpose. A duplicate is what separates a
// complete answer from a plausible one: an index that keeps a single row per key
// answers `k = 7` with one row and looks entirely healthy doing it.
TEST_CASE("integration::cpp::index_ordered_read_through_agent::every_predicate_is_answered_by_the_agent") {
    auto config = test_create_config(integration_fixture_path("test_index_ordered_read_through_agent/predicates"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE orddb;")->is_success());
    REQUIRE(exec("CREATE TABLE orddb.t (id bigint, k bigint);")->is_success());
    // NO `USING` — this is the statement the whole file is about.
    REQUIRE(exec("CREATE INDEX t_k ON orddb.t (k);")->is_success());

    // k: 7,8,7,9,8,7  -> 7 three times, 8 twice, 9 once.
    REQUIRE(exec("INSERT INTO orddb.t (id, k) VALUES (1, 7), (2, 8), (3, 7), (4, 9), (5, 8), (6, 7);")->is_success());

    // `probe` runs one predicate and holds it to all three standards at once.
    auto probe = [&](const std::string& predicate, const std::vector<int64_t>& expected) {
        {
            auto plan = exec("EXPLAIN SELECT id FROM orddb.t WHERE " + predicate + ";");
            REQUIRE(plan->is_success());
            auto text = plan_text(plan);
            INFO("predicate: " << predicate << "\nplan:\n" << text);
            INFO("without an Index Scan this would be a full-scan test wearing an index's name");
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        services::index::reset_index_agent_reads();
        auto cur = exec("SELECT id FROM orddb.t WHERE " + predicate + ";");
        REQUIRE(cur->is_success());
        const auto reads = services::index::index_agent_reads();
        INFO("predicate: " << predicate << "  agent reads: " << reads);
        INFO("zero agent reads means the answer came from an in-memory copy, not from the b+tree");
        CHECK(reads >= 1);
        CHECK(ids_of(cur) == expected);
    };

    probe("k = 7", {1, 3, 6});
    probe("k = 9", {4}); // the singleton control: right even for a reader that loses duplicates
    probe("k = 6", {});  // a key nothing carries; an empty answer here is the TRUE one
    probe("k < 8", {1, 3, 6});
    probe("k <= 8", {1, 2, 3, 5, 6}); // the inclusive bound: `<` and `<=` must differ
    probe("k > 7", {2, 4, 5});
    probe("k >= 7", {1, 2, 3, 4, 5, 6});
    probe("k > 9", {});   // above every key
    probe("k < 7", {});   // below every key
    probe("k >= 9", {4}); // the inclusive bound at the top end
}

// The behavioural gate: the rows a default index reports must follow the table
// through INSERT, UPDATE, DELETE and a restart.
TEST_CASE("integration::cpp::index_ordered_read_through_agent::dml_and_restart_are_reflected") {
    auto config = test_create_config(integration_fixture_path("test_index_ordered_read_through_agent/dml"));
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

        REQUIRE(exec("CREATE DATABASE dmldb;")->is_success());
        REQUIRE(exec("CREATE TABLE dmldb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX t_k ON dmldb.t (k);")->is_success());
        REQUIRE(exec("INSERT INTO dmldb.t (id, k) VALUES (1, 10), (2, 20), (3, 20), (4, 30);")->is_success());

        {
            auto cur = exec("SELECT id FROM dmldb.t WHERE k = 20;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{2, 3});
        }

        // UPDATE moves a row from one key to another: it must leave the old key and
        // arrive under the new one. A stale entry left behind is the classic index bug
        // and only shows up when BOTH keys are asked.
        REQUIRE(exec("UPDATE dmldb.t SET k = 40 WHERE id = 3;")->is_success());
        {
            auto cur = exec("SELECT id FROM dmldb.t WHERE k = 20;");
            REQUIRE(cur->is_success());
            INFO("the updated row must be gone from its OLD key");
            CHECK(ids_of(cur) == std::vector<int64_t>{2});
        }
        {
            auto cur = exec("SELECT id FROM dmldb.t WHERE k = 40;");
            REQUIRE(cur->is_success());
            INFO("and present under its NEW one");
            CHECK(ids_of(cur) == std::vector<int64_t>{3});
        }
        {
            auto cur = exec("SELECT id FROM dmldb.t WHERE k >= 30;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{3, 4});
        }

        REQUIRE(exec("DELETE FROM dmldb.t WHERE id = 4;")->is_success());
        {
            auto cur = exec("SELECT id FROM dmldb.t WHERE k = 30;");
            REQUIRE(cur->is_success());
            INFO("a deleted row's index entry must go with it");
            CHECK(cur->size() == 0);
        }
        {
            auto cur = exec("SELECT id FROM dmldb.t WHERE k >= 10;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{1, 2, 3});
        }
    }

    // Reopen. There is no in-memory copy to rebuild any more: whatever comes back now
    // was read out of the b+tree on disk.
    {
        test_spaces restarted(config);
        auto* rd = restarted.dispatcher();
        auto rexec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return rd->execute_sql(session, sql);
        };

        services::index::reset_index_agent_reads();
        auto cur = rexec("SELECT id FROM dmldb.t WHERE k = 40;");
        REQUIRE(cur->is_success());
        INFO("after a restart the tree is the only copy, so this read must reach the agent");
        CHECK(services::index::index_agent_reads() >= 1);
        CHECK(ids_of(cur) == std::vector<int64_t>{3});

        auto range = rexec("SELECT id FROM dmldb.t WHERE k >= 10;");
        REQUIRE(range->is_success());
        INFO("the UPDATE and the DELETE from before the restart must both still hold");
        CHECK(ids_of(range) == std::vector<int64_t>{1, 2, 3});

        auto gone = rexec("SELECT id FROM dmldb.t WHERE k = 30;");
        REQUIRE(gone->is_success());
        CHECK(gone->size() == 0);
    }
}

// A transaction's own uncommitted writes must be visible to ITSELF through a RANGE
// predicate, and to nobody else.
//
// This is the half of an answer that never reaches disk (per-txn buckets, no
// write-through), so it can only come from the facade's own merge. What makes the
// ordered case different from the hashed one is the predicate: a pending row keyed
// 25 belongs in the answer to `k > 20` and not in the answer to `k = 20`. A merge
// that could only test equality — the only thing a hash bucket ever needs — would
// drop it, and the transaction would fail to see its own insert with nothing
// reporting a problem.
TEST_CASE("integration::cpp::index_ordered_read_through_agent::own_uncommitted_rows_satisfy_a_range") {
    auto config = test_create_config(integration_fixture_path("test_index_ordered_read_through_agent/visibility"));
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

    REQUIRE(exec("CREATE DATABASE visord;")->is_success());
    REQUIRE(exec("CREATE TABLE visord.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX t_k ON visord.t (k);")->is_success());
    // Two committed rows, one on each side of the probes below, so the range answers
    // stay non-trivial while an uncommitted row sits beside them.
    REQUIRE(exec("INSERT INTO visord.t (id, k) VALUES (1, 10), (2, 30);")->is_success());

    REQUIRE(d->execute_sql(writer, "BEGIN;")->is_success());
    REQUIRE(d->execute_sql(writer, "INSERT INTO visord.t (id, k) VALUES (3, 25);")->is_success());

    {
        auto own = d->execute_sql(writer, "SELECT id FROM visord.t WHERE k > 20;");
        REQUIRE(own->is_success());
        INFO("the inserting transaction must find its own uncommitted row through a RANGE predicate");
        CHECK(ids_of(own) == std::vector<int64_t>{2, 3});
    }
    {
        auto own_eq = d->execute_sql(writer, "SELECT id FROM visord.t WHERE k = 25;");
        REQUIRE(own_eq->is_success());
        INFO("and through an equality one");
        CHECK(ids_of(own_eq) == std::vector<int64_t>{3});
    }
    {
        auto own_out = d->execute_sql(writer, "SELECT id FROM visord.t WHERE k < 20;");
        REQUIRE(own_out->is_success());
        INFO("a pending row keyed 25 must NOT be folded into a predicate it does not satisfy");
        CHECK(ids_of(own_out) == std::vector<int64_t>{1});
    }
    {
        auto other = d->execute_sql(reader, "SELECT id FROM visord.t WHERE k > 20;");
        REQUIRE(other->is_success());
        INFO("no other transaction may see an uncommitted index entry");
        CHECK(ids_of(other) == std::vector<int64_t>{2});
    }

    REQUIRE(d->execute_sql(writer, "COMMIT;")->is_success());
    {
        auto other = d->execute_sql(reader, "SELECT id FROM visord.t WHERE k > 20;");
        REQUIRE(other->is_success());
        INFO("after the commit the row is everyone's");
        CHECK(ids_of(other) == std::vector<int64_t>{2, 3});
    }

    // The delete side of the same rule, again through a range: an uncommitted DELETE
    // must drop out of the deleting transaction's own answers and nobody else's.
    auto deleter = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(deleter, "BEGIN;")->is_success());
    REQUIRE(d->execute_sql(deleter, "DELETE FROM visord.t WHERE k = 25;")->is_success());
    {
        auto own = d->execute_sql(deleter, "SELECT id FROM visord.t WHERE k > 20;");
        REQUIRE(own->is_success());
        INFO("the deleting transaction must not find the row it just removed");
        CHECK(ids_of(own) == std::vector<int64_t>{2});
    }
    {
        auto other = d->execute_sql(reader, "SELECT id FROM visord.t WHERE k > 20;");
        REQUIRE(other->is_success());
        INFO("nobody else may lose it yet");
        CHECK(ids_of(other) == std::vector<int64_t>{2, 3});
    }
    REQUIRE(d->execute_sql(deleter, "COMMIT;")->is_success());
    {
        auto after = d->execute_sql(reader, "SELECT id FROM visord.t WHERE k > 20;");
        REQUIRE(after->is_success());
        CHECK(ids_of(after) == std::vector<int64_t>{2});
    }
}
