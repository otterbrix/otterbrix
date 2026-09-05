// Plain `CREATE INDEX` (no USING) is index_type::single. Its rows must now be read
// through the index agent's on-disk tree, not an in-memory copy: each probe checks
// the plan uses Index Scan, rows are exactly right by id, and index_agent_reads()
// moved, since a facade could otherwise pass on row counts alone. Range predicates
// (<, <=, >, >=) exercise a read message that used to be equality-only.

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

    // Sorted ids from column 0; compared as a set so a right-count/wrong-rows bug
    // can't hide behind size().
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

// Duplicate keys are deliberate: an index that silently kept one row per key would
// still answer `k = 7` and look healthy.
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

    REQUIRE(exec("INSERT INTO orddb.t (id, k) VALUES (1, 7), (2, 8), (3, 7), (4, 9), (5, 8), (6, 7);")->is_success());

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
    probe("k > 9", {});
    probe("k < 7", {});
    probe("k >= 9", {4}); // the inclusive bound at the top end
}

// The rows a default index reports must follow the table through INSERT, UPDATE,
// DELETE, and a restart.
TEST_CASE("integration::cpp::index_ordered_read_through_agent::dml_and_restart_are_reflected") {
    auto config = test_create_config(integration_fixture_path("test_index_ordered_read_through_agent/dml"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    // Scoped: the restart below needs this instance torn down first (one otterbrix
    // instance per directory).
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

        // A stale old-key entry left behind is the classic index bug; only shows up
        // when both keys are checked.
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

    // Reopen: no in-memory copy left, so this can only be answered from the on-disk
    // tree.
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

// A transaction's own uncommitted writes (per-txn buckets, never on disk) must be
// visible to itself through a RANGE predicate too, not just equality: a pending row
// keyed 25 must satisfy `k > 20` even though it isn't `k = 20`.
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
    // Two committed rows straddle the probes below so an uncommitted row in between
    // changes the range answer.
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

    // Delete side of the same rule: an uncommitted DELETE must vanish from the
    // deleter's own range answers and nobody else's.
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
