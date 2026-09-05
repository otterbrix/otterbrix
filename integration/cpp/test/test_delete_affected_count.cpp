// ============================================================================
// CHARACTERIZATION: a re-DELETE inside a transaction reports 0, not a stale count.
//
// Record #219 flags that operator_delete credits affected_rows_ += modified_size
// (the SCAN-matched count) rather than the count storage actually marked, and
// notes it is "legal today": the two never diverge through SQL, because the MVCC
// snapshot hides rows this transaction already deleted, so a re-DELETE matches
// nothing and modified_size is 0 exactly where a stored row is already stamped.
// No code change was made — forcing the count to storage's answer would alter a
// user-visible number with no reachable defect behind it (rule 6 "loud" does not
// mean "change a correct number"). This test PINS that standing contract so a
// future change to the count path cannot silently regress it.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <unistd.h>

TEST_CASE("integration::cpp::delete_affected_count::txn_re_delete_reports_zero", "[deletecount]") {
    auto config = test_create_config("/tmp/test_delete_affected_count_" + std::to_string(::getpid()) + "/txn");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](otterbrix::session_id_t& s, const std::string& sql) { return d->execute_sql(s, sql); };

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(exec(s, "CREATE DATABASE dc;")->is_success());
        REQUIRE(exec(s, "CREATE TABLE dc.t (id bigint);")->is_success());
        REQUIRE(exec(s, "INSERT INTO dc.t (id) VALUES (1), (2), (3);")->is_success());
    }

    auto txn = otterbrix::session_id_t();
    REQUIRE(exec(txn, "BEGIN;")->is_success());
    {
        auto cur = exec(txn, "DELETE FROM dc.t WHERE id <= 2;");
        REQUIRE(cur->is_success());
        INFO("two rows matched, two marks placed, two reported");
        CHECK(cur->size() == 2);
    }
    {
        // The scan hides rows this transaction already deleted, so the same
        // predicate now matches NOTHING — and the report says 0, not 2.
        auto cur = exec(txn, "DELETE FROM dc.t WHERE id <= 2;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 0);
    }
    REQUIRE(exec(txn, "COMMIT;")->is_success());

    {
        auto s = otterbrix::session_id_t();
        auto cur = exec(s, "SELECT id FROM dc.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == 3);
    }
}
