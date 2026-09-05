#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <services/collection/executor.hpp>

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

// A DDL WHOSE OID ALLOCATION DID NOT DELIVER MUST REFUSE THE STATEMENT.
//
// CREATE TABLE mints its identities in one round before the rewrite: the executor asks
// compute_oid_demand how many OIDs the statement needs, runs an allocation round against the
// disk actor, and hands the answer to the planner, which stamps pg_class / pg_attribute /
// pg_depend rows out of it.
//
// BEFORE: the round reported BOTH of its failures — a physical plan that would not build, and
// a drive that came back with an error — as an EMPTY vector, and the caller never compared the
// batch it got against the demand it had just computed. The only guard left was an assert
// inside oid_batch_t::allocate()/peek(), so a batch that came up short meant:
//   * Debug — the assert fires and the process dies mid-statement;
//   * NDEBUG — the assert is gone and allocate()/peek() read PAST THE END of the vector. For a
//     SHORT batch that is a read of live memory beyond the last OID, so CREATE TABLE reports
//     SUCCESS after writing pg_class / pg_attribute / pg_depend rows carrying whatever that
//     read produced. Those rows are DURABLE: the garbage identity survives restart and every
//     later lookup of the table goes through it.
// Neither outcome is a refusal, which is the only correct answer here (rule 6).
//
// THE INJECTION. The allocation round is a message round-trip to the disk actor over an
// in-memory atomic counter — no file, no page — so neither the .otbx interposer nor the WAL one
// can reach it, and there is no device to make fail. The round therefore carries its own narrow
// DEV_MODE seam (services::collection::executor::dev_set_oid_alloc_interposer), which
// substitutes the batch the round hands back. The two armed shapes are not invented states:
// an EMPTY batch is the exact value the round's real failure branches answered with, and a
// SHORT batch is what nothing in the path ever checked for.
//
// SENSITIVITY IS PROVEN INSIDE EACH TEST, not assumed: the same seam object is installed for
// the control statement (where it passes the batch through and the CREATE TABLE succeeds) and
// for the faulted one, and the test asserts on its own counters that the round was seen both
// times and substituted exactly once. A seam that had gone dead — wrong statement, wrong
// process, wrong build flags — cannot satisfy both halves.

namespace {

    using components::catalog::oid_t;

    class oid_alloc_fault_scope_t final : public services::collection::executor::oid_alloc_interposer_t {
    public:
        oid_alloc_fault_scope_t() { services::collection::executor::dev_set_oid_alloc_interposer(this); }
        ~oid_alloc_fault_scope_t() override {
            services::collection::executor::dev_set_oid_alloc_interposer(nullptr);
        }

        oid_alloc_fault_scope_t(const oid_alloc_fault_scope_t&) = delete;
        oid_alloc_fault_scope_t& operator=(const oid_alloc_fault_scope_t&) = delete;

        // Live knobs: a test arms them AFTER the traffic that has to succeed.
        bool arm = false;
        std::size_t drop_last = 0; // how many of the delivered OIDs the armed round loses

        std::size_t rounds_seen = 0; // rounds this seam actually observed
        std::size_t rounds_failed = 0;

        std::vector<oid_t> substitute(std::size_t /*requested*/, std::vector<oid_t> allocated) override {
            ++rounds_seen;
            if (!arm) {
                return allocated;
            }
            ++rounds_failed;
            allocated.resize(allocated.size() - std::min(drop_last, allocated.size()));
            return allocated;
        }
    };

} // namespace

// ===========================================================================
// item 1 — THE ROUND DELIVERS NOTHING (both failure branches answered with this).
// ===========================================================================
TEST_CASE("integration::cpp::test_oid_alloc_refusal::create_table_refuses_when_the_oid_round_delivers_nothing") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_oid_alloc_refusal/empty"),
                                                 /*wal_on=*/false);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE oidfail;")->is_success());

    oid_alloc_fault_scope_t fault;

    // CONTROL — the seam is installed and passing through. The statement must succeed, and the
    // seam must have seen its round: that is what makes the failure below attributable to the
    // injection rather than to anything else about the statement.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE oidfail.ok (id bigint, payload text);")->is_success());
    REQUIRE(fault.rounds_seen == 1);
    REQUIRE(fault.rounds_failed == 0);
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO oidfail.ok (id, payload) VALUES (1, 'a');")->is_success());

    // FAULT — the round delivers nothing at all.
    fault.arm = true;
    fault.drop_last = 64; // more than any statement here asks for: the batch comes back empty
    auto refused = test_helpers::exec(dispatcher, "CREATE TABLE oidfail.broken (id bigint, payload text);");
    fault.arm = false;

    INFO("a CREATE TABLE whose OID allocation delivered nothing must FAIL, not be rewritten from a "
         "batch that ran out");
    REQUIRE(refused->is_error());
    // The refusal really travelled through the injected round.
    REQUIRE(fault.rounds_seen == 2);
    REQUIRE(fault.rounds_failed == 1);

    // AND NOTHING WAS WRITTEN. Two independent readings of the same fact:
    //   * the table the refused statement named does not exist;
    //   * the name is still free — a retry creates it and it works, which it could not do if a
    //     pg_class row for it were sitting in the catalog.
    REQUIRE(test_helpers::exec(dispatcher, "SELECT * FROM oidfail.broken;")->is_error());

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE oidfail.broken (id bigint, payload text);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO oidfail.broken (id, payload) VALUES (7, 'g');")->is_success());
    auto rows = test_helpers::exec(dispatcher, "SELECT id FROM oidfail.broken;");
    REQUIRE(rows->is_success());
    REQUIRE(rows->size() == 1);

    // The control table is untouched by all of it.
    auto ok_rows = test_helpers::exec(dispatcher, "SELECT id FROM oidfail.ok;");
    REQUIRE(ok_rows->is_success());
    REQUIRE(ok_rows->size() == 1);
}

// ===========================================================================
// item 2 — THE ROUND DELIVERS FEWER OIDS THAN ASKED.
//
// This is the shape that produced the durable damage under NDEBUG: the batch has a live
// buffer, so the read past its last OID returns a plausible number instead of crashing, and
// the pg_attribute row for the last column of the table is written with it.
// ===========================================================================
TEST_CASE("integration::cpp::test_oid_alloc_refusal::create_table_refuses_when_the_oid_round_is_one_short") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_oid_alloc_refusal/short"),
                                                 /*wal_on=*/false);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE oidshort;")->is_success());

    oid_alloc_fault_scope_t fault;

    // CONTROL, same statement shape as the faulted one below.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE oidshort.ok (id bigint, payload text);")->is_success());
    REQUIRE(fault.rounds_seen == 1);
    REQUIRE(fault.rounds_failed == 0);

    // FAULT — one OID short of the demand: enough for pg_class and the first column, not for
    // the second column's attoid.
    fault.arm = true;
    fault.drop_last = 1;
    auto refused = test_helpers::exec(dispatcher, "CREATE TABLE oidshort.broken (id bigint, payload text);");
    fault.arm = false;

    INFO("a CREATE TABLE one OID short must FAIL, not write a pg_attribute row stamped with an "
         "identity nothing allocated");
    REQUIRE(refused->is_error());
    REQUIRE(fault.rounds_seen == 2);
    REQUIRE(fault.rounds_failed == 1);

    REQUIRE(test_helpers::exec(dispatcher, "SELECT * FROM oidshort.broken;")->is_error());

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE oidshort.broken (id bigint, payload text);")->is_success());
    REQUIRE(
        test_helpers::exec(dispatcher, "INSERT INTO oidshort.broken (id, payload) VALUES (7, 'g');")->is_success());
    auto rows = test_helpers::exec(dispatcher, "SELECT payload FROM oidshort.broken;");
    REQUIRE(rows->is_success());
    REQUIRE(rows->size() == 1);
}
