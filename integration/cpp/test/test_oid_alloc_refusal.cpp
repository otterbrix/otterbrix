#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <services/collection/executor.hpp>

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

// CREATE TABLE allocates all its OIDs in one round to the disk actor, then stamps
// pg_class/pg_attribute/pg_depend from the batch. That round used to report BOTH failure
// modes (unbuildable plan, disk-actor error) as an EMPTY vector, and nothing compared the
// batch against the demand -- the only guard was an assert inside
// oid_batch_t::allocate()/peek(). Under NDEBUG the assert is gone, so a SHORT batch reads
// past its end and writes DURABLE catalog rows carrying whatever memory that read produced.
// Neither empty nor short delivery is a refusal, and refusal is the only correct answer here
//.
//
// The round is an in-memory atomic-counter round-trip, unreachable by the .otbx/WAL fault
// interposers, so it carries its own seam (dev_set_oid_alloc_interposer) that substitutes the
// returned batch -- EMPTY and SHORT are exactly the two shapes the real failure paths could
// produce. Each test proves the seam is live by running a passing control through it first.

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

// item 1 -- the round delivers nothing (both failure branches answered with this).
TEST_CASE("integration::cpp::test_oid_alloc_refusal::create_table_refuses_when_the_oid_round_delivers_nothing") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_oid_alloc_refusal/empty"),
                                                 /*wal_on=*/false);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE oidfail;")->is_success());

    oid_alloc_fault_scope_t fault;

    // CONTROL: the seam passes the batch through and must see its round, so the failure
    // below is attributable to the injection and not to anything else about the statement.
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

    // Nothing was written: the table doesn't exist, and the name is still free -- a retry
    // creates it, which it could not do if a pg_class row for it were already in the catalog.
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

// item 2 -- the round delivers fewer OIDs than asked: the batch has a live buffer, so the
// read past its last OID returns a plausible number instead of crashing, and the
// pg_attribute row for the last column is written with it.
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
