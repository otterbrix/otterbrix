// column_definition_t::set_attoid is reached from the CATALOG LOAD and BOOTSTRAP paths --
// services/disk/manager_disk_bootstrap.cpp, services/disk/manager_disk_io.cpp,
// services/disk/agent_disk.cpp -- i.e. it is stamped from bytes on disk, not only
// from freshly planned DDL. A disagreement between two identity sources is therefore INPUT,
// and the answer has to be the same in every build: refuse the re-stamp, keep the first stamp,
// and say so. An abort would make a database with a disagreeing catalog unopenable, and an
// assert makes the Debug build do exactly that while the release build refuses -- two
// behaviours for one input.
//
// This test pins the refusal. It cannot run at all while the assert stands: it takes the
// process down before the first REQUIRE.

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_definition.hpp>

using namespace components::table;

TEST_CASE("components::table::column_definition::a_disagreeing_attoid_restamp_is_refused_not_applied") {
    column_definition_t col("price", components::types::logical_type::DOUBLE);
    REQUIRE(col.attoid() == 0);

    col.set_attoid(42);
    REQUIRE(col.attoid() == 42);

    // Idempotent re-stamp: legal, and already covered by components/catalog/tests/test_oids.cpp.
    col.set_attoid(42);
    REQUIRE(col.attoid() == 42);

    // The disagreement. The first stamp stays authoritative and the process survives to say it.
    col.set_attoid(43);
    INFO("attoid after a disagreeing re-stamp: " << col.attoid());
    REQUIRE(col.attoid() == 42);
}
