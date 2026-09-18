// set_attoid also fires from on-disk catalog data (services/disk/manager_disk_bootstrap.cpp,
// manager_disk_io.cpp, agent_disk.cpp), so a disagreeing re-stamp is INPUT and must be refused
// the same way in every build, not assert()'d (Debug-only abort). This test pins that refusal.

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

    // Disagreeing re-stamp: refused, first stamp (42) wins.
    col.set_attoid(43);
    INFO("attoid after a disagreeing re-stamp: " << col.attoid());
    REQUIRE(col.attoid() == 42);
}
