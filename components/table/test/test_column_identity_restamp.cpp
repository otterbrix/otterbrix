// set_attoid also fires from on-disk catalog data (services/disk/manager_disk_bootstrap.cpp,
// manager_disk_io.cpp, agent_disk.cpp), so a disagreeing re-stamp is INPUT and must be refused
// the same way in every build, not assert()'d (Debug-only abort). This test pins that refusal.

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/pmr.hpp>

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

// vector_t copies are shallow, and a column_definition_t is sent between actors: the copy must own its default.
TEST_CASE("components::table::column_definition::a_copy_owns_its_default") {
    core::pmr::otterbrix_resource resource;
    column_definition_t original("name",
                                 components::types::complex_logical_type{components::types::logical_type::TEXT},
                                 components::vector::make_scalar(&resource, std::string{"original"}));
    REQUIRE(original.has_default_value());

    column_definition_t copy(original);
    REQUIRE(copy.has_default_value());
    CHECK(copy.default_value().get_value<std::string_view>(0) == "original");
    CHECK(copy.default_value().auxiliary().get() != original.default_value().auxiliary().get());
    CHECK(copy.default_value().data() != original.default_value().data());

    column_definition_t assigned("other",
                                 components::types::complex_logical_type{components::types::logical_type::BIGINT});
    assigned = original;
    CHECK(assigned.default_value().get_value<std::string_view>(0) == "original");
    CHECK(assigned.default_value().auxiliary().get() != original.default_value().auxiliary().get());

    copy.set_default_value(components::vector::make_null(&resource, copy.type()));
    CHECK(copy.default_value().is_null(0));
    CHECK_FALSE(original.default_value().is_null(0));
    CHECK(original.default_value().get_value<std::string_view>(0) == "original");
}
