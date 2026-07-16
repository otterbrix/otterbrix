#include <catch2/catch_test_macros.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/table_id.hpp>
#include <core/pmr.hpp>

#include <string_view>

using namespace components::catalog;

// qualified_name_t ordering must be a strict weak ordering: asymmetric,
// transitive, with a consistent equivalence. The comparison is lexicographic
// over (unique_identifier, database, schema, collection) — the 4-part
// uid.db.schema.rel syntax order, uid outermost.
TEST_CASE("catalog::identity::qualified_name_ordering_is_asymmetric") {
    const qualified_name_t a("1", "z", "s", "t");
    const qualified_name_t b("2", "a", "s", "t");
    REQUIRE_FALSE(((a < b) && (b < a)));
}

TEST_CASE("catalog::identity::qualified_name_ordering_trichotomy") {
    const qualified_name_t names[] = {
        qualified_name_t("1", "b", "", "t"),
        qualified_name_t("2", "a", "", "t"),
        qualified_name_t("2", "c", "", "t"),
    };
    for (const auto& p : names) {
        for (const auto& q : names) {
            const bool lt = p < q;
            const bool gt = q < p;
            const bool equiv = !lt && !gt;
            const int holds = static_cast<int>(lt) + static_cast<int>(gt) + static_cast<int>(equiv);
            REQUIRE(holds == 1);
        }
    }
}

TEST_CASE("catalog::identity::qualified_name_equality_matches_ordering") {
    const qualified_name_t a("u", "db", "s", "t");
    const qualified_name_t b("u", "db", "s", "t");
    const qualified_name_t c("u", "db", "s", "other");
    REQUIRE(a == b);
    REQUIRE_FALSE(a < b);
    REQUIRE_FALSE(b < a);
    REQUIRE_FALSE(a == c);
}
