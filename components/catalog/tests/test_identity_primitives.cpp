#include <catch2/catch_test_macros.hpp>
#include <components/base/collection_full_name.hpp>

TEST_CASE("catalog::identity::qualified_name_equality") {
    const qualified_name_t a(core::connection_name_t{"u"},
                             core::dbname_t{"db"},
                             core::schema_t{"s"},
                             core::relname_t{"t"});
    const qualified_name_t b(core::connection_name_t{"u"},
                             core::dbname_t{"db"},
                             core::schema_t{"s"},
                             core::relname_t{"t"});
    const qualified_name_t c(core::connection_name_t{"u"},
                             core::dbname_t{"db"},
                             core::schema_t{"s"},
                             core::relname_t{"other"});
    REQUIRE(a == b);
    REQUIRE_FALSE(a == c);
}
