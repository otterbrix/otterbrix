#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <integration/cpp/catalog_listing.hpp>

// list_tables() used to collapse no-cursor/error/empty-cursor into "return []", hiding a
// failed query as an empty database. pg_class isn't yet resolvable as a FROM target,
// so kListTablesQuery may legitimately error; tests below accept that outcome too.

namespace {

    using namespace components;

    cursor::cursor_t_ptr make_failed_cursor(std::pmr::memory_resource* resource) {
        return cursor::make_cursor(
            resource,
            core::error_t{core::error_code_t::schema_error, std::pmr::string{"path: 'oid' was not found", resource}});
    }

    struct pg_class_row_t {
        std::uint32_t oid;
        std::string relname;
        char relkind;
    };

    cursor::cursor_t_ptr make_pg_class_cursor(std::pmr::memory_resource* resource,
                                              const std::vector<pg_class_row_t>& rows) {
        std::pmr::vector<types::complex_logical_type> types{resource};
        types.emplace_back(types::logical_type::UINTEGER);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        types[0].set_alias("oid");
        types[1].set_alias("relname");
        types[2].set_alias("relkind");

        vector::data_chunk_t chunk{resource, types, vector::DEFAULT_VECTOR_CAPACITY};
        for (std::size_t i = 0; i < rows.size(); ++i) {
            const auto& r = rows[i];
            chunk.set_value(0, i, r.oid);
            chunk.set_value(1, i, std::string_view{r.relname});
            chunk.set_value(2, i, std::string_view{&r.relkind, 1});
        }
        chunk.set_cardinality(rows.size());
        return cursor::make_cursor(resource, std::move(chunk));
    }

} // namespace

TEST_CASE("integration::cpp::list_tables::failed_query_is_not_an_empty_database") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_failed_cursor(&resource);
    REQUIRE(cursor->is_error());

    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    REQUIRE(names.has_error());
    REQUIRE(names.error().type == core::error_code_t::schema_error);
}

// A cursor that never arrived is a hard engine fault, not an empty catalog.
TEST_CASE("integration::cpp::list_tables::absent_cursor_is_not_an_empty_database") {
    core::pmr::otterbrix_resource resource;
    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor::cursor_t_ptr{});
    REQUIRE(names.has_error());
}

// A successful read of an empty catalog is the one case an empty list is allowed to mean.
TEST_CASE("integration::cpp::list_tables::empty_catalog_is_an_empty_list") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_pg_class_cursor(&resource, {});
    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    REQUIRE_FALSE(names.has_error());
    REQUIRE(names.value().empty());
}

// Production filter: oid >= FIRST_USER_OID and relkind == 'r'.
TEST_CASE("integration::cpp::list_tables::filters_system_rows_and_non_tables") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_pg_class_cursor(
        &resource,
        {
            {catalog::well_known_oid::pg_class_table, "pg_class", catalog::relkind::regular},
            {catalog::FIRST_USER_OID + 1, "alpha", catalog::relkind::regular},
            {catalog::FIRST_USER_OID + 2, "alpha_idx", catalog::relkind::index},
            {catalog::FIRST_USER_OID + 3, "alpha_view", catalog::relkind::view},
            {catalog::FIRST_USER_OID + 4, "beta", catalog::relkind::regular},
        });

    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    REQUIRE_FALSE(names.has_error());
    REQUIRE(names.value().size() == 2);
    REQUIRE(names.value()[0] == "alpha");
    REQUIRE(names.value()[1] == "beta");
}

// Two real tables must never come back as an empty list, whichever outcome the query gives.
TEST_CASE("integration::cpp::list_tables::two_tables_never_read_as_empty") {
    auto config = test_create_config(integration_fixture_path("list_tables/two_tables"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Alpha (id BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Beta (id BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.Alpha;")->is_success());
        auto session2 = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session2, "SELECT * FROM TestDatabase.Beta;")->is_success());
    }

    auto session = otterbrix::session_id_t();
    auto cursor = dispatcher->execute_sql(session, std::string{otterbrix::kListTablesQuery});
    auto names = otterbrix::user_table_names_from_pg_class(dispatcher->resource(), cursor);
    if (names.has_error()) {
        SUCCEED("kListTablesQuery reports a loud error; pg_class is not a resolvable FROM target yet");
    } else {
        REQUIRE(names.value().size() == 2);
    }
}

namespace {

    using namespace components;

    // relname/relkind are NOT NULL in the schema; a NULL is a catalog defect, not a filter.
    enum class null_cell_t
    {
        relname,
        relkind
    };

    components::cursor::cursor_t_ptr make_pg_class_cursor_with_null(std::pmr::memory_resource* resource,
                                                                    null_cell_t which) {
        using namespace components;
        std::pmr::vector<types::complex_logical_type> types{resource};
        types.emplace_back(types::logical_type::UINTEGER);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        types[0].set_alias("oid");
        types[1].set_alias("relname");
        types[2].set_alias("relkind");

        vector::data_chunk_t chunk{resource, types, vector::DEFAULT_VECTOR_CAPACITY};
        // Row 0: a healthy user table, so a wrongly-tolerant decoder still answers a list.
        chunk.set_value(0, 0, std::uint32_t{catalog::FIRST_USER_OID + 1});
        chunk.set_value(1, 0, std::string_view{"alpha"});
        const char regular = catalog::relkind::regular;
        chunk.set_value(2, 0, std::string_view{&regular, 1});
        chunk.set_value(0, 1, std::uint32_t{catalog::FIRST_USER_OID + 2});
        if (which == null_cell_t::relname) {
            chunk.set_value(2, 1, std::string_view{&regular, 1});
            chunk.data[1].set_null(1, true);
        } else {
            chunk.set_value(1, 1, std::string_view{"beta"});
            chunk.data[2].set_null(1, true);
        }
        chunk.set_cardinality(2);
        return cursor::make_cursor(resource, std::move(chunk));
    }

} // namespace

// BEFORE: a NULL relname row was silently skipped, hiding a real table.
TEST_CASE("integration::cpp::list_tables::a_null_relname_is_a_catalog_error_not_an_omission") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_pg_class_cursor_with_null(&resource, null_cell_t::relname);

    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    INFO("a pg_class row with NULL relname violates the schema; the read must refuse");
    REQUIRE(names.has_error());
}

// BEFORE: a NULL relkind row was accepted as a regular table.
TEST_CASE("integration::cpp::list_tables::a_null_relkind_is_a_catalog_error_not_a_table") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_pg_class_cursor_with_null(&resource, null_cell_t::relkind);

    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    INFO("a pg_class row with NULL relkind violates the schema; the read must refuse");
    REQUIRE(names.has_error());
}
