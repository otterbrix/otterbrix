#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <integration/cpp/catalog_listing.hpp>

// ---------------------------------------------------------------------------
// Task B9. `listTables()` (integration/python/pyconnection) enumerates user
// tables by running kListTablesQuery over pg_class and decoding the cursor.
// It used to collapse `!cursor || cursor->is_error() || size()==0` into a
// single "return an empty list", so a FAILED query and an EMPTY DATABASE were
// indistinguishable to the Python caller — rule 6, silent degradation.
//
// The decode lives in cpp_otterbrix (integration/cpp/catalog_listing.cpp) so
// that the C++ suite can gate it; the pybind11 wrapper around it is a thin
// translation of the error channel into a Python exception and carries no
// filtering logic of its own.
//
// NOTE on the query itself: as of this branch pg_class is NOT resolvable as a
// FROM target — the bootstrap seeds no self-describing pg_class/pg_attribute
// rows for system tables, so kListTablesQuery errors out. That is a separate,
// larger defect (making system catalogs queryable from SQL); these tests are
// written so that they stay meaningful either way.
// ---------------------------------------------------------------------------

namespace {

    using namespace components;

    // A cursor carrying an engine error, as execute_sql returns for a failed query.
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

    // A hand-built pg_class projection: exactly the three columns kListTablesQuery
    // asks for, aliased the way the executor aliases them.
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

// RED before the fix: the decoder flattened an engine error into an empty list,
// so the caller could not tell "the catalog read failed" from "there are no
// tables". The error must survive as an error.
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

// A successful read of a catalog holding no user tables IS an empty list —
// the one meaning an empty list is allowed to carry.
TEST_CASE("integration::cpp::list_tables::empty_catalog_is_an_empty_list") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_pg_class_cursor(&resource, {});
    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    REQUIRE_FALSE(names.has_error());
    REQUIRE(names.value().empty());
}

// The two filters that pick user tables out of the pg_class projection:
// oid >= FIRST_USER_OID, and relkind == 'r'.
TEST_CASE("integration::cpp::list_tables::filters_system_rows_and_non_tables") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_pg_class_cursor(
        &resource,
        {
            {catalog::well_known_oid::pg_class_table, "pg_class", catalog::relkind::regular}, // system oid
            {catalog::FIRST_USER_OID + 1, "alpha", catalog::relkind::regular},                // kept
            {catalog::FIRST_USER_OID + 2, "alpha_idx", catalog::relkind::index},              // not a table
            {catalog::FIRST_USER_OID + 3, "alpha_view", catalog::relkind::view},              // not a table
            {catalog::FIRST_USER_OID + 4, "beta", catalog::relkind::regular},                 // kept
        });

    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    REQUIRE_FALSE(names.has_error());
    REQUIRE(names.value().size() == 2);
    REQUIRE(names.value()[0] == "alpha");
    REQUIRE(names.value()[1] == "beta");
}

// End-to-end invariant, written to outlive the pg_class defect: on a database
// that demonstrably holds two user tables, listTables' query must never report
// "no tables". Today it reports an error (pg_class does not resolve); once
// system catalogs become queryable it must report the two names. Reporting an
// empty list is wrong in both worlds, and that is exactly what it used to do.
TEST_CASE("integration::cpp::list_tables::two_tables_never_read_as_empty") {
    auto config = test_create_config(integration_fixture_path("list_tables/two_tables"));
    test_clear_directory(config);
    config.wal.on = false;
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
    // Both tables are really there.
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

    // A pg_class projection with one cell forced NULL — the shape a corrupt catalog row
    // takes. Both relname and relkind are NOT NULL in the schema, so a NULL there is not a
    // row to be filtered, it is a catalog that cannot be trusted.
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
        // Row 1: the corrupt row.
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

// ===========================================================================
// A NULL IN A NOT-NULL CATALOG COLUMN IS AN ERROR, NOT A ROW TO SKIP.
//
// relname is declared NOT NULL. A row without a name is a catalog defect, and OMITTING it
// hands the caller a list that silently misses a table that exists.
//
// BEFORE: the row was skipped and the list came back "successful" without it.
// ===========================================================================
TEST_CASE("integration::cpp::list_tables::a_null_relname_is_a_catalog_error_not_an_omission") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_pg_class_cursor_with_null(&resource, null_cell_t::relname);

    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    INFO("a pg_class row with NULL relname violates the schema; the read must refuse");
    REQUIRE(names.has_error());
}

// ===========================================================================
// THE SAME FOR relkind — AND "NULL MEANS REGULAR TABLE" IS THE WORSE HALF.
//
// relkind is declared NOT NULL. A row whose kind is NULL (or empty) used to be ACCEPTED as a
// regular table, so an index or view with a corrupted kind byte showed up in listTables.
//
// BEFORE: the corrupt row was listed as a table.
// ===========================================================================
TEST_CASE("integration::cpp::list_tables::a_null_relkind_is_a_catalog_error_not_a_table") {
    core::pmr::otterbrix_resource resource;
    auto cursor = make_pg_class_cursor_with_null(&resource, null_cell_t::relkind);

    auto names = otterbrix::user_table_names_from_pg_class(&resource, cursor);
    INFO("a pg_class row with NULL relkind violates the schema; the read must refuse");
    REQUIRE(names.has_error());
}
