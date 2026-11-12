#include "information_schema.hpp"

#include "catalog_codes.hpp"
#include "ddl_metadata_builder.hpp"
#include "system_table_schemas.hpp"

#include <components/table/column_definition.hpp>

#include <iterator>
#include <utility>

namespace components::catalog {

    namespace {

        using types::logical_type;

        struct view_column_t {
            const char* name;
            logical_type type;
        };

        // A view: its output columns, the pg_catalog relations its body reads, and its body for one database.
        struct system_view_t {
            const char* name;
            std::vector<view_column_t> columns;
            std::vector<oid_t> reads;
            std::string (*body)(const std::string& catalog, oid_t database_oid);
        };

        // `text` as an SQL string literal.
        std::string quoted_literal(const std::string& text) {
            std::string out{"'"};
            for (const char character : text) {
                out += character;
                if (character == '\'') {
                    out += '\'';
                }
            }
            out += '\'';
            return out;
        }

        // The objects a view of the database of `database_oid` shows: the database's own and pg_catalog's.
        std::string of_database(const std::string& namespace_alias, oid_t database_oid) {
            return "(" + namespace_alias + ".nspdatabase = " + std::to_string(static_cast<unsigned>(database_oid)) +
                   " OR " + namespace_alias +
                   ".oid = " + std::to_string(static_cast<unsigned>(well_known_oid::pg_catalog_namespace)) + ")";
        }

        std::string schemata_body(const std::string& catalog, oid_t database_oid) {
            return "SELECT CAST(" + catalog +
                   " AS text) AS catalog_name, CAST(n.nspname AS text) AS schema_name, CAST(NULL AS text) AS "
                   "schema_owner, CAST(NULL AS text) AS default_character_set_catalog, CAST(NULL AS text) AS "
                   "default_character_set_schema, CAST(NULL AS text) AS default_character_set_name, CAST(NULL AS "
                   "text) AS sql_path FROM pg_catalog.pg_namespace n WHERE " +
                   of_database("n", database_oid);
        }

        // data_type spells a type as PostgreSQL 18's information_schema does: the SQL standard name of a built-in
        // type, ARRAY, a pg_catalog type's own name, or USER-DEFINED. column_default is NULL: pg_attribute keeps a
        // default encoded, not as the text PostgreSQL shows.
        std::string columns_body(const std::string& catalog, oid_t database_oid) {
            return "SELECT CAST(" + catalog +
                   " AS text) AS table_catalog, CAST(n.nspname AS text) AS table_schema, CAST(c.relname AS text) AS "
                   "table_name, CAST(a.attname AS text) AS column_name, CAST(a.attnum AS integer) AS "
                   "ordinal_position, CAST(NULL AS text) AS column_default, CAST(CASE WHEN a.attnotnull = true THEN "
                   "'NO' ELSE 'YES' END AS text) AS is_nullable, CAST(CASE t.oid WHEN 16 THEN 'boolean' WHEN 17 THEN "
                   "'bytea' WHEN 20 THEN 'bigint' WHEN 21 THEN 'smallint' WHEN 23 THEN 'integer' WHEN 25 THEN 'text' "
                   "WHEN 26 THEN 'oid' WHEN 700 THEN 'real' WHEN 701 THEN 'double precision' WHEN 1082 THEN 'date' "
                   "WHEN 1083 THEN 'time without time zone' WHEN 1114 THEN 'timestamp without time zone' WHEN 1184 "
                   "THEN 'timestamp with time zone' WHEN 1186 THEN 'interval' WHEN 1266 THEN 'time with time zone' "
                   "WHEN 1700 THEN 'numeric' WHEN 2950 THEN 'uuid' ELSE CASE WHEN t.typcategory = 'A' THEN 'ARRAY' "
                   "WHEN t.typnamespace = 11 THEN t.typname ELSE 'USER-DEFINED' END END AS text) AS data_type, "
                   "CAST(NULL AS integer) AS character_maximum_length, CAST(CASE t.oid WHEN 25 THEN 1073741824 ELSE "
                   "NULL END AS integer) AS character_octet_length, CAST(CASE t.oid WHEN 21 THEN 16 WHEN 23 THEN 32 "
                   "WHEN 20 THEN 64 WHEN 700 THEN 24 WHEN 701 THEN 53 WHEN 1700 THEN CASE WHEN a.atttypmod = -1 THEN "
                   "NULL ELSE (a.atttypmod - 4) / 65536 END ELSE NULL END AS integer) AS numeric_precision, CAST(CASE "
                   "t.oid WHEN 21 THEN 2 WHEN 23 THEN 2 WHEN 20 THEN 2 WHEN 700 THEN 2 WHEN 701 THEN 2 WHEN 1700 THEN "
                   "10 ELSE NULL END AS integer) AS numeric_precision_radix, CAST(CASE t.oid WHEN 21 THEN 0 WHEN 23 "
                   "THEN 0 WHEN 20 THEN 0 WHEN 1700 THEN CASE WHEN a.atttypmod = -1 THEN NULL ELSE (a.atttypmod - 4) "
                   "% 65536 END ELSE NULL END AS integer) AS numeric_scale, CAST(CASE t.oid WHEN 1082 THEN 0 WHEN "
                   "1083 THEN 6 WHEN 1114 THEN 6 WHEN 1184 THEN 6 WHEN 1186 THEN 6 WHEN 1266 THEN 6 ELSE NULL END AS "
                   "integer) AS datetime_precision, CAST(NULL AS text) AS interval_type, CAST(NULL AS integer) AS "
                   "interval_precision, CAST(NULL AS text) AS character_set_catalog, CAST(NULL AS text) AS "
                   "character_set_schema, CAST(NULL AS text) AS character_set_name, CAST(NULL AS text) AS "
                   "collation_catalog, CAST(NULL AS text) AS collation_schema, CAST(NULL AS text) AS collation_name, "
                   "CAST(NULL AS text) AS domain_catalog, CAST(NULL AS text) AS domain_schema, CAST(NULL AS text) AS "
                   "domain_name, CAST(" +
                   catalog +
                   " AS text) AS udt_catalog, CAST(tn.nspname AS text) AS udt_schema, CAST(t.typname AS text) AS "
                   "udt_name, CAST(NULL AS text) AS scope_catalog, CAST(NULL AS text) AS scope_schema, CAST(NULL AS "
                   "text) AS scope_name, CAST(NULL AS integer) AS maximum_cardinality, CAST(a.attnum AS text) AS "
                   "dtd_identifier, CAST('NO' AS text) AS is_self_referencing, CAST('NO' AS text) AS is_identity, "
                   "CAST(NULL AS text) AS identity_generation, CAST(NULL AS text) AS identity_start, CAST(NULL AS "
                   "text) AS identity_increment, CAST(NULL AS text) AS identity_maximum, CAST(NULL AS text) AS "
                   "identity_minimum, CAST('NO' AS text) AS identity_cycle, CAST('NEVER' AS text) AS is_generated, "
                   "CAST(NULL AS text) AS generation_expression, CAST(CASE WHEN c.relkind = 'r' THEN 'YES' ELSE 'NO' "
                   "END AS text) AS is_updatable FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid "
                   "= a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace JOIN pg_catalog.pg_type t "
                   "ON t.oid = a.atttypid JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace WHERE a.attnum "
                   "> 0 AND a.attisdropped = false AND c.relkind IN ('r', 'v', 'm', 'f', 'p') AND " +
                   of_database("n", database_oid);
        }

        const std::vector<system_view_t>& system_views() {
            static const std::vector<system_view_t> views{
                {"schemata",
                 {{"catalog_name", logical_type::TEXT},
                  {"schema_name", logical_type::TEXT},
                  {"schema_owner", logical_type::TEXT},
                  {"default_character_set_catalog", logical_type::TEXT},
                  {"default_character_set_schema", logical_type::TEXT},
                  {"default_character_set_name", logical_type::TEXT},
                  {"sql_path", logical_type::TEXT}},
                 {well_known_oid::pg_namespace_table},
                 &schemata_body},
                {"columns",
                 {{"table_catalog", logical_type::TEXT},
                  {"table_schema", logical_type::TEXT},
                  {"table_name", logical_type::TEXT},
                  {"column_name", logical_type::TEXT},
                  {"ordinal_position", logical_type::INTEGER},
                  {"column_default", logical_type::TEXT},
                  {"is_nullable", logical_type::TEXT},
                  {"data_type", logical_type::TEXT},
                  {"character_maximum_length", logical_type::INTEGER},
                  {"character_octet_length", logical_type::INTEGER},
                  {"numeric_precision", logical_type::INTEGER},
                  {"numeric_precision_radix", logical_type::INTEGER},
                  {"numeric_scale", logical_type::INTEGER},
                  {"datetime_precision", logical_type::INTEGER},
                  {"interval_type", logical_type::TEXT},
                  {"interval_precision", logical_type::INTEGER},
                  {"character_set_catalog", logical_type::TEXT},
                  {"character_set_schema", logical_type::TEXT},
                  {"character_set_name", logical_type::TEXT},
                  {"collation_catalog", logical_type::TEXT},
                  {"collation_schema", logical_type::TEXT},
                  {"collation_name", logical_type::TEXT},
                  {"domain_catalog", logical_type::TEXT},
                  {"domain_schema", logical_type::TEXT},
                  {"domain_name", logical_type::TEXT},
                  {"udt_catalog", logical_type::TEXT},
                  {"udt_schema", logical_type::TEXT},
                  {"udt_name", logical_type::TEXT},
                  {"scope_catalog", logical_type::TEXT},
                  {"scope_schema", logical_type::TEXT},
                  {"scope_name", logical_type::TEXT},
                  {"maximum_cardinality", logical_type::INTEGER},
                  {"dtd_identifier", logical_type::TEXT},
                  {"is_self_referencing", logical_type::TEXT},
                  {"is_identity", logical_type::TEXT},
                  {"identity_generation", logical_type::TEXT},
                  {"identity_start", logical_type::TEXT},
                  {"identity_increment", logical_type::TEXT},
                  {"identity_maximum", logical_type::TEXT},
                  {"identity_minimum", logical_type::TEXT},
                  {"identity_cycle", logical_type::TEXT},
                  {"is_generated", logical_type::TEXT},
                  {"generation_expression", logical_type::TEXT},
                  {"is_updatable", logical_type::TEXT}},
                 {well_known_oid::pg_attribute_table,
                  well_known_oid::pg_class_table,
                  well_known_oid::pg_namespace_table,
                  well_known_oid::pg_type_table},
                 &columns_body},
            };
            return views;
        }

        // The view, its row type and array type, and its rule, besides one attoid per column.
        constexpr std::size_t oids_per_view = 4;

    } // namespace

    std::size_t information_schema_oid_demand() {
        std::size_t demand = 0;
        for (const auto& view : system_views()) {
            demand += oids_per_view + view.columns.size();
        }
        return demand;
    }

    std::vector<catalog_write_t> build_information_schema_writes(std::pmr::memory_resource* resource,
                                                                 const std::string& database,
                                                                 oid_t database_oid,
                                                                 oid_t namespace_oid,
                                                                 oid_batch_t& oid_batch) {
        const std::string catalog = quoted_literal(database);
        std::vector<catalog_write_t> writes;
        for (const auto& view : system_views()) {
            const oid_t view_oid = oid_batch.allocate();
            const oid_t row_type_oid = oid_batch.allocate();
            const oid_t row_array_oid = oid_batch.allocate();
            const oid_t rule_oid = oid_batch.allocate();
            const std::string name{view.name};
            writes.push_back(
                build_view_class_row(resource, view_oid, name, namespace_oid, relkind::view, row_type_oid));
            auto row_type = build_row_type_writes(resource, name, namespace_oid, view_oid, row_type_oid, row_array_oid);
            writes.insert(writes.end(),
                          std::make_move_iterator(row_type.begin()),
                          std::make_move_iterator(row_type.end()));

            std::vector<table::column_definition_t> columns;
            columns.reserve(view.columns.size());
            for (const auto& column : view.columns) {
                columns.emplace_back(column.name, types::complex_logical_type{column.type});
            }
            // Its names are pg_catalog's, which is pinned: bound, but no pg_depend edge, as PostgreSQL records none.
            std::vector<view_binding_t> bindings;
            for (const auto read : view.reads) {
                view_binding_t binding;
                binding.refkind = view_refkind::relation;
                binding.schema = core::schema_t{std::string{"pg_catalog"}};
                binding.relname = core::relname_t{std::string{find_system_table(read)->name}};
                binding.refobjid = read;
                bindings.push_back(std::move(binding));
            }
            auto body = build_view_body_writes(resource,
                                               view_oid,
                                               columns,
                                               bindings,
                                               {},
                                               namespace_oid,
                                               rule_oid,
                                               name,
                                               view.body(catalog, database_oid),
                                               relkind::view,
                                               oid_batch);
            writes.insert(writes.end(), std::make_move_iterator(body.begin()), std::make_move_iterator(body.end()));
        }
        return writes;
    }

} // namespace components::catalog
