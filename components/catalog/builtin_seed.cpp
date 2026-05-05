#include "builtin_seed.hpp"
#include <components/catalog/catalog_oids.hpp>

namespace components::catalog {

    namespace wk = well_known_oid;

    ns_seed_row_t builtin_database_row() {
        return {wk::main_database, "main"};
    }

    std::vector<ns_seed_row_t> builtin_namespace_rows() {
        return {
            {wk::pg_catalog_namespace,          "pg_catalog"},
            {wk::public_namespace,              "public"},
            {wk::information_schema_namespace,  "information_schema"},
        };
    }

    std::vector<type_seed_row_t> builtin_type_rows() {
        return {
            // Canonical otterbrix names
            {wk::boolean_type,   "bool"},
            {wk::int8_type,      "int1"},
            {wk::int16_type,     "int16"},
            {wk::int32_type,     "int32"},
            {wk::int64_type,     "int64"},
            {wk::float32_type,   "float32"},
            {wk::float64_type,   "float64"},
            {wk::string_type,    "string"},
            {wk::timestamp_type, "timestamp"},
            {wk::date_type,      "date"},
            {wk::time_type,      "time"},
            {wk::blob_type,      "blob"},
            {wk::numeric_type,   "numeric"},
            {wk::uuid_type,      "uuid"},
            // PostgreSQL internal typnames
            {wk::int16_type,     "int2"},
            {wk::int32_type,     "int4"},
            {wk::int64_type,     "int8"},
            {wk::int64_type,     "int8_t"},
            {wk::float32_type,   "float4"},
            {wk::float64_type,   "float8"},
            {wk::string_type,    "text"},
            {wk::string_type,    "varchar"},
            {wk::string_type,    "bpchar"},
            {wk::string_type,    "name"},
            {wk::blob_type,      "bytea"},
            // SQL-facing user aliases
            {wk::boolean_type,   "boolean"},
            {wk::int8_type,      "tinyint"},
            {wk::int16_type,     "smallint"},
            {wk::int32_type,     "integer"},
            {wk::int32_type,     "int"},
            {wk::int64_type,     "bigint"},
            {wk::float64_type,   "double"},
            {wk::float64_type,   "double precision"},
            {wk::numeric_type,   "decimal"},
            // Timestamp variants
            {wk::timestamp_type, "timestamp_sec"},
            {wk::timestamp_type, "timestamp_ms"},
            {wk::timestamp_type, "timestamp_us"},
            {wk::timestamp_type, "timestamp_ns"},
        };
    }

    std::vector<proc_seed_row_t> builtin_proc_rows() {
        return {
            {wk::fn_count, "count"},
            {wk::fn_sum,   "sum"},
            {wk::fn_avg,   "avg"},
            {wk::fn_min,   "min"},
            {wk::fn_max,   "max"},
        };
    }

} // namespace components::catalog
