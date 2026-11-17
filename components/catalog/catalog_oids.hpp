#pragma once

#include <components/types/oid.hpp>

#include <atomic>
#include <cstdint>
#include <cstdlib>

namespace components::catalog {

    // PostgreSQL-style 32-bit Object Identifier (OID).
    // Distinct from components::table::column_definition_t::oid() (storage block id, uint64_t)
    // and components::oid::oid_t (12-byte BSON ObjectId). This OID is the catalog identity:
    // pg_class.oid, pg_attribute.attoid, pg_type.oid, pg_proc.oid, etc.
    using oid_t = types::oid_t;

    inline constexpr oid_t INVALID_OID{0};
    // OIDs below this are reserved for built-in / well-known catalog objects.
    // Allocated user objects start here. Mirrors PostgreSQL's FirstNormalObjectId.
    inline constexpr oid_t FIRST_USER_OID{16384};

    // True when `oid` identifies a built-in pg_catalog table (pg_class, pg_attribute,
    // pg_depend, ...). Catalog DML routes through the WAL-first append_pg_catalog_row /
    // delete_pg_catalog_rows path (operator_insert / operator_delete catalog branch)
    // rather than the user storage_append / storage_delete_rows path. Mirrors the
    // oid < FIRST_USER_OID partition used by manager_disk_t::pool_idx_for_oid.
    inline constexpr bool is_catalog_table(oid_t oid) noexcept { return oid != INVALID_OID && oid < FIRST_USER_OID; }

    // Objects PostgreSQL has are numbered as in PostgreSQL 18; otterbrix's own take 8000-9999, the range PostgreSQL
    // keeps for development and never ships a built-in in:
    //   8000-8049 scalar types, 8050-8099 their array types, 8100-8899 other types, 8900-8999 system tables,
    //   9000- system columns (pg_attribute.attoid, which PostgreSQL does not have).

    // System tables' columns are numbered from here in definition order; a new system column is only ever appended.
    inline constexpr oid_t FIRST_SYSTEM_ATTOID{9000};

    // Built-in pg_cast rows, numbered at bootstrap in (castsource, casttarget) order from here: the range PostgreSQL's
    // genbki numbers its own pg_cast rows in (10000-11999).
    inline constexpr oid_t FIRST_BUILTIN_CAST_OID{10000};

    // main's information_schema views
    inline constexpr oid_t FIRST_INFORMATION_SCHEMA_OID{12001};

    // Well-known OIDs — built-in objects assigned at bootstrap.
    namespace well_known_oid {
        // Default database (pg_database.oid), PostgreSQL's "postgres". Otterbrix has no cluster-vs-database split, so
        // a single default database "main" is bootstrapped at first start. Additional
        // databases get OIDs from oid_generator (>= FIRST_USER_OID) via ddl_create_database.
        inline constexpr oid_t main_database{5};
        inline constexpr const char* main_database_name = "main";

        // Namespaces (pg_namespace.oid). PostgreSQL assigns information_schema at initdb, from 12000 up.
        inline constexpr oid_t pg_catalog_namespace{11};
        inline constexpr oid_t public_namespace{2200};
        inline constexpr oid_t information_schema_namespace{12000};

        // System tables (pg_class.oid)
        inline constexpr oid_t pg_type_table{1247};
        inline constexpr oid_t pg_attribute_table{1249};
        inline constexpr oid_t pg_proc_table{1255};
        inline constexpr oid_t pg_class_table{1259};
        inline constexpr oid_t pg_database_table{1262};
        inline constexpr oid_t pg_sequence_table{2224};
        inline constexpr oid_t pg_cast_table{2605};
        inline constexpr oid_t pg_constraint_table{2606};
        inline constexpr oid_t pg_depend_table{2608};
        inline constexpr oid_t pg_index_table{2610};
        inline constexpr oid_t pg_namespace_table{2615};
        inline constexpr oid_t pg_rewrite_table{2618};
        inline constexpr oid_t pg_enum_table{3501};
        inline constexpr oid_t pg_range_table{3541};
        // A view in PostgreSQL, with no fixed oid.
        inline constexpr oid_t pg_settings_table{8900};
        inline constexpr oid_t pg_computed_column_table{8901};
        inline constexpr oid_t pg_rewrite_ref_table{8902};

        // Built-in scalar types (pg_type.oid). The names count bits: int8_type is the 1-byte type.
        inline constexpr oid_t boolean_type{16};
        inline constexpr oid_t blob_type{17};
        inline constexpr oid_t int64_type{20};
        inline constexpr oid_t int16_type{21};
        inline constexpr oid_t int32_type{23};
        inline constexpr oid_t string_type{25};
        inline constexpr oid_t oid_type{26};
        inline constexpr oid_t float32_type{700};
        inline constexpr oid_t float64_type{701};
        inline constexpr oid_t date_type{1082};
        inline constexpr oid_t time_type{1083};
        inline constexpr oid_t timestamp_type{1114};
        inline constexpr oid_t timestamp_tz_type{1184};
        inline constexpr oid_t interval_type{1186};
        inline constexpr oid_t time_tz_type{1266};
        inline constexpr oid_t numeric_type{1700};
        inline constexpr oid_t uuid_type{2950};
        inline constexpr oid_t int8_type{8000};
        inline constexpr oid_t uint8_type{8001};
        inline constexpr oid_t uint16_type{8002};
        inline constexpr oid_t uint32_type{8003};
        inline constexpr oid_t uint64_type{8004};
        inline constexpr oid_t int128_type{8005};
        inline constexpr oid_t uint128_type{8006};

        // Row types of the system catalogs PostgreSQL fixes them for (pg_type.dat); the others' are numbered from
        // FIRST_SYSTEM_ROW_TYPE, as PostgreSQL leaves them to initdb.
        inline constexpr oid_t pg_type_row_type{71};
        inline constexpr oid_t pg_attribute_row_type{75};
        inline constexpr oid_t pg_proc_row_type{81};
        inline constexpr oid_t pg_class_row_type{83};
        inline constexpr oid_t pg_type_row_array_type{210};
        inline constexpr oid_t pg_attribute_row_array_type{270};
        inline constexpr oid_t pg_proc_row_array_type{272};
        inline constexpr oid_t pg_class_row_array_type{273};
        inline constexpr oid_t FIRST_SYSTEM_ROW_TYPE{8100};

        // Their array types (pg_type.typarray), "_" + the element's typname.
        inline constexpr oid_t boolean_array_type{1000};
        inline constexpr oid_t blob_array_type{1001};
        inline constexpr oid_t int16_array_type{1005};
        inline constexpr oid_t int32_array_type{1007};
        inline constexpr oid_t string_array_type{1009};
        inline constexpr oid_t int64_array_type{1016};
        inline constexpr oid_t float32_array_type{1021};
        inline constexpr oid_t float64_array_type{1022};
        inline constexpr oid_t oid_array_type{1028};
        inline constexpr oid_t timestamp_array_type{1115};
        inline constexpr oid_t date_array_type{1182};
        inline constexpr oid_t time_array_type{1183};
        inline constexpr oid_t timestamp_tz_array_type{1185};
        inline constexpr oid_t interval_array_type{1187};
        inline constexpr oid_t numeric_array_type{1231};
        inline constexpr oid_t time_tz_array_type{1270};
        inline constexpr oid_t uuid_array_type{2951};
        inline constexpr oid_t int8_array_type{8050};
        inline constexpr oid_t uint8_array_type{8051};
        inline constexpr oid_t uint16_array_type{8052};
        inline constexpr oid_t uint32_array_type{8053};
        inline constexpr oid_t uint64_array_type{8054};
        inline constexpr oid_t int128_array_type{8055};
        inline constexpr oid_t uint128_array_type{8056};

        // Built-in functions (pg_proc.oid) — subset.
        inline constexpr oid_t fn_count{101};
        inline constexpr oid_t fn_sum{102};
        inline constexpr oid_t fn_avg{103};
        inline constexpr oid_t fn_min{104};
        inline constexpr oid_t fn_max{105};
    } // namespace well_known_oid

    // Thread-safe monotonic OID allocator. Single global instance lives in manager_disk_t
    // (one disk actor → one writer); seed() rebases the counter from on-disk max+1 at startup.
    //
    // Overflow: an oid is 32 bits → 4 billion objects per process lifetime.  Restart
    // reseeds from max(oid)+1, so the bound is per-process, not persistent lifetime.
    // Wraparound is undetected: if a workload creates >2^32 objects between restarts,
    // OIDs collide silently.  Near-limit, allocate() aborts to prevent silent corruption
    // (see OID_HARD_LIMIT below).  Most installations restart at least monthly; 4B
    // allocations between restarts is not a realistic concern for typical workloads.
    static constexpr oid_t OID_HARD_LIMIT{0xFFFF0000u}; // ~4.29B - 64k headroom

    class oid_generator {
    public:
        explicit oid_generator(oid_t start = FIRST_USER_OID) noexcept
            : next_(static_cast<unsigned>(start)) {}

        // Allocate a fresh OID. Lock-free; safe under concurrent allocate() callers.
        // Aborts if the counter approaches wraparound — see OID_HARD_LIMIT.
        oid_t allocate() noexcept {
            const oid_t oid{next_.fetch_add(1, std::memory_order_relaxed)};
            if (oid >= OID_HARD_LIMIT) [[unlikely]] {
                // OID space exhausted — cannot safely continue.
                // Restart the process to reseed from the on-disk max.
                std::abort();
            }
            return oid;
        }

        // Reset the counter to high_water + 1 (called at startup after scanning pg_class /
        // pg_attribute / pg_type / pg_proc / pg_constraint to find the highest used OID).
        // Idempotent: never lowers the counter.
        void seed(oid_t high_water) noexcept {
            const std::uint32_t target = high_water < FIRST_USER_OID ? static_cast<unsigned>(FIRST_USER_OID)
                                                                     : static_cast<unsigned>(high_water) + 1;
            std::uint32_t current = next_.load(std::memory_order_relaxed);
            while (current < target && !next_.compare_exchange_weak(current, target, std::memory_order_relaxed)) {
                // CAS retry; another thread may have advanced the counter further already.
            }
        }

        // For inspection / tests. Not for live allocation.
        oid_t peek() const noexcept { return oid_t{next_.load(std::memory_order_relaxed)}; }

    private:
        std::atomic<std::uint32_t> next_;
    };

    // Every relation of main.pg_catalog is the system's for a user statement: DML, DROP, ALTER, CREATE INDEX and
    // CREATE in the schema are refused by the schema, not by an oid range.
    inline bool in_system_schema(oid_t namespace_oid) noexcept {
        return namespace_oid == well_known_oid::pg_catalog_namespace;
    }

} // namespace components::catalog
