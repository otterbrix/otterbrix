#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>

namespace components::catalog {

    // PostgreSQL-style 32-bit Object Identifier (OID).
    // Distinct from components::table::column_definition_t::oid() (storage block id, uint64_t)
    // and components::oid::oid_t (12-byte BSON ObjectId). This OID is the catalog identity:
    // pg_class.oid, pg_attribute.attoid, pg_type.oid, pg_proc.oid, etc.
    using oid_t = std::uint32_t;

    inline constexpr oid_t INVALID_OID = 0;
    // OIDs below this are reserved for built-in / well-known catalog objects.
    // Allocated user objects start here. Mirrors PostgreSQL's FirstNormalObjectId.
    inline constexpr oid_t FIRST_USER_OID = 16384;

    // Well-known OIDs — built-in objects assigned at bootstrap.
    namespace well_known_oid {
        // Default database (pg_database.oid). Otterbrix has no cluster-vs-database split, so
        // a single default database "main" is bootstrapped at first start. Additional
        // databases get OIDs from oid_generator (>= FIRST_USER_OID) via ddl_create_database.
        inline constexpr oid_t main_database = 4;

        // Namespaces (pg_namespace.oid)
        inline constexpr oid_t pg_catalog_namespace = 1;
        inline constexpr oid_t public_namespace = 2;
        inline constexpr oid_t information_schema_namespace = 3;

        // System tables (pg_class.oid)
        inline constexpr oid_t pg_namespace_table = 10;
        inline constexpr oid_t pg_class_table = 11;
        inline constexpr oid_t pg_attribute_table = 12;
        inline constexpr oid_t pg_type_table = 13;
        inline constexpr oid_t pg_proc_table = 14;
        inline constexpr oid_t pg_depend_table = 15;
        inline constexpr oid_t pg_constraint_table = 16;
        inline constexpr oid_t pg_index_table = 17;
        inline constexpr oid_t pg_computed_column_table = 18;
        inline constexpr oid_t pg_database_table = 19;

        // Built-in scalar types (pg_type.oid) — subset, see catalog-migration doc §4.
        inline constexpr oid_t boolean_type = 20;
        inline constexpr oid_t int8_type = 21;
        inline constexpr oid_t int16_type = 22;
        inline constexpr oid_t int32_type = 23;
        inline constexpr oid_t int64_type = 24;
        inline constexpr oid_t float32_type = 25;
        inline constexpr oid_t float64_type = 26;
        inline constexpr oid_t string_type = 27;
        inline constexpr oid_t timestamp_type = 28;
        inline constexpr oid_t timestamp_tz_type = 29;
        inline constexpr oid_t date_type = 30;
        inline constexpr oid_t time_type = 31;
        inline constexpr oid_t time_tz_type = 32;
        inline constexpr oid_t interval_type = 33;
        inline constexpr oid_t blob_type = 34;
        inline constexpr oid_t numeric_type = 35;
        inline constexpr oid_t uuid_type = 36;

        // System tables added beyond the initial 10.
        inline constexpr oid_t pg_sequence_table = 37;
        inline constexpr oid_t pg_rewrite_table = 38;
        inline constexpr oid_t pg_settings_table = 39;

        // Built-in functions (pg_proc.oid) — subset.
        inline constexpr oid_t fn_count = 101;
        inline constexpr oid_t fn_sum = 102;
        inline constexpr oid_t fn_avg = 103;
        inline constexpr oid_t fn_min = 104;
        inline constexpr oid_t fn_max = 105;
    } // namespace well_known_oid

    // Thread-safe monotonic OID allocator. Single global instance lives in manager_disk_t
    // (one disk actor → one writer); seed() rebases the counter from on-disk max+1 at startup.
    //
    // Overflow: oid_t is uint32_t → 4 billion objects per process lifetime.  Restart
    // reseeds from max(oid)+1, so the bound is per-process, not persistent lifetime.
    // Wraparound is undetected: if a workload creates >2^32 objects between restarts,
    // OIDs collide silently.  Near-limit, allocate() aborts to prevent silent corruption
    // (see OID_HARD_LIMIT below).  Most installations restart at least monthly; 4B
    // allocations between restarts is not a realistic concern for typical workloads.
    static constexpr oid_t OID_HARD_LIMIT = 0xFFFF0000u; // ~4.29B - 64k headroom

    class oid_generator {
    public:
        explicit oid_generator(oid_t start = FIRST_USER_OID) noexcept
            : next_(start) {}

        // Allocate a fresh OID. Lock-free; safe under concurrent allocate() callers.
        // Aborts if the counter approaches wraparound — see OID_HARD_LIMIT.
        oid_t allocate() noexcept {
            const oid_t oid = next_.fetch_add(1, std::memory_order_relaxed);
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
            const oid_t target = high_water < FIRST_USER_OID ? FIRST_USER_OID : high_water + 1;
            oid_t current = next_.load(std::memory_order_relaxed);
            while (current < target && !next_.compare_exchange_weak(current, target, std::memory_order_relaxed)) {
                // CAS retry; another thread may have advanced the counter further already.
            }
        }

        // For inspection / tests. Not for live allocation.
        oid_t peek() const noexcept { return next_.load(std::memory_order_relaxed); }

    private:
        std::atomic<oid_t> next_;
    };

} // namespace components::catalog
