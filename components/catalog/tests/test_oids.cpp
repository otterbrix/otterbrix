#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/pmr.hpp>

#include <atomic>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace components::catalog;

// 1. INVALID_OID is the conventional zero sentinel (matches PostgreSQL).
TEST_CASE("catalog::oid::invalid_oid_is_zero") {
    REQUIRE(INVALID_OID == oid_t{0});
    REQUIRE(FIRST_USER_OID > INVALID_OID);
    REQUIRE(FIRST_USER_OID > well_known_oid::pg_catalog_namespace);
    REQUIRE(FIRST_USER_OID > well_known_oid::fn_max);
}

// 2. Well-known OIDs are stable + distinct (ODR check).
TEST_CASE("catalog::oid::well_known_distinct_and_stable") {
    std::unordered_set<oid_t> ids = {
        well_known_oid::pg_catalog_namespace,
        well_known_oid::public_namespace,
        well_known_oid::information_schema_namespace,
        well_known_oid::pg_namespace_table,
        well_known_oid::pg_class_table,
        well_known_oid::pg_attribute_table,
        well_known_oid::pg_type_table,
        well_known_oid::pg_proc_table,
        well_known_oid::pg_depend_table,
        well_known_oid::pg_constraint_table,
        well_known_oid::pg_index_table,
        well_known_oid::pg_computed_column_table,
        well_known_oid::int64_type,
        well_known_oid::string_type,
        well_known_oid::fn_count,
    };
    // 15 distinct values were inserted.
    REQUIRE(ids.size() == 15);
}

// Objects PostgreSQL 18 has carry its oids (pg_type.dat, pg_class.h, pg_namespace.dat, pg_database.dat).
TEST_CASE("catalog::oid::postgresql_objects_have_postgresql_oids") {
    REQUIRE(well_known_oid::main_database == oid_t{5});
    REQUIRE(well_known_oid::pg_catalog_namespace == oid_t{11});
    REQUIRE(well_known_oid::public_namespace == oid_t{2200});

    REQUIRE(well_known_oid::pg_type_table == oid_t{1247});
    REQUIRE(well_known_oid::pg_attribute_table == oid_t{1249});
    REQUIRE(well_known_oid::pg_proc_table == oid_t{1255});
    REQUIRE(well_known_oid::pg_class_table == oid_t{1259});
    REQUIRE(well_known_oid::pg_database_table == oid_t{1262});
    REQUIRE(well_known_oid::pg_sequence_table == oid_t{2224});
    REQUIRE(well_known_oid::pg_cast_table == oid_t{2605});
    REQUIRE(well_known_oid::pg_constraint_table == oid_t{2606});
    REQUIRE(well_known_oid::pg_depend_table == oid_t{2608});
    REQUIRE(well_known_oid::pg_index_table == oid_t{2610});
    REQUIRE(well_known_oid::pg_namespace_table == oid_t{2615});
    REQUIRE(well_known_oid::pg_rewrite_table == oid_t{2618});

    REQUIRE(well_known_oid::boolean_type == oid_t{16});
    REQUIRE(well_known_oid::blob_type == oid_t{17});
    REQUIRE(well_known_oid::int64_type == oid_t{20});
    REQUIRE(well_known_oid::int16_type == oid_t{21});
    REQUIRE(well_known_oid::int32_type == oid_t{23});
    REQUIRE(well_known_oid::string_type == oid_t{25});
    REQUIRE(well_known_oid::oid_type == oid_t{26});
    REQUIRE(well_known_oid::float32_type == oid_t{700});
    REQUIRE(well_known_oid::float64_type == oid_t{701});
    REQUIRE(well_known_oid::date_type == oid_t{1082});
    REQUIRE(well_known_oid::time_type == oid_t{1083});
    REQUIRE(well_known_oid::timestamp_type == oid_t{1114});
    REQUIRE(well_known_oid::timestamp_tz_type == oid_t{1184});
    REQUIRE(well_known_oid::interval_type == oid_t{1186});
    REQUIRE(well_known_oid::time_tz_type == oid_t{1266});
    REQUIRE(well_known_oid::numeric_type == oid_t{1700});
    REQUIRE(well_known_oid::uuid_type == oid_t{2950});
}

// Objects PostgreSQL lacks sit in 8000-9999, which no PostgreSQL release assigns.
TEST_CASE("catalog::oid::otterbrix_objects_sit_in_the_development_range") {
    for (const oid_t oid : {well_known_oid::int8_type,
                            well_known_oid::uint8_type,
                            well_known_oid::uint16_type,
                            well_known_oid::uint32_type,
                            well_known_oid::uint64_type,
                            well_known_oid::int128_type,
                            well_known_oid::uint128_type,
                            well_known_oid::pg_settings_table,
                            well_known_oid::pg_computed_column_table,
                            well_known_oid::pg_rewrite_ref_table,
                            well_known_oid::FIRST_SYSTEM_ROW_TYPE,
                            FIRST_SYSTEM_ATTOID}) {
        REQUIRE(oid >= oid_t{8000});
        REQUIRE(oid <= oid_t{9999});
    }
}

// 3. Sequential allocate() yields strictly increasing unique OIDs starting at FIRST_USER_OID.
TEST_CASE("test_oid_generation_uniqueness") {
    oid_generator gen;
    std::unordered_set<oid_t> seen;
    oid_t prev{static_cast<unsigned>(FIRST_USER_OID) - 1};
    for (int i = 0; i < 1000; i++) {
        oid_t id = gen.allocate();
        REQUIRE(id >= FIRST_USER_OID);
        REQUIRE(id > prev);
        REQUIRE(seen.insert(id).second);
        prev = id;
    }
}

// 4. Concurrent allocate() from multiple threads — every OID still unique (lock-free atomic).
TEST_CASE("catalog::oid::allocate_thread_safety") {
    oid_generator gen;
    constexpr std::size_t THREADS = 8;
    constexpr std::size_t PER_THREAD = 5000;

    std::vector<std::vector<oid_t>> per_thread(THREADS);
    std::vector<std::thread> workers;
    for (std::size_t t = 0; t < THREADS; t++) {
        workers.emplace_back([&, t]() {
            per_thread[t].reserve(PER_THREAD);
            for (std::size_t i = 0; i < PER_THREAD; i++) {
                per_thread[t].push_back(gen.allocate());
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }

    std::unordered_set<oid_t> all;
    for (auto& vec : per_thread) {
        for (auto id : vec) {
            REQUIRE(all.insert(id).second);
        }
    }
    REQUIRE(all.size() == THREADS * PER_THREAD);
}

// 5. seed(high_water) advances counter to high_water+1.
//    Doc test alias: test_oid_generator_restore (one of three covering seed semantics).
TEST_CASE("test_oid_generator_restore") {
    oid_generator gen;
    gen.seed(oid_t{50000});
    oid_t next = gen.allocate();
    REQUIRE(next == oid_t{50001});
}

// 6. seed() never lowers the counter (idempotent for stale on-disk values).
TEST_CASE("catalog::oid::seed_doesnt_lower") {
    oid_generator gen;
    gen.seed(oid_t{100000});
    REQUIRE(gen.allocate() == oid_t{100001});
    gen.seed(oid_t{50}); // stale / out-of-order startup scan result
    oid_t after_stale = gen.allocate();
    REQUIRE(after_stale > oid_t{100001});
}

// 7. seed(value < FIRST_USER_OID) clamps to FIRST_USER_OID — never hands out reserved space.
TEST_CASE("catalog::oid::seed_below_first_user_clamps") {
    oid_generator gen{FIRST_USER_OID};
    gen.seed(oid_t{50}); // reserved range
    oid_t next = gen.allocate();
    REQUIRE(next == FIRST_USER_OID);
}

// 8. column_definition_t::attoid is independent of storage_oid()/oid() (which are storage block ids).
//    Doc test alias: test_column_oid_assignment.
TEST_CASE("test_column_oid_assignment") {
    components::table::column_definition_t col("price", components::types::logical_type::DOUBLE);
    REQUIRE(col.attoid() == INVALID_OID);
    col.set_attoid(oid_t{42});
    REQUIRE(col.attoid() == oid_t{42});
    // storage block ids are unrelated and remain at their sentinel default.
    REQUIRE(col.storage_oid() == components::table::storage::INVALID_INDEX);
    REQUIRE(col.oid() == components::table::storage::INVALID_INDEX);
}

// 9. Re-stamping the same attoid is idempotent. column_definition_t::set_attoid
//    (components/table/column_definition.cpp) REFUSES rather than aborting on a different value, because its stamps
//    can arrive FROM DISK on the load/bootstrap path, where aborting would make the database unopenable.
TEST_CASE("test_oid_immutability") {
    SECTION("column_definition_t::set_attoid") {
        components::table::column_definition_t col("price", components::types::logical_type::DOUBLE);
        col.set_attoid(oid_t{42});
        col.set_attoid(oid_t{42}); // idempotent
        REQUIRE(col.attoid() == oid_t{42});
    }
}
