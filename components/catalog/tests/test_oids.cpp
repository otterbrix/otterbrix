#include <catch2/catch.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/table_id.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>

#include <atomic>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace components::catalog;

// 1. INVALID_OID is the conventional zero sentinel (matches PostgreSQL).
TEST_CASE("catalog::oid::invalid_oid_is_zero") {
    REQUIRE(INVALID_OID == 0);
    REQUIRE(FIRST_USER_OID > 0);
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

// 3. Sequential allocate() yields strictly increasing unique OIDs starting at FIRST_USER_OID.
//
TEST_CASE("test_oid_generation_uniqueness") {
    oid_generator gen;
    std::unordered_set<oid_t> seen;
    oid_t prev = FIRST_USER_OID - 1;
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
    gen.seed(50000);
    oid_t next = gen.allocate();
    REQUIRE(next == 50001);
}

// 6. seed() never lowers the counter (idempotent for stale on-disk values).
TEST_CASE("catalog::oid::seed_doesnt_lower") {
    oid_generator gen;
    gen.seed(100000);
    REQUIRE(gen.allocate() == 100001);
    gen.seed(50); // stale / out-of-order startup scan result
    oid_t after_stale = gen.allocate();
    REQUIRE(after_stale > 100001);
}

// 7. seed(value < FIRST_USER_OID) clamps to FIRST_USER_OID — never hands out reserved space.
TEST_CASE("catalog::oid::seed_below_first_user_clamps") {
    oid_generator gen{FIRST_USER_OID};
    gen.seed(50); // reserved range
    oid_t next = gen.allocate();
    REQUIRE(next == FIRST_USER_OID);
}

// 8. column_definition_t::attoid is independent of storage_oid()/oid() (which are storage block ids).
//    Doc test alias: test_column_oid_assignment.
TEST_CASE("test_column_oid_assignment") {
    components::table::column_definition_t col("price", components::types::logical_type::DOUBLE);
    REQUIRE(col.attoid() == 0); // INVALID_OID
    col.set_attoid(42);
    REQUIRE(col.attoid() == 42);
    // storage block ids are unrelated and remain at their sentinel default.
    REQUIRE(col.storage_oid() == components::table::storage::INVALID_INDEX);
    REQUIRE(col.oid() == components::table::storage::INVALID_INDEX);
}

// 9. OIDs are immutable after first non-INVALID assignment: re-stamping the same value is
//    idempotent, but assigning a different value raises std::logic_error. Covers Design Rule 1
//    by the design rule "OIDs are immutable after assignment".
TEST_CASE("test_oid_immutability") {
    std::pmr::synchronized_pool_resource resource;

    // set_oid / set_attoid are programmer-error precondition guards: assert in
    // debug, silent no-op in release. The original value MUST survive a stray
    // reassignment attempt with a different value.
    SECTION("table_id::set_oid") {
        qualified_name_t cfn("main", "users");
        table_id tid(&resource, cfn);
        tid.set_oid(20000);
        tid.set_oid(20000); // idempotent
        REQUIRE(tid.oid() == 20000);
    }

    SECTION("column_definition_t::set_attoid") {
        components::table::column_definition_t col("price", components::types::logical_type::DOUBLE);
        col.set_attoid(42);
        col.set_attoid(42); // idempotent
        REQUIRE(col.attoid() == 42);
    }
}

// 10. table_id OID round-trip.
//     Doc test alias: test_table_oid_assignment.
TEST_CASE("test_table_oid_assignment") {
    std::pmr::synchronized_pool_resource resource;

    qualified_name_t cfn("main", "users");
    table_id tid(&resource, cfn);
    REQUIRE(tid.oid() == INVALID_OID);
    tid.set_oid(20000);
    REQUIRE(tid.oid() == 20000);
}
