#include <catch2/catch.hpp>

#include <services/dispatcher/versioned_plan_cache.hpp>

using namespace services::dispatcher;
using session_id_t = components::session::session_id_t;

namespace {
    resolved_data_t mk(std::size_t bytes) {
        resolved_data_t r;
        r.memory_bytes = bytes;
        return r;
    }
} // namespace

// 1. Empty cache: no entries, no memory, no active txns.
TEST_CASE("dispatcher::plan_cache::restart_empty") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    REQUIRE(cache.entry_count() == 0);
    REQUIRE(cache.version_count() == 0);
    REQUIRE(cache.total_memory_bytes() == 0);
    REQUIRE(cache.active_txn_count() == 0);
}

// 2. Probe before store — miss.
TEST_CASE("dispatcher::plan_cache::probe_misses_when_empty") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    REQUIRE(cache.probe(/*hash*/ 100, /*version*/ 5) == nullptr);
}

// 3. Store then probe — hit; bytes accounted.
TEST_CASE("dispatcher::plan_cache::first_query_populates") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    cache.store(/*hash*/ 100, /*version*/ 5, mk(1024));
    auto* hit = cache.probe(100, 5);
    REQUIRE(hit != nullptr);
    REQUIRE(hit->memory_bytes == 1024);
    REQUIRE(cache.entry_count() == 1);
    REQUIRE(cache.total_memory_bytes() == 1024);
}

// 4. Alias dedup: second store with the same key is a no-op (no double-counting).
TEST_CASE("dispatcher::plan_cache::alias_no_ddl_same_data") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    cache.store(100, 5, mk(2000));
    cache.store(100, 5, mk(99999)); // alias — must not replace, must not double-count
    REQUIRE(cache.entry_count() == 1);
    REQUIRE(cache.total_memory_bytes() == 2000);
}

// 5. DDL bumps catalog_version → store under new version creates a separate entry.
TEST_CASE("dispatcher::plan_cache::alias_ddl_creates_new_data") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    cache.store(100, 5, mk(1000));
    cache.store(100, 6, mk(1500)); // post-DDL version — distinct entry
    REQUIRE(cache.entry_count() == 2);
    REQUIRE(cache.version_count() == 2);
    REQUIRE(cache.total_memory_bytes() == 2500);
    REQUIRE(cache.has_entry(100, 5));
    REQUIRE(cache.has_entry(100, 6));
}

// 6. GC removes entries from versions older than min(active_txns_) when nothing pins them.
TEST_CASE("dispatcher::plan_cache::gc_removes_old") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    cache.store(100, 5, mk(1000));
    cache.store(101, 6, mk(1000));
    // No pinned sessions → min_active_version() == max → ALL versions are < floor → gc'able.
    cache.gc();
    REQUIRE(cache.entry_count() == 0);
    REQUIRE(cache.total_memory_bytes() == 0);
}

// 7. GC preserves entries pinned by an active txn.
TEST_CASE("dispatcher::plan_cache::gc_keeps_active") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    cache.store(100, 5, mk(1000));
    session_id_t s1{};
    cache.pin_version(s1, 5);
    cache.gc();
    REQUIRE(cache.entry_count() == 1); // s1 still holds it
    cache.unpin_version(s1);
    cache.gc();
    REQUIRE(cache.entry_count() == 0); // released, gone
}

// 8. Long-running txn at older version keeps that snapshot alive even as DDL advances.
TEST_CASE("dispatcher::plan_cache::long_txn_holds_version") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    session_id_t s_long{};
    cache.pin_version(s_long, 5);
    cache.store(100, 5, mk(1000));

    session_id_t s_short{};
    cache.pin_version(s_short, 7);
    cache.store(101, 7, mk(1000));

    // Short session ends — its v=7 entries can go. v=5 still pinned by s_long.
    cache.unpin_version(s_short);
    cache.gc();
    REQUIRE(cache.has_entry(100, 5)); // long-txn snapshot alive
    REQUIRE_FALSE(cache.has_entry(101, 7));
}

// 9. Memory soft limit triggers eviction (lazy strategy).
TEST_CASE("dispatcher::plan_cache::memory_limit_evicts") {
    plan_cache_config_t cfg;
    cfg.max_memory_bytes = 1500; // very tight
    cfg.gc_strategy = gc_strategy_t::lazy;
    versioned_plan_cache_t cache(std::pmr::get_default_resource(), cfg);

    // No pinned sessions → all entries are evictable.
    cache.store(100, 5, mk(1000));
    cache.store(101, 6, mk(1000));     // total 2000, over limit → eviction kicks in
    REQUIRE(cache.total_memory_bytes() <= cfg.max_memory_bytes);
}

// 10. Active session blocks eviction even when over limit (correctness > memory).
TEST_CASE("dispatcher::plan_cache::eviction_does_not_evict_active") {
    plan_cache_config_t cfg;
    cfg.max_memory_bytes = 500;
    cfg.gc_strategy = gc_strategy_t::lazy;
    versioned_plan_cache_t cache(std::pmr::get_default_resource(), cfg);
    session_id_t s{};
    cache.pin_version(s, 5);
    cache.store(100, 5, mk(2000));
    // Even though we're over limit, the entry is pinned and must stay.
    REQUIRE(cache.has_entry(100, 5));
}

// 11. Concurrent txns at distinct versions both see their own snapshots.
TEST_CASE("dispatcher::plan_cache::concurrent_txns_snapshots") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    session_id_t s1{}, s2{};
    cache.pin_version(s1, 5);
    cache.pin_version(s2, 7);
    cache.store(100, 5, mk(1000));
    cache.store(100, 7, mk(1500));
    REQUIRE(cache.has_entry(100, 5));
    REQUIRE(cache.has_entry(100, 7));
    REQUIRE(cache.active_txn_count() == 2);
}

// 12. clear() drops all entries; pins remain (next store re-pins refcount).
TEST_CASE("dispatcher::plan_cache::clear_drops_all_keeps_pins") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    session_id_t s{};
    cache.pin_version(s, 5);
    cache.store(100, 5, mk(1000));
    cache.clear();
    REQUIRE(cache.entry_count() == 0);
    REQUIRE(cache.total_memory_bytes() == 0);
    REQUIRE(cache.active_txn_count() == 1); // pin survives — used after invalidation overflow
}

// 13. every_commit GC strategy collects on unpin, not lazy.
TEST_CASE("dispatcher::plan_cache::gc_every_commit_strategy") {
    plan_cache_config_t cfg;
    cfg.gc_strategy = gc_strategy_t::every_commit;
    versioned_plan_cache_t cache(std::pmr::get_default_resource(), cfg);
    session_id_t s{};
    cache.pin_version(s, 5);
    cache.store(100, 5, mk(1000));
    cache.unpin_version(s); // automatic gc here
    REQUIRE(cache.entry_count() == 0);
}

// 14. Lazy strategy fires gc() when memory crosses soft ratio (before hitting hard cap).
TEST_CASE("dispatcher::plan_cache::gc_lazy_triggers_at_soft_limit") {
    plan_cache_config_t cfg;
    cfg.max_memory_bytes = 1000;
    cfg.gc_soft_limit_ratio = 0.5f; // soft threshold = 500
    cfg.gc_strategy = gc_strategy_t::lazy;
    versioned_plan_cache_t cache(std::pmr::get_default_resource(), cfg);

    // 600 bytes crosses the soft threshold (500). No active sessions, so the gc()
    // pass triggered by store() reclaims the just-stored entry.
    cache.store(100, 5, mk(600));
    REQUIRE(cache.total_memory_bytes() == 0);
    REQUIRE(cache.entry_count() == 0);
}

// 15. Periodic strategy: gc() fires every N store/unpin operations.
TEST_CASE("dispatcher::plan_cache::gc_periodic_strategy") {
    plan_cache_config_t cfg;
    cfg.gc_strategy = gc_strategy_t::periodic;
    cfg.periodic_gc_ops_interval = 3;
    cfg.max_memory_bytes = 1ull << 30; // big — periodic shouldn't depend on memory
    versioned_plan_cache_t cache(std::pmr::get_default_resource(), cfg);

    cache.store(100, 5, mk(100));
    cache.store(101, 6, mk(100));
    REQUIRE(cache.entry_count() == 2); // only 2 ops, periodic gc not yet
    cache.store(102, 7, mk(100));      // 3rd op → gc fires; no pins → all swept
    REQUIRE(cache.entry_count() == 0);
}

// 16. version_count limit triggers eviction even when memory is fine.
TEST_CASE("dispatcher::plan_cache::max_versions_evicts") {
    plan_cache_config_t cfg;
    cfg.max_data_versions = 2;
    cfg.gc_strategy = gc_strategy_t::lazy;
    versioned_plan_cache_t cache(std::pmr::get_default_resource(), cfg);

    cache.store(1, 5, mk(10));
    cache.store(1, 6, mk(10));
    cache.store(1, 7, mk(10)); // 3 versions, over limit → oldest evicted
    REQUIRE(cache.version_count() <= 2);
}

// ===== V4 per-name probe/store tests (task #25) =====
// Validates the object-keyed APIs added in task #23. Each kind of object (table,
// function, type, namespace) exercises probe/store, alias dedup, and memory
// accounting. GC and pinning are covered by the plan-hash bucket tests above —
// the named buckets share the same machinery via shared helpers.

namespace {
    // Table builder with a known memory_bytes() result for predictable assertions.
    resolved_table_t mk_table(components::catalog::oid_t oid,
                                components::catalog::oid_t ns_oid,
                                std::string name) {
        resolved_table_t t;
        t.oid = oid;
        t.namespace_oid = ns_oid;
        t.name = std::move(name);
        return t;
    }

    resolved_function_t mk_function(components::catalog::oid_t oid,
                                      std::string name,
                                      std::vector<components::catalog::oid_t> args,
                                      components::catalog::oid_t ret) {
        resolved_function_t f;
        f.oid = oid;
        f.name = std::move(name);
        f.arg_type_oids = std::move(args);
        (void)ret; // resolved_function_t no longer carries return_type_oid (V4: use signature.output_types).
        return f;
    }
} // namespace

// 17. probe_table miss returns nullptr.
TEST_CASE("dispatcher::plan_cache::probe_table_miss_returns_null") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    REQUIRE(cache.probe_table(/*ns*/ 16384, "users", /*version*/ 5) == nullptr);
}

// 18. probe_table after store_table returns the payload by value-equality.
TEST_CASE("dispatcher::plan_cache::probe_table_hit_returns_payload") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    auto t = mk_table(16385, 16384, "users");
    const std::size_t expected_bytes = t.memory_bytes();
    cache.store_table(16384, "users", 5, std::move(t));
    auto* hit = cache.probe_table(16384, "users", 5);
    REQUIRE(hit != nullptr);
    REQUIRE(hit->oid == 16385);
    REQUIRE(hit->namespace_oid == 16384);
    REQUIRE(hit->name == "users");
    REQUIRE(cache.total_memory_bytes() == expected_bytes);
}

// 19. Two store_table calls with same (namespace_oid, name, version) — alias dedup,
// second is a no-op, memory only counted once.
TEST_CASE("dispatcher::plan_cache::store_table_alias_dedup_same_version") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    auto t1 = mk_table(16385, 16384, "users");
    const std::size_t bytes1 = t1.memory_bytes();
    cache.store_table(16384, "users", 5, std::move(t1));
    cache.store_table(16384, "users", 5, mk_table(99999, 16384, "users")); // alias — wins=first
    auto* hit = cache.probe_table(16384, "users", 5);
    REQUIRE(hit != nullptr);
    REQUIRE(hit->oid == 16385); // first store wins
    REQUIRE(cache.total_memory_bytes() == bytes1);
}

// 20. Same name in different namespaces produce distinct cache keys.
TEST_CASE("dispatcher::plan_cache::probe_table_namespace_keyed") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    cache.store_table(16384, "users", 5, mk_table(100, 16384, "users"));
    cache.store_table(16385, "users", 5, mk_table(200, 16385, "users"));
    auto* a = cache.probe_table(16384, "users", 5);
    auto* b = cache.probe_table(16385, "users", 5);
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    REQUIRE(a->oid == 100);
    REQUIRE(b->oid == 200);
}

// 21. probe_function with arg_type_oids — exact arg signature match required.
TEST_CASE("dispatcher::plan_cache::probe_function_with_arg_types") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    std::vector<components::catalog::oid_t> args_int_int{23, 23};
    std::vector<components::catalog::oid_t> args_text_text{25, 25};

    cache.store_function(">", args_int_int, 5, mk_function(101, ">", args_int_int, 16));
    cache.store_function(">", args_text_text, 5, mk_function(102, ">", args_text_text, 16));

    auto* gt_int = cache.probe_function(">", args_int_int, 5);
    auto* gt_text = cache.probe_function(">", args_text_text, 5);
    REQUIRE(gt_int != nullptr);
    REQUIRE(gt_text != nullptr);
    REQUIRE(gt_int->oid == 101);
    REQUIRE(gt_text->oid == 102);

    // Wrong arg signature → miss.
    std::vector<components::catalog::oid_t> args_int_text{23, 25};
    REQUIRE(cache.probe_function(">", args_int_text, 5) == nullptr);
}

// 22. probe_namespace + store_namespace round-trip.
TEST_CASE("dispatcher::plan_cache::probe_namespace_round_trip") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    REQUIRE(cache.probe_namespace("public", 5) == nullptr);
    resolved_namespace_t ns;
    ns.oid = 2;
    ns.name = "public";
    cache.store_namespace("public", 5, std::move(ns));
    auto* hit = cache.probe_namespace("public", 5);
    REQUIRE(hit != nullptr);
    REQUIRE(hit->oid == 2);
}

// 23. probe_type round-trip with namespace key.
TEST_CASE("dispatcher::plan_cache::probe_type_round_trip") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    resolved_type_t t;
    t.oid = 1000;
    t.namespace_oid = 16384;
    t.name = "money";
    t.typdefspec = "scale=2,precision=18";
    cache.store_type(16384, "money", 5, std::move(t));
    auto* hit = cache.probe_type(16384, "money", 5);
    REQUIRE(hit != nullptr);
    REQUIRE(hit->oid == 1000);
    REQUIRE(hit->name == "money");
    REQUIRE(hit->typdefspec == "scale=2,precision=18");
    REQUIRE(cache.probe_type(16385, "money", 5) == nullptr); // wrong ns
}

// 24. clear() drops named buckets too (not just plan-hash).
TEST_CASE("dispatcher::plan_cache::clear_drops_named_buckets") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    cache.store_table(16384, "users", 5, mk_table(1, 16384, "users"));
    cache.store_function("count", {}, 5, mk_function(101, "count", {}, 23));
    cache.store_type(16384, "money", 5,
                      resolved_type_t{1000, 16384, "money",
                                       components::types::complex_logical_type{}, ""});
    cache.store_namespace("public", 5, resolved_namespace_t{2, "public"});
    REQUIRE(cache.total_memory_bytes() > 0);
    cache.clear();
    REQUIRE(cache.total_memory_bytes() == 0);
    REQUIRE(cache.probe_table(16384, "users", 5) == nullptr);
    REQUIRE(cache.probe_function("count", {}, 5) == nullptr);
    REQUIRE(cache.probe_type(16384, "money", 5) == nullptr);
    REQUIRE(cache.probe_namespace("public", 5) == nullptr);
}

// 25. GC includes named-bucket entries — unpinned entries at any version get reaped.
TEST_CASE("dispatcher::plan_cache::gc_includes_named_buckets") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    cache.store_table(16384, "users", 5, mk_table(1, 16384, "users"));
    cache.store_function("count", {}, 5, mk_function(101, "count", {}, 23));
    REQUIRE(cache.total_memory_bytes() > 0);
    // No active txns → all entries unreferenced → gc reaps them.
    cache.gc();
    REQUIRE(cache.total_memory_bytes() == 0);
    REQUIRE(cache.probe_table(16384, "users", 5) == nullptr);
    REQUIRE(cache.probe_function("count", {}, 5) == nullptr);
}

// 26. Pinning a session bumps ref_count of named entries at that version, protecting
// them from gc.
TEST_CASE("dispatcher::plan_cache::pin_protects_named_entries") {
    versioned_plan_cache_t cache(std::pmr::get_default_resource());
    session_id_t s1{};
    cache.pin_version(s1, 5);
    cache.store_table(16384, "users", 5, mk_table(1, 16384, "users"));
    cache.gc(); // pinned → entries survive
    REQUIRE(cache.probe_table(16384, "users", 5) != nullptr);
    cache.unpin_version(s1);
    cache.gc(); // unpinned → reaped
    REQUIRE(cache.probe_table(16384, "users", 5) == nullptr);
}
