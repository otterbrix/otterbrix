#include <catch2/catch.hpp>

#include <services/disk/invalidation_ring_buffer.hpp>

using namespace services::disk;

namespace {
    invalidation_event_t make_event(std::uint64_t version, components::catalog::oid_t oid) {
        invalidation_event_t e;
        e.version = version;
        e.kind = invalidation_kind::relation_dropped;
        e.object_oid = oid;
        e.parent_oid = components::catalog::INVALID_OID;
        return e;
    }
} // namespace

// 1. Empty ring — since(0) returns nothing, no overflow.
//    Doc test alias: test_ring_buffer_empty_after_restart.
TEST_CASE("test_ring_buffer_empty_after_restart") {
    invalidation_ring_buffer_t ring;
    auto snap = ring.since(0);
    REQUIRE(snap.events.empty());
    REQUIRE_FALSE(snap.overflow);
    REQUIRE(snap.latest_version == 0);
    REQUIRE(ring.latest_version() == 0);
}

// 2. Push 5 events, since(0) returns all 5 in order.
//    Doc test alias: test_ring_buffer_events_since.
TEST_CASE("test_ring_buffer_events_since") {
    invalidation_ring_buffer_t ring;
    for (std::uint64_t v = 1; v <= 5; v++) {
        ring.push(make_event(v, /*oid*/ static_cast<components::catalog::oid_t>(100 + v)));
    }
    auto snap = ring.since(0);
    REQUIRE_FALSE(snap.overflow);
    REQUIRE(snap.events.size() == 5);
    REQUIRE(snap.latest_version == 5);
    for (std::size_t i = 0; i < snap.events.size(); i++) {
        REQUIRE(snap.events[i].version == i + 1);
        REQUIRE(snap.events[i].object_oid == 101 + i);
    }
}

// 3. since(N) returns only events with version > N — incremental tail pull.
//    Doc test alias: test_ring_buffer_monotonic_version.
TEST_CASE("test_ring_buffer_monotonic_version") {
    invalidation_ring_buffer_t ring;
    for (std::uint64_t v = 1; v <= 10; v++) {
        ring.push(make_event(v, static_cast<components::catalog::oid_t>(v)));
    }
    auto snap = ring.since(7);
    REQUIRE_FALSE(snap.overflow);
    REQUIRE(snap.events.size() == 3);
    REQUIRE(snap.events[0].version == 8);
    REQUIRE(snap.events[2].version == 10);
    // Subsequent pull at latest_version returns nothing.
    auto snap2 = ring.since(snap.latest_version);
    REQUIRE(snap2.events.empty());
    REQUIRE(snap2.latest_version == 10);
}

// 4. Capacity is exactly CAPACITY — push CAPACITY events, since(0) still returns all.
//    Doc test alias: test_ring_buffer_capacity.
TEST_CASE("test_ring_buffer_capacity") {
    invalidation_ring_buffer_t ring;
    for (std::uint64_t v = 1; v <= invalidation_ring_buffer_t::CAPACITY; v++) {
        ring.push(make_event(v, static_cast<components::catalog::oid_t>(v)));
    }
    auto snap = ring.since(0);
    REQUIRE_FALSE(snap.overflow);
    REQUIRE(snap.events.size() == invalidation_ring_buffer_t::CAPACITY);
    REQUIRE(snap.latest_version == invalidation_ring_buffer_t::CAPACITY);
}

// 5. Overflow — push CAPACITY+1 events with consumer at since=0; oldest is overwritten,
//    overflow flag is set.
//    Doc test alias: test_ring_buffer_overflow.
TEST_CASE("test_ring_buffer_overflow") {
    invalidation_ring_buffer_t ring;
    for (std::uint64_t v = 1; v <= invalidation_ring_buffer_t::CAPACITY + 1; v++) {
        ring.push(make_event(v, static_cast<components::catalog::oid_t>(v)));
    }
    auto snap = ring.since(0);
    REQUIRE(snap.overflow);
    REQUIRE(snap.events.empty());
    REQUIRE(snap.latest_version == invalidation_ring_buffer_t::CAPACITY + 1);
}
