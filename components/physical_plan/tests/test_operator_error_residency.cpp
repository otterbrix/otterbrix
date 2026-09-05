#include <catch2/catch_test_macros.hpp>

#include <components/physical_plan/operators/operator.hpp>
#include <core/resource_tracer.hpp>

#include <cstddef>
#include <string_view>
#include <utility>

using components::operators::operator_t;
using components::operators::operator_type;

namespace {

    // 137 bytes, well past the small-string threshold, so the message is a real heap block a
    // resource_tracer_t can be asked about. Shaped like the DROP COLUMN refusal.
    constexpr std::string_view refusal =
        "ALTER TABLE: column \"parent_id\" of relation \"edb.child\" may not be dropped: "
        "FOREIGN KEY constraint \"fk_child_parent\" still depends on it.";

    core::error_t produced_on(std::pmr::memory_resource* producer) {
        return core::error_t{core::error_code_t::schema_error,
                             std::pmr::string{refusal.begin(), refusal.end(), producer}};
    }

} // namespace

// `error_ = error` (the copy ~98 of ~100 call sites use) reallocates onto the DEFAULT resource
// (std::pmr::string's copy ctor does not propagate the allocator); `error_ = std::move(error)` would keep
// the producer's allocator instead, leaving the operator holding — and later freeing into — an arena it
// never owned.
TEST_CASE("components::operators::set_error_leaves_the_message_on_the_operator_resource") {
    resource_tracer_t producer;
    resource_tracer_t owner;

    {
        operator_t op{&owner, log_t{}, operator_type::empty};
        REQUIRE_FALSE(op.has_error());

        core::error_t error = produced_on(&producer);
        const std::size_t produced = producer.live_allocations();
        REQUIRE(produced >= 1);
        REQUIRE(error.what.get_allocator().resource() == &producer);

        op.set_error(error);

        REQUIRE(op.has_error());
        CHECK(op.get_error().type == core::error_code_t::schema_error);
        CHECK(std::string_view{op.get_error().what} == refusal);

        // The whole contract in one line: the operator's error lives on the operator's arena.
        CHECK(op.get_error().what.get_allocator().resource() == &owner);

        // ... and it is a rebuild, not a hand-over: the caller's error_t is untouched, still
        // on its own arena, and holding a different buffer.
        CHECK(error.what.get_allocator().resource() == &producer);
        CHECK(std::string_view{error.what} == refusal);
        CHECK(error.what.data() != op.get_error().what.data());
        CHECK(producer.live_allocations() == produced);
        CHECK(owner.live_allocations() >= 1);
    }

    // Nothing was freed into the wrong arena on the way out.
    CHECK(producer.live_allocations() == 0);
    CHECK(owner.live_allocations() == 0);
}

// The same entry point reached with an rvalue. There is deliberately only ONE set_error
// overload: an &&-overload could not do better than the rebuild (it would have to keep the
// producer's allocator to be worth having), so it can only reintroduce the adoption.
TEST_CASE("components::operators::set_error_of_a_temporary_still_lands_on_the_operator_resource") {
    resource_tracer_t producer;
    resource_tracer_t owner;

    {
        operator_t op{&owner, log_t{}, operator_type::empty};

        op.set_error(produced_on(&producer));

        REQUIRE(op.has_error());
        CHECK(std::string_view{op.get_error().what} == refusal);
        CHECK(op.get_error().what.get_allocator().resource() == &owner);

        // The temporary is gone; if the operator had adopted its buffer, the producer would
        // still be holding a live block here.
        CHECK(producer.live_allocations() == 0);
    }

    CHECK(producer.live_allocations() == 0);
    CHECK(owner.live_allocations() == 0);
}

// An operator that never failed must not be made to allocate: no_error() carries no message,
// so there is nothing to rebuild and nothing to put anywhere.
TEST_CASE("components::operators::set_error_of_no_error_allocates_nothing") {
    resource_tracer_t owner;

    {
        operator_t op{&owner, log_t{}, operator_type::empty};
        op.set_error(core::error_t::no_error());
        CHECK_FALSE(op.has_error());
        CHECK(owner.live_allocations() == 0);
    }

    CHECK(owner.live_allocations() == 0);
}
