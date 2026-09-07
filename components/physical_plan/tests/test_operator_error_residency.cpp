#include <catch2/catch_test_macros.hpp>

#include <components/physical_plan/operators/operator.hpp>
#include <core/resource_tracer.hpp>

#include <cstddef>
#include <string_view>
#include <utility>

using components::operators::operator_t;
using components::operators::operator_type;

namespace {

    // Well past the small-string threshold, so the message is a real heap block resource_tracer_t can track.
    constexpr std::string_view refusal =
        "ALTER TABLE: column \"parent_id\" of relation \"edb.child\" may not be dropped: "
        "FOREIGN KEY constraint \"fk_child_parent\" still depends on it.";

    core::error_t produced_on(std::pmr::memory_resource* producer) {
        return core::error_t{core::error_code_t::schema_error,
                             std::pmr::string{refusal.begin(), refusal.end(), producer}};
    }

} // namespace

// `error_ = error` (~98 of ~100 call sites) doesn't propagate std::pmr::string's allocator, so it lands
// on the DEFAULT resource; a move would instead adopt the producer's arena and leave the operator freeing
// memory it never owned.
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

        CHECK(op.get_error().what.get_allocator().resource() == &owner);

        // Rebuild, not hand-over: the caller's error_t keeps its own arena and buffer.
        CHECK(error.what.get_allocator().resource() == &producer);
        CHECK(std::string_view{error.what} == refusal);
        CHECK(error.what.data() != op.get_error().what.data());
        CHECK(producer.live_allocations() == produced);
        CHECK(owner.live_allocations() >= 1);
    }

    CHECK(producer.live_allocations() == 0);
    CHECK(owner.live_allocations() == 0);
}

// Deliberately only one set_error overload: an &&-overload could only reintroduce the arena-adoption bug
// it would need to avoid.
TEST_CASE("components::operators::set_error_of_a_temporary_still_lands_on_the_operator_resource") {
    resource_tracer_t producer;
    resource_tracer_t owner;

    {
        operator_t op{&owner, log_t{}, operator_type::empty};

        op.set_error(produced_on(&producer));

        REQUIRE(op.has_error());
        CHECK(std::string_view{op.get_error().what} == refusal);
        CHECK(op.get_error().what.get_allocator().resource() == &owner);

        // If the operator had adopted the buffer, producer would still show a live block here.
        CHECK(producer.live_allocations() == 0);
    }

    CHECK(producer.live_allocations() == 0);
    CHECK(owner.live_allocations() == 0);
}

// no_error() carries no message, so there is nothing to rebuild or place.
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
