#include <catch2/catch_test_macros.hpp>

#include <core/resource_tracer.hpp>
#include <core/result_wrapper.hpp>

#include <cstddef>
#include <memory_resource>
#include <string_view>
#include <utility>

namespace {

    // Long enough to be a real heap block rather than a small-string buffer, so a
    // resource_tracer_t can be asked which arena it came from.
    constexpr std::string_view refusal =
        "ALTER TABLE: column \"parent_id\" of relation \"edb.child\" may not be dropped: "
        "FOREIGN KEY constraint \"fk_child_parent\" still depends on it.";

    core::error_t produced_on(std::pmr::memory_resource* producer) {
        return core::error_t{core::error_code_t::schema_error,
                             std::pmr::string{refusal.begin(), refusal.end(), producer}};
    }

} // namespace

// error_on() is the ONE place that answers "where does this message live", and the answer is
// always "on the resource you named". It exists because neither of error_t's own paths gives
// that answer: the two cases below pin down exactly what those paths do instead, so that a
// change to either is visible here rather than as a corrupted refusal three layers up.
TEST_CASE("core::error_on_rebuilds_the_message_on_the_named_resource") {
    resource_tracer_t producer;
    resource_tracer_t owner;

    {
        core::error_t error = produced_on(&producer);
        const std::size_t produced = producer.live_allocations();
        REQUIRE(produced >= 1);

        {
            core::error_t adopted = core::error_on(&owner, error);

            CHECK(adopted.type == core::error_code_t::schema_error);
            CHECK(std::string_view{adopted.what} == refusal);
            CHECK(adopted.what.get_allocator().resource() == &owner);

            // A rebuild, not a hand-over: the producer's error_t keeps its own buffer.
            CHECK(error.what.get_allocator().resource() == &producer);
            CHECK(error.what.data() != adopted.what.data());
            CHECK(producer.live_allocations() == produced);
            CHECK(owner.live_allocations() == 1);
        }

        CHECK(owner.live_allocations() == 0);
    }

    CHECK(producer.live_allocations() == 0);
}

TEST_CASE("core::error_on_of_no_error_allocates_nothing") {
    resource_tracer_t owner;

    {
        core::error_t none = core::error_on(&owner, core::error_t::no_error());
        CHECK_FALSE(none.contains_error());
        CHECK(owner.live_allocations() == 0);
    }

    CHECK(owner.live_allocations() == 0);
}

// Why error_on has to exist, stated as two facts about std::pmr::string that are easy to
// forget and impossible to see at a call site.
TEST_CASE("core::copying_an_error_does_not_keep_its_arena") {
    resource_tracer_t producer;

    core::error_t error = produced_on(&producer);
    const std::size_t produced = producer.live_allocations();
    REQUIRE(produced >= 1);

    // std::pmr::polymorphic_allocator does not propagate on container copy construction, so a
    // copied error_t does NOT land on the producer's arena. It lands on the default resource,
    // which belongs to nobody in this codebase.
    core::error_t copied{error};
    CHECK(std::string_view{copied.what} == refusal);
    CHECK(copied.what.get_allocator().resource() != &producer);
    CHECK(producer.live_allocations() == produced);
}

TEST_CASE("core::moving_an_error_carries_the_producers_arena_along") {
    resource_tracer_t producer;

    core::error_t error = produced_on(&producer);
    const char* const buffer = error.what.data();
    const std::size_t produced = producer.live_allocations();
    REQUIRE(produced >= 1);

    // The mirror image: a move keeps the SOURCE allocator and steals the SOURCE buffer, so a
    // destination that "took ownership" is in fact pointing into, and will later free into,
    // the producer's arena.
    core::error_t moved{std::move(error)};
    CHECK(std::string_view{moved.what} == refusal);
    CHECK(moved.what.get_allocator().resource() == &producer);
    CHECK(moved.what.data() == buffer);
    CHECK(producer.live_allocations() == produced);
}

// A moved-from result must hand its message over, not reallocate it. The NDEBUG branch of
// result_wrapper_t's move assignment is `= default` and has always moved; the DEV_MODE branch
// read `other.error_` by name, so in a debug build the message was copied — onto the default
// resource — and the two builds disagreed about where a moved result's error lived.
TEST_CASE("core::moving_a_result_wrapper_hands_the_message_over") {
    resource_tracer_t producer;

    core::result_wrapper_t<int> failed{produced_on(&producer)};
    REQUIRE(failed.has_error());
    const char* const buffer = failed.error().what.data();
    const std::pmr::memory_resource* const arena = failed.error().what.get_allocator().resource();
    const std::size_t produced = producer.live_allocations();
    REQUIRE(produced >= 1);

    core::result_wrapper_t<int> received{0};
    received = std::move(failed);

    REQUIRE(received.has_error());
    CHECK(std::string_view{received.error().what} == refusal);
    CHECK(received.error().what.data() == buffer);
    CHECK(received.error().what.get_allocator().resource() == arena);
    CHECK(producer.live_allocations() == produced);
}
