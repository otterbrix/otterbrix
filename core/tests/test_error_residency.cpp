#include <catch2/catch_test_macros.hpp>

#include <core/resource_tracer.hpp>
#include <core/result_wrapper.hpp>

#include <cstddef>
#include <memory_resource>
#include <string_view>
#include <utility>

#if !defined(NDEBUG) && (defined(__unix__) || defined(__APPLE__))
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

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

}

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

TEST_CASE("core::copying_an_error_does_not_keep_its_arena") {
    resource_tracer_t producer;

    core::error_t error = produced_on(&producer);
    const std::size_t produced = producer.live_allocations();
    REQUIRE(produced >= 1);

    // polymorphic_allocator doesn't propagate on copy construction, so this lands on the
    // default resource, not the producer's arena.
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

    // A move keeps the source allocator and buffer, so the destination points into (and frees
    // into) the producer's arena.
    core::error_t moved{std::move(error)};
    CHECK(std::string_view{moved.what} == refusal);
    CHECK(moved.what.get_allocator().resource() == &producer);
    CHECK(moved.what.data() == buffer);
    CHECK(producer.live_allocations() == produced);
}

// Guards against Debug/Release disagreeing: the NDEBUG move assignment is `= default` (moves),
// so the dev-mode branch must also move the message, not copy it onto the default resource.
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

// error_t{other, resource}'s null-resource assert must stand in the initializer list, not the
// body: `what` is built from the same pointer there, so a message too long for the
// small-string buffer calls nullptr->allocate() before the body would ever run.
#if !defined(NDEBUG) && (defined(__unix__) || defined(__APPLE__))

namespace {

    // libc++'s std::pmr::string keeps up to 22 chars in-object, so the allocator is never asked
    // below that boundary and always asked above it.
    constexpr std::string_view short_refusal = "table t: not found";
    static_assert(short_refusal.size() <= 22, "the control message must fit the small-string buffer");
    static_assert(refusal.size() > 22, "the subject message must be too long for the small-string buffer");

    template<typename body_t>
    int child_status(body_t&& body) {
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            ::signal(SIGABRT, SIG_DFL);
            ::signal(SIGSEGV, SIG_DFL);
            ::signal(SIGBUS, SIG_DFL);
            _exit(body());
        }
        int status = 0;
        REQUIRE(::waitpid(child, &status, 0) == child);
        return status;
    }

    constexpr int survived = 42;

}

TEST_CASE("core::a_short_message_copied_onto_a_null_resource_is_refused") {
    resource_tracer_t producer;
    core::error_t error{core::error_code_t::schema_error,
                        std::pmr::string{short_refusal.begin(), short_refusal.end(), &producer}};
    CHECK(producer.live_allocations() == 0);

    int status = child_status([&error] {
        core::error_t adopted{error, nullptr};
        return adopted.contains_error() ? survived : survived + 1;
    });

    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);
}

// Subject: same copy with an allocating message; must abort, not null-dereference.
TEST_CASE("core::a_long_message_copied_onto_a_null_resource_is_refused") {
    resource_tracer_t producer;
    core::error_t error = produced_on(&producer);
    REQUIRE(producer.live_allocations() >= 1);

    int status = child_status([&error] {
        core::error_t adopted{error, nullptr};
        return adopted.contains_error() ? survived : survived + 1;
    });

    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);
}

#endif
