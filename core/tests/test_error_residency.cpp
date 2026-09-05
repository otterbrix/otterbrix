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

} // namespace

// error_on() always lands the message on the resource you named; the cases below pin down
// what plain copy/move do instead (neither gives that answer).
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

// Two std::pmr::string facts that are invisible at a call site, hence error_on().
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

    // Mirror image: a move keeps the source allocator and buffer, so the destination points
    // into (and will free into) the producer's arena.
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
// small-string buffer calls nullptr->allocate() before the body would ever run. Both cases
// below share the null resource and code path, differing only in message length, and run in a
// child process since the correct outcome is an abort.
#if !defined(NDEBUG) && (defined(__unix__) || defined(__APPLE__))

namespace {

    // libc++'s std::pmr::string keeps up to 22 chars in-object. Below that boundary the
    // allocator is never asked for anything; above it, it is asked immediately.
    constexpr std::string_view short_refusal = "table t: not found";
    static_assert(short_refusal.size() <= 22, "the control message must fit the small-string buffer");
    static_assert(refusal.size() > 22, "the subject message must be too long for the small-string buffer");

    // Runs `body` in a child, returning the raw wait(2) status so an abort is an observation
    // rather than the end of the test run.
    template<typename body_t>
    int child_status(body_t&& body) {
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            // Reset Catch2's SIGABRT handler and the other fatal signals so the child dies
            // with a status the parent can read, instead of reporting from a half-torn-down runner.
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

} // namespace

// Control: a message that fits the small-string buffer never allocates, so the guard has
// always worked here.
TEST_CASE("core::a_short_message_copied_onto_a_null_resource_is_refused") {
    resource_tracer_t producer;
    core::error_t error{core::error_code_t::schema_error,
                        std::pmr::string{short_refusal.begin(), short_refusal.end(), &producer}};
    CHECK(producer.live_allocations() == 0); // proof the control really is allocation-free

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
    REQUIRE(producer.live_allocations() >= 1); // proof the subject really does allocate

    int status = child_status([&error] {
        core::error_t adopted{error, nullptr};
        return adopted.contains_error() ? survived : survived + 1;
    });

    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);
}

#endif
