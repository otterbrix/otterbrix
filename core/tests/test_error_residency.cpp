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
// result_wrapper_t's move assignment is `= default` and therefore moves; a DEV_MODE branch that
// reads `other.error_` by name copies instead — onto the default resource — and the two builds
// then disagree about where a moved result's error lives.
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

// --- The allocator-extended copy's own guard -------------------------------------------
//
// error_t{other, resource} asserts that `resource` is not null. WHERE that assert stands
// decides whether it ever runs: `what` is initialized from the same pointer in the
// initializer list, so for a message too long to fit the small-string buffer the copy calls
// nullptr->allocate() while the constructor BODY is still unreached. A body-level assert
// therefore fired only for messages short enough NOT to allocate -- exactly the inputs that
// were harmless anyway -- and stood aside for the one input it was written for.
//
// The two cases below are the control and the subject: same null resource, same code path,
// only the length of the message differs. They run in a child process because the correct
// answer is a deliberate abort, which would otherwise take the runner down.
#if !defined(NDEBUG) && (defined(__unix__) || defined(__APPLE__))

namespace {

    // libc++'s std::pmr::string keeps up to 22 chars in-object. Below that boundary the
    // allocator is never asked for anything; above it, it is asked immediately.
    constexpr std::string_view short_refusal = "table t: not found";
    static_assert(short_refusal.size() <= 22, "the control message must fit the small-string buffer");
    static_assert(refusal.size() > 22, "the subject message must be too long for the small-string buffer");

    // Runs `body` in a child and answers with the raw wait(2) status, so an abort is an
    // observation instead of the end of the test run. A body that returns hands its answer
    // back as the child's exit code.
    template<typename body_t>
    int child_status(body_t&& body) {
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            // Catch2 installs its own SIGABRT handler; reset it so an abort reaches waitpid
            // as a signal death rather than a report on a half-torn-down runner.
            ::signal(SIGABRT, SIG_DFL);
            // Same for the fatal signals a WRONG answer produces, so the child dies with a
            // status the parent can read instead of printing a report from a forked runner.
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

// CONTROL: a message that fits the small-string buffer never asks the null resource for
// memory, so the constructor body is reached and the guard has always worked here.
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

// SUBJECT: the same copy with a message one allocation long. The guard must reach it too --
// a diagnosed abort, not a null dereference inside std::pmr::string.
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
