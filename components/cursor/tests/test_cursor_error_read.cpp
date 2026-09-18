#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>
#include <core/pmr.hpp>
#include <core/resource_tracer.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

using components::cursor::cursor_t;
using components::cursor::cursor_t_ptr;
using components::cursor::make_cursor;

namespace {

    // Mirrors test_alter_missing_column/test_fk_parent_column_drop.cpp: two get_error() calls in one
    // expression. A by-value get_error() would mint copies on unrelated buffers, corrupting the read.
    std::string error_text(const cursor_t& cur) {
        return std::string{cur.get_error().what.begin(), cur.get_error().what.end()};
    }

    std::ptrdiff_t error_span(const cursor_t& cur) { return cur.get_error().what.end() - cur.get_error().what.begin(); }

    constexpr std::string_view short_refusal =
        "ALTER TABLE: column \"nosuchcol\" does not exist on relation \"edb.t\".";

    // Past the 120-byte mark the allocator's stride between blocks is larger, so a by-value read comes apart by more.
    constexpr std::string_view long_refusal =
        "ALTER TABLE: column \"parent_id\" of relation \"edb.child\" may not be dropped: "
        "FOREIGN KEY constraint \"fk_child_parent\" still depends on it.";

    // The producer's error is a temporary -> cursor_t(resource, core::error_t&&).
    cursor_t_ptr refuse_with_temporary(std::pmr::memory_resource* producer,
                                       std::pmr::memory_resource* owner,
                                       std::string_view message) {
        return make_cursor(owner,
                           core::error_t{core::error_code_t::schema_error,
                                         std::pmr::string{message.begin(), message.end(), producer}});
    }

    // The producer's error is a named variable (lvalue) -> cursor_t(resource, const core::error_t&),
    // the overload services/collection/executor.cpp's refusal sites take directly.
    cursor_t_ptr
    refuse_by_name(std::pmr::memory_resource* producer, std::pmr::memory_resource* owner, std::string_view message) {
        core::error_t err{core::error_code_t::schema_error, std::pmr::string{message.begin(), message.end(), producer}};
        REQUIRE(err.what.get_allocator().resource() == producer);
        return make_cursor(owner, err);
    }

    using refusal_factory_t = cursor_t_ptr (*)(std::pmr::memory_resource*,
                                               std::pmr::memory_resource*,
                                               std::string_view);

    void eight_refusals_in_one_process(std::string_view message, refusal_factory_t refuse) {
        auto producer = core::pmr::otterbrix_resource();
        auto owner = core::pmr::otterbrix_resource();
        const std::string expected{message};
        const auto expected_span = static_cast<std::ptrdiff_t>(expected.size());

        // Keep every cursor alive: addresses drift as the arena fills, growing a bogus length each time.
        std::vector<cursor_t_ptr> cursors;
        cursors.reserve(8);

        for (int i = 0; i < 8; ++i) {
            cursors.push_back(refuse(&producer, &owner, message));
            const cursor_t& cur = *cursors.back();
            INFO("refusal #" << i << " of 8, message is " << expected.size() << " bytes");

            REQUIRE(cur.is_error());
            REQUIRE(cur.get_error().type == core::error_code_t::schema_error);

            const core::error_t& error = cur.get_error();
            CHECK(error.what.size() == expected.size());
            CHECK(std::string_view{error.what} == message);

            CHECK(error.what.get_allocator().resource() == &owner);

            const std::ptrdiff_t span = error_span(cur);
            INFO("two get_error() reads in one expression spanned " << span << " bytes");
            CHECK(span == expected_span);

            if (span == expected_span) {
                std::string text;
                CHECK_NOTHROW(text = error_text(cur));
                CHECK(text.size() == expected.size());
                CHECK(text == expected);
            }
        }

        for (std::size_t i = 0; i < cursors.size(); ++i) {
            INFO("re-reading refusal #" << i << " after all eight were built");
            const core::error_t& error = cursors[i]->get_error();
            CHECK(error.what.size() == expected.size());
            CHECK(std::string_view{error.what} == message);
            CHECK(error.what.get_allocator().resource() == &owner);
            CHECK(error_span(*cursors[i]) == expected_span);
        }
    }

} // namespace

TEST_CASE("components::cursor::eight_short_refusals_read_back_whole") {
    REQUIRE(short_refusal.size() == 67);
    eight_refusals_in_one_process(short_refusal, refuse_with_temporary);
}

TEST_CASE("components::cursor::eight_long_refusals_read_back_whole") {
    REQUIRE(long_refusal.size() > 120);
    eight_refusals_in_one_process(long_refusal, refuse_with_temporary);
}

TEST_CASE("components::cursor::eight_short_refusals_named_read_back_whole") {
    REQUIRE(short_refusal.size() == 67);
    eight_refusals_in_one_process(short_refusal, refuse_by_name);
}

TEST_CASE("components::cursor::eight_long_refusals_named_read_back_whole") {
    REQUIRE(long_refusal.size() > 120);
    eight_refusals_in_one_process(long_refusal, refuse_by_name);
}

TEST_CASE("components::cursor::repeated_reads_denote_one_error") {
    auto producer = core::pmr::otterbrix_resource();
    auto owner = core::pmr::otterbrix_resource();
    auto cur = refuse_with_temporary(&producer, &owner, short_refusal);
    REQUIRE(cur->is_error());

    const core::error_t& first = cur->get_error();
    const core::error_t& second = cur->get_error();
    CHECK(&first == &second);
    CHECK(first.what.data() == second.what.data());
    CHECK(first.what.size() == short_refusal.size());
}

TEST_CASE("components::cursor::error_string_lives_on_the_cursor_resource") {
    resource_tracer_t producer;
    resource_tracer_t owner;

    {
        core::error_t error{core::error_code_t::schema_error,
                            std::pmr::string{long_refusal.begin(), long_refusal.end(), &producer}};
        const std::size_t produced = producer.live_allocations();
        REQUIRE(produced >= 1);

        {
            auto cur = make_cursor(&owner, std::move(error));
            REQUIRE(cur->is_error());

            // Moving a pmr string keeps its source allocator, so a plain move would point into an unowned arena.
            CHECK(cur->get_error().what.get_allocator().resource() == &owner);
            CHECK(std::string_view{cur->get_error().what} == long_refusal);
        }

        INFO("producer arena live allocations must not drop when the cursor dies");
        CHECK(producer.live_allocations() == produced);
    }

    CHECK(producer.live_allocations() == 0);
    CHECK(owner.live_allocations() == 0);
}

// Same contract for the const&-constructor, entered directly rather than via delegation.
TEST_CASE("components::cursor::error_string_lives_on_the_cursor_resource_when_passed_by_name") {
    resource_tracer_t producer;
    resource_tracer_t owner;

    {
        core::error_t error{core::error_code_t::schema_error,
                            std::pmr::string{long_refusal.begin(), long_refusal.end(), &producer}};
        const std::size_t produced = producer.live_allocations();
        REQUIRE(produced >= 1);

        {
            // Copying an error_t doesn't propagate the allocator, landing a plain copy on the default resource.
            auto cur = make_cursor(&owner, error);
            REQUIRE(cur->is_error());

            CHECK(cur->get_error().what.get_allocator().resource() == &owner);
            CHECK(std::string_view{cur->get_error().what} == long_refusal);

            CHECK(error.what.get_allocator().resource() == &producer);
            CHECK(std::string_view{error.what} == long_refusal);
            CHECK(error.what.data() != cur->get_error().what.data());

            CHECK(owner.live_allocations() >= 1);
        }

        CHECK(producer.live_allocations() == produced);
    }

    CHECK(producer.live_allocations() == 0);
    CHECK(owner.live_allocations() == 0);
}
