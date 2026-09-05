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

    // How the DDL integration cases read a refusal
    // (integration/cpp/test/test_alter_missing_column.cpp:89, test_fk_parent_column_drop.cpp:74):
    // TWO calls to get_error() inside ONE full expression, begin() taken from the first call
    // and end() from the second. That is only well defined when both calls denote the SAME
    // error object. While get_error() returned by value, each call minted a fresh error_t
    // whose pmr string landed on a fresh buffer, so the iterator pair spanned two unrelated
    // heap blocks and the length became second.data() - first.data() + second.size(): the
    // message read back doubled or tripled, or the difference went negative, wrapped in
    // size_t and threw std::length_error("basic_string").
    std::string error_text(const cursor_t& cur) {
        return std::string{cur.get_error().what.begin(), cur.get_error().what.end()};
    }

    // The same two reads, measured instead of materialised. Pointer arithmetic only — it
    // never dereferences, so it reports the defect without reading unrelated heap.
    std::ptrdiff_t error_span(const cursor_t& cur) {
        return cur.get_error().what.end() - cur.get_error().what.begin();
    }

    // 67 bytes: an ALTER TABLE refusal naming a missing column.
    constexpr std::string_view short_refusal = "ALTER TABLE: column \"nosuchcol\" does not exist on relation \"edb.t\".";

    // 137 bytes: the DROP COLUMN refusal shape. Past the 120-byte mark the allocator's
    // stride between blocks is larger, so the by-value read came apart by more.
    constexpr std::string_view long_refusal =
        "ALTER TABLE: column \"parent_id\" of relation \"edb.child\" may not be dropped: "
        "FOREIGN KEY constraint \"fk_child_parent\" still depends on it.";

    // The producer's arena and the cursor's arena are DIFFERENT resources on purpose.
    //
    // A refusal is built by whoever detected it — an operator, a catalog read, a disk agent —
    // on THAT actor's resource, and then handed to a cursor built on the caller's. While both
    // ends shared one resource here, "the message lives on the cursor's arena" and "the
    // message still lives on the producer's arena" were the same observation, and every case
    // below was blind to the difference between rebuilding the string and adopting it.
    //
    // Two entry points, because cursor_t has two error constructors and a caller reaches
    // exactly one of them depending on what it holds:

    // (1) The producer's error is a temporary -> cursor_t(resource, core::error_t&&).
    cursor_t_ptr refuse_with_temporary(std::pmr::memory_resource* producer,
                                       std::pmr::memory_resource* owner,
                                       std::string_view message) {
        return make_cursor(owner,
                           core::error_t{core::error_code_t::schema_error,
                                         std::pmr::string{message.begin(), message.end(), producer}});
    }

    // (2) The producer's error is a NAMED variable the caller still holds, passed by name
    //     -> cursor_t(resource, const core::error_t&), reached as an entry point of its own
    //     rather than by delegation from the &&-constructor.
    //
    //     This is the live shape on the main refusal path: services/collection/executor.cpp
    //     :1023, :1105, :1149, :1157, :1170 and :1224 all read
    //         if (auto err = ...; err.contains_error()) { error = make_cursor(resource(), err); }
    //     and pass `err` — an lvalue — so they enter the const&-constructor directly. Nothing
    //     in this file reached that overload independently before, so its body could be
    //     replaced by the plain copy it used to be with every case still green.
    cursor_t_ptr
    refuse_by_name(std::pmr::memory_resource* producer, std::pmr::memory_resource* owner, std::string_view message) {
        core::error_t err{core::error_code_t::schema_error,
                          std::pmr::string{message.begin(), message.end(), producer}};
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

        // Keep every cursor alive, the way a caller collecting failures would. The allocator
        // hands out addresses further and further apart as the process runs on, which is what
        // made the bogus length grow from one refusal to the next.
        std::vector<cursor_t_ptr> cursors;
        cursors.reserve(8);

        for (int i = 0; i < 8; ++i) {
            cursors.push_back(refuse(&producer, &owner, message));
            const cursor_t& cur = *cursors.back();
            INFO("refusal #" << i << " of 8, message is " << expected.size() << " bytes");

            REQUIRE(cur.is_error());
            REQUIRE(cur.get_error().type == core::error_code_t::schema_error);

            // Size and content of this refusal, read the safe way.
            const core::error_t& error = cur.get_error();
            CHECK(error.what.size() == expected.size());
            CHECK(std::string_view{error.what} == message);

            // ... and on the cursor's arena, not the producer's and not the default one.
            CHECK(error.what.get_allocator().resource() == &owner);

            // Size and content as the DDL callers read it: twice in one expression.
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

        // Every refusal still reads back whole after all eight cursors exist.
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

    // Reading a cursor's error twice must hand back the cursor's one error, not two
    // freshly minted copies — otherwise no caller may combine the two reads.
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

            // A cursor's data lives on the cursor's resource. Moving a pmr string keeps the
            // SOURCE allocator, so a plain move left the cursor pointing into — and later
            // freeing into — an arena it never owned.
            CHECK(cur->get_error().what.get_allocator().resource() == &owner);
            CHECK(std::string_view{cur->get_error().what} == long_refusal);
        }

        // Destroying the cursor must not hand anything back to the producer's arena.
        INFO("producer arena live allocations must not drop when the cursor dies");
        CHECK(producer.live_allocations() == produced);
    }

    CHECK(producer.live_allocations() == 0);
    CHECK(owner.live_allocations() == 0);
}

// The same contract for the OTHER constructor, entered on its own rather than by delegation:
// the caller keeps its error_t and hands the cursor a reference to it.
TEST_CASE("components::cursor::error_string_lives_on_the_cursor_resource_when_passed_by_name") {
    resource_tracer_t producer;
    resource_tracer_t owner;

    {
        core::error_t error{core::error_code_t::schema_error,
                            std::pmr::string{long_refusal.begin(), long_refusal.end(), &producer}};
        const std::size_t produced = producer.live_allocations();
        REQUIRE(produced >= 1);

        {
            // By name — this is cursor_t(resource, const core::error_t&), the overload the six
            // executor refusal sites take. Copying an error_t does NOT propagate the
            // allocator, so a plain copy here put the message on the DEFAULT resource: not the
            // producer's, not the cursor's, and invisible to both tracers.
            auto cur = make_cursor(&owner, error);
            REQUIRE(cur->is_error());

            CHECK(cur->get_error().what.get_allocator().resource() == &owner);
            CHECK(std::string_view{cur->get_error().what} == long_refusal);

            // The caller's own error_t is untouched and still on its own arena.
            CHECK(error.what.get_allocator().resource() == &producer);
            CHECK(std::string_view{error.what} == long_refusal);
            CHECK(error.what.data() != cur->get_error().what.data());

            // The cursor's message is a block of the owner's arena, so the owner must be
            // holding at least the chunk bookkeeping plus this string.
            CHECK(owner.live_allocations() >= 1);
        }

        CHECK(producer.live_allocations() == produced);
    }

    CHECK(producer.live_allocations() == 0);
    CHECK(owner.live_allocations() == 0);
}
