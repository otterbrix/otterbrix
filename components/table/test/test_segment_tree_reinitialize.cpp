#include <catch2/catch_test_macros.hpp>

#include <components/table/segment_tree.hpp>

// reinitialize() rebuilds the row_start map after set_start() re-based the segments. Its gap
// tripwire used to THROW std::runtime_error -- the same failure class the segment_index()
// conversion removed from this header (rules 2/9: a throw here unwinds across the disk agent's
// mailbox into a coroutine whose unhandled_exception() is empty, so the statement HUNG instead
// of failing). The tripwire must stay (a gap means the tree invariant is broken somewhere
// else), but it must answer, not throw.

namespace {
    struct dummy_segment_t : components::table::segment_base_t<dummy_segment_t> {
        dummy_segment_t(int64_t start, uint64_t count)
            : segment_base_t(start, count) {}
    };
} // namespace

TEST_CASE("components::table::segment_tree::reinitialize_gap_answers_instead_of_throwing") {
    components::table::segment_tree_t<dummy_segment_t> tree;
    tree.append_segment(std::make_unique<dummy_segment_t>(0, 10));
    // A gap: the second segment starts at 20 while the first ends at 10.
    tree.append_segment(std::make_unique<dummy_segment_t>(20, 5));

    // RED before the fix: this threw std::runtime_error("... gap found between nodes!").
    // Now the tripwire answers: false, and the row_start map is left untouched.
    bool contiguous = true;
    REQUIRE_NOTHROW(contiguous = tree.reinitialize());
    REQUIRE_FALSE(contiguous);
}

TEST_CASE("components::table::segment_tree::reinitialize_rebuilds_contiguous_row_starts") {
    components::table::segment_tree_t<dummy_segment_t> tree;
    tree.append_segment(std::make_unique<dummy_segment_t>(100, 10));
    tree.append_segment(std::make_unique<dummy_segment_t>(110, 5));

    REQUIRE(tree.reinitialize());

    // The map answers point lookups again at the re-based positions.
    auto l = tree.lock();
    uint64_t index = 0;
    REQUIRE(tree.try_segment_index(l, 100, index));
    REQUIRE(index == 0);
    REQUIRE(tree.try_segment_index(l, 112, index));
    REQUIRE(index == 1);
    REQUIRE_FALSE(tree.try_segment_index(l, 99, index));
}
