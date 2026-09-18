#include <catch2/catch_test_macros.hpp>

#include <components/table/segment_tree.hpp>

// The gap tripwire in reinitialize() must ANSWER false, not throw: a throw here would unwind
// into a coroutine with no unhandled_exception(), hanging the statement instead of failing it
//.

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

    // The tripwire answers false and leaves the row_start map untouched.
    bool contiguous = true;
    REQUIRE_NOTHROW(contiguous = tree.reinitialize());
    REQUIRE_FALSE(contiguous);
}

TEST_CASE("components::table::segment_tree::reinitialize_rebuilds_contiguous_row_starts") {
    components::table::segment_tree_t<dummy_segment_t> tree;
    tree.append_segment(std::make_unique<dummy_segment_t>(100, 10));
    tree.append_segment(std::make_unique<dummy_segment_t>(110, 5));

    REQUIRE(tree.reinitialize());

    auto l = tree.lock();
    uint64_t index = 0;
    REQUIRE(tree.try_segment_index(l, 100, index));
    REQUIRE(index == 0);
    REQUIRE(tree.try_segment_index(l, 112, index));
    REQUIRE(index == 1);
    REQUIRE_FALSE(tree.try_segment_index(l, 99, index));
}
