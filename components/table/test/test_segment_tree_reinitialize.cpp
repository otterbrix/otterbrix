#include <catch2/catch_test_macros.hpp>

#include <components/table/segment_tree.hpp>

// The gap tripwire in contiguous() must ANSWER false, not throw: a throw here would unwind
// into a coroutine with no unhandled_exception(), hanging the statement instead of failing it.

namespace {
    struct dummy_segment_t : components::table::segment_base_t<dummy_segment_t> {
        dummy_segment_t(int64_t start, uint64_t count)
            : segment_base_t(start, count) {}
    };
    using dummy_tree_t = components::table::segment_tree_t<dummy_segment_t>;
    // One owner: the tree is neither copied nor moved out of its collection.
    static_assert(!std::is_copy_constructible_v<dummy_tree_t> && !std::is_move_constructible_v<dummy_tree_t>);
} // namespace

TEST_CASE("components::table::segment_tree::the_successor_follows_erase_and_replace_without_a_link",
          "[segment_tree_next]") {
    dummy_tree_t tree;
    tree.append_segment(std::make_unique<dummy_segment_t>(0, 10));
    tree.append_segment(std::make_unique<dummy_segment_t>(10, 10));
    tree.append_segment(std::make_unique<dummy_segment_t>(20, 10));
    auto* first = tree.segment_at(0);
    REQUIRE(tree.next_segment(first) == tree.segment_at(1));
    REQUIRE(tree.next_segment(tree.segment_at(2)) == nullptr);

    tree.replace_segment_at_index(1, std::make_unique<dummy_segment_t>(10, 10));
    REQUIRE(tree.next_segment(first) == tree.segment_at(1));
    REQUIRE(tree.next_segment(tree.segment_at(1)) == tree.segment_at(2));

    tree.erase_segments(1);
    REQUIRE(tree.next_segment(first) == nullptr);
    REQUIRE(tree.get_segment(15) == nullptr);
    uint64_t visited = 0;
    for (auto& segment : tree.segments()) {
        REQUIRE(&segment == first);
        visited++;
    }
    REQUIRE(visited == 1);
}

TEST_CASE("components::table::segment_tree::a_gap_answers_instead_of_throwing") {
    components::table::segment_tree_t<dummy_segment_t> tree;
    tree.append_segment(std::make_unique<dummy_segment_t>(0, 10));
    // A gap: the second segment starts at 20 while the first ends at 10.
    tree.append_segment(std::make_unique<dummy_segment_t>(20, 5));

    bool contiguous = true;
    REQUIRE_NOTHROW(contiguous = tree.contiguous());
    REQUIRE_FALSE(contiguous);
}

TEST_CASE("components::table::segment_tree::contiguous_segments_are_found_by_row") {
    components::table::segment_tree_t<dummy_segment_t> tree;
    tree.append_segment(std::make_unique<dummy_segment_t>(100, 10));
    tree.append_segment(std::make_unique<dummy_segment_t>(110, 5));

    REQUIRE(tree.contiguous());

    uint64_t index = 0;
    REQUIRE(tree.try_segment_index(100, index));
    REQUIRE(index == 0);
    REQUIRE(tree.try_segment_index(112, index));
    REQUIRE(index == 1);
    REQUIRE_FALSE(tree.try_segment_index(99, index));
}
