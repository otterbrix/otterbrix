#include <catch2/catch.hpp>

#include <components/vector/validation.hpp>

// validity_mask_t over external buffers carries its resource
// ==========================================================
//
// The pointer constructor validity_mask_t(resource, ptr) wraps an externally
// owned bit buffer AND records the memory resource that every allocating
// member function draws from: copy ctor, copy operator=, combine(),
// slice(offset), and the lazy-resize paths of set()/set_invalid/set_valid.
// The [validity-null-resource] cases assert those allocating paths on
// pointer-constructed masks.
//
// Note: all_valid() is defined as !validity_mask_, so a pointer-constructed
// mask over a non-null buffer is never "all valid" even when every bit is set;
// copying such a mask therefore ALWAYS takes the allocating branch.
//
// Production callers of the pointer constructor pass the buffer manager's
// resource (components/table/column_segment.cpp: validity_fetch_row,
// validity_check_row, validity_append, column_segment_t::revert_append).

using components::vector::validity_mask_t;

namespace {
    constexpr uint64_t test_capacity = components::vector::DEFAULT_VECTOR_CAPACITY;
    constexpr uint64_t entry_count = validity_mask_t::STANDARD_ENTRY_COUNT;
} // namespace

// ---------------------------------------------------------------------------
// Control cases: safe behavior that must keep working.
// ---------------------------------------------------------------------------

TEST_CASE("validity_mask_t: pointer-constructed mask reads and writes the external buffer",
          "[validity-mask]") {
    // This is exactly the column_segment.cpp usage pattern: wrap a pinned
    // buffer and operate on bits in place. validity_mask_ is non-null, so the
    // lazy resize/allocation paths are never taken and resource_ is unused.
    auto resource = std::pmr::synchronized_pool_resource();
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }

    validity_mask_t mask(&resource, buffer);
    REQUIRE(mask.is_mask_set());
    REQUIRE_FALSE(mask.all_valid()); // pointer-backed: all_valid() is pointer-null, not bit state
    REQUIRE(mask.count() == test_capacity);
    REQUIRE(mask.data() == buffer);
    REQUIRE(mask.resource() == &resource);

    REQUIRE(mask.row_is_valid(0));
    REQUIRE(mask.row_is_valid(42));

    mask.set_invalid(uint64_t(42));
    REQUIRE_FALSE(mask.row_is_valid(42));
    REQUIRE(mask.row_is_valid(41));
    REQUIRE(mask.row_is_valid(43));
    // mutation went into the external buffer, not a private allocation
    REQUIRE((buffer[0] & (uint64_t(1) << 42)) == 0);

    mask.set(42, true);
    REQUIRE(mask.row_is_valid(42));
    REQUIRE(buffer[0] == components::vector::validity_data_t::MAX_ENTRY);

    REQUIRE(mask.count_valid(test_capacity) == test_capacity);
}

TEST_CASE("validity_mask_t: copy and move of all-valid / pointer-constructed masks stay safe",
          "[validity-mask]") {
    // A pointer-constructed mask over nullptr is all_valid(); copying it takes
    // the non-allocating branch and never touches resource_.
    auto resource = std::pmr::synchronized_pool_resource();
    validity_mask_t null_ptr_mask(&resource, static_cast<uint64_t*>(nullptr));
    REQUIRE(null_ptr_mask.all_valid());
    validity_mask_t copy(null_ptr_mask);
    REQUIRE(copy.all_valid());
    REQUIRE(copy.row_is_valid(0));

    // Moving a pointer-constructed mask never allocates either.
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t ptr_mask(&resource, buffer);
    ptr_mask.set_invalid(uint64_t(7));

    validity_mask_t moved(std::move(ptr_mask));
    REQUIRE(moved.data() == buffer);
    REQUIRE_FALSE(moved.row_is_valid(7));
    REQUIRE(moved.row_is_valid(8));
}

// ---------------------------------------------------------------------------
// Allocating paths on pointer-constructed masks: each must allocate from the
// carried resource and leave the external buffer untouched.
// ---------------------------------------------------------------------------

TEST_CASE("validity_mask_t: copy-constructing from a pointer-constructed mask with an invalid bit",
          "[validity-null-resource]") {
    // copy ctor (validation.cpp): other is not all_valid(), so it allocates a
    // private validity_data_t from the carried resource and copies the bits.
    auto resource = std::pmr::synchronized_pool_resource();
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t source(&resource, buffer);
    source.set_invalid(uint64_t(3)); // in-place on the buffer, still fine

    validity_mask_t copy(source);
    REQUIRE(copy.is_mask_set());
    REQUIRE(copy.data() != buffer); // private allocation, not the external buffer
    REQUIRE_FALSE(copy.row_is_valid(3));
    REQUIRE(copy.row_is_valid(2));
    REQUIRE(copy.row_is_valid(4));
    REQUIRE(copy.count_valid(test_capacity) == test_capacity - 1);
    // bit-for-bit equal to the source buffer
    for (uint64_t entry = 0; entry < entry_count; entry++) {
        REQUIRE(copy.data()[entry] == buffer[entry]);
    }
    // the source still wraps the external buffer
    REQUIRE(source.data() == buffer);
}

TEST_CASE("validity_mask_t: copy-assigning between two pointer-constructed masks",
          "[validity-null-resource]") {
    // copy operator= (validation.cpp) requires matching resources
    // (assert(resource_ == other.resource_)), then allocates a private copy
    // of the source bits — the target's external buffer is left untouched.
    auto resource = std::pmr::synchronized_pool_resource();
    uint64_t src_buffer[entry_count];
    uint64_t dst_buffer[entry_count];
    for (uint64_t i = 0; i < entry_count; i++) {
        src_buffer[i] = components::vector::validity_data_t::MAX_ENTRY;
        dst_buffer[i] = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t source(&resource, src_buffer);
    source.set_invalid(uint64_t(5));
    validity_mask_t target(&resource, dst_buffer);

    target = source;
    REQUIRE_FALSE(target.row_is_valid(5));
    REQUIRE(target.row_is_valid(4));
    REQUIRE(target.row_is_valid(6));
    REQUIRE(target.count_valid(test_capacity) == test_capacity - 1);
    REQUIRE(target.data() != src_buffer); // private copy of the source bits
    REQUIRE(target.data() != dst_buffer); // detached from the old external buffer
    REQUIRE(dst_buffer[0] == components::vector::validity_data_t::MAX_ENTRY);
}

TEST_CASE("validity_mask_t: combine() on a pointer-constructed mask",
          "[validity-null-resource]") {
    // combine (validation.cpp): this is not all_valid() (pointer set) and the
    // masks differ, so it copies its own bits into a private allocation and
    // ANDs the other mask in. The external buffer stays untouched.
    auto resource = std::pmr::synchronized_pool_resource();
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t ptr_mask(&resource, buffer);

    validity_mask_t other(&resource, test_capacity);
    other.set_invalid(uint64_t(1));

    ptr_mask.combine(other, test_capacity);
    REQUIRE_FALSE(ptr_mask.row_is_valid(1));
    REQUIRE(ptr_mask.row_is_valid(0));
    REQUIRE(ptr_mask.row_is_valid(2));
    REQUIRE(ptr_mask.count_valid(test_capacity) == test_capacity - 1);
    REQUIRE(ptr_mask.data() != buffer); // combine reallocates away from the external buffer
    REQUIRE(buffer[0] == components::vector::validity_data_t::MAX_ENTRY);
}

TEST_CASE("validity_mask_t: slice() at non-zero offset on a pointer-constructed mask",
          "[validity-null-resource]") {
    // slice (validation.cpp): other is not all_valid() and offset != 0, so it
    // builds a fresh validity_mask_t(resource(), count) and shifts the source
    // bits into it: target bit i == source bit (offset + i).
    auto resource = std::pmr::synchronized_pool_resource();
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t ptr_mask(&resource, buffer);

    validity_mask_t other(&resource, test_capacity);
    other.set_invalid(uint64_t(2));

    ptr_mask.slice(other, 1, test_capacity - 1);
    REQUIRE(ptr_mask.count() == test_capacity - 1);
    // target bit i == source bit (1 + i): only bit 1 (source bit 2) is invalid
    for (uint64_t row = 0; row < test_capacity - 1; row++) {
        REQUIRE(ptr_mask.row_is_valid(row) == (row != 1));
    }
    REQUIRE(ptr_mask.data() != buffer); // sliced into a private allocation
    REQUIRE(buffer[0] == components::vector::validity_data_t::MAX_ENTRY);
}
