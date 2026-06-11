#include <catch2/catch.hpp>

#include <components/vector/validation.hpp>

// validity_mask_t null-resource hazard
// ====================================
//
// The pointer constructor (components/vector/validation.hpp:52-55)
//
//     explicit validity_mask_t(uint64_t* ptr)
//         : resource_(nullptr), validity_mask_(ptr), count_(DEFAULT_VECTOR_CAPACITY) {}
//
// is the only constructor that leaves resource_ == nullptr. Several member
// functions allocate through resource_ / resource() and dereference it without
// a null check (components/vector/validation.cpp):
//
//   - copy ctor      (:31-41)  -> make_shared<validity_data_t>(resource_, ...)
//   - copy operator= (:43-54)  -> make_shared<validity_data_t>(resource_, ...)
//   - combine()      (:264/:273) -> make_shared<validity_data_t>(resource_, ...)
//   - slice()        (:185)    -> validity_mask_t(resource(), count)
//   - set(row,false) (:164) and set_invalid/set_valid resize(resource_, ...)
//     lazy paths (:73-94) — only reachable when validity_mask_ is null
//
// validity_data_t's constructors (:7-23) immediately call resource->allocate(),
// so every path above is a null-pointer dereference (SIGSEGV) when invoked on
// (or, for the copy ctor, from) a pointer-constructed mask.
//
// Note: all_valid() is defined as !validity_mask_, so a pointer-constructed
// mask over a non-null buffer is never "all valid" even when every bit is set;
// copying such a mask therefore ALWAYS takes the allocating branch.
//
// Production callers of the pointer constructor (latent today — none of them
// copies/assigns/combines/slices the mask, they only call row_is_valid /
// set_invalid / set_valid on a non-null buffer):
//
//   - components/table/column_segment.cpp:205  (validity_fetch_row)
//   - components/table/column_segment.cpp:247  (validity_check_row)
//   - components/table/column_segment.cpp:353  (validity_append)
//   - components/table/column_segment.cpp:1513 (column_segment_t::revert_append)
//
// The crashing reproductions below are HIDDEN behind the tag
// [.][validity-null-resource] so the default suite stays green until the fix
// lands (same convention as the hidden [group_contracts] integration tests).
// Run them explicitly with: test_vector "[validity-null-resource]"

using components::vector::validity_mask_t;

namespace {
    constexpr uint64_t test_capacity = components::vector::DEFAULT_VECTOR_CAPACITY;
    constexpr uint64_t entry_count = validity_mask_t::STANDARD_ENTRY_COUNT;
} // namespace

// ---------------------------------------------------------------------------
// Control cases (NOT hidden): safe behavior that must keep working.
// ---------------------------------------------------------------------------

TEST_CASE("validity_mask_t: pointer-constructed mask reads and writes the external buffer",
          "[validity-mask]") {
    // This is exactly the column_segment.cpp usage pattern: wrap a pinned
    // buffer and operate on bits in place. validity_mask_ is non-null, so the
    // lazy resize/allocation paths are never taken and resource_ is unused.
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }

    validity_mask_t mask(buffer);
    REQUIRE(mask.is_mask_set());
    REQUIRE_FALSE(mask.all_valid()); // pointer-backed: all_valid() is pointer-null, not bit state
    REQUIRE(mask.count() == test_capacity);
    REQUIRE(mask.data() == buffer);

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
    validity_mask_t null_ptr_mask(static_cast<uint64_t*>(nullptr));
    REQUIRE(null_ptr_mask.all_valid());
    validity_mask_t copy(null_ptr_mask);
    REQUIRE(copy.all_valid());
    REQUIRE(copy.row_is_valid(0));

    // Moving a pointer-constructed mask never allocates either.
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t ptr_mask(buffer);
    ptr_mask.set_invalid(uint64_t(7));

    validity_mask_t moved(std::move(ptr_mask));
    REQUIRE(moved.data() == buffer);
    REQUIRE_FALSE(moved.row_is_valid(7));
    REQUIRE(moved.row_is_valid(8));
}

// ---------------------------------------------------------------------------
// Hidden reproductions: each case dereferences the null resource_ and crashes
// (SIGSEGV) today. Unhide once the fix lands.
// ---------------------------------------------------------------------------

TEST_CASE("validity_mask_t: copy-constructing from a pointer-constructed mask with an invalid bit",
          "[.][validity-null-resource]") {
    // copy ctor (validation.cpp:31-41): other is not all_valid(), so it calls
    // make_shared<validity_data_t>(resource_ /* = nullptr */, ...), and
    // validity_data_t's ctor (validation.cpp:15-17) does resource->allocate().
    // Observed 2026-06-11 (Debug, AppleClang/arm64): FAILED "due to a fatal
    // error condition: SIGSEGV - Segmentation violation signal".
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t source(buffer);
    source.set_invalid(uint64_t(3)); // in-place on the buffer, still fine

    validity_mask_t copy(source); // null resource_->allocate() -> SIGSEGV
    REQUIRE_FALSE(copy.row_is_valid(3));
}

TEST_CASE("validity_mask_t: copy-assigning between two pointer-constructed masks",
          "[.][validity-null-resource]") {
    // copy operator= (validation.cpp:43-54): assert(resource_ == other.resource_)
    // PASSES because both are nullptr, then the same
    // make_shared<validity_data_t>(nullptr, ...) null dereference follows.
    // Observed 2026-06-11 (Debug, AppleClang/arm64): FAILED "due to a fatal
    // error condition: SIGSEGV - Segmentation violation signal".
    uint64_t src_buffer[entry_count];
    uint64_t dst_buffer[entry_count];
    for (uint64_t i = 0; i < entry_count; i++) {
        src_buffer[i] = components::vector::validity_data_t::MAX_ENTRY;
        dst_buffer[i] = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t source(src_buffer);
    source.set_invalid(uint64_t(5));
    validity_mask_t target(dst_buffer);

    target = source; // null resource_->allocate() -> SIGSEGV
    REQUIRE_FALSE(target.row_is_valid(5));
}

TEST_CASE("validity_mask_t: combine() on a pointer-constructed mask",
          "[.][validity-null-resource]") {
    // combine (validation.cpp:257-280): this is not all_valid() (pointer set)
    // and the masks differ, so it reaches
    // make_shared<validity_data_t>(resource_ /* = nullptr */, validity_mask_, count_)
    // at validation.cpp:273.
    // Observed 2026-06-11 (Debug, AppleClang/arm64): FAILED "due to a fatal
    // error condition: SIGSEGV - Segmentation violation signal".
    auto resource = std::pmr::synchronized_pool_resource();
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t ptr_mask(buffer);

    validity_mask_t other(&resource, test_capacity);
    other.set_invalid(uint64_t(1));

    ptr_mask.combine(other, test_capacity); // null resource_->allocate() -> SIGSEGV
    REQUIRE_FALSE(ptr_mask.row_is_valid(1));
}

TEST_CASE("validity_mask_t: slice() at non-zero offset on a pointer-constructed mask",
          "[.][validity-null-resource]") {
    // slice (validation.cpp:173-190): other is not all_valid() and offset != 0,
    // so it constructs validity_mask_t(resource() /* = nullptr */, count) at
    // validation.cpp:185, whose ctor allocates from the null resource.
    // Observed 2026-06-11 (Debug, AppleClang/arm64): FAILED "due to a fatal
    // error condition: SIGSEGV - Segmentation violation signal".
    auto resource = std::pmr::synchronized_pool_resource();
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t ptr_mask(buffer);

    validity_mask_t other(&resource, test_capacity);
    other.set_invalid(uint64_t(2));

    ptr_mask.slice(other, 1, test_capacity - 1); // null resource_->allocate() -> SIGSEGV
    REQUIRE_FALSE(ptr_mask.row_is_valid(1));
}
