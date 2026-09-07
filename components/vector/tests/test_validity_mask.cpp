#include <catch2/catch_test_macros.hpp>

#include <components/vector/validation.hpp>
#include <components/vector/vector.hpp>
#include <core/resource_tracer.hpp>
#include <memory_resource>

// The pointer ctor validity_mask_t(resource, ptr) wraps an external buffer but still stores the
// resource for allocating paths (copy/assign/combine/slice/lazy-resize). all_valid() means "no
// buffer at all", so a pointer-constructed mask is never all_valid even with every bit set.

using components::vector::validity_mask_t;

namespace {
    constexpr uint64_t test_capacity = components::vector::DEFAULT_VECTOR_CAPACITY;
    constexpr uint64_t entry_count = validity_mask_t::STANDARD_ENTRY_COUNT;
} // namespace

TEST_CASE("validity_mask_t: pointer-constructed mask reads and writes the external buffer", "[validity-mask]") {
    // Matches the column_segment.cpp pattern: buffer is non-null, so lazy-allocation paths are never taken.
    auto resource = core::pmr::otterbrix_resource();
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }

    validity_mask_t mask(&resource, buffer);
    REQUIRE(mask.is_mask_set());
    REQUIRE_FALSE(mask.all_valid());
    REQUIRE(mask.count() == test_capacity);
    REQUIRE(mask.data() == buffer);
    REQUIRE(mask.resource() == &resource);

    REQUIRE(mask.row_is_valid(0));
    REQUIRE(mask.row_is_valid(42));

    mask.set_invalid(uint64_t(42));
    REQUIRE_FALSE(mask.row_is_valid(42));
    REQUIRE(mask.row_is_valid(41));
    REQUIRE(mask.row_is_valid(43));
    REQUIRE((buffer[0] & (uint64_t(1) << 42)) == 0);

    mask.set(42, true);
    REQUIRE(mask.row_is_valid(42));
    REQUIRE(buffer[0] == components::vector::validity_data_t::MAX_ENTRY);

    REQUIRE(mask.count_valid(test_capacity) == test_capacity);
}

TEST_CASE("validity_mask_t: copy and move of all-valid / pointer-constructed masks stay safe", "[validity-mask]") {
    // Over nullptr the mask IS all_valid(), so copying takes the non-allocating branch.
    auto resource = core::pmr::otterbrix_resource();
    validity_mask_t null_ptr_mask(&resource, static_cast<uint64_t*>(nullptr));
    REQUIRE(null_ptr_mask.all_valid());
    validity_mask_t copy(null_ptr_mask);
    REQUIRE(copy.all_valid());
    REQUIRE(copy.row_is_valid(0));

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

TEST_CASE("validity_mask_t: copy-constructing from a pointer-constructed mask with an invalid bit",
          "[validity-null-resource]") {
    auto resource = core::pmr::otterbrix_resource();
    uint64_t buffer[entry_count];
    for (auto& entry : buffer) {
        entry = components::vector::validity_data_t::MAX_ENTRY;
    }
    validity_mask_t source(&resource, buffer);
    source.set_invalid(uint64_t(3));

    validity_mask_t copy(source);
    REQUIRE(copy.is_mask_set());
    REQUIRE(copy.data() != buffer);
    REQUIRE_FALSE(copy.row_is_valid(3));
    REQUIRE(copy.row_is_valid(2));
    REQUIRE(copy.row_is_valid(4));
    REQUIRE(copy.count_valid(test_capacity) == test_capacity - 1);
    for (uint64_t entry = 0; entry < entry_count; entry++) {
        REQUIRE(copy.data()[entry] == buffer[entry]);
    }
    REQUIRE(source.data() == buffer);
}

TEST_CASE("validity_mask_t: copy-assigning between two pointer-constructed masks", "[validity-null-resource]") {
    // copy operator= asserts matching resources before allocating a private copy of the source bits.
    auto resource = core::pmr::otterbrix_resource();
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
    REQUIRE(target.data() != src_buffer);
    REQUIRE(target.data() != dst_buffer);
    REQUIRE(dst_buffer[0] == components::vector::validity_data_t::MAX_ENTRY);
}

TEST_CASE("validity_mask_t: combine() on a pointer-constructed mask", "[validity-null-resource]") {
    auto resource = core::pmr::otterbrix_resource();
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
    REQUIRE(ptr_mask.data() != buffer);
    REQUIRE(buffer[0] == components::vector::validity_data_t::MAX_ENTRY);
}

TEST_CASE("validity_mask_t: slice() at non-zero offset on a pointer-constructed mask", "[validity-null-resource]") {
    auto resource = core::pmr::otterbrix_resource();
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
    REQUIRE(ptr_mask.data() != buffer);
    REQUIRE(buffer[0] == components::vector::validity_data_t::MAX_ENTRY);
}

// Three allocation sites once passed the row count straight through instead of entry_count(rows),
// over-allocating harmlessly -- it only surfaced as memset dominating the SSB query profile.
TEST_CASE("validity_mask_t allocates one entry per 64 rows, not one per row", "[validity-size]") {
    using namespace components::vector;

    resource_tracer_t tracer(std::pmr::new_delete_resource());
    constexpr uint64_t rows = 1024;
    const uint64_t expected = validity_data_t::entry_count(rows) * sizeof(uint64_t);

    {
        validity_mask_t mask(&tracer, rows);
        const auto allocated = tracer.total_allocated();
        INFO("bytes allocated for a " << rows << "-row mask: " << allocated << ", one entry per row would be "
                                      << rows * sizeof(uint64_t) << ", correct is " << expected);
        // Positive control: a tracer reading zero would satisfy any upper bound.
        REQUIRE(allocated > 0);
        CHECK(allocated <= expected);

        CHECK(mask.row_is_valid(rows - 1));
        mask.set_invalid(rows - 1);
        CHECK_FALSE(mask.row_is_valid(rows - 1));
        CHECK(mask.row_is_valid(0));
    }
}

// vector_t skips building a validity_mask_t when create_data is set (body just resets and discards it).
TEST_CASE("a data-creating vector_t allocates no validity mask", "[validity-size]") {
    using namespace components::vector;

    resource_tracer_t tracer(std::pmr::new_delete_resource());
    constexpr uint64_t capacity = 1024;

    {
        vector_t v(&tracer, components::types::logical_type::BIGINT, true, false, capacity);
        const auto allocated = tracer.total_allocated();
        const uint64_t data_bytes = capacity * sizeof(int64_t);
        INFO("bytes allocated by a data-creating BIGINT vector: " << allocated << ", the data alone is " << data_bytes);
        REQUIRE(allocated > 0);
        CHECK(allocated < data_bytes + validity_data_t::entry_count(capacity) * sizeof(uint64_t));
    }
}
