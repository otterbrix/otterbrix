// Round-trip tests for the Unified Format (OTSC1.0) spill serializer, covering
// the FLAT fixed-width path plus the STRING / NULL / LIST / nested aux paths.
//
// The STRING/NULL/LIST cases guard against two subtle serializer bugs:
//   B1: the writer must serialize auxiliary_ and child vectors, not just
//      vec.get_buffer()->data(). For STRING the in-vector payload is a 16-byte
//      string_view whose chars live in auxiliary_; for LIST data_ holds
//      list_entry_t while the element values live in a nested child vector. If
//      auxiliary_/children are dropped, deserialized values dangle/garbage.
//   B2: the null mask is written between MVCC and column data; the reader must
//      advance past it before reading the trailer (total_size + checksum),
//      otherwise the CRC is computed over the wrong bytes and deserialize fails
//      for ANY input that contains a NULL.

#include <catch2/catch.hpp>

#include <components/table/storage/file_buffer.hpp>
#include <components/table/storage/unified_format.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <core/date/date_types.hpp>

#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace {

    // pmr resource shared across cases (R8: pmr everywhere).
    std::pmr::synchronized_pool_resource& shared_resource() {
        static std::pmr::synchronized_pool_resource resource;
        return resource;
    }

    // A canonical header used by every case. Only table_oid / column_count /
    // row_count / row_group_count are read back by deserialize_unified; the
    // snapshot fields are written verbatim and not validated here.
    components::table::storage::unified_format_header make_header(uint32_t table_oid,
                                                                  uint32_t column_count,
                                                                  uint64_t row_count) {
        using namespace components::table::storage;
        unified_format_header h{};
        std::memcpy(h.magic, "OTSC1.0", 8);
        h.version = 1;
        h.reserved0 = 0;
        h.snapshot_horizon = 100;
        h.min_visible_commit_id = 50;
        h.max_visible_commit_id = 200;
        h.table_oid = table_oid;
        h.column_count = column_count;
        h.row_count = row_count;
        h.row_group_count = (row_count + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;
        return h;
    }

    // TINY_BUFFER has header_size == 0 (file_buffer.cpp calculate_memory), so
    // internal_buffer() and buffer() alias and serialize_unified's use of
    // internal_buffer() lines up with the bytes it writes.
    components::table::storage::file_buffer_t make_buffer(uint64_t size) {
        return components::table::storage::file_buffer_t(&shared_resource(),
                                                         components::table::storage::file_buffer_type::TINY_BUFFER,
                                                         size);
    }

    // Build a single-column type list on the pmr resource. pmr::vector takes the
    // initializer-list first, allocator second.
    std::pmr::vector<components::types::complex_logical_type>
    single_col_type(components::types::complex_logical_type type) {
        return std::pmr::vector<components::types::complex_logical_type>({std::move(type)}, &shared_resource());
    }

} // namespace

// ---------------------------------------------------------------------------
// FLAT fixed-width path round-trip (INT, DOUBLE).
// ---------------------------------------------------------------------------

TEST_CASE("unified_format: round-trip INT column (FLAT path works)", "[unified_format][green]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 4;
    std::pmr::vector<types::complex_logical_type> types =
        single_col_type(types::complex_logical_type(types::logical_type::INTEGER));
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        chunk.set_value(0, i, types::logical_value_t(resource, static_cast<int32_t>(1000 + i)));
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/1, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    auto se = components::table::storage::serialize_unified(chunk, buffer, header);
    REQUIRE_FALSE(se.contains_error());

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);
    REQUIRE(roundtrip.column_count() == 1);

    for (uint64_t i = 0; i < N; ++i) {
        auto v = roundtrip.value(0, i);
        REQUIRE(v.type().type() == types::logical_type::INTEGER);
        REQUIRE(v.value<int32_t>() == static_cast<int32_t>(1000 + i));
    }
}

TEST_CASE("unified_format: round-trip DOUBLE column (FLAT path works)", "[unified_format][green]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 4;
    std::pmr::vector<types::complex_logical_type> types =
        single_col_type(types::complex_logical_type(types::logical_type::DOUBLE));
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        chunk.set_value(0, i, types::logical_value_t(resource, 1.5 * static_cast<double>(i + 1)));
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/2, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    auto se = components::table::storage::serialize_unified(chunk, buffer, header);
    REQUIRE_FALSE(se.contains_error());

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);

    for (uint64_t i = 0; i < N; ++i) {
        auto v = roundtrip.value(0, i);
        REQUIRE(v.type().type() == types::logical_type::DOUBLE);
        REQUIRE(v.value<double>() == Approx(1.5 * static_cast<double>(i + 1)));
    }
}

// ---------------------------------------------------------------------------
// Aux-path round-trips (STRING / NULL / LIST). Each documents the concrete
// serializer bug it guards against.
// ---------------------------------------------------------------------------

// B1 (STRING): a STRING value is a 16-byte string_view in data_ whose chars live
// in auxiliary_. The serializer must round-trip auxiliary_ — if the writer copies
// only get_buffer()->data() (the 16-byte views) and the reader builds a vector_t
// with an empty auxiliary_ heap, the deserialized views dangle into the SOURCE
// chunk's auxiliary heap.
//
// To make that dangling read observable we back the source chunk with a private
// monotonic_buffer_resource, then memset that backing buffer with 'Z' after the
// source is destroyed. A correct implementation rebuilds auxiliary_ so the
// round-tripped strings live in the new chunk's own heap; a dropped auxiliary_
// resolves the views into the poisoned buffer and the values come back as
// "ZZZ..." garbage.
TEST_CASE("unified_format: round-trip STRING column (RED B1: auxiliary dropped)", "[unified_format][red]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 4;
    const std::string expected[4] = {"alpha", "bravo", "charlie", "delta"};
    auto header = make_header(/*table_oid=*/3, /*column_count=*/1, /*row_count=*/N);

    // Owned backing storage for the source chunk. Its lifetime outlives the
    // chunk so we can poison it after the chunk (and its auxiliary heap) are
    // gone. The arena is sized generously and the monotonic_buffer_resource is
    // built WITHOUT an upstream so every source-chunk allocation must come from
    // this block (no silent spill elsewhere) -- otherwise a dangling string_view
    // could resolve into untouched upstream memory and hide the B1 bug. 64 KiB
    // dwarfs the few short strings + small vectors here, so the arena never
    // overflows into the implicit default upstream.
    constexpr size_t SOURCE_ARENA = 1 << 16;
    auto source_arena = std::make_unique<std::byte[]>(SOURCE_ARENA);
    std::pmr::monotonic_buffer_resource source_resource(source_arena.get(), SOURCE_ARENA);

    auto buffer = make_buffer(/*generous upper bound*/ 4096);
    {
        std::pmr::vector<types::complex_logical_type> types =
            single_col_type(types::complex_logical_type(types::logical_type::STRING_LITERAL));
        vector::data_chunk_t chunk(&source_resource, types, N);
        for (uint64_t i = 0; i < N; ++i) {
            chunk.set_value(0, i, types::logical_value_t(&source_resource, expected[i]));
        }
        chunk.set_cardinality(N);

        buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));
        auto se = components::table::storage::serialize_unified(chunk, buffer, header);
        REQUIRE_FALSE(se.contains_error());
    } // chunk destroyed -> auxiliary_ string heap (in source_arena) no longer in use.

    // Poison the entire source arena. Any string_view still pointing into it
    // (the B1 bug) will now resolve to "ZZZ..." instead of the original text.
    std::memset(source_arena.get(), 'Z', SOURCE_ARENA);

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);

    for (uint64_t i = 0; i < N; ++i) {
        auto v = roundtrip.value(0, i);
        REQUIRE(v.type().type() == types::logical_type::STRING_LITERAL);
        std::string got{v.value<std::string_view>()};
        INFO("row " << i << ": expected=\"" << expected[i] << "\", got=\"" << got << "\"");
        REQUIRE(got == expected[i]);
    }
}

// B2 (NULLs): the writer emits the null mask between MVCC and column data; the
// reader must advance past it before reading the trailer (total_size +
// checksum). If the reader skips the mask without advancing its pointer, the
// trailer is parsed from inside the null-mask bytes, the recomputed CRC cannot
// match, and deserialize_unified fails for any input that contains a NULL.
TEST_CASE("unified_format: round-trip INT column with NULLs (RED B2: null mask not read)", "[unified_format][red]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 4;
    std::pmr::vector<types::complex_logical_type> types =
        single_col_type(types::complex_logical_type(types::logical_type::INTEGER));
    vector::data_chunk_t chunk(resource, types, N);
    // Even rows are NULL, odd rows carry a value.
    for (uint64_t i = 0; i < N; ++i) {
        if (i % 2 == 0) {
            chunk.data[0].set_null(i, true);
        } else {
            chunk.set_value(0, i, types::logical_value_t(resource, static_cast<int32_t>(700 + i)));
        }
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/4, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    auto se = components::table::storage::serialize_unified(chunk, buffer, header);
    REQUIRE_FALSE(se.contains_error());

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    // deserialize must succeed and the per-row validity must survive the round-trip.
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);

    for (uint64_t i = 0; i < N; ++i) {
        INFO("row " << i << " (expected " << (i % 2 == 0 ? "NULL" : "value") << ")");
        REQUIRE(roundtrip.data[0].is_null(i) == (i % 2 == 0));
        if (i % 2 != 0) {
            auto v = roundtrip.value(0, i);
            REQUIRE(v.value<int32_t>() == static_cast<int32_t>(700 + i));
        }
    }
}

// B1 (LIST): a LIST column stores list_entry_t {offset,length} in data_ and the
// element values in a nested child vector held off the parent. The serializer
// must round-trip that child — if the writer copies only data_ (16 bytes per
// row), the element values are lost and the deserialized lists are wrong.
TEST_CASE("unified_format: round-trip LIST<UBIGINT> column (RED B1: child dropped)", "[unified_format][red]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 3;
    // Element payloads per row. create_list takes a std::vector<logical_value_t>
    // (logical_value.hpp:74); this mirrors the construction idiom in test_column.cpp.
    const uint64_t elems[3][3] = {{1, 2, 3}, {10, 20, 0}, {100, 0, 0}};
    const size_t elem_count[3] = {3, 2, 1};

    std::pmr::vector<types::complex_logical_type> types =
        single_col_type(types::complex_logical_type::create_list(types::logical_type::UBIGINT));
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        std::vector<types::logical_value_t> list;
        list.reserve(elem_count[i]);
        for (size_t j = 0; j < elem_count[i]; ++j) {
            list.emplace_back(resource, elems[i][j]);
        }
        chunk.set_value(0, i, types::logical_value_t::create_list(resource, types::logical_type::UBIGINT, list));
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/5, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    auto se = components::table::storage::serialize_unified(chunk, buffer, header);
    REQUIRE_FALSE(se.contains_error());

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);

    for (uint64_t i = 0; i < N; ++i) {
        auto v = roundtrip.value(0, i);
        REQUIRE(v.type().type() == types::logical_type::LIST);
        const auto& children = v.children();
        REQUIRE(children.size() == elem_count[i]);
        for (size_t j = 0; j < elem_count[i]; ++j) {
            INFO("row " << i << " elem " << j << ": expected=" << elems[i][j]
                       << ", got=" << children[j].value<uint64_t>());
            REQUIRE(children[j].value<uint64_t>() == elems[i][j]);
        }
    }
}

// ---------------------------------------------------------------------------
// Types the codec cannot faithfully serialize must be rejected loudly, and a
// corrupt trailer must fail without an out-of-bounds read.
// ---------------------------------------------------------------------------

// INTERVAL (physical STRUCT): the frame codec's write_type/read_type do not
// carry the fixed child layout an interval relies on, so a reload would lose the
// payload. Rather than corrupt the data, serialize_unified must reject any column
// whose type the codec cannot faithfully handle and report ok=false (R6: no
// silent fallback).
TEST_CASE("unified_format: INTERVAL column is rejected loudly (finding #1)", "[unified_format][red]") {
    using namespace components;
    using core::date::days;
    using core::date::interval_t;
    using core::date::microseconds;
    using core::date::months;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 3;
    std::pmr::vector<types::complex_logical_type> types =
        single_col_type(types::complex_logical_type(types::logical_type::INTERVAL));
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        chunk.set_value(0,
                        i,
                        types::logical_value_t{resource,
                                               interval_t{microseconds{static_cast<int64_t>(i) * 1000LL},
                                                          days{static_cast<int32_t>(i)},
                                                          months{static_cast<int32_t>(i % 12)}}});
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/6, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    // The codec cannot faithfully store INTERVAL, so it must fail loudly.
    REQUIRE(components::table::storage::serialize_unified(chunk, buffer, header).contains_error());
}

// ENUM is physically INT32, so the flat path round-trips the codes but
// write_type/read_type drop the dictionary (code->name mapping). The reloaded
// column would therefore be an ENUM with no dictionary -- unusable.
// serialize_unified must reject ENUM with ok=false (same fail-loud policy).
TEST_CASE("unified_format: ENUM column is rejected loudly (finding #2)", "[unified_format][red]") {
    using namespace components;
    auto* resource = &shared_resource();

    std::vector<types::logical_value_t> entries;
    {
        types::logical_value_t red(resource, static_cast<int32_t>(0));
        red.set_alias("red");
        types::logical_value_t green(resource, static_cast<int32_t>(1));
        green.set_alias("green");
        entries.push_back(std::move(red));
        entries.push_back(std::move(green));
    }
    auto enum_type = types::complex_logical_type::create_enum("color", std::move(entries));

    constexpr uint64_t N = 2;
    std::pmr::vector<types::complex_logical_type> types = single_col_type(enum_type);
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        chunk.set_value(0, i, types::logical_value_t::create_enum(resource, enum_type, static_cast<int32_t>(i)));
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/7, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    // ENUM drops its dictionary through the codec, so it must be rejected loudly.
    REQUIRE(components::table::storage::serialize_unified(chunk, buffer, header).contains_error());
}

// deserialize_unified reads an untrusted total_size from the trailer and feeds it
// straight into absl::ComputeCrc32c({base, total_size}). A corrupt/huge
// total_size makes the CRC read out of bounds (crash / ASAN). deserialize must
// bound total_size against the buffer before hashing and fail cleanly. This test
// serializes a valid chunk, corrupts ONLY the total_size field, and asserts
// deserialize fails without crashing.
TEST_CASE("unified_format: corrupt trailer total_size fails without OOB (finding #3)", "[unified_format][red]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 4;
    std::pmr::vector<types::complex_logical_type> types =
        single_col_type(types::complex_logical_type(types::logical_type::INTEGER));
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        chunk.set_value(0, i, types::logical_value_t(resource, static_cast<int32_t>(2000 + i)));
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/8, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    auto se = components::table::storage::serialize_unified(chunk, buffer, header);
    REQUIRE_FALSE(se.contains_error());

    // Trailer layout: [uint64 total_size][uint32 checksum][uint32 reserved] in
    // the last TRAILER_SIZE (16) bytes. Overwrite total_size with a huge value
    // that would drive ComputeCrc32c far past the end of the buffer.
    const size_t sz = buffer.size();
    REQUIRE(sz >= 16);
    std::byte* raw = buffer.internal_buffer();
    const uint64_t huge = static_cast<uint64_t>(sz) * 1024ULL + 1'000'000ULL;
    std::memcpy(raw + (sz - 16), &huge, sizeof(huge)); // little-endian host

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE(de.has_error()); // clean failure, no crash / no OOB read
}

// ---------------------------------------------------------------------------
// Nested-column coverage for the LIST/ARRAY/STRUCT aux paths: a fixed-size
// ARRAY, a STRUCT with a STRING field, a LIST<STRING> (nested string heap), and
// a nested NULL row. These lock in existing coverage so it cannot weaken (R17).
// ---------------------------------------------------------------------------

// ARRAY<INTEGER> fixed-size: data_ carries no offsets (array stride is implicit
// in the type), so the child must be serialized as row_count*array_size dense
// elements. row r holds [r*10+0, r*10+1, r*10+2].
TEST_CASE("unified_format: round-trip ARRAY<INTEGER> fixed-size column", "[unified_format][nested]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 3;
    constexpr size_t ARRAY_SIZE = 3;
    std::pmr::vector<types::complex_logical_type> types =
        single_col_type(types::complex_logical_type::create_array(
            types::complex_logical_type(types::logical_type::INTEGER), ARRAY_SIZE));
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        std::vector<types::logical_value_t> arr;
        arr.reserve(ARRAY_SIZE);
        for (size_t j = 0; j < ARRAY_SIZE; ++j) {
            arr.emplace_back(resource, static_cast<int32_t>(i * 10 + j));
        }
        chunk.set_value(0,
                        i,
                        types::logical_value_t::create_array(
                            resource, types::complex_logical_type(types::logical_type::INTEGER), arr));
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/10, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    auto se = components::table::storage::serialize_unified(chunk, buffer, header);
    REQUIRE_FALSE(se.contains_error());

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);

    for (uint64_t i = 0; i < N; ++i) {
        auto v = roundtrip.value(0, i);
        REQUIRE(v.type().type() == types::logical_type::ARRAY);
        const auto& children = v.children();
        REQUIRE(children.size() == ARRAY_SIZE);
        for (size_t j = 0; j < ARRAY_SIZE; ++j) {
            INFO("row " << i << " elem " << j);
            REQUIRE(children[j].value<int32_t>() == static_cast<int32_t>(i * 10 + j));
        }
    }
}

// STRUCT{n:INTEGER, s:STRING}: the two fields are serialized as independent
// child frames (the STRING field recurses through the STRING heap path).
TEST_CASE("unified_format: round-trip STRUCT{INT,STRING} column", "[unified_format][nested]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 4;
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::INTEGER, "n");
    fields.emplace_back(types::logical_type::STRING_LITERAL, "s");
    auto struct_type = types::complex_logical_type::create_struct("rec", fields);

    std::pmr::vector<types::complex_logical_type> types = single_col_type(struct_type);
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        std::vector<types::logical_value_t> vals;
        vals.emplace_back(resource, static_cast<int32_t>(100 + i));
        vals.emplace_back(resource, std::string{"field_string_" + std::to_string(i)});
        chunk.set_value(0, i, types::logical_value_t::create_struct(resource, struct_type, vals));
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/11, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    auto se = components::table::storage::serialize_unified(chunk, buffer, header);
    REQUIRE_FALSE(se.contains_error());

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);

    for (uint64_t i = 0; i < N; ++i) {
        auto v = roundtrip.value(0, i);
        REQUIRE(v.type().type() == types::logical_type::STRUCT);
        const auto& children = v.children();
        REQUIRE(children.size() == 2);
        REQUIRE(children[0].value<int32_t>() == static_cast<int32_t>(100 + i));
        std::string got{children[1].value<std::string_view>()};
        INFO("row " << i << ": got struct string \"" << got << "\"");
        REQUIRE(got == ("field_string_" + std::to_string(i)));
    }
}

// LIST<STRING>: the list child is itself a STRING column, so the nested string
// heap must be rebuilt into the destination chunk's own heap. As in the B1
// STRING test we back the source chunk with a private arena and poison it after
// the source is destroyed -- any string_view still pointing into the source heap
// (a dropped nested aux) resolves to 'Z' garbage.
TEST_CASE("unified_format: round-trip LIST<STRING> column (nested string heap rebuilt)", "[unified_format][nested]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 3;
    const std::vector<std::string> rows[3] = {{"alpha", "beta", "gamma"}, {"delta", "epsilon"}, {"zeta"}};
    auto header = make_header(/*table_oid=*/12, /*column_count=*/1, /*row_count=*/N);

    constexpr size_t SOURCE_ARENA = 1 << 16;
    auto source_arena = std::make_unique<std::byte[]>(SOURCE_ARENA);
    std::pmr::monotonic_buffer_resource source_resource(source_arena.get(), SOURCE_ARENA);

    auto buffer = make_buffer(/*placeholder*/ 4096);
    {
        std::pmr::vector<types::complex_logical_type> types =
            single_col_type(types::complex_logical_type::create_list(types::logical_type::STRING_LITERAL));
        vector::data_chunk_t chunk(&source_resource, types, N);
        for (uint64_t i = 0; i < N; ++i) {
            std::vector<types::logical_value_t> list;
            list.reserve(rows[i].size());
            for (const auto& s : rows[i]) {
                list.emplace_back(&source_resource, s);
            }
            chunk.set_value(0, i, types::logical_value_t::create_list(&source_resource, types::logical_type::STRING_LITERAL, list));
        }
        chunk.set_cardinality(N);

        buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));
        auto se = components::table::storage::serialize_unified(chunk, buffer, header);
        REQUIRE_FALSE(se.contains_error());
    } // source chunk destroyed -> its nested string heap is free to be poisoned.

    std::memset(source_arena.get(), 'Z', SOURCE_ARENA);

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);

    for (uint64_t i = 0; i < N; ++i) {
        auto v = roundtrip.value(0, i);
        REQUIRE(v.type().type() == types::logical_type::LIST);
        const auto& children = v.children();
        REQUIRE(children.size() == rows[i].size());
        for (size_t j = 0; j < rows[i].size(); ++j) {
            std::string got{children[j].value<std::string_view>()};
            INFO("row " << i << " elem " << j << ": expected=\"" << rows[i][j] << "\", got=\"" << got << "\"");
            REQUIRE(got == rows[i][j]);
        }
    }
}

// Nested NULL: a STRUCT column where one row is entirely NULL (set_null nulls the
// parent validity AND the field children). The parent null mask and the
// independent per-field null masks must all survive the round-trip.
TEST_CASE("unified_format: round-trip STRUCT column with a NULL row (nested validity)", "[unified_format][nested]") {
    using namespace components;
    auto* resource = &shared_resource();

    constexpr uint64_t N = 3;
    constexpr uint64_t NULL_ROW = 1;
    std::pmr::vector<types::complex_logical_type> fields(resource);
    fields.emplace_back(types::logical_type::INTEGER, "n");
    fields.emplace_back(types::logical_type::STRING_LITERAL, "s");
    auto struct_type = types::complex_logical_type::create_struct("rec", fields);

    std::pmr::vector<types::complex_logical_type> types = single_col_type(struct_type);
    vector::data_chunk_t chunk(resource, types, N);
    for (uint64_t i = 0; i < N; ++i) {
        if (i == NULL_ROW) {
            chunk.data[0].set_null(i, true);
            continue;
        }
        std::vector<types::logical_value_t> vals;
        vals.emplace_back(resource, static_cast<int32_t>(500 + i));
        vals.emplace_back(resource, std::string{"row_" + std::to_string(i)});
        chunk.set_value(0, i, types::logical_value_t::create_struct(resource, struct_type, vals));
    }
    chunk.set_cardinality(N);

    auto header = make_header(/*table_oid=*/13, /*column_count=*/1, /*row_count=*/N);
    auto buffer = make_buffer(components::table::storage::estimate_unified_size(chunk, header));

    auto se = components::table::storage::serialize_unified(chunk, buffer, header);
    REQUIRE_FALSE(se.contains_error());

    components::table::storage::unified_format_header out_header{};
    auto de = components::table::storage::deserialize_unified(buffer, resource, out_header);
    REQUIRE_FALSE(de.has_error());
    auto roundtrip = std::move(de.value());
    REQUIRE(roundtrip.size() == N);

    for (uint64_t i = 0; i < N; ++i) {
        INFO("row " << i << " (expected " << (i == NULL_ROW ? "NULL" : "value") << ")");
        REQUIRE(roundtrip.data[0].is_null(i) == (i == NULL_ROW));
        if (i != NULL_ROW) {
            auto v = roundtrip.value(0, i);
            const auto& children = v.children();
            REQUIRE(children.size() == 2);
            REQUIRE(children[0].value<int32_t>() == static_cast<int32_t>(500 + i));
            std::string got{children[1].value<std::string_view>()};
            REQUIRE(got == ("row_" + std::to_string(i)));
        }
    }
}
