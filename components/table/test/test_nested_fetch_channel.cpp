// The nested half of the fetch channel: column_fetch_state carries TWO fields that a nested
// column has to hand to the child that actually reads the bytes, and it used to hand over
// neither.
//
// A STRUCT node owns no segments: every byte of a struct cell is read by a child column, on a
// CHILD column_fetch_state. struct_column_data_t::fetch_row built each of those children with
// `std::make_unique<column_fetch_state>()` -- a default-constructed state, i.e.
// result_outlives_pins == false and a fetch_error nobody above ever read. LIST and ARRAY built
// their validity child the same way, and took their element leg through a local
// column_scan_state whose scan_error was equally unread. Two silent failures follow:
//
//   1. BORROWED VIEWS THAT OUTLIVE THEIR PINS. A caller whose result outlives the state says so
//      (table_storage_adapter_t::fetch and row_group_t's late-materialisation gather both set
//      result_outlives_pins = true) and the string leg then COPIES the bytes into the result's
//      own heap. The flag stopped at the struct: the field's child state said false, so
//      string_fetch_row wrote a view BORROWED from the pinned block -- and the pins live in the
//      child's `handles`, which die with the parent state the moment the fetch returns. The
//      caller keeps a chunk pointing into a block the pool is free to evict, spill and reload
//      at another address.
//
//   2. LOST data_corruption. A big string (>= DEFAULT_STRING_BLOCK_LIMIT = 4096 bytes) lives in
//      an overflow block, and A0b's rule is that only the segment's own registry may resolve
//      one: an unregistered id is a LOUD data_corruption, written into the reading state's
//      fetch_error. In a struct field that state was the child's, so the report died there and
//      the statement answered with a silently EMPTY field. T1
//      (test_storage_adapter_fetch.cpp) pinned exactly this for a FLAT column; the nested path
//      was still cut.
//
// ISOLATING THE COLUMN VIEW (the F6 lesson). B2 packing puts a nested child's segment in the
// same block as a flat column's, so a gate that also holds a flat string column can be answered
// by the flat column BY ACCIDENT and go green on unfixed code. Every table below therefore has
// exactly ONE column, a nested one, and the gates assert that the nested node owns NO top-level
// segment of its own (`current == nullptr`) -- so the only STRING segment in the whole table is
// the leaf the case corrupts, reachable only through the nested read path.

#include <catch2/catch_test_macros.hpp>

#include <components/storage/table_storage_adapter.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/storage/transient_block_manager.hpp>
#include <components/table/table_state.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/file/local_file_system.hpp>

#include <cstring>
#include <string>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    struct nested_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        nested_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    // The three shapes under test. Each is a SINGLE column so no flat column can answer for the
    // nested one (see the F6 note at the top).
    enum class shape_t
    {
        STRUCT_OF_STRING,        // s STRUCT(payload STRING)
        STRUCT_OF_STRUCT_STRING, // s STRUCT(inner STRUCT(payload STRING))  -- second level
        STRUCT_OF_LIST_STRING    // s STRUCT(items LIST(STRING))            -- second level, other kind
    };

    // The struct field the big string sits in (or under). Every field carries its own alias:
    // an unnamed struct field is refused by struct_column_data_t outright.
    complex_logical_type field_type(nested_env_t& env, shape_t shape) {
        switch (shape) {
            case shape_t::STRUCT_OF_STRING:
                return complex_logical_type(logical_type::STRING_LITERAL, "payload");
            case shape_t::STRUCT_OF_STRUCT_STRING: {
                std::pmr::vector<complex_logical_type> inner_fields(&env.resource);
                inner_fields.emplace_back(logical_type::STRING_LITERAL, "payload");
                return complex_logical_type::create_struct("payload_box", inner_fields, "inner");
            }
            case shape_t::STRUCT_OF_LIST_STRING:
                return complex_logical_type::create_list(complex_logical_type{logical_type::STRING_LITERAL}, "items");
        }
        return complex_logical_type{logical_type::NA};
    }

    complex_logical_type outer_struct_type(nested_env_t& env, shape_t shape) {
        std::pmr::vector<complex_logical_type> fields(&env.resource);
        fields.push_back(field_type(env, shape));
        return complex_logical_type::create_struct("row_box", fields);
    }

    logical_value_t one_row_value(nested_env_t& env, shape_t shape, const std::string& big) {
        auto outer = outer_struct_type(env, shape);
        auto field = field_type(env, shape);
        std::vector<logical_value_t> members;
        switch (shape) {
            case shape_t::STRUCT_OF_STRING:
                members.emplace_back(&env.resource, big);
                break;
            case shape_t::STRUCT_OF_STRUCT_STRING: {
                std::vector<logical_value_t> inner_members;
                inner_members.emplace_back(&env.resource, big);
                members.push_back(logical_value_t::create_struct(&env.resource, field, inner_members));
                break;
            }
            case shape_t::STRUCT_OF_LIST_STRING: {
                std::vector<logical_value_t> elements;
                elements.emplace_back(&env.resource, big);
                members.push_back(logical_value_t::create_list(&env.resource,
                                                               complex_logical_type{logical_type::STRING_LITERAL},
                                                               elements));
                break;
            }
        }
        return logical_value_t::create_struct(&env.resource, outer, members);
    }

    struct built_table_t {
        std::unique_ptr<data_table_t> table;
        // The segment that physically holds the big string: always a leaf STRING column BELOW
        // the struct, never a top-level one.
        column_segment_t* leaf_segment{nullptr};
    };

    // One row, one nested column, one big string at the bottom of it.
    built_table_t build(nested_env_t& env, tstorage::block_manager_t& bm, shape_t shape, const std::string& big) {
        built_table_t out;
        std::vector<column_definition_t> columns;
        columns.emplace_back("s", outer_struct_type(env, shape));
        out.table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "nested_fetch");

        auto types = out.table->copy_types();
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, one_row_value(env, shape, big));

        table_append_state state(&env.resource);
        REQUIRE_FALSE(out.table->append_lock(state).has_error());
        REQUIRE_FALSE(out.table->initialize_append(state).has_error());
        REQUIRE_FALSE(out.table->append(chunk, state).has_error());

        REQUIRE(state.append_state.states != nullptr);
        auto& struct_append = state.append_state.states[0];
        // THE ISOLATION GATE. A struct node owns no segments; if this were non-null the case
        // below could be corrupting (and reading) a top-level segment instead of a child's.
        REQUIRE(struct_append.current == nullptr);
        REQUIRE(struct_append.child_appends.size() == 2); // [0] whole-cell validity, [1] the one field
        auto* field_append = &struct_append.child_appends[1];
        if (shape != shape_t::STRUCT_OF_STRING) {
            // Second level: the field is itself nested (struct or list) and owns the leaf only
            // through ITS children -- child_appends[1] again ([0] is that node's validity).
            REQUIRE(field_append->child_appends.size() == 2);
            if (shape == shape_t::STRUCT_OF_STRUCT_STRING) {
                REQUIRE(field_append->current == nullptr); // an inner struct owns no segments either
            }
            field_append = &field_append->child_appends[1];
        }
        out.leaf_segment = field_append->current;
        REQUIRE(out.leaf_segment != nullptr);
        REQUIRE(out.leaf_segment->type.to_physical_type() == physical_type::STRING);
        out.table->finalize_append(state, transaction_data{0, 0});
        return out;
    }

    // Same layout surgery as test_big_strings.cpp / test_storage_adapter_fetch.cpp:
    // [uint32 dict_size][uint32 dict_end] at the segment start, one 16-byte
    // (uint64 block id, int64 offset) marker packed at dict_end - dict_size. The REQUIREs make a
    // layout change fail loudly instead of corrupting the wrong bytes.
    void overwrite_only_overflow_marker(nested_env_t& env, column_segment_t& segment, uint64_t new_block_id) {
        auto pinned = env.buffer_manager.pin(segment.block);
        REQUIRE_FALSE(pinned.has_error());
        auto* base = pinned.value().ptr() + segment.block_offset();
        uint32_t dict_size = 0;
        uint32_t dict_end = 0;
        std::memcpy(&dict_size, base, sizeof(uint32_t));
        std::memcpy(&dict_end, base + sizeof(uint32_t), sizeof(uint32_t));
        REQUIRE(dict_size == 16); // exactly one big string == exactly one 16-byte marker
        auto* marker = base + dict_end - dict_size;
        uint64_t named_block = 0;
        std::memcpy(&named_block, marker, sizeof(uint64_t));
        REQUIRE(named_block >= tstorage::MAXIMUM_BLOCK); // really a transient overflow id
        std::memcpy(marker, &new_block_id, sizeof(uint64_t));
    }

    // Stand-in for what the buffer pool is allowed to do to an UNPINNED block: reuse its memory.
    // A view the fetch OWNS (copied into the result's heap) does not notice; a view it BORROWED
    // reads the poison. This is the deterministic form of the eviction race -- an eviction-timed
    // test would be flaky in the direction that matters (passing while the defect is present).
    void poison_overflow_blocks(nested_env_t& env, column_segment_t& segment) {
        auto* raw_state = segment.segment_state();
        REQUIRE(raw_state != nullptr);
        auto& string_state = raw_state->cast<uncompressed_string_segment_state>();
        // Positive control on the fixture: the string really did go to an overflow block, so
        // poisoning it really does cover the bytes a borrowed view points at.
        REQUIRE_FALSE(string_state.overflow_blocks.empty());
        for (auto& entry : string_state.overflow_blocks) {
            REQUIRE(entry.second != nullptr);
            auto pinned = env.buffer_manager.pin(entry.second->block);
            REQUIRE_FALSE(pinned.has_error());
            std::memset(pinned.value().ptr(), 0x5A, entry.second->size);
        }
    }

    // The STRING leaf vector inside the fetched chunk, for each shape.
    std::string_view leaf_view(data_chunk_t& out, shape_t shape) {
        auto& struct_vec = out.data[0];
        auto& field = *struct_vec.entries()[0];
        switch (shape) {
            case shape_t::STRUCT_OF_STRING:
                return field.data<std::string_view>()[0];
            case shape_t::STRUCT_OF_STRUCT_STRING:
                return field.entries()[0]->data<std::string_view>()[0];
            case shape_t::STRUCT_OF_LIST_STRING:
                return field.entry().data<std::string_view>()[0];
        }
        return {};
    }

    core::result_wrapper_t<bool> fetch_row_zero(nested_env_t& env, data_table_t& table, data_chunk_t& out) {
        components::storage::table_storage_adapter_t adapter(table, &env.resource);
        components::storage::storage_t& storage = adapter;
        vector_t row_ids(&env.resource, logical_type::BIGINT, 1);
        row_ids.data<int64_t>()[0] = 0;
        // The row was appended at txn 0 and never deleted, so any snapshot sees it; the mode is
        // named explicitly because fetch_visibility_t has no default (C4b).
        return storage.fetch(out, row_ids, 1, {}, transaction_data{}, fetch_visibility_t::SNAPSHOT);
    }

} // namespace

// (2b) The same break on the SCAN leg, which a plain SELECT takes. row_group_t judges ONLY the
// top-level column_scan_state, and a struct's fields scan on child states, so a leaf's
// scan_error had no reader either — the field simply read back empty.
TEST_CASE("nested scan: a data_corruption raised under a struct stops the scan") {
    nested_env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);
    const std::string big(5000, 's');

    shape_t shape = shape_t::STRUCT_OF_STRING;
    SECTION("STRUCT(payload STRING)") { shape = shape_t::STRUCT_OF_STRING; }
    SECTION("STRUCT(inner STRUCT(payload STRING))") { shape = shape_t::STRUCT_OF_STRUCT_STRING; }
    SECTION("STRUCT(items LIST(STRING))") { shape = shape_t::STRUCT_OF_LIST_STRING; }

    auto built = build(env, bm, shape, big);
    components::storage::table_storage_adapter_t adapter(*built.table, &env.resource);
    components::storage::storage_t& storage = adapter;

    // Positive control on the fixture: intact, this very scan reads the string back.
    {
        std::pmr::vector<data_chunk_t> batches(&env.resource);
        auto ok = storage.scan_batched(batches, nullptr, -1, nullptr, transaction_data{});
        REQUIRE_FALSE(ok.has_error());
        REQUIRE(batches.size() == 1);
        REQUIRE(batches.front().size() == 1);
        REQUIRE(leaf_view(batches.front(), shape) == big);
    }

    overwrite_only_overflow_marker(env, *built.leaf_segment, tstorage::MAXIMUM_BLOCK + 424242);

    std::pmr::vector<data_chunk_t> batches(&env.resource);
    auto scanned = storage.scan_batched(batches, nullptr, -1, nullptr, transaction_data{});
    REQUIRE(scanned.has_error());
    REQUIRE(scanned.error().type == core::error_code_t::data_corruption);
}

// (2) The error channel. Corrupt the ONE overflow marker of the leaf STRING segment and the
// statement must fail. Before the fix the child's fetch_error had no reader and the adapter
// returned success with an empty field (RED run: the fetch reported no error at all).
TEST_CASE("nested fetch: a data_corruption raised under a struct reaches the statement") {
    nested_env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);
    const std::string big(5000, 'n');

    shape_t shape = shape_t::STRUCT_OF_STRING;
    SECTION("STRUCT(payload STRING)") { shape = shape_t::STRUCT_OF_STRING; }
    SECTION("STRUCT(inner STRUCT(payload STRING))") { shape = shape_t::STRUCT_OF_STRUCT_STRING; }
    SECTION("STRUCT(items LIST(STRING))") { shape = shape_t::STRUCT_OF_LIST_STRING; }

    auto built = build(env, bm, shape, big);

    // Positive control on the fixture: intact, this very fetch reads the string back.
    {
        auto types = built.table->copy_types();
        data_chunk_t out(&env.resource, types, 1);
        auto ok = fetch_row_zero(env, *built.table, out);
        REQUIRE_FALSE(ok.has_error());
        REQUIRE(out.size() == 1);
        REQUIRE(leaf_view(out, shape) == big);
    }

    // The exact shape of the original crash report: a transient-domain id the block manager has
    // never registered.
    overwrite_only_overflow_marker(env, *built.leaf_segment, tstorage::MAXIMUM_BLOCK + 424242);

    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, 1);
    auto fetch_r = fetch_row_zero(env, *built.table, out);
    REQUIRE(fetch_r.has_error());
    REQUIRE(fetch_r.error().type == core::error_code_t::data_corruption);
}

// (1) The pin channel. The adapter sets result_outlives_pins because the chunk it fills is moved
// across a mailbox while its pins die with the call. Under a struct that promise was dropped, so
// the field held a view into a block nothing pins any more.
//
// Only the two STRUCT-under-STRUCT shapes are judged here: a LIST element is read by scan_count,
// and the bulk scan leg (string_scan_partial -> fetch_string_owned) has always interned into the
// result's heap, so a list element is owned whatever the flag says.
TEST_CASE("nested fetch: a big string in a struct field outlives the pins that read it") {
    nested_env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);
    const std::string big(5000, 'p');

    shape_t shape = shape_t::STRUCT_OF_STRING;
    SECTION("STRUCT(payload STRING)") { shape = shape_t::STRUCT_OF_STRING; }
    SECTION("STRUCT(inner STRUCT(payload STRING))") { shape = shape_t::STRUCT_OF_STRUCT_STRING; }

    auto built = build(env, bm, shape, big);

    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, 1);
    auto fetch_r = fetch_row_zero(env, *built.table, out);
    REQUIRE_FALSE(fetch_r.has_error());
    REQUIRE(out.size() == 1);

    // Every pin the fetch took is gone (they lived in the column_fetch_state the adapter
    // declared inside fetch). Whatever the chunk still points at is fair game for the pool.
    poison_overflow_blocks(env, *built.leaf_segment);

    // RED before the fix: 5000 bytes of 0x5A read back out of the struct field. Judged as a
    // bool so a failure reports "false" instead of dumping 5000 poison bytes into the log.
    const auto view = leaf_view(out, shape);
    const bool intact = view == std::string_view(big);
    INFO("field length " << view.size() << ", first byte '" << (view.empty() ? '?' : view.front()) << "'");
    CHECK(intact);
}
