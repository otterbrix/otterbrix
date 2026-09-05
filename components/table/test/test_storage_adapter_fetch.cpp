// T1: table_storage_adapter_t::fetch used to swallow column_fetch_state::fetch_error.
//
// The adapter's fetch sets result_outlives_pins = true, which routes big strings to
// fetch_string_owned; when the overflow marker cannot be resolved that leg writes
// data_corruption into state.fetch_error — and the adapter's override was declared
// void, so nobody could read it. Before F1 this input called std::abort(); after F1
// it returned an EMPTY string quietly, which is worse: the live path
// (agent_disk_t::storage_fetch_inner -> storage->fetch) shipped the wrong answer
// across the mailbox as if the read had succeeded.
//
// These cases pin the fix: storage_t::fetch returns core::result_wrapper_t<bool>
// (the same shape fetch_next_batch already uses for scan_error), the adapter
// surfaces state.fetch_error through it, and the intact path still returns the
// exact bytes — owned, not borrowed from a pin that dies with the call.

#include <catch2/catch_test_macros.hpp>

#include <components/storage/table_storage_adapter.hpp>
#include <components/table/column_state.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/table_state.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/file/local_file_system.hpp>

#include <cstring>
#include <string>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    struct adapter_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        adapter_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    // Overwrites the block id named by the single big-string marker of `segment`.
    // Same layout surgery as test_big_strings.cpp: [uint32 dict_size][uint32 dict_end]
    // at the segment start, one 16-byte (uint64 block id, int64 offset) marker packed
    // at dict_end - dict_size. The REQUIREs make a layout change fail loudly.
    void overwrite_only_overflow_marker(adapter_env_t& env, column_segment_t& segment, uint64_t new_block_id) {
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

    struct built_table_t {
        std::unique_ptr<data_table_t> table;
        column_segment_t* payload_segment{nullptr};
    };

    // One-big-string table: (id BIGINT, payload STRING) with `big` in row 0.
    // Keeps a pointer to the payload column's segment so a case can corrupt its marker.
    built_table_t build_big_string_table(adapter_env_t& env, tstorage::block_manager_t& bm, const std::string& big) {
        built_table_t out;
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("payload", logical_type::STRING_LITERAL);
        out.table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "adapter_fetch");

        auto types = out.table->copy_types();
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, static_cast<int64_t>(0));
        chunk.set_value(1, 0, std::string_view{big});

        table_append_state state(&env.resource);
        REQUIRE_FALSE(out.table->append_lock(state).has_error());
        REQUIRE_FALSE(out.table->initialize_append(state).has_error());
        REQUIRE_FALSE(out.table->append(chunk, state).has_error());
        // The payload column's active segment, grabbed before finalize: the
        // segment object itself lives in the column's segment tree and stays valid.
        REQUIRE(state.append_state.states != nullptr);
        out.payload_segment = state.append_state.states[1].current;
        REQUIRE(out.payload_segment != nullptr);
        out.table->finalize_append(state, transaction_data{0, 0});
        return out;
    }

} // namespace

TEST_CASE("storage_adapter: fetch returns owned big-string bytes on the intact path") {
    adapter_env_t env;
    tstorage::in_memory_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);
    const std::string big(5000, 'q');
    auto built = build_big_string_table(env, bm, big);

    components::storage::table_storage_adapter_t adapter(*built.table, &env.resource);
    components::storage::storage_t& storage = adapter;

    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, 1);
    vector_t row_ids(&env.resource, logical_type::BIGINT, 1);
    row_ids.data<int64_t>()[0] = 0;

    auto fetch_r = storage.fetch(out, row_ids, 1, {});
    REQUIRE_FALSE(fetch_r.has_error());
    REQUIRE(out.size() == 1);
    const auto cell = out.value(1, 0); // named local: chunk.value() is a temporary
    REQUIRE(cell.value<std::string_view>() == big);
}

TEST_CASE("storage_adapter: a fetch failure reaches the storage caller as an error") {
    adapter_env_t env;
    tstorage::in_memory_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);
    const std::string big(5000, 'r');
    auto built = build_big_string_table(env, bm, big);

    // The exact shape of the original crash report: a transient-domain id the
    // block manager has never registered.
    overwrite_only_overflow_marker(env, *built.payload_segment, tstorage::MAXIMUM_BLOCK + 424242);

    components::storage::table_storage_adapter_t adapter(*built.table, &env.resource);
    components::storage::storage_t& storage = adapter;

    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, 1);
    vector_t row_ids(&env.resource, logical_type::BIGINT, 1);
    row_ids.data<int64_t>()[0] = 0;

    // Before the fix this was `void storage.fetch(...)` — the data_corruption the
    // string leg recorded had NO reader, and the caller shipped a silently EMPTY
    // payload as if the read had succeeded (RED run: `0 == 5000 (0x1388)`).
    auto fetch_r = storage.fetch(out, row_ids, 1, {});
    REQUIRE(fetch_r.has_error());
    REQUIRE(fetch_r.error().type == core::error_code_t::data_corruption);
}
