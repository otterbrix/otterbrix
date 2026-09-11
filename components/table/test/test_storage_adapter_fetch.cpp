// table_storage_adapter_t::fetch must not swallow column_fetch_state::fetch_error: with big strings
// routed through fetch_string_owned, an unresolved overflow marker writes data_corruption into
// state.fetch_error that a `void` fetch has nobody to read, so the caller ships an empty payload as
// success. Fix shape: storage_t::fetch returns core::result_wrapper_t<bool>, same as scan_error.

#include <catch2/catch_test_macros.hpp>

#include <cstdio>
#include <string>
#include <unistd.h>

#include <components/storage/table_storage_adapter.hpp>
#include <components/table/column_state.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
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

    const std::string& adapter_fetch_db_path() {
        static const std::string path =
            "/tmp/test_otterbrix_storage_adapter_fetch_" + std::to_string(::getpid()) + ".otbx";
        std::remove(path.c_str());
        return path;
    }

    struct adapter_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;
        // A real disk manager over a scratch file: what these tests need is a column without a
        // catalog, not a storage layer that cannot do I/O.
        tstorage::single_file_block_manager_t block_manager;

        adapter_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, adapter_fetch_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~adapter_env_t() { std::remove(adapter_fetch_db_path().c_str()); }
    };

    // Overwrites the block id named by the sole big-string marker: [dict_size][dict_end] header,
    // one 16-byte marker at dict_end - dict_size. The REQUIREs fail loudly if that layout changes.
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
        REQUIRE(named_block >= tstorage::MAXIMUM_BLOCK);
        std::memcpy(marker, &new_block_id, sizeof(uint64_t));
    }

    struct built_table_t {
        std::unique_ptr<data_table_t> table;
        column_segment_t* payload_segment{nullptr};
    };

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
        // Grabbed before finalize; stays valid since the segment lives in the column's segment tree.
        REQUIRE(state.append_state.states != nullptr);
        out.payload_segment = state.append_state.states[1].current;
        REQUIRE(out.payload_segment != nullptr);
        out.table->finalize_append(state, transaction_data{0, 0});
        return out;
    }

} // namespace

TEST_CASE("storage_adapter: fetch returns owned big-string bytes on the intact path") {
    adapter_env_t env;
    auto& bm = env.block_manager;
    const std::string big(5000, 'q');
    auto built = build_big_string_table(env, bm, big);

    components::storage::table_storage_adapter_t adapter(*built.table, &env.resource);
    components::storage::storage_t& storage = adapter;

    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, 1);
    vector_t row_ids(&env.resource, logical_type::BIGINT, 1);
    row_ids.data<int64_t>()[0] = 0;

    auto fetch_r = storage.fetch(out, row_ids, 1, {}, transaction_data{}, fetch_visibility_t::SNAPSHOT);
    REQUIRE_FALSE(fetch_r.has_error());
    REQUIRE(out.size() == 1);
    const auto cell = out.value(1, 0); // named local: chunk.value() is a temporary
    REQUIRE(cell.value<std::string_view>() == big);
}

TEST_CASE("storage_adapter: a fetch failure reaches the storage caller as an error") {
    adapter_env_t env;
    auto& bm = env.block_manager;
    const std::string big(5000, 'r');
    auto built = build_big_string_table(env, bm, big);

    // The shape of the original crash report: an unregistered transient-domain id.
    overwrite_only_overflow_marker(env, *built.payload_segment, tstorage::MAXIMUM_BLOCK + 424242);

    components::storage::table_storage_adapter_t adapter(*built.table, &env.resource);
    components::storage::storage_t& storage = adapter;

    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, 1);
    vector_t row_ids(&env.resource, logical_type::BIGINT, 1);
    row_ids.data<int64_t>()[0] = 0;

    auto fetch_r = storage.fetch(out, row_ids, 1, {}, transaction_data{}, fetch_visibility_t::SNAPSHOT);
    REQUIRE(fetch_r.has_error());
    REQUIRE(fetch_r.error().type == core::error_code_t::data_corruption);
}
