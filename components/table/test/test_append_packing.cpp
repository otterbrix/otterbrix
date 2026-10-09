// Blocks issued while appending, measured without the services stack. Rejected: a packer per
// append call -- one 256 KiB block per filled 16 KiB string segment (19 blocks here).

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <string>
#include <unistd.h>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    struct pk_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        pk_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::string payload_of(uint64_t row) {
        std::string s = "row_" + std::to_string(row) + "_";
        while (s.size() < 64) {
            s.push_back(static_cast<char>('a' + (row + s.size()) % 26));
        }
        return s;
    }

} // namespace

// 50-row statements, as INSERT ... VALUES appends.
TEST_CASE("append_packing: blocks issued while appending stay proportional to the payload", "[packing]") {
    const std::string path =
        "/tmp/test_otterbrix_append_packing_" + std::to_string(::getpid()) + ".otbx";
    std::remove(path.c_str());
    pk_env_t env;
    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    std::vector<column_definition_t> columns;
    columns.emplace_back("id", logical_type::BIGINT);
    columns.emplace_back("payload", logical_type::STRING_LITERAL);
    data_table_t table(&env.resource, bm, std::move(columns), "packing");
    auto types = table.copy_types();

    constexpr uint64_t ROWS = 4096;
    constexpr uint64_t BATCH = 50;
    uint64_t payload_bytes = 0;
    bm.dev_reset_tracking();
    for (uint64_t base = 0; base < ROWS; base += BATCH) {
        const uint64_t batch = std::min(ROWS - base, BATCH);
        data_chunk_t chunk(&env.resource, types, batch);
        chunk.set_cardinality(batch);
        std::vector<std::string> values;
        values.reserve(batch);
        for (uint64_t i = 0; i < batch; i++) {
            values.push_back(payload_of(base + i));
            payload_bytes += values.back().size();
            chunk.set_value(0, i, static_cast<int64_t>(base + i));
            chunk.set_value(1, i, std::string_view{values.back()});
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    const uint64_t issued = bm.dev_issued_ids().size();
    const uint64_t block = tstorage::DEFAULT_BLOCK_ALLOC_SIZE;
    INFO("[packing] " << ROWS << " rows, payload " << payload_bytes << " B: " << issued << " blocks issued");
    // The packed payload (strings + 8 B ids + offsets) fits twice the raw payload, plus the open tails.
    CHECK(issued * block <= 2 * payload_bytes + tstorage::partial_block_manager_t::MAX_OPEN_TAILS * block);

    std::remove(path.c_str());
}
