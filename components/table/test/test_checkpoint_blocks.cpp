// A checkpoint packs the segments of every column of the table into shared blocks: a table whose
// whole payload fits one block names ONE data block after its checkpoint, and a later round with a
// one-row change adds a block, not a block per column. Measured with a packer per column (100 rows
// x 32 INTEGER, 17 KB of payload): 67 blocks and a 17.6 MB file after the first checkpoint, 134
// blocks and 35 MB after a second round with one more row -- the live tail of every column (and of
// its validity child) took a dedicated 256 KiB block each round.

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <components/types/logical_value.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <set>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    constexpr uint64_t NCOLS = 32;

    void append_rows(data_table_t& table, std::pmr::memory_resource* resource, uint64_t first_row, uint64_t rows) {
        auto types = table.copy_types();
        data_chunk_t chunk(resource, types, rows);
        chunk.set_cardinality(rows);
        for (uint64_t col = 0; col < NCOLS; col++) {
            auto* data = chunk.data[col].data<int32_t>();
            for (uint64_t i = 0; i < rows; i++) {
                data[i] = static_cast<int32_t>((first_row + i) * 7 + col);
            }
        }
        table_append_state state(resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    core::error_t checkpoint_result(data_table_t& table, tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        if (auto cp = table.checkpoint(writer); cp.has_error()) {
            return cp.error();
        }
        if (auto flushed = writer.flush(); flushed.has_error()) {
            return flushed.error();
        }
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        if (free_ptr.has_error()) {
            return free_ptr.error();
        }
        if (auto synced = bm.file_sync(); synced.has_error()) {
            return synced.error();
        }
        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        if (auto written = bm.write_header(header); written.has_error()) {
            return written.error();
        }
        return core::error_t::no_error();
    }

    void checkpoint(data_table_t& table, tstorage::single_file_block_manager_t& bm) {
        REQUIRE_FALSE(checkpoint_result(table, bm).contains_error());
    }

    // Distinct data blocks the live segments name.
    size_t live_data_blocks(data_table_t& table) {
        std::set<uint64_t> ids;
        for (const auto& info : table.get_column_segment_info()) {
            if (info.segment_type == "PERSISTENT") {
                ids.insert(info.block_id);
            }
        }
        return ids.size();
    }

    uint64_t file_bytes(const std::string& path) {
        struct stat st {};
        REQUIRE(::stat(path.c_str(), &st) == 0);
        return static_cast<uint64_t>(st.st_size);
    }

    // --- the refusal case: 4 INTEGER columns and a STRING column with three big strings ---

    constexpr uint64_t MIXED_ROWS = 100;
    constexpr uint64_t MIXED_INTS = 4;
    // Above the dedicated-block threshold of the packer (0.8 x 256 KiB): each big string's
    // overflow record is written by place() itself, the root copy's and the live twin's alike.
    constexpr uint64_t BIG_STRING_BYTES = 210 * 1024;

    bool is_big_row(uint64_t row) { return row == 10 || row == 50 || row == 90; }

    std::string string_of(uint64_t row) {
        if (is_big_row(row)) {
            return std::string(BIG_STRING_BYTES, static_cast<char>('a' + row % 26));
        }
        return "s" + std::to_string(row);
    }

    std::unique_ptr<data_table_t> make_mixed_table(std::pmr::memory_resource* resource,
                                                   tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        for (uint64_t col = 0; col < MIXED_INTS; col++) {
            columns.emplace_back("c" + std::to_string(col), logical_type::INTEGER);
        }
        columns.emplace_back("s", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(resource, bm, std::move(columns), "checkpoint_refusal");
    }

    void append_mixed_rows(data_table_t& table, std::pmr::memory_resource* resource, uint64_t first_row, uint64_t rows) {
        auto types = table.copy_types();
        data_chunk_t chunk(resource, types, rows);
        chunk.set_cardinality(rows);
        for (uint64_t i = 0; i < rows; i++) {
            const uint64_t row = first_row + i;
            for (uint64_t col = 0; col < MIXED_INTS; col++) {
                chunk.set_value(col, i, static_cast<int32_t>(row * 7 + col));
            }
            const std::string s = string_of(row);
            chunk.set_value(MIXED_INTS, i, std::string_view{s});
        }
        table_append_state state(resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    // Rows seen; every cell checked against what was appended. The scan's error, if any, fails the check.
    uint64_t scan_mixed(data_table_t& table, std::pmr::memory_resource* resource) {
        std::vector<storage_index_t> column_ids;
        for (uint64_t col = 0; col <= MIXED_INTS; col++) {
            column_ids.emplace_back(col);
        }
        table_scan_state state(resource);
        table.initialize_scan(state, column_ids, transaction_data::committed(), nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t seen = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            if (state.table_state.has_error()) {
                INFO("scan error: " << state.table_state.scan_error.what);
                CHECK_FALSE(state.table_state.has_error());
                break;
            }
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                const uint64_t row = seen + i;
                for (uint64_t col = 0; col < MIXED_INTS; col++) {
                    CHECK(chunk.value(col, i).value<int32_t>() == static_cast<int32_t>(row * 7 + col));
                }
                const auto cell = chunk.value(MIXED_INTS, i);
                const auto expected = string_of(row);
                CHECK(cell.value<std::string_view>() == expected);
            }
            seen += chunk.size();
        }
        return seen;
    }

    // One cut: the file refuses the `fail_at`-th write of the checkpoint and every later one
    // (0: a dry run that counts the writes). Returns the writes the checkpoint attempted.
    uint64_t refusal_at(uint64_t fail_at) {
        const std::string path = "/tmp/test_otterbrix_checkpoint_refusal_" + std::to_string(::getpid()) + "." +
                                 std::to_string(fail_at) + ".otbx";
        std::remove(path.c_str());
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t pool(&resource, uint64_t(1) << 26, false, uint64_t(1) << 24);
        tstorage::standard_buffer_manager_t buffer_manager(&resource, fs, pool);
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);
        uint64_t attempted = 0;
        {
            tstorage::single_file_block_manager_t bm(buffer_manager, fs, path);
            REQUIRE_FALSE(bm.create_new_database().has_error());
            const uint64_t block_size = bm.block_size();
            auto table = make_mixed_table(&resource, bm);
            append_mixed_rows(*table, &resource, 0, MIXED_ROWS);
            REQUIRE(scan_mixed(*table, &resource) == MIXED_ROWS);

            const uint64_t blocks_before = bm.total_blocks();
            bm.dev_reset_tracking();
            const uint64_t writes_before = plan.writes_seen;
            if (fail_at != 0) {
                plan.fail_writes_from = writes_before + fail_at;
            }
            auto cp = checkpoint_result(*table, bm);
            attempted = plan.writes_seen - writes_before;
            plan.fail_writes_from = 0;
            if (fail_at == 0) {
                REQUIRE_FALSE(cp.contains_error());
                REQUIRE(scan_mixed(*table, &resource) == MIXED_ROWS);
                std::remove(path.c_str());
                return attempted;
            }
            INFO("refusal cut at write " << fail_at << " of " << attempted << " attempted");
            if (!cp.contains_error()) {
                // The last write of the round copies the root into the other header slot; the
                // header writer rightly tolerates its refusal (the root is durable in its own slot).
                CHECK(scan_mixed(*table, &resource) == MIXED_ROWS);
                std::remove(path.c_str());
                return attempted;
            }
            CHECK(cp.type == core::error_code_t::io_error);

            // What the disk manager does after a refused checkpoint (table_storage_t::checkpoint).
            const uint64_t rolled_back = bm.roll_back_uncommitted_round();

            // A live segment switched only if its block is on the file: it is registered (and so
            // kept by the rollback), and its bytes lie inside the file.
            size_t persistent = 0;
            size_t transient = 0;
            std::set<uint64_t> named;
            for (const auto& info : table->get_column_segment_info()) {
                if (info.segment_type == "PERSISTENT") {
                    persistent++;
                    named.insert(info.block_id);
                    for (auto extra : info.additional_blocks) {
                        named.insert(extra);
                    }
                } else {
                    transient++;
                }
            }
            const uint64_t file = file_bytes(path);
            for (uint64_t id : named) {
                INFO("block " << id << " named by a live segment");
                CHECK(bm.registry_alive(id));
                CHECK(file >= tstorage::BLOCK_START + (id + 1) * block_size);
            }

            // Every id issued in the round is either named by a live segment or back for reuse:
            // nothing of the round is stranded in the file.
            const auto reusable = bm.dev_reusable_snapshot();
            const auto pending = bm.dev_pending_free_snapshot();
            const uint64_t high_water = bm.total_blocks();
            std::set<uint64_t> issued(bm.dev_issued_ids().begin(), bm.dev_issued_ids().end());
            size_t kept = 0;
            size_t back = 0;
            size_t trimmed = 0;
            for (uint64_t id : issued) {
                INFO("issued block " << id);
                if (bm.registry_alive(id)) {
                    CHECK(named.count(id) != 0);
                    CHECK(reusable.count(id) == 0);
                    CHECK(pending.count(id) == 0);
                    kept++;
                } else if (id >= high_water) {
                    CHECK(reusable.count(id) == 0);
                    trimmed++;
                } else {
                    CHECK(reusable.count(id) != 0);
                    CHECK(pending.count(id) == 0);
                    back++;
                }
            }
            INFO("segments persistent=" << persistent << " transient=" << transient << " | issued=" << issued.size()
                                        << " back=" << back << " trimmed=" << trimmed
                                        << " rolled_back=" << rolled_back);
            CHECK(kept == named.size());
            // In use after the rollback: what was in use before plus the blocks the live segments
            // name. A released id below a kept one stays in the file as reusable (it cannot be
            // trimmed off the top), so the file's block count may exceed this, not its use.
            CHECK(bm.total_blocks() - bm.free_blocks() <= blocks_before + named.size());

            // The rows are still readable, from memory or from the blocks that did reach the file.
            CHECK(scan_mixed(*table, &resource) == MIXED_ROWS);
            append_mixed_rows(*table, &resource, MIXED_ROWS, 1);
            CHECK(scan_mixed(*table, &resource) == MIXED_ROWS + 1);
        }
        std::remove(path.c_str());
        return attempted;
    }

} // namespace

TEST_CASE("checkpoint: the columns of a small table share one data block, a one-row round adds one",
          "[checkpoint][packing]") {
    const std::string path = "/tmp/test_otterbrix_checkpoint_blocks_" + std::to_string(::getpid()) + ".otbx";
    std::remove(path.c_str());

    core::pmr::otterbrix_resource resource;
    core::filesystem::local_file_system_t fs;
    tstorage::buffer_pool_t pool(&resource, uint64_t(1) << 30, false, uint64_t(1) << 24);
    tstorage::standard_buffer_manager_t buffer_manager(&resource, fs, pool);
    tstorage::single_file_block_manager_t bm(buffer_manager, fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    const uint64_t block_size = bm.block_size();

    std::vector<column_definition_t> columns;
    for (uint64_t col = 0; col < NCOLS; col++) {
        columns.emplace_back("c" + std::to_string(col), logical_type::INTEGER);
    }
    data_table_t table(&resource, bm, std::move(columns), "checkpoint_blocks");

    // 100 rows x 32 INTEGER: 12.8 KB of values + 32 validity masks, far below one 256 KiB block.
    append_rows(table, &resource, 0, 100);
    checkpoint(table, bm);
    const uint64_t blocks_after_first = bm.total_blocks();
    INFO("[checkpoint_blocks] first checkpoint: " << blocks_after_first << " blocks, " << file_bytes(path)
                                                   << " B file, " << live_data_blocks(table) << " live data blocks");
    // The payload (root copy and live copy alike) fits one shared block; the rest is the root's
    // metadata and the free list.
    CHECK(live_data_blocks(table) <= 1);
    CHECK(blocks_after_first <= 4);
    CHECK(file_bytes(path) <= 5 * block_size);

    // A round with one more row: one more segment per column, packed together, not a block each.
    append_rows(table, &resource, 100, 1);
    checkpoint(table, bm);
    const uint64_t blocks_after_second = bm.total_blocks();
    INFO("[checkpoint_blocks] second checkpoint (+1 row): " << blocks_after_second << " blocks, "
                                                           << file_bytes(path) << " B file, "
                                                           << live_data_blocks(table) << " live data blocks");
    CHECK(live_data_blocks(table) <= 2);
    CHECK(blocks_after_second - blocks_after_first <= 4);
    REQUIRE(table.calculate_size() == 101);

    std::remove(path.c_str());
}

TEST_CASE("checkpoint: a write refused in the middle of a table checkpoint leaves the unwritten tails transient "
          "and the round's blocks to the rollback",
          "[checkpoint][packing][refusal]") {
    const uint64_t writes = refusal_at(0);
    REQUIRE(writes >= 1);
    for (uint64_t n = 1; n <= writes; n++) {
        refusal_at(n);
    }
}

#ifdef DEV_MODE
namespace {

    // One column of each shape: flat, string, and the three nested ones with their validity and element children.
    enum class shape_t
    {
        INTEGER,
        VARCHAR,
        LIST,
        STRUCT,
        ARRAY
    };

    constexpr uint64_t NESTED_WIDTH = 3;

    const char* shape_name(shape_t shape) {
        switch (shape) {
            case shape_t::INTEGER:
                return "INTEGER";
            case shape_t::VARCHAR:
                return "VARCHAR";
            case shape_t::LIST:
                return "LIST(UBIGINT)";
            case shape_t::STRUCT:
                return "STRUCT(BIGINT, VARCHAR)";
            case shape_t::ARRAY:
                return "ARRAY(UBIGINT, 3)";
        }
        return "?";
    }

    complex_logical_type pair_type(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::BIGINT, "num");
        fields.emplace_back(logical_type::STRING_LITERAL, "name");
        return complex_logical_type::create_struct("pair", fields);
    }

    complex_logical_type shape_type(shape_t shape, std::pmr::memory_resource* resource) {
        switch (shape) {
            case shape_t::INTEGER:
                return logical_type::INTEGER;
            case shape_t::VARCHAR:
                return logical_type::STRING_LITERAL;
            case shape_t::LIST:
                return complex_logical_type::create_list(logical_type::UBIGINT);
            case shape_t::STRUCT:
                return pair_type(resource);
            case shape_t::ARRAY:
                return complex_logical_type::create_array(logical_type::UBIGINT, NESTED_WIDTH);
        }
        return logical_type::INTEGER;
    }

    void append_shaped_rows(data_table_t& table,
                            shape_t shape,
                            std::pmr::memory_resource* resource,
                            uint64_t first_row,
                            uint64_t rows) {
        auto types = table.copy_types();
        data_chunk_t chunk(resource, types, rows);
        chunk.set_cardinality(rows);
        for (uint64_t i = 0; i < rows; i++) {
            const uint64_t row = first_row + i;
            switch (shape) {
                case shape_t::INTEGER:
                    chunk.set_value(0, i, static_cast<int32_t>(row));
                    break;
                case shape_t::VARCHAR: {
                    const std::string s = "s" + std::to_string(row);
                    chunk.set_value(0, i, std::string_view{s});
                    break;
                }
                case shape_t::LIST:
                case shape_t::ARRAY: {
                    std::vector<uint64_t> elements;
                    for (uint64_t j = 0; j < NESTED_WIDTH; j++) {
                        elements.push_back(row * 10 + j);
                    }
                    chunk.set_value(0, i, elements);
                    break;
                }
                case shape_t::STRUCT: {
                    std::vector<logical_value_t> members;
                    members.emplace_back(resource, static_cast<int64_t>(row));
                    members.emplace_back(resource, "name_" + std::to_string(row));
                    chunk.set_value(0, i, logical_value_t::create_struct(resource, types[0], members));
                    break;
                }
            }
        }
        table_append_state state(resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    // Every segment of the table's column trees, by (row group, column path, index), that is in memory.
    std::set<std::string> transient_segments(data_table_t& table) {
        std::set<std::string> keys;
        for (const auto& info : table.get_column_segment_info()) {
            if (info.segment_type == "TRANSIENT" && info.segment_count > 0) {
                keys.insert(std::to_string(info.row_group_index) + info.column_path + "#" +
                            std::to_string(info.segment_idx));
            }
        }
        return keys;
    }

} // namespace

// The checkpoint hands the packer one placement per live segment it switches to the file; a second
// placement of the same segment is never adopted, and its bytes sit in a block the root names
// under nobody's segment. Measured with a validity child placed by its own checkpoint() AND by
// its parent's transition_to_disk(): 128 B per 1024 rows per standard column and per round.
TEST_CASE("checkpoint: every live segment of a column tree is placed once", "[checkpoint][packing]") {
    for (auto shape : {shape_t::INTEGER, shape_t::VARCHAR, shape_t::LIST, shape_t::STRUCT, shape_t::ARRAY}) {
        const std::string path = "/tmp/test_otterbrix_checkpoint_placements_" + std::to_string(::getpid()) + ".otbx";
        std::remove(path.c_str());

        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t pool(&resource, uint64_t(1) << 30, false, uint64_t(1) << 24);
        tstorage::standard_buffer_manager_t buffer_manager(&resource, fs, pool);
        tstorage::single_file_block_manager_t bm(buffer_manager, fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());

        std::vector<column_definition_t> columns;
        columns.emplace_back("c", shape_type(shape, &resource));
        data_table_t table(&resource, bm, std::move(columns), "checkpoint_placements");

        for (uint64_t round = 1; round <= 2; round++) {
            append_shaped_rows(table, shape, &resource, round == 1 ? 0 : 100, round == 1 ? 100 : 1);
            const auto before = transient_segments(table);
            reset_transitions_with_live_pin();
            checkpoint(table, bm);
            const auto after = transient_segments(table);
            uint64_t switched = 0;
            for (const auto& key : before) {
                if (!after.contains(key)) {
                    ++switched;
                }
            }
            INFO(shape_name(shape) << ", round " << round << ": " << before.size() << " transient segments, "
                                   << switched << " switched, " << segment_placements() << " placements, "
                                   << segment_transitions() << " adopted");
            CHECK(segment_transitions() == switched);
            CHECK(segment_placements() == switched);
        }
        REQUIRE(table.calculate_size() == 101);
        std::remove(path.c_str());
    }
}
#endif
