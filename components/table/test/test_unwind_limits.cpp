#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <components/types/logical_value.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    struct env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        env_t()
            : buffer_pool(&resource, uint64_t(1) << 22, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::vector<tstorage::buffer_handle_t> exhaust_pool(env_t& env) {
        std::vector<tstorage::buffer_handle_t> held;
        while (true) {
            auto allocated = env.buffer_manager.allocate(tstorage::memory_tag::BASE_TABLE,
                                                         env.buffer_manager.block_size(),
                                                         true);
            if (allocated.has_error()) {
                break;
            }
            held.push_back(std::move(allocated.value()));
            REQUIRE(held.size() < 1024);
        }
        return held;
    }

    struct held_small_t {
        std::shared_ptr<tstorage::block_handle_t> block;
        tstorage::buffer_handle_t pin;
    };

    // Fills what the whole-block pins left with `size`-byte pins, then frees `keep` of them.
    std::vector<held_small_t> top_up_pool(env_t& env, uint64_t size, uint64_t keep) {
        std::vector<held_small_t> held;
        while (true) {
            auto block = env.buffer_manager.register_small_memory(tstorage::memory_tag::BASE_TABLE, size);
            if (block.has_error()) {
                break;
            }
            auto pinned = env.buffer_manager.pin(block.value());
            if (pinned.has_error()) {
                break;
            }
            held.push_back(held_small_t{std::move(block.value()), std::move(pinned.value())});
            REQUIRE(held.size() < 4096);
        }
        REQUIRE(held.size() >= keep);
        held.resize(held.size() - keep);
        return held;
    }

    struct row_t {
        int64_t k;
        logical_value_t v;
    };

    // Returns the rows seen; the scan's error (if any) lands in *error.
    uint64_t scan_rows(data_table_t& table, env_t& env, std::vector<row_t>* cells, core::error_t* error) {
        std::vector<storage_index_t> column_ids{storage_index_t(0), storage_index_t(1)};
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, transaction_data::committed(), nullptr);
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t seen = 0;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            if (state.table_state.has_error()) {
                if (error) {
                    *error = state.table_state.scan_error;
                }
                break;
            }
            if (chunk.size() == 0) {
                break;
            }
            if (cells) {
                for (uint64_t i = 0; i < chunk.size(); i++) {
                    cells->push_back(row_t{chunk.value(0, i).value<int64_t>(), chunk.value(1, i)});
                }
            }
            seen += chunk.size();
        }
        return seen;
    }

    const column_data_t& column_of(data_table_t& table, uint64_t index) {
        auto* row_group = table.row_group()->row_group_tree()->segment_at(0);
        REQUIRE(row_group != nullptr);
        const auto* column = row_group->column_identity(index);
        REQUIRE(column != nullptr);
        return *column;
    }

    std::unique_ptr<data_table_t>
    make_table(env_t& env, tstorage::single_file_block_manager_t& bm, complex_logical_type v_type) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("k", logical_type::BIGINT);
        columns.emplace_back("v", std::move(v_type));
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "unwind_limits");
    }

    data_chunk_t
    string_chunk(env_t& env, data_table_t& table, const std::vector<std::string>& payloads, int64_t first_k) {
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, payloads.size());
        chunk.set_cardinality(payloads.size());
        for (uint64_t i = 0; i < payloads.size(); i++) {
            chunk.set_value(0, i, first_k + static_cast<int64_t>(i));
            chunk.set_value(1, i, std::string_view{payloads[i]});
        }
        return chunk;
    }

    void committed_append(data_table_t& table, data_chunk_t& chunk, env_t& env) {
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    std::string db_path(const char* tag) {
        return "/tmp/test_otterbrix_unwind_limits_" + std::string{tag} + "_" + std::to_string(::getpid()) + ".otbx";
    }

    core::error_t checkpoint_production(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        if (auto cp = table.checkpoint(writer); cp.has_error()) {
            return cp.error();
        }
        if (auto fl = writer.flush(); fl.has_error()) {
            return fl.error();
        }
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        if (free_ptr.has_error()) {
            return free_ptr.error();
        }
        if (auto s = bm.file_sync(); s.has_error()) {
            return s.error();
        }
        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        if (auto h = bm.write_header(header); h.has_error()) {
            return h.error();
        }
        if (auto s = bm.file_sync(); s.has_error()) {
            return s.error();
        }
        return core::error_t::no_error();
    }

    // 1022 committed rows, then a 4-row chunk that crosses into the next row group while the pool
    // keeps only `free_small_pins` 4 KiB pins. The append is refused and the kept rows stay.
    void crossing_chunk_refused(const char* tag, uint64_t free_small_pins) {
        const std::string path = db_path(tag);
        std::remove(path.c_str());
        env_t env;
        const uint64_t kept_rows = DEFAULT_VECTOR_CAPACITY - 2;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.create_new_database().has_error());
            auto table = make_table(env, bm, complex_logical_type{logical_type::STRING_LITERAL});
            {
                auto chunk = string_chunk(env, *table, std::vector<std::string>(kept_rows, "kept"), 0);
                committed_append(*table, chunk, env);
            }
            {
                auto chunk = string_chunk(env, *table, {"a", "b", "c", "d"}, 1000000);
                table_append_state state(&env.resource);
                REQUIRE_FALSE(table->append_lock(state).has_error());
                REQUIRE_FALSE(table->initialize_append(state).has_error());
                auto held = exhaust_pool(env);
                auto small = top_up_pool(env, uint64_t(1) << 12, free_small_pins);
                auto appended = table->append(chunk, state);
                REQUIRE(appended.has_error());
                CHECK(appended.error().type == core::error_code_t::out_of_memory);
            }
            CHECK(table->row_group()->row_group_tree()->segment_at(1) == nullptr);
            CHECK(column_of(*table, 0).count() == kept_rows);
            CHECK(column_of(*table, 1).count() == kept_rows);
            std::vector<row_t> cells;
            core::error_t err = core::error_t::no_error();
            const auto seen = scan_rows(*table, env, &cells, &err);
            CHECK_FALSE(err.contains_error());
            CHECK(seen == kept_rows);
        }
        std::remove(path.c_str());
    }

    // L2 at one cut: 1022 committed rows in row group 1; a 4-row chunk crosses into row group 2, so
    // row group 1 is re-pointed at its close. The file refuses the `fail_at`-th write of the append
    // and every later one (0: none -- a dry run that only counts the writes). Returns the writes
    // the append attempted.
    uint64_t l2_at(uint64_t fail_at) {
        const std::string path = db_path("l2") + "." + std::to_string(fail_at);
        std::remove(path.c_str());
        env_t env;
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);
        const uint64_t kept_rows = DEFAULT_VECTOR_CAPACITY - 2;
        uint64_t attempted = 0;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.create_new_database().has_error());
            auto table = make_table(env, bm, complex_logical_type{logical_type::STRING_LITERAL});
            REQUIRE(table->row_group()->row_group_size() == DEFAULT_VECTOR_CAPACITY);
            {
                auto chunk = string_chunk(env, *table, std::vector<std::string>(kept_rows, "kept"), 0);
                committed_append(*table, chunk, env);
            }
            {
                core::error_t err = core::error_t::no_error();
                REQUIRE(scan_rows(*table, env, nullptr, &err) == kept_rows);
                REQUIRE_FALSE(err.contains_error());
            }
            const uint64_t writes_before = plan.writes_seen;
            if (fail_at != 0) {
                plan.fail_writes_from = writes_before + fail_at; // the disk is full from here on
            }
            auto chunk = string_chunk(env, *table, {"a", "b", "c", "d"}, 1000000);
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            auto appended = table->append(chunk, state);
            attempted = plan.writes_seen - writes_before;
            if (fail_at == 0) {
                REQUIRE_FALSE(appended.has_error());
                table->finalize_append(state, transaction_data::committed());
                std::remove(path.c_str());
                return attempted;
            }
            INFO("L2 cut at write " << fail_at << " of " << attempted << " attempted");
            REQUIRE(appended.has_error());
            INFO("refusal: " << appended.error().what);
            CHECK(appended.error().type == core::error_code_t::io_error);
            plan.fail_writes_from = 0; // space freed
            CHECK(table->row_group()->row_group_tree()->segment_at(1) == nullptr);
            CHECK(column_of(*table, 0).count() == kept_rows);
            CHECK(column_of(*table, 1).count() == kept_rows);
            std::vector<row_t> cells;
            core::error_t err = core::error_t::no_error();
            const auto seen = scan_rows(*table, env, &cells, &err);
            INFO("scan after the refusal: " << seen << " row(s), error: " << err.what);
            CHECK_FALSE(err.contains_error());
            CHECK(seen == kept_rows);
            if (cells.size() == kept_rows) {
                CHECK(cells.front().k == 0);
                CHECK(cells.front().v.value<std::string_view>() == "kept");
                CHECK(cells.back().k == static_cast<int64_t>(kept_rows) - 1);
                CHECK(cells.back().v.value<std::string_view>() == "kept");
            }
            // The latched durability error keeps the file at its last good root.
            auto cp = checkpoint_production(bm, *table);
            CHECK(cp.contains_error());
        }
        std::remove(path.c_str());
        return attempted;
    }

    // L3 at one cut: 100 committed rows in an open (transient) row group; a checkpoint re-points the
    // live tail and flushes it. The file refuses the `fail_at`-th write of the checkpoint and every
    // later one (0: dry run). Returns the writes the checkpoint attempted.
    uint64_t l3_at(uint64_t fail_at) {
        const std::string path = db_path("l3") + "." + std::to_string(fail_at);
        std::remove(path.c_str());
        env_t env;
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);
        const uint64_t rows = 100;
        uint64_t attempted = 0;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.create_new_database().has_error());
            auto table = make_table(env, bm, complex_logical_type{logical_type::STRING_LITERAL});
            {
                auto chunk = string_chunk(env, *table, std::vector<std::string>(rows, "kept"), 0);
                committed_append(*table, chunk, env);
            }
            {
                core::error_t err = core::error_t::no_error();
                REQUIRE(scan_rows(*table, env, nullptr, &err) == rows);
                REQUIRE_FALSE(err.contains_error());
            }
            const uint64_t writes_before = plan.writes_seen;
            if (fail_at != 0) {
                plan.fail_writes_from = writes_before + fail_at;
            }
            auto cp = checkpoint_production(bm, *table);
            attempted = plan.writes_seen - writes_before;
            if (fail_at == 0) {
                REQUIRE_FALSE(cp.contains_error());
                std::remove(path.c_str());
                return attempted;
            }
            INFO("L3 cut at write " << fail_at << " of " << attempted << " attempted");
            // The refusal itself is not checked: the last write of a root is its copy into the other
            // header slot, which the header writer tolerates (the root is durable in its own slot).
            plan.fail_writes_from = 0;
            std::vector<row_t> cells;
            core::error_t err = core::error_t::no_error();
            const auto seen = scan_rows(*table, env, &cells, &err);
            INFO("scan after the refused checkpoint: " << seen << " row(s), error: " << err.what);
            CHECK_FALSE(err.contains_error());
            CHECK(seen == rows);
            if (cells.size() == rows) {
                CHECK(cells.front().v.value<std::string_view>() == "kept");
                CHECK(cells.back().k == static_cast<int64_t>(rows) - 1);
            }
            auto more = string_chunk(env, *table, {"after"}, 1000);
            committed_append(*table, more, env);
            cells.clear();
            err = core::error_t::no_error();
            const auto seen_after = scan_rows(*table, env, &cells, &err);
            CHECK_FALSE(err.contains_error());
            CHECK(seen_after == rows + 1);
        }
        std::remove(path.c_str());
        return attempted;
    }

    // L4 at one cut: two crossing appends share one open tail. Append B closes row group 1 into
    // tail T (written, committed); append C closes row group 2 into the SAME tail T, so T's slot
    // and range are rewritten over bytes that row group 1 already reads. The file refuses the
    // `fail_at`-th write of append C and every later one (0: dry run).
    uint64_t l4_at(uint64_t fail_at) {
        const std::string path = db_path("l4") + "." + std::to_string(fail_at);
        std::remove(path.c_str());
        env_t env;
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);
        const uint64_t rg = DEFAULT_VECTOR_CAPACITY;
        const uint64_t committed = rg + 2;
        uint64_t attempted = 0;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.create_new_database().has_error());
            auto table = make_table(env, bm, complex_logical_type{logical_type::STRING_LITERAL});
            REQUIRE(table->row_group()->row_group_size() == rg);
            {
                auto chunk = string_chunk(env, *table, std::vector<std::string>(rg - 2, "a"), 0);
                committed_append(*table, chunk, env);
            }
            {
                auto chunk = string_chunk(env, *table, {"b", "b", "b", "b"}, static_cast<int64_t>(rg) - 2);
                committed_append(*table, chunk, env);
            }
            {
                core::error_t err = core::error_t::no_error();
                REQUIRE(scan_rows(*table, env, nullptr, &err) == committed);
                REQUIRE_FALSE(err.contains_error());
            }
            const uint64_t writes_before = plan.writes_seen;
            if (fail_at != 0) {
                plan.fail_writes_from = writes_before + fail_at;
            }
            auto chunk = string_chunk(env, *table, std::vector<std::string>(rg, "c"), 2000);
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table->append_lock(state).has_error());
            REQUIRE_FALSE(table->initialize_append(state).has_error());
            auto appended = table->append(chunk, state);
            attempted = plan.writes_seen - writes_before;
            if (fail_at == 0) {
                REQUIRE_FALSE(appended.has_error());
                table->finalize_append(state, transaction_data::committed());
                std::remove(path.c_str());
                return attempted;
            }
            INFO("L4 cut at write " << fail_at << " of " << attempted << " attempted");
            REQUIRE(appended.has_error());
            CHECK(appended.error().type == core::error_code_t::io_error);
            plan.fail_writes_from = 0;
            CHECK(table->row_group()->row_group_tree()->segment_at(2) == nullptr);
            // The scan before the append loaded row group 1's block; the packer patches a resident
            // copy as it grows the tail, so the rows must be read back from the FILE to count.
            REQUIRE_FALSE(env.buffer_pool.set_limit(uint64_t(1) << 17).has_error());
            REQUIRE_FALSE(env.buffer_pool.set_limit(uint64_t(1) << 22).has_error());
            std::vector<row_t> cells;
            core::error_t err = core::error_t::no_error();
            const auto seen = scan_rows(*table, env, &cells, &err);
            INFO("scan after the refusal: " << seen << " row(s), error: " << err.what);
            CHECK_FALSE(err.contains_error());
            CHECK(seen == committed);
            if (cells.size() == committed) {
                CHECK(cells.front().k == 0);
                CHECK(cells.front().v.value<std::string_view>() == "a");
                CHECK(cells[rg - 1].v.value<std::string_view>() == "b");
                CHECK(cells.back().k == static_cast<int64_t>(committed) - 1);
                CHECK(cells.back().v.value<std::string_view>() == "b");
            }
        }
        std::remove(path.c_str());
        return attempted;
    }

} // namespace

// L0. The next row group cannot open its append: the pool refuses its transient segments.
TEST_CASE("unwind_limits: L0 a crossing chunk whose next row group cannot open its append", "[unwind_limits][l0]") {
    crossing_chunk_refused("l0", 0);
}

// L0b. Two 4 KiB pins free: the next row group opens, re-pointing the filled one at the disk is
// refused; 4 and more let the whole append through.
TEST_CASE("unwind_limits: L0b a crossing chunk whose filled row group cannot be re-pointed at the disk",
          "[unwind_limits][l0b]") {
    crossing_chunk_refused("l0b", 2);
}

TEST_CASE("unwind_limits: L2 a crossing chunk whose flush fails keeps the committed rows readable",
          "[unwind_limits][l2]") {
    const uint64_t writes = l2_at(0);
    REQUIRE(writes >= 1);
    for (uint64_t n = 1; n <= writes; n++) {
        l2_at(n);
    }
}

TEST_CASE("unwind_limits: L3 a checkpoint whose re-point flush fails keeps the committed rows readable",
          "[unwind_limits][l3]") {
    const uint64_t writes = l3_at(0);
    REQUIRE(writes >= 1);
    for (uint64_t n = 1; n <= writes; n++) {
        l3_at(n);
    }
}

// L4 at every cut: the second crossing append writes into the tail the first one's committed row
// group already reads.
TEST_CASE("unwind_limits: L4 a refused write into a shared tail keeps the earlier row group readable",
          "[unwind_limits][l4]") {
    const uint64_t writes = l4_at(0);
    REQUIRE(writes >= 1);
    for (uint64_t n = 1; n <= writes; n++) {
        l4_at(n);
    }
}
