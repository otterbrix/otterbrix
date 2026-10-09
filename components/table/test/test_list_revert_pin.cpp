// The revert of an append must not need memory the exhausted pool refuses.
//   R1..R2: the LIST unwind of a REFUSED append; reading the previous row's end offset would need a
//           pin of a disk-loaded offsets segment the same exhaustion evicted.
//   R3..R4: the transaction revert (data_table_t::revert_append after finalize_append), same read;
//           R4 reverts two ranges in reverse order, as a multi-statement rollback does.
//   R5:     a STRING/validity cut inside a transient segment; spilled under the exhaustion, a pin
//           of its block would need memory too.
//   C1..C2: STRUCT and ARRAY controls: no read in their unwind.

#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/storage/block_handle.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
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
            auto allocated =
                env.buffer_manager.allocate(tstorage::memory_tag::BASE_TABLE, env.buffer_manager.block_size(), true);
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

    // Fills what the whole-block pins left with `size`-byte pins.
    std::vector<held_small_t> top_up_pool(env_t& env, uint64_t size) {
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
        return held;
    }

    struct exhaustion_t {
        std::vector<tstorage::buffer_handle_t> blocks;
        std::vector<held_small_t> small;
        void release() {
            small.clear();
            blocks.clear();
        }
    };

    exhaustion_t exhaust(env_t& env) {
        exhaustion_t e;
        e.blocks = exhaust_pool(env);
        e.small = top_up_pool(env, uint64_t(1) << 12);
        return e;
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

    core::result_wrapper_t<std::unique_ptr<data_table_t>> reload_table(env_t& env,
                                                                       tstorage::single_file_block_manager_t& bm) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        return data_table_t::load_from_disk(&env.resource, bm, reader);
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
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "list_revert_pin");
    }

    data_chunk_t one_row(env_t& env, data_table_t& table, int64_t k, const logical_value_t& v) {
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, k);
        chunk.set_value(1, 0, v);
        return chunk;
    }

    logical_value_t
    list_of(env_t& env, const complex_logical_type& list_type, const std::vector<std::string>& elements) {
        std::vector<logical_value_t> values;
        for (const auto& e : elements) {
            values.emplace_back(&env.resource, std::string_view{e});
        }
        return logical_value_t::create_list_from_type(&env.resource, list_type, values);
    }

    void committed_append(data_table_t& table, data_chunk_t& chunk, env_t& env) {
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    // An uncommitted statement of one row: what storage_revert_appends_inner reverts later.
    void transaction_append(data_table_t& table, data_chunk_t& chunk, env_t& env, uint64_t txn_id) {
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{txn_id, txn_id});
    }

    std::string db_path(const char* tag) {
        return "/tmp/test_otterbrix_list_revert_pin_" + std::string{tag} + "_" + std::to_string(::getpid()) + ".otbx";
    }

    const std::string& big_payload() {
        static const std::string payload(1000, 'y');
        return payload;
    }

    // Seeds one committed row, checkpoints, reloads: the column's segments are disk-loaded.
    // Then a refused append (the pool is exhausted mid-append) must leave the table as it was.
    // `build(env, payload, repeat)` makes the "seed" row (k=1), the refused row (k=7) and the "ok" row
    // (k=8); `check_ok(cell)` checks the ok row's value.
    template<class Build, class CheckOk>
    void refused_after_reload(const char* tag,
                              const complex_logical_type& v_type,
                              const Build& build,
                              uint64_t refused_repeat,
                              const CheckOk& check_ok) {
        const std::string path = db_path(tag);
        std::remove(path.c_str());
        env_t env;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.create_new_database().has_error());
            auto table = make_table(env, bm, v_type);
            auto seed = one_row(env, *table, 1, build(env, "seed", 1));
            committed_append(*table, seed, env);
            REQUIRE_FALSE(checkpoint_production(bm, *table).contains_error());
        }
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.load_existing_database().has_error());
            auto loaded = reload_table(env, bm);
            REQUIRE_FALSE(loaded.has_error());
            auto& table = *loaded.value();
            {
                core::error_t err = core::error_t::no_error();
                REQUIRE(scan_rows(table, env, nullptr, &err) == 1);
                REQUIRE_FALSE(err.contains_error());
            }
            auto chunk = one_row(env, table, 7, build(env, big_payload(), refused_repeat));
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            auto held = exhaust(env);
            INFO("held whole-block pins: " << held.blocks.size() << ", 4 KiB pins: " << held.small.size());
            auto appended = table.append(chunk, state);
            REQUIRE(appended.has_error());
            INFO("refusal: " << appended.error().what);
            CHECK(appended.error().type == core::error_code_t::out_of_memory);
            held.release();

            CHECK(column_of(table, 1).count() == 1);
            auto good = one_row(env, table, 8, build(env, "ok", 1));
            committed_append(table, good, env);
            std::vector<row_t> cells;
            core::error_t err = core::error_t::no_error();
            REQUIRE(scan_rows(table, env, &cells, &err) == 2);
            INFO("scan error: " << err.what);
            REQUIRE_FALSE(err.contains_error());
            CHECK(cells[1].k == 8);
            check_ok(cells[1].v);

            auto cp = checkpoint_production(bm, table);
            INFO("checkpoint: " << cp.what);
            CHECK_FALSE(cp.contains_error());
        }
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE(!bm.load_existing_database().has_error());
            auto loaded = reload_table(env, bm);
            INFO("reload: " << (loaded.has_error() ? loaded.error().what : std::pmr::string{"ok"}));
            REQUIRE_FALSE(loaded.has_error());
            std::vector<row_t> cells;
            core::error_t err = core::error_t::no_error();
            REQUIRE(scan_rows(*loaded.value(), env, &cells, &err) == 2);
            REQUIRE_FALSE(err.contains_error());
            check_ok(cells[1].v);
        }
        std::remove(path.c_str());
    }

    void check_list_ok(const logical_value_t& cell) {
        INFO("the ok row has " << cell.children().size() << " element(s)");
        REQUIRE(cell.children().size() == 1);
        CHECK(*cell.children()[0].value<std::string*>() == "ok");
    }

} // namespace

// R1. LIST<STRING> reloaded: its offsets segment is disk-loaded. 40 x 1000-byte elements fill the
// element column's first transient segment; the second one is refused. Reading row 0's end offset
// in the unwind would need a pin of the evicted offsets segment, which the pool refuses as well.
TEST_CASE("list_revert_pin: R1 LIST<STRING> reloaded, refused append under pool exhaustion", "[list_revert_pin][r1]") {
    const auto list_type = complex_logical_type::create_list(logical_type::STRING_LITERAL);
    refused_after_reload(
        "r1",
        list_type,
        [&](env_t& env, const std::string& payload, uint64_t repeat) {
            return list_of(env, list_type, std::vector<std::string>(repeat, payload));
        },
        40,
        check_list_ok);
}

// R2. LIST<LIST<STRING>>: both LIST levels would read a stored offset in their unwind.
TEST_CASE("list_revert_pin: R2 LIST<LIST<STRING>> reloaded, refused append under pool exhaustion",
          "[list_revert_pin][r2]") {
    const auto inner_type = complex_logical_type::create_list(logical_type::STRING_LITERAL);
    const auto outer_type = complex_logical_type::create_list(inner_type);
    refused_after_reload(
        "r2",
        outer_type,
        [&](env_t& env, const std::string& payload, uint64_t repeat) {
            std::vector<logical_value_t> inner{list_of(env, inner_type, std::vector<std::string>(repeat, payload))};
            return logical_value_t::create_list_from_type(&env.resource, outer_type, inner);
        },
        40,
        [](const logical_value_t& cell) {
            REQUIRE(cell.children().size() == 1);
            check_list_ok(cell.children()[0]);
        });
}

// C1. STRUCT<STRING>: the unwind cuts a fresh transient segment per child (erased, no pin).
TEST_CASE("list_revert_pin: C1 STRUCT<STRING> reloaded, refused append under pool exhaustion (control)",
          "[list_revert_pin][c1]") {
    core::pmr::otterbrix_resource type_resource;
    std::pmr::vector<complex_logical_type> field_types(&type_resource);
    field_types.emplace_back(logical_type::STRING_LITERAL, "s");
    const auto struct_type = complex_logical_type::create_struct("rec", field_types, "rec_t");
    refused_after_reload(
        "c1",
        struct_type,
        [&](env_t& env, const std::string& payload, uint64_t repeat) {
            std::string big;
            for (uint64_t i = 0; i < repeat; i++) {
                big += payload;
            }
            std::vector<logical_value_t> fields{logical_value_t(&env.resource, std::string_view{big})};
            return logical_value_t::create_struct(&env.resource, struct_type, fields);
        },
        5,
        [](const logical_value_t& cell) {
            REQUIRE(cell.children().size() == 1);
            CHECK(cell.children()[0].value<std::string_view>() == "ok");
        });
}

// C2. ARRAY<STRING, 1>: same shape as C1 with the child addressed in elements.
TEST_CASE("list_revert_pin: C2 ARRAY<STRING> reloaded, refused append under pool exhaustion (control)",
          "[list_revert_pin][c2]") {
    const auto array_type = complex_logical_type::create_array(logical_type::STRING_LITERAL, 1);
    refused_after_reload(
        "c2",
        array_type,
        [&](env_t& env, const std::string& payload, uint64_t repeat) {
            std::string big;
            for (uint64_t i = 0; i < repeat; i++) {
                big += payload;
            }
            std::vector<logical_value_t> values{logical_value_t(&env.resource, std::string_view{big})};
            return logical_value_t::create_array(&env.resource, logical_type::STRING_LITERAL, values);
        },
        5,
        [](const logical_value_t& cell) {
            REQUIRE(cell.children().size() == 1);
            CHECK(cell.children()[0].value<std::string_view>() == "ok");
        });
}

// R3. The transaction path: a reloaded LIST<STRING> table takes an uncommitted row; the pool is
// exhausted when the transaction's revert runs (storage_revert_appends_inner ->
// data_table_t::revert_append).
TEST_CASE("list_revert_pin: R3 LIST<STRING> reloaded, transaction revert under pool exhaustion",
          "[list_revert_pin][r3]") {
    const std::string path = db_path("r3");
    std::remove(path.c_str());
    env_t env;
    const auto list_type = complex_logical_type::create_list(logical_type::STRING_LITERAL);
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm, list_type);
        auto seed = one_row(env, *table, 1, list_of(env, list_type, {"seed"}));
        committed_append(*table, seed, env);
        REQUIRE_FALSE(checkpoint_production(bm, *table).contains_error());
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        REQUIRE_FALSE(loaded.has_error());
        auto& table = *loaded.value();
        auto pending = one_row(env, table, 7, list_of(env, list_type, {"a", "b", "c"}));
        transaction_append(table, pending, env, 5);
        REQUIRE(column_of(table, 1).count() == 2);

        auto held = exhaust(env);
        INFO("held whole-block pins: " << held.blocks.size() << ", 4 KiB pins: " << held.small.size());
        auto reverted = table.revert_append(1, 1);
        INFO("revert: " << (reverted.has_error() ? reverted.error().what : std::pmr::string{"ok"}));
        CHECK_FALSE(reverted.has_error());
        held.release();

        CHECK(column_of(table, 1).count() == 1);
        auto good = one_row(env, table, 8, list_of(env, list_type, {"ok"}));
        committed_append(table, good, env);
        std::vector<row_t> cells;
        core::error_t err = core::error_t::no_error();
        REQUIRE(scan_rows(table, env, &cells, &err) == 2);
        INFO("scan error: " << err.what);
        REQUIRE_FALSE(err.contains_error());
        CHECK(cells[1].k == 8);
        check_list_ok(cells[1].v);

        auto cp = checkpoint_production(bm, table);
        CHECK_FALSE(cp.contains_error());
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        INFO("reload: " << (loaded.has_error() ? loaded.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(loaded.has_error());
        std::vector<row_t> cells;
        core::error_t err = core::error_t::no_error();
        REQUIRE(scan_rows(*loaded.value(), env, &cells, &err) == 2);
        REQUIRE_FALSE(err.contains_error());
        check_list_ok(cells[1].v);
    }
    std::remove(path.c_str());
}

// R4. A multi-statement rollback: two uncommitted one-row statements on a fresh (never reloaded)
// LIST<STRING> table, reverted in reverse order as storage_revert_appends_inner does, with the
// pool exhausted: the transient segments were spilled, so a read would need memory again.
TEST_CASE("list_revert_pin: R4 LIST<STRING> two statements reverted in reverse order under pool exhaustion",
          "[list_revert_pin][r4]") {
    const std::string path = db_path("r4");
    std::remove(path.c_str());
    env_t env;
    const auto list_type = complex_logical_type::create_list(logical_type::STRING_LITERAL);
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm, list_type);
        auto seed = one_row(env, *table, 1, list_of(env, list_type, {"seed"}));
        committed_append(*table, seed, env);
        auto first = one_row(env, *table, 7, list_of(env, list_type, {"a", "b"}));
        transaction_append(*table, first, env, 5);
        auto second = one_row(env, *table, 7, list_of(env, list_type, {"c", "d", "e"}));
        transaction_append(*table, second, env, 5);
        REQUIRE(column_of(*table, 1).count() == 3);

        auto held = exhaust(env);
        INFO("held whole-block pins: " << held.blocks.size() << ", 4 KiB pins: " << held.small.size());
        auto second_reverted = table->revert_append(2, 1);
        INFO(
            "second revert: " << (second_reverted.has_error() ? second_reverted.error().what : std::pmr::string{"ok"}));
        CHECK_FALSE(second_reverted.has_error());
        auto first_reverted = table->revert_append(1, 1);
        INFO("first revert: " << (first_reverted.has_error() ? first_reverted.error().what : std::pmr::string{"ok"}));
        CHECK_FALSE(first_reverted.has_error());
        held.release();

        CHECK(column_of(*table, 1).count() == 1);
        auto good = one_row(env, *table, 8, list_of(env, list_type, {"ok"}));
        committed_append(*table, good, env);
        std::vector<row_t> cells;
        core::error_t err = core::error_t::no_error();
        REQUIRE(scan_rows(*table, env, &cells, &err) == 2);
        INFO("scan error: " << err.what);
        REQUIRE_FALSE(err.contains_error());
        CHECK(cells[1].k == 8);
        check_list_ok(cells[1].v);

        auto cp = checkpoint_production(bm, *table);
        CHECK_FALSE(cp.contains_error());
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        INFO("reload: " << (loaded.has_error() ? loaded.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(loaded.has_error());
        std::vector<row_t> cells;
        core::error_t err = core::error_t::no_error();
        REQUIRE(scan_rows(*loaded.value(), env, &cells, &err) == 2);
        REQUIRE_FALSE(err.contains_error());
        check_list_ok(cells[1].v);
    }
    std::remove(path.c_str());
}

// R5. STRING: a committed row and an uncommitted row share one transient segment. The revert cuts
// inside the segment; rolling the dictionary back through a pin of its block would need memory, the
// block being spilled under the exhaustion.
TEST_CASE("list_revert_pin: R5 STRING transaction revert inside a spilled transient segment", "[list_revert_pin][r5]") {
    const std::string path = db_path("r5");
    std::remove(path.c_str());
    env_t env;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm, complex_logical_type{logical_type::STRING_LITERAL});
        auto seed = one_row(env, *table, 1, logical_value_t(&env.resource, std::string_view{"seed"}));
        committed_append(*table, seed, env);
        auto pending = one_row(env, *table, 7, logical_value_t(&env.resource, std::string_view{"rolled-back"}));
        transaction_append(*table, pending, env, 5);
        // A NULL row too: its cleared validity bit must be valid again before the row is reused.
        auto pending_null =
            one_row(env, *table, 7, logical_value_t(&env.resource, complex_logical_type{logical_type::STRING_LITERAL}));
        transaction_append(*table, pending_null, env, 5);
        REQUIRE(column_of(*table, 1).count() == 3);

        auto held = exhaust(env);
        INFO("held whole-block pins: " << held.blocks.size() << ", 4 KiB pins: " << held.small.size());
        auto reverted = table->revert_append(1, 2);
        INFO("revert: " << (reverted.has_error() ? reverted.error().what : std::pmr::string{"ok"}));
        CHECK_FALSE(reverted.has_error());
        held.release();

        CHECK(column_of(*table, 1).count() == 1);
        auto good = one_row(env, *table, 8, logical_value_t(&env.resource, std::string_view{"ok"}));
        committed_append(*table, good, env);
        std::vector<row_t> cells;
        core::error_t err = core::error_t::no_error();
        REQUIRE(scan_rows(*table, env, &cells, &err) == 2);
        INFO("scan error: " << err.what);
        REQUIRE_FALSE(err.contains_error());
        CHECK(cells[1].k == 8);
        REQUIRE_FALSE(cells[1].v.is_null());
        const auto seen = std::string{cells[1].v.value<std::string_view>()};
        CHECK(seen == "ok");

        auto cp = checkpoint_production(bm, *table);
        CHECK_FALSE(cp.contains_error());
    }
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());
        auto loaded = reload_table(env, bm);
        INFO("reload: " << (loaded.has_error() ? loaded.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(loaded.has_error());
        std::vector<row_t> cells;
        core::error_t err = core::error_t::no_error();
        REQUIRE(scan_rows(*loaded.value(), env, &cells, &err) == 2);
        REQUIRE_FALSE(err.contains_error());
        CHECK(cells[1].v.value<std::string_view>() == "ok");
    }
    std::remove(path.c_str());
}

// The revert takes a session back from its first row only: a cut inside a session has no recorded
// counts, and finding them would need the reads R1-R5 keep out of a revert.
TEST_CASE("list_revert_pin: a revert from inside an append session is refused", "[list_revert_pin][contract]") {
    const std::string path = db_path("contract");
    std::remove(path.c_str());
    env_t env;
    const auto list_type = complex_logical_type::create_list(logical_type::STRING_LITERAL);
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = make_table(env, bm, list_type);
        auto seed = one_row(env, *table, 1, list_of(env, list_type, {"seed"}));
        committed_append(*table, seed, env);

        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, 4);
        chunk.set_cardinality(4);
        for (uint64_t i = 0; i < 4; i++) {
            chunk.set_value(0, i, static_cast<int64_t>(10 + i));
            chunk.set_value(1, i, list_of(env, list_type, {"a", "b"}));
        }
        transaction_append(*table, chunk, env, 5);
        REQUIRE(column_of(*table, 1).count() == 5);

        auto inside = table->revert_append(3, 2);
        REQUIRE(inside.has_error());
        CHECK(inside.error().type == core::error_code_t::invalid_parameter);
        CHECK(column_of(*table, 1).count() == 5);

        auto whole = table->revert_append(1, 4);
        INFO("revert: " << (whole.has_error() ? whole.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(whole.has_error());
        CHECK(column_of(*table, 1).count() == 1);
        std::vector<row_t> cells;
        core::error_t err = core::error_t::no_error();
        REQUIRE(scan_rows(*table, env, &cells, &err) == 1);
        REQUIRE_FALSE(err.contains_error());
        CHECK(cells[0].k == 1);
    }
    std::remove(path.c_str());
}

namespace {

    // A committed seed row, then an uncommitted 4-row session at row 1: what the transaction will
    // revert after its own ALTER.
    std::unique_ptr<data_table_t> table_with_open_session(env_t& env,
                                                          tstorage::single_file_block_manager_t& bm,
                                                          const complex_logical_type& list_type) {
        auto table = make_table(env, bm, list_type);
        auto seed = one_row(env, *table, 1, list_of(env, list_type, {"seed"}));
        committed_append(*table, seed, env);
        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, 4);
        chunk.set_cardinality(4);
        for (uint64_t i = 0; i < 4; i++) {
            chunk.set_value(0, i, static_cast<int64_t>(10 + i));
            chunk.set_value(1, i, list_of(env, list_type, {"a", "b"}));
        }
        transaction_append(*table, chunk, env, 5);
        REQUIRE(column_of(*table, 1).count() == 5);
        return table;
    }

    // Element counts of the LIST column at `column`, one entry per row, in row order.
    std::vector<uint64_t> list_sizes(data_table_t& table, env_t& env, uint64_t column) {
        auto types = table.copy_types();
        std::vector<storage_index_t> column_ids;
        for (uint64_t c = 0; c < types.size(); c++) {
            column_ids.emplace_back(c);
        }
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids, transaction_data::committed(), nullptr);
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        std::vector<uint64_t> sizes;
        while (true) {
            chunk.reset();
            table.scan(chunk, state);
            REQUIRE_FALSE(state.table_state.has_error());
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                sizes.push_back(chunk.value(column, i).children().size());
            }
        }
        return sizes;
    }

} // namespace

// The transaction appended before its own ADD COLUMN and rolls back after it: the revert runs on the
// successor, which must hold the session's cut with the added column's counts at that row.
TEST_CASE("list_revert_pin: a session begun before ADD COLUMN reverts after it", "[list_revert_pin][alter]") {
    const std::string path = db_path("alter_add");
    std::remove(path.c_str());
    env_t env;
    const auto list_type = complex_logical_type::create_list(logical_type::STRING_LITERAL);
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = table_with_open_session(env, bm, list_type);
        column_definition_t added("w", complex_logical_type::create_list(logical_type::STRING_LITERAL));
        auto extended = std::make_unique<data_table_t>(*table, added);
        REQUIRE_FALSE(extended->has_construction_error());

        auto reverted = extended->revert_append(1, 4);
        INFO("revert: " << (reverted.has_error() ? reverted.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(reverted.has_error());
        CHECK(column_of(*extended, 1).count() == 1);
        CHECK(column_of(*extended, 2).count() == 1);

        auto types = extended->copy_types();
        data_chunk_t good(&env.resource, types, 1);
        good.set_cardinality(1);
        good.set_value(0, 0, int64_t{8});
        good.set_value(1, 0, list_of(env, list_type, {"ok"}));
        good.set_value(2, 0, list_of(env, list_type, {"x", "y", "z"}));
        committed_append(*extended, good, env);
        CHECK(list_sizes(*extended, env, 1) == std::vector<uint64_t>{1, 1});
        CHECK(list_sizes(*extended, env, 2) == std::vector<uint64_t>{0, 3});
    }
    std::remove(path.c_str());
}

// The same after DROP COLUMN of the column in front of the LIST: the cut loses that column's span.
TEST_CASE("list_revert_pin: a session begun before DROP COLUMN reverts after it", "[list_revert_pin][alter]") {
    const std::string path = db_path("alter_drop");
    std::remove(path.c_str());
    env_t env;
    const auto list_type = complex_logical_type::create_list(logical_type::STRING_LITERAL);
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        auto table = table_with_open_session(env, bm, list_type);
        auto reduced = std::make_unique<data_table_t>(*table, uint64_t{0});
        REQUIRE_FALSE(reduced->has_construction_error());

        auto reverted = reduced->revert_append(1, 4);
        INFO("revert: " << (reverted.has_error() ? reverted.error().what : std::pmr::string{"ok"}));
        REQUIRE_FALSE(reverted.has_error());
        CHECK(column_of(*reduced, 0).count() == 1);

        auto types = reduced->copy_types();
        data_chunk_t good(&env.resource, types, 1);
        good.set_cardinality(1);
        good.set_value(0, 0, list_of(env, list_type, {"ok"}));
        committed_append(*reduced, good, env);
        CHECK(list_sizes(*reduced, env, 0) == std::vector<uint64_t>{1, 1});
    }
    std::remove(path.c_str());
}
