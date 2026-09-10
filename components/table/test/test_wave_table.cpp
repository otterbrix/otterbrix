#include <catch2/catch_test_macros.hpp>

#include <components/table/data_table.hpp>
#include <components/table/base_statistics.hpp>
#include <components/table/column_data.hpp>
#include <components/table/persistent_column_data.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <string>
#include <unistd.h>

#include "table_segment_scan.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    std::string wave_db_path(const std::string& name) {
        std::string path = "/tmp/test_otterbrix_wave_table_" + name + "_" + std::to_string(::getpid()) + ".otbx";
        std::remove(path.c_str());
        return path;
    }

    struct wave_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        std::string path;
        storage::single_file_block_manager_t block_manager;

        explicit wave_env(const std::string& name)
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , path(wave_db_path(name))
            , block_manager(buffer_manager, fs, path) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~wave_env() { std::remove(path.c_str()); }
    };

    void append_bigint_rows(data_table_t& table, wave_env& env, int64_t start, uint64_t count) {
        auto types = table.copy_types();
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        for (uint64_t offset = 0; offset < count; offset += DEFAULT_VECTOR_CAPACITY) {
            const uint64_t batch = std::min<uint64_t>(count - offset, DEFAULT_VECTOR_CAPACITY);
            auto chunk = data_chunk_t(&env.resource, types, batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.data[0].set_value(
                    i,
                    logical_value_t(&env.resource, start + static_cast<int64_t>(offset + i)));
            }
            chunk.set_cardinality(batch);
            REQUIRE_FALSE(table.append(chunk, state).has_error());
        }
        table.finalize_append(state, transaction_data::committed());
    }

} // namespace

// block_handle_t::load() отвечал ПУСТЫМ buffer_handle_t без ошибки для блока, который
// загрузить нечем (UNLOADED, без temp-копии, block_id >= MAXIMUM_BLOCK), а
// standard_buffer_manager_t::pin затем разыменовывал нулевой буфер. Без фикса: SIGSEGV в pin.
TEST_CASE("components::table::wave::pin_of_an_unloadable_block_reports_an_error") {
    wave_env env("unloadable_pin");
    auto handle = std::make_shared<storage::block_handle_t>(env.block_manager,
                                                            storage::MAXIMUM_BLOCK + 7,
                                                            storage::memory_tag::BASE_TABLE);
    auto pinned = env.buffer_manager.pin(handle);
    REQUIRE(pinned.has_error());
}

// unload_and_take_block ассертит «байты либо на диске, либо в спилле», а под NDEBUG молча
// выбрасывает буфер, которого больше нигде нет. Без фикса (Debug): SIGABRT на этом assert.
TEST_CASE("components::table::wave::unload_of_a_spill_less_transient_refuses") {
    wave_env env("unload_refusal");
    auto allocated = env.buffer_manager.allocate(storage::memory_tag::BASE_TABLE, 4096, false);
    REQUIRE_FALSE(allocated.has_error());
    auto block = allocated.value().block_handle()->shared_from_this();
    // Заполняем узнаваемым узором, пока pin жив.
    auto* payload = allocated.value().ptr();
    for (uint64_t i = 0; i < 128; i++) {
        payload[i] = static_cast<std::byte>(i * 3 + 1);
    }
    { auto dropped = std::move(allocated.value()); } // отпустить pin: readers -> 0

    {
        auto lock = block->get_lock();
        block->unload(lock); // до фикса: SIGABRT; после: громкий отказ, буфер жив
    }

    auto repinned = env.buffer_manager.pin(block);
    REQUIRE_FALSE(repinned.has_error());
    for (uint64_t i = 0; i < 128; i++) {
        REQUIRE(repinned.value().ptr()[i] == static_cast<std::byte>(i * 3 + 1));
    }
}

// initialize_column молча реконструировал счётчик строк из суммы сегментов при персистентном
// count == 0: два несогласных числа на диске примирялись тихо. Без фикса: успех с
// реконструированным count() == 5 вместо data_corruption.
TEST_CASE("components::table::wave::a_zero_count_with_rows_on_disk_is_corruption") {
    wave_env env("count_mismatch");

    auto column =
        column_data_t::create_column(&env.resource, env.block_manager, 0, 0, complex_logical_type(logical_type::BIGINT));

    auto make_pcd = [&](uint64_t seg_size) {
        persistent_column_data_t pcd(&env.resource);
        pcd.count = 0; // писатель заявляет: строк нет
        storage::data_pointer_t dp;
        dp.row_start = 0;
        dp.tuple_count = 5; // а сегмент заявляет: строк пять
        dp.block_pointer.block_id = 1;
        dp.block_pointer.offset = 0;
        dp.segment_size = seg_size;
        dp.compression = components::table::compression::compression_type::UNCOMPRESSED;
        pcd.data_pointers.push_back(std::move(dp));
        return pcd;
    };
    // Обе ноги (своя колонка + validity) намеренно короткие: единственное противоречие —
    // count == 0 при сумме сегментов 5.
    auto persistent = make_pcd(40);
    persistent.child_columns.push_back(std::make_unique<persistent_column_data_t>(make_pcd(64)));

    auto loaded = column->initialize_column(persistent);
    REQUIRE(loaded.has_error());
}

// base_statistics_t::update не имел ветки HUGEINT/UHUGEINT/DECIMAL: широкая DECIMAL-колонка
// получала только счётчики NULL, без min/max (has_stats() == false).
TEST_CASE("components::table::wave::hugeint_and_decimal_columns_get_minmax_statistics") {
    wave_env env("stats_wide");

    SECTION("HUGEINT min/max") {
        base_statistics_t stats(&env.resource, logical_type::HUGEINT);
        vector_t vec(&env.resource, logical_type::HUGEINT, 10);
        auto data = vec.data<int128_t>();
        for (uint64_t i = 0; i < 10; i++) {
            data[i] = int128_t(static_cast<int64_t>(i)) - int128_t(4);
        }
        stats.update(vec, 10);
        REQUIRE(stats.has_stats());
        CHECK(stats.min_value().value<int128_t>() == int128_t(-4));
        CHECK(stats.max_value().value<int128_t>() == int128_t(5));
    }

    SECTION("UHUGEINT min/max") {
        base_statistics_t stats(&env.resource, logical_type::UHUGEINT);
        vector_t vec(&env.resource, logical_type::UHUGEINT, 6);
        auto data = vec.data<uint128_t>();
        for (uint64_t i = 0; i < 6; i++) {
            data[i] = uint128_t(100 + i);
        }
        stats.update(vec, 6);
        REQUIRE(stats.has_stats());
        CHECK(stats.min_value().value<uint128_t>() == uint128_t(100));
        CHECK(stats.max_value().value<uint128_t>() == uint128_t(105));
    }

    SECTION("wide DECIMAL(38,2) min/max survive a serialize round-trip") {
        auto dec_type_r = complex_logical_type::create_decimal(&env.resource, 38, 2);
        REQUIRE_FALSE(dec_type_r.has_error());
        auto dec_type = dec_type_r.value();
        base_statistics_t stats(&env.resource, logical_type::DECIMAL);
        vector_t vec(&env.resource, dec_type, 8);
        auto data = vec.data<int128_t>();
        for (uint64_t i = 0; i < 8; i++) {
            data[i] = int128_t(static_cast<int64_t>(i * 1000)) - int128_t(2500);
        }
        stats.update(vec, 8);
        REQUIRE(stats.has_stats());
        CHECK(stats.min_value().value<int128_t>() == int128_t(-2500));
        CHECK(stats.max_value().value<int128_t>() == int128_t(4500));

        storage::metadata_manager_t meta(env.block_manager);
        storage::meta_block_pointer_t pointer;
        {
            storage::metadata_writer_t writer(meta);
            stats.serialize(writer);
            pointer = writer.get_block_pointer();
            REQUIRE_FALSE(writer.flush().has_error());
        }
        {
            storage::metadata_reader_t reader(meta, pointer);
            auto loaded = base_statistics_t::deserialize(&env.resource, reader);
            REQUIRE_FALSE(reader.has_error());
            REQUIRE(loaded.has_stats());
            CHECK(loaded.min_value().value<int128_t>() == int128_t(-2500));
            CHECK(loaded.max_value().value<int128_t>() == int128_t(4500));
        }
    }
}

// До фикса add_column гасил OOM бэкфилла ассертами (под NDEBUG наследник тихо получал
// КОРОТКУЮ колонку при полном count); теперь ошибка едет по каналу (row_group.cpp) и
// наследник громко отказывает в записи, родитель остаётся корнем.
TEST_CASE("components::table::wave::a_failed_add_column_backfill_refuses_loudly") {
    wave_env env("addcol_oom");
    std::vector<column_definition_t> columns;
    columns.emplace_back("value", complex_logical_type(logical_type::BIGINT));
    auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");
    append_bigint_rows(*table, env, 0, 3000);

    // Детерминированный отказ бэкфилла: default-строка больше блока не имеет
    // представимой on-disk формы, и append новой колонки обязан отказать
    // (write_string_memory: "string value ... exceeds the maximum storable string size").
    column_definition_t new_column("added", complex_logical_type(logical_type::STRING_LITERAL));
    new_column.set_default_value(
        logical_value_t(&env.resource, std::string(300 * 1024, 'x')));
    auto extended = std::make_unique<data_table_t>(*table, new_column);

    // Отказ защёлкнут и виден; родитель остался корнем (DDL не случился) и пишется.
    REQUIRE(extended->has_construction_error());
    CHECK(extended->column_count() == 1); // фантомной колонки в определениях нет
    append_bigint_rows(*table, env, 3000, 8);

    // Наследник обязан отказывать в записи, а не притворяться целым. После защёлки его
    // схема — родительская (без фантомной колонки), так что чанк одноколоночный.
    {
        auto types = extended->copy_types();
        REQUIRE(types.size() == 1);
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.data[0].set_value(0, logical_value_t(&env.resource, int64_t(1)));
        chunk.set_cardinality(1);
        table_append_state state(&env.resource);
        auto locked = extended->append_lock(state);
        bool refused = locked.has_error();
        if (!refused) {
            auto initialized = extended->initialize_append(state);
            refused = initialized.has_error();
            if (!refused) {
                auto appended = extended->append(chunk, state);
                refused = appended.has_error();
            }
        }
        REQUIRE(refused);
    }
}
