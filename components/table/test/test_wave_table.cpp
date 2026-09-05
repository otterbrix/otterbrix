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
        table.finalize_append(state, transaction_data{0, 0});
    }

} // namespace

// =====================================================================================
// РАЗРЫВ МАРШРУТИЗАЦИИ update_column.
// collection_t::update_column отдаёт column_path в row_group_t::update, который читает его
// как список колонок ВЕРХНЕГО УРОВНЯ: для пути {0, 2} он трактует «2» как вторую колонку
// таблицы и лезет в updates.data[1], которого нет. row_group_t::update_column (единственный
// вход в семью column_data_t::update_column) не имеет вызывающих.
// Без фикса: SIGABRT на assert(col_data.type().type() == update_chunk.data[i].type().type())
// (STRUCT против BIGINT) — либо чтение updates.data[1] за границей.
// Ожидаемо: путь спускается в поле структуры; поле b обновлено, поле a нетронуто.
// =====================================================================================
TEST_CASE("components::table::wave::update_column_descends_into_a_struct_field") {
    wave_env env("struct_update");

    std::pmr::vector<complex_logical_type> fields(&env.resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(logical_type::BIGINT, "b");
    auto struct_type = complex_logical_type::create_struct("pair", fields);

    std::vector<column_definition_t> columns;
    columns.emplace_back("s", struct_type);
    auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");

    constexpr uint64_t NUM_ROWS = 8;
    {
        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, NUM_ROWS);
        chunk.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; i++) {
            std::vector<logical_value_t> members;
            members.emplace_back(&env.resource, static_cast<int64_t>(i * 10));
            members.emplace_back(&env.resource, static_cast<int64_t>(i * 10 + 1));
            chunk.set_value(0, i, logical_value_t::create_struct(&env.resource, struct_type, members));
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
    }

    // Обновляем ПОЛЕ b (путь {колонка 0, дочерний ординал 2 = второе поле; 0 — validity})
    // у строк 2 и 5.
    vector_t row_ids(&env.resource, logical_type::BIGINT, 2);
    row_ids.set_value(0, logical_value_t(&env.resource, int64_t(2)));
    row_ids.set_value(1, logical_value_t(&env.resource, int64_t(5)));

    std::pmr::vector<complex_logical_type> update_types(&env.resource);
    update_types.emplace_back(logical_type::BIGINT);
    data_chunk_t updates(&env.resource, update_types, 2);
    updates.data[0].set_value(0, logical_value_t(&env.resource, int64_t(777)));
    updates.data[0].set_value(1, logical_value_t(&env.resource, int64_t(888)));
    updates.set_cardinality(2);

    auto updated = table->update_column(row_ids, {0, 2}, updates);
    REQUIRE_FALSE(updated.has_error());

    uint64_t scanned = 0;
    otterbrix_test::scan_table_segment(*table, 0, NUM_ROWS, [&](data_chunk_t& chunk) {
        for (uint64_t i = 0; i < chunk.size(); i++) {
            const uint64_t row = scanned + i;
            auto sv = chunk.data[0].value(i);
            REQUIRE(sv.children().size() == 2);
            const int64_t expect_a = static_cast<int64_t>(row * 10);
            const int64_t expect_b = (row == 2) ? 777 : (row == 5) ? 888 : static_cast<int64_t>(row * 10 + 1);
            CHECK(sv.children()[0].value<int64_t>() == expect_a);
            CHECK(sv.children()[1].value<int64_t>() == expect_b);
        }
        scanned += chunk.size();
    });
    REQUIRE(scanned == NUM_ROWS);
}

// =====================================================================================
// segment_tree_t::segment_index бросает std::runtime_error,
// и это единственный канал отказа точечных чтений. Через data_table_t::update строка с
// несуществующим row id доходит до get_segment -> throw, который в проде пересекает
// корутину актора (пустой unhandled_exception -> зависание).
// Без фикса: непойманный std::runtime_error валит тест ("Could not find node in column
// segment tree"). Ожидаемо: update возвращает error_t.
// =====================================================================================
TEST_CASE("components::table::wave::an_out_of_range_row_id_is_an_error_not_a_throw") {
    wave_env env("row_out_of_range");
    std::vector<column_definition_t> columns;
    columns.emplace_back("value", complex_logical_type(logical_type::BIGINT));
    auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");
    append_bigint_rows(*table, env, 0, 100);

    vector_t row_ids(&env.resource, logical_type::BIGINT, 1);
    row_ids.set_value(0, logical_value_t(&env.resource, int64_t(5000)));

    auto types = table->copy_types();
    data_chunk_t updates(&env.resource, types, 1);
    updates.data[0].set_value(0, logical_value_t(&env.resource, int64_t(1)));
    updates.set_cardinality(1);

    auto updated = table->update_column(row_ids, {0}, updates);
    REQUIRE(updated.has_error());
}

// Тот же разрыв через LIST-ногу: fetch_list_offset(row-1) на строке за концом колонки
// доходит до get_segment -> throw; после фикса отказ едет по result_wrapper каналу
// gather_child_update -> update -> collection -> data_table (fetch_list_offset получает
// собственный канал ошибки).
TEST_CASE("components::table::wave::a_list_update_of_a_missing_row_reports_not_throws") {
    wave_env env("list_row_out_of_range");
    auto list_type = complex_logical_type::create_list(logical_type::BIGINT);
    std::vector<column_definition_t> columns;
    columns.emplace_back("l", list_type);
    auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");

    constexpr uint64_t NUM_ROWS = 4;
    {
        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, NUM_ROWS);
        chunk.set_cardinality(NUM_ROWS);
        for (uint64_t i = 0; i < NUM_ROWS; i++) {
            std::vector<logical_value_t> elems;
            elems.emplace_back(&env.resource, static_cast<int64_t>(i));
            chunk.set_value(0,
                            i,
                            logical_value_t::create_list(&env.resource,
                                                         complex_logical_type(logical_type::BIGINT),
                                                         elems));
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
    }

    vector_t row_ids(&env.resource, logical_type::BIGINT, 1);
    row_ids.set_value(0, logical_value_t(&env.resource, int64_t(4000)));

    auto types = table->copy_types();
    data_chunk_t updates(&env.resource, types, 1);
    std::vector<logical_value_t> elems;
    elems.emplace_back(&env.resource, int64_t(99));
    updates.data[0].set_value(0,
                              logical_value_t::create_list(&env.resource,
                                                           complex_logical_type(logical_type::BIGINT),
                                                           elems));
    updates.set_cardinality(1);

    auto updated = table->update_column(row_ids, {0}, updates);
    REQUIRE(updated.has_error());
}

// =====================================================================================
// block_handle_t::load() отвечает ПУСТЫМ buffer_handle_t без ошибки на блок,
// который загрузить нечем (UNLOADED, без temp-копии, block_id >= MAXIMUM_BLOCK);
// standard_buffer_manager_t::pin затем разыменовывает нулевой буфер.
// Без фикса: SIGSEGV внутри pin (get_buffer(lock)->allocation_size() по nullptr).
// Ожидаемо: pin возвращает error_t.
// =====================================================================================
TEST_CASE("components::table::wave::pin_of_an_unloadable_block_reports_an_error") {
    wave_env env("unloadable_pin");
    auto handle = std::make_shared<storage::block_handle_t>(env.block_manager,
                                                            storage::MAXIMUM_BLOCK + 7,
                                                            storage::memory_tag::BASE_TABLE);
    auto pinned = env.buffer_manager.pin(handle);
    REQUIRE(pinned.has_error());
}

// =====================================================================================
// unload_and_take_block ассертит инвариант «байты либо на диске, либо в
// спилле», а под NDEBUG молча выбрасывает буфер, которого больше нигде нет.
// Без фикса (Debug): SIGABRT на assert(can_unload() || has_temp_copy()).
// Ожидаемо: отказ — буфер остаётся резидентным, повторный pin отдаёт те же байты.
// =====================================================================================
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

// =====================================================================================
// initialize_column молча реконструирует счётчик строк из суммы сегментов,
// когда персистентный счётчик равен 0: два несогласных числа на диске примиряются тихо.
// Без фикса: initialize_column отвечает успехом и count() == 5 (реконструированное).
// Ожидаемо: data_corruption.
// =====================================================================================
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
    // Согласованно короткая пара «своя колонка + validity»: единственное противоречие —
    // count == 0 при сумме сегментов 5. До фикса ОБА узла молча реконструируют 5 и загрузка
    // отвечает успехом; после — data_corruption.
    auto persistent = make_pcd(40);
    persistent.child_columns.push_back(std::make_unique<persistent_column_data_t>(make_pcd(64)));

    auto loaded = column->initialize_column(persistent);
    REQUIRE(loaded.has_error());
}

// =====================================================================================
// base_statistics_t::update без ноги HUGEINT/UHUGEINT/DECIMAL: широкая
// DECIMAL-колонка получает только счётчики NULL, без min/max.
// Без фикса: has_stats() == false после update по HUGEINT/DECIMAL вектору.
// Ожидаемо: min/max заполнены и переживают serialize/deserialize.
// =====================================================================================
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
        auto dec_type_r = complex_logical_type::create_decimal(38, 2);
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

// =====================================================================================
// row_group_t::add_column гасит OOM бэкфилла ассертами; под NDEBUG цикл
// молча рвётся, и наследник возвращается с КОРОТКОЙ колонкой при полном count.
// Без фикса (Debug): SIGABRT на assert(!init.has_error() && "row_group::add_column:
// initialize_append OOM"). Ожидаемо: отказ громкий — таблица-наследник отказывает
// в записи, родитель остаётся корнем и читается.
// =====================================================================================
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
