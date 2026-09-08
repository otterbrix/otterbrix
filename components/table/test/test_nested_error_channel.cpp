// Refusals here must not THROW: components/table runs inside the disk agent's actor-zeta
// coroutines, whose unhandled_exception() is EMPTY, so a throw HANGS the statement instead of
// erroring. Each refusal is pinned to the error channel its caller already reads.

#include <catch2/catch_test_macros.hpp>

#include <components/table/array_column_data.hpp>
#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/data_table.hpp>
#include <components/table/list_column_data.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/struct_column_data.hpp>
#include <components/table/table_state.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <vector>

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    const std::string& scratch_db_path() {
        static const std::string path =
            "/tmp/test_otterbrix_nested_error_channel_" + std::to_string(::getpid()) + ".otbx";
        std::remove(path.c_str());
        return path;
    }

    struct env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;
        // A real disk manager over a scratch file: what these tests need is a column without a
        // catalog, not a storage layer that cannot do I/O.
        tstorage::single_file_block_manager_t block_manager;

        env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, scratch_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~env_t() { std::remove(scratch_db_path().c_str()); }
    };

    // Returns the append state too, so a case can reach the segments the append just wrote.
    struct built_column_t {
        std::unique_ptr<column_data_t> column;
        column_append_state append_state;
    };

    built_column_t build_nested_column(env_t& env,
                                       tstorage::block_manager_t& bm,
                                       const complex_logical_type& type,
                                       const std::vector<std::vector<uint64_t>>& rows) {
        built_column_t out;
        out.column = column_data_t::create_column(&env.resource, bm, 0, 0, type);
        REQUIRE_FALSE(out.column->initialize_append(out.append_state).has_error());

        vector_t v(&env.resource, type, rows.size());
        for (uint64_t i = 0; i < rows.size(); i++) {
            v.set_value(i, rows[i]);
        }
        REQUIRE_FALSE(out.column->append(out.append_state, v, rows.size()).has_error());
        return out;
    }

    struct built_table_t {
        std::unique_ptr<data_table_t> table;
    };

    built_table_t build_table(env_t& env,
                              tstorage::block_manager_t& bm,
                              const std::string& column_name,
                              const complex_logical_type& type,
                              const std::vector<logical_value_t>& rows,
                              const char* table_name) {
        built_table_t out;
        std::vector<column_definition_t> columns;
        columns.emplace_back(column_name, type);
        out.table = std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), table_name);

        auto types = out.table->copy_types();
        data_chunk_t chunk(&env.resource, types, rows.size());
        chunk.set_cardinality(rows.size());
        for (uint64_t i = 0; i < rows.size(); i++) {
            chunk.set_value(0, i, rows[i]);
        }

        table_append_state state(&env.resource);
        REQUIRE_FALSE(out.table->append_lock(state).has_error());
        REQUIRE_FALSE(out.table->initialize_append(state).has_error());
        REQUIRE_FALSE(out.table->append(chunk, state).has_error());
        out.table->finalize_append(state, transaction_data{0, 0});
        return out;
    }

    logical_value_t list_value(env_t& env, const std::vector<uint64_t>& elements) {
        std::vector<logical_value_t> members;
        members.reserve(elements.size());
        for (auto element : elements) {
            members.emplace_back(&env.resource, element);
        }
        return logical_value_t::create_list(&env.resource, complex_logical_type{logical_type::UBIGINT}, members);
    }

    vector_t single_row_id(env_t& env, int64_t row_id) {
        vector_t ids(&env.resource, logical_type::BIGINT, 1);
        ids.data<int64_t>()[0] = row_id;
        return ids;
    }

} // namespace

// (1)+(2) LIST/ARRAY point fetch: NOT IMPLEMENTED, reported not thrown; unreachable via SQL, tested directly.
TEST_CASE("nested column: a LIST point fetch refuses on the scan state instead of throwing") {
    env_t env;
    auto& bm = env.block_manager;

    auto list_type = complex_logical_type::create_list(complex_logical_type{logical_type::UBIGINT});
    auto built = build_nested_column(env, bm, list_type, {{1, 2}, {3, 4, 5}});

    column_scan_state state;
    state.initialize(list_type);
    vector_t result(&env.resource, list_type, 1);

    const auto fetched = built.column->fetch(state, 0, result);
    REQUIRE(fetched == 0);
    REQUIRE(state.has_error());
    REQUIRE(state.scan_error.type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("nested column: an ARRAY point fetch refuses on the scan state instead of throwing") {
    env_t env;
    auto& bm = env.block_manager;

    auto array_type = complex_logical_type::create_array(complex_logical_type{logical_type::UBIGINT}, 2);
    auto built = build_nested_column(env, bm, array_type, {{1, 2}, {3, 4}});

    column_scan_state state;
    state.initialize(array_type);
    vector_t result(&env.resource, array_type, 1);

    const auto fetched = built.column->fetch(state, 0, result);
    REQUIRE(fetched == 0);
    REQUIRE(state.has_error());
    REQUIRE(state.scan_error.type == core::error_code_t::unimplemented_yet);
}

// (3) A stored list offset past the element column reports data_corruption: REACHABLE FROM DATA
// (own segment payload), so a corrupt run is a read failure, not a program error.
TEST_CASE("nested column: a list offset past the element column reports data_corruption") {
    env_t env;
    auto& bm = env.block_manager;

    auto list_type = complex_logical_type::create_list(complex_logical_type{logical_type::UBIGINT});
    auto built = build_nested_column(env, bm, list_type, {{10, 20}, {30, 40, 50}});

    {
        column_scan_state state;
        state.initialize(list_type);
        built.column->initialize_scan(state);
        vector_t result(&env.resource, list_type, DEFAULT_VECTOR_CAPACITY);
        const auto scanned = built.column->scan_count(state, result, 2);
        REQUIRE_FALSE(state.has_error());
        REQUIRE(scanned == 2);
        const auto row0 = result.value(0);
        REQUIRE(row0.children().size() == 2);
        const auto row1 = result.value(1);
        REQUIRE(row1.children().size() == 3);
    }

    auto* offsets_segment = built.append_state.current;
    REQUIRE(offsets_segment != nullptr);
    REQUIRE(offsets_segment->type.to_physical_type() == physical_type::LIST);
    REQUIRE(offsets_segment->type_size == sizeof(uint64_t));

    {
        auto pinned = env.buffer_manager.pin(offsets_segment->block);
        REQUIRE_FALSE(pinned.has_error());
        auto* base = pinned.value().ptr() + offsets_segment->block_offset();
        uint64_t stored = 0;
        std::memcpy(&stored, base, sizeof(uint64_t));
        REQUIRE(stored == 2);
        // Row 0 now claims 100 elements over an element column that holds 5.
        const uint64_t poisoned = 100;
        std::memcpy(base, &poisoned, sizeof(uint64_t));
    }

    column_scan_state state;
    state.initialize(list_type);
    built.column->initialize_scan(state);
    vector_t result(&env.resource, list_type, DEFAULT_VECTOR_CAPACITY);
    const auto scanned = built.column->scan_count(state, result, 1);
    REQUIRE(scanned == 0);
    REQUIRE(state.has_error());
    REQUIRE(state.scan_error.type == core::error_code_t::data_corruption);
}

// (4) An in-place LIST update changing a row's list length is refused: REACHABLE via the WAL
// REPLAY leg of update (the txn leg is delete+append and never hits this), on the disk agent's thread.
TEST_CASE("nested column: an in-place LIST update cannot change the list length, and says so") {
    env_t env;
    auto& bm = env.block_manager;

    auto list_type = complex_logical_type::create_list(complex_logical_type{logical_type::UBIGINT});
    std::vector<logical_value_t> rows;
    rows.push_back(list_value(env, {10, 20}));
    auto built = build_table(env, bm, "v", list_type, rows, "list_len_update");

    auto row_ids = single_row_id(env, 0);
    auto types = built.table->copy_types();

    {
        data_chunk_t upd(&env.resource, types, 1);
        upd.set_cardinality(1);
        upd.set_value(0, 0, list_value(env, {11, 21}));
        auto state = built.table->initialize_update({});
        auto updated = built.table->update(*state, row_ids, upd);
        REQUIRE_FALSE(updated.has_error());
    }

    data_chunk_t upd(&env.resource, types, 1);
    upd.set_cardinality(1);
    upd.set_value(0, 0, list_value(env, {1, 2, 3}));
    auto state = built.table->initialize_update({});
    auto updated = built.table->update(*state, row_ids, upd);
    REQUIRE(updated.has_error());
    REQUIRE(updated.error().type == core::error_code_t::unimplemented_yet);
}

// (5)+(6) A malformed STRUCT sub-column path is refused; this call is update_column's only reachable surface.
TEST_CASE("nested column: a struct sub-column update path is validated on the update channel") {
    env_t env;
    auto& bm = env.block_manager;

    std::pmr::vector<complex_logical_type> fields(&env.resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(logical_type::BIGINT, "b");
    auto struct_type = complex_logical_type::create_struct("pair", fields, "s");

    auto column = column_data_t::create_column(&env.resource, bm, 0, 0, struct_type);
    {
        std::vector<logical_value_t> members;
        members.emplace_back(&env.resource, int64_t{11});
        members.emplace_back(&env.resource, int64_t{12});
        vector_t v(&env.resource, struct_type, 1);
        v.set_value(0, logical_value_t::create_struct(&env.resource, struct_type, members));
        column_append_state append_state;
        REQUIRE_FALSE(column->initialize_append(append_state).has_error());
        REQUIRE_FALSE(column->append(append_state, v, 1).has_error());
    }

    vector_t update_vector(&env.resource, logical_type::BIGINT, 1);
    update_vector.set_value(0, int64_t{99});
    int64_t row_ids[1] = {0};
    // depth 1 is what row_group_t::update_column starts the walk at (element 0 is the top-level column).
    constexpr uint64_t start_depth = 1;

    SECTION("a path that ends ON the struct names nothing writable") {
        const std::vector<uint64_t> path{0};
        auto updated = column->update_column(path, update_vector, row_ids, 1, start_depth);
        REQUIRE(updated.has_error());
        REQUIRE(updated.error().type == core::error_code_t::invalid_parameter);
    }
    SECTION("a path that names a field the struct does not have is refused") {
        const std::vector<uint64_t> path{0, 99};
        auto updated = column->update_column(path, update_vector, row_ids, 1, start_depth);
        REQUIRE(updated.has_error());
        REQUIRE(updated.error().type == core::error_code_t::invalid_parameter);
    }
    SECTION("POSITIVE CONTROL: a well-formed path into the first field still writes") {
        const std::vector<uint64_t> path{0, 1};
        auto updated = column->update_column(path, update_vector, row_ids, 1, start_depth);
        REQUIRE_FALSE(updated.has_error());
    }
}

// (7) An unnamed struct is refused at the append gate: REACHABLE via create_variant's LIST(struct) with no alias.
TEST_CASE("nested column: an unnamed nested struct is refused by initialize_append") {
    env_t env;
    auto& bm = env.block_manager;

    std::pmr::vector<complex_logical_type> fields(&env.resource);
    fields.emplace_back(logical_type::BIGINT, "a");

    SECTION("LIST of an unnamed struct is refused") {
        auto element = complex_logical_type::create_struct("pair", fields);
        REQUIRE(element.is_unnamed());
        auto list_type = complex_logical_type::create_list(element, "v");

        std::vector<column_definition_t> columns;
        columns.emplace_back("v", list_type);
        data_table_t table(&env.resource, bm, std::move(columns), "unnamed_struct");

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        auto init = table.initialize_append(state);
        REQUIRE(init.has_error());
        REQUIRE(init.error().type == core::error_code_t::invalid_parameter);
    }

    SECTION("POSITIVE CONTROL: naming the element struct makes the same table appendable") {
        auto element = complex_logical_type::create_struct("pair", fields, "elem");
        REQUIRE_FALSE(element.is_unnamed());
        auto list_type = complex_logical_type::create_list(element, "v");

        std::vector<column_definition_t> columns;
        columns.emplace_back("v", list_type);
        data_table_t table(&env.resource, bm, std::move(columns), "named_struct");

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        auto init = table.initialize_append(state);
        REQUIRE_FALSE(init.has_error());
    }
}

// (8) A flat-vector scan over a non-flat result reports, not throws: reached through the public
// fetch(), which passes the caller's vector straight down; no SQL path names it directly.
TEST_CASE("column scan: a flat-vector scan over a non-flat result refuses on the scan state") {
    env_t env;
    auto& bm = env.block_manager;

    auto column = column_data_t::create_column(&env.resource, bm, 0, 0, complex_logical_type{logical_type::UBIGINT});
    {
        vector_t v(&env.resource, logical_type::UBIGINT, 8);
        for (uint64_t i = 0; i < 8; i++) {
            v.set_value(i, uint64_t{i});
        }
        column_append_state append_state;
        REQUIRE_FALSE(column->initialize_append(append_state).has_error());
        REQUIRE_FALSE(column->append(append_state, v, 8).has_error());
    }

    vector_t non_flat(&env.resource, logical_type::UBIGINT, DEFAULT_VECTOR_CAPACITY);
    non_flat.set_vector_type(vector_type::CONSTANT);

    column_scan_state state;
    state.initialize(complex_logical_type{logical_type::UBIGINT});
    const auto fetched = column->fetch(state, 0, non_flat);
    REQUIRE(fetched == 0);
    REQUIRE(state.has_error());
    REQUIRE(state.scan_error.type == core::error_code_t::invalid_parameter);

    column_scan_state mode_state;
    mode_state.initialize(complex_logical_type{logical_type::UBIGINT});
    column->initialize_scan(mode_state);
    REQUIRE(column->get_vector_scan_type(mode_state, 8, non_flat) == scan_vector_type::SCAN_ENTIRE_VECTOR);
}

// (9) An index-build scan over a column with updates refuses: REACHABLE through the public
// create_index_scan API, though no production caller passes COMMITTED_ROWS_DISALLOW_UPDATES today.
TEST_CASE("column scan: an index-build scan over a column with updates refuses") {
    env_t env;
    auto& bm = env.block_manager;

    std::vector<logical_value_t> rows;
    for (int64_t i = 0; i < 4; i++) {
        rows.emplace_back(&env.resource, i);
    }
    auto built = build_table(env, bm, "n", complex_logical_type{logical_type::BIGINT}, rows, "index_scan_updates");

    std::vector<storage_index_t> column_indices;
    column_indices.emplace_back(static_cast<uint64_t>(0));

    {
        table_scan_state scan_state(&env.resource);
        built.table->initialize_scan(scan_state, column_indices);
        auto types = built.table->copy_types();
        data_chunk_t out(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        built.table->create_index_scan(scan_state, out, table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES);
        REQUIRE_FALSE(scan_state.table_state.has_error());
        REQUIRE(out.size() == 4);
    }

    {
        auto row_ids = single_row_id(env, 0);
        auto types = built.table->copy_types();
        data_chunk_t upd(&env.resource, types, 1);
        upd.set_cardinality(1);
        upd.set_value(0, 0, int64_t{77});
        auto state = built.table->initialize_update({});
        REQUIRE_FALSE(built.table->update(*state, row_ids, upd).has_error());
    }

    table_scan_state scan_state(&env.resource);
    built.table->initialize_scan(scan_state, column_indices);
    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
    built.table->create_index_scan(scan_state, out, table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES);
    REQUIRE(scan_state.table_state.has_error());
    REQUIRE(scan_state.table_state.scan_error.type == core::error_code_t::index_create_fail);
}
