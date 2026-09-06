// TEN REFUSALS IN THE COLUMN LAYER THAT MUST NOT BE THROWS.
//
// components/table runs inside the disk agent's actor-zeta coroutines, whose
// unhandled_exception() is EMPTY: a throw there unwinds out of the coroutine and the statement
// HANGS instead of erroring. Each refusal is pinned to the error channel its caller
// ALREADY reads:
//   * column_scan_state::scan_error — read by column_data_t::update after fetch(), aggregated by
//     row_group_t into collection_scan_state::scan_error;
//   * core::result_wrapper_t<bool> — returned by update / update_column / initialize_append all
//     the way up to data_table_t.
//
// REACHABILITY IS STATED PER CASE, not assumed. Two of the ten (LIST/ARRAY point fetch) have NO
// caller: column_data_t::fetch is only called from column_data_t::update and
// struct_column_data_t::fetch, and LIST/ARRAY/STRUCT override both update and update_column, so
// neither is ever entered with a nested node as `this`. Still tested directly: deleting the
// override would let the base impl answer a LIST fetch with raw ELEMENT OFFSETS, or walk an
// ARRAY's segment tree that node never fills.

#include <catch2/catch_test_macros.hpp>

#include <components/table/array_column_data.hpp>
#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/data_table.hpp>
#include <components/table/list_column_data.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/storage/transient_block_manager.hpp>
#include <components/table/struct_column_data.hpp>
#include <components/table/table_state.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/file/local_file_system.hpp>

#include <cstring>
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
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    // One nested column, filled from `rows`, handed back with the append state so a case can reach
    // the segments the append just wrote.
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

    // A single-column table with `rows` rows already committed.
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

// ---------------------------------------------------------------------------------------------
// (1) + (2) LIST / ARRAY point fetch: NOT IMPLEMENTED, and it says so instead of throwing.
//
// Unreachable via SQL (see file header); tested by direct call. The refusal lands in
// state.scan_error, exactly where column_data_t::update checks it (column_data.cpp).
TEST_CASE("nested column: a LIST point fetch refuses on the scan state instead of throwing") {
    env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

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
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

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

// ---------------------------------------------------------------------------------------------
// (3) A stored list offset that runs past the element column is data_corruption, not a throw.
//
// REACHABLE FROM DATA: the offsets are this column's own segment payload (cumulative element
// counts, one uint64 per row -- column_segment.cpp::stored_element_size pins the width at 8 for a
// LIST), so a corrupt run is a read failure, not a program error. The surgery below writes one
// such value directly, the same thing a bad block would hand the scan.
TEST_CASE("nested column: a list offset past the element column reports data_corruption") {
    env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

    auto list_type = complex_logical_type::create_list(complex_logical_type{logical_type::UBIGINT});
    // Two rows, 2 + 3 elements: the stored cumulative offsets are 2 and 5, and the element column
    // holds exactly 5 entries.
    auto built = build_nested_column(env, bm, list_type, {{10, 20}, {30, 40, 50}});

    // POSITIVE CONTROL ON THE FIXTURE: intact, this very scan reads both lists back.
    {
        column_scan_state state;
        state.initialize(list_type);
        built.column->initialize_scan(state);
        vector_t result(&env.resource, list_type, DEFAULT_VECTOR_CAPACITY);
        const auto scanned = built.column->scan_count(state, result, 2);
        REQUIRE_FALSE(state.has_error());
        REQUIRE(scanned == 2);
        const auto row0 = result.value(0); // named local: value() hands back a temporary
        REQUIRE(row0.children().size() == 2);
        const auto row1 = result.value(1);
        REQUIRE(row1.children().size() == 3);
    }

    // THE ISOLATION GATE: the segment being corrupted is the LIST node's OWN offsets segment, the
    // one column_data_t::initialize_append assigned — not a child's.
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
        REQUIRE(stored == 2); // the layout really is one cumulative uint64 per row
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

// ---------------------------------------------------------------------------------------------
// (4) An in-place LIST update that changes a row's list length is refused, through data_table_t.
//
// REACHABLE: data_table_t::update is the WAL REPLAY leg of an update
// (table_storage_adapter_t::update); the txn leg is delete+append and never comes here. So the
// offending length arrives from a journal, on the disk agent's thread -- exactly where a throw
// becomes a hang.
TEST_CASE("nested column: an in-place LIST update cannot change the list length, and says so") {
    env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

    auto list_type = complex_logical_type::create_list(complex_logical_type{logical_type::UBIGINT});
    std::vector<logical_value_t> rows;
    rows.push_back(list_value(env, {10, 20}));
    auto built = build_table(env, bm, "v", list_type, rows, "list_len_update");

    auto row_ids = single_row_id(env, 0);
    auto types = built.table->copy_types();

    // POSITIVE CONTROL: a SAME-length rewrite goes through, so the refusal below is about the
    // length and not about LIST updates in general.
    {
        data_chunk_t upd(&env.resource, types, 1);
        upd.set_cardinality(1);
        upd.set_value(0, 0, list_value(env, {11, 21}));
        auto state = built.table->initialize_update({});
        auto updated = built.table->update(nontransactional_update_access_t::for_test(), *state, row_ids, upd);
        REQUIRE_FALSE(updated.has_error());
    }

    data_chunk_t upd(&env.resource, types, 1);
    upd.set_cardinality(1);
    upd.set_value(0, 0, list_value(env, {1, 2, 3}));
    auto state = built.table->initialize_update({});
    auto updated = built.table->update(nontransactional_update_access_t::for_test(), *state, row_ids, upd);
    REQUIRE(updated.has_error());
    REQUIRE(updated.error().type == core::error_code_t::unimplemented_yet);
}

// ---------------------------------------------------------------------------------------------
// (5) + (6) A malformed sub-column path into a STRUCT is refused on the update channel.
//
// PATH NOT NAMED: nothing outside components/table calls data_table_t::update_column, so the
// direct call below is the whole reachable surface. It is still the contract that matters: both
// shapes are caller errors on a function that already returns result_wrapper_t<bool>, so they
// must leave by the return rather than by a throw across the disk agent's mailbox.
TEST_CASE("nested column: a struct sub-column update path is validated on the update channel") {
    env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

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
    // depth 1 is what row_group_t::update_column starts the walk at: element 0 of the path is the
    // top-level column, and the struct reads the NEXT element.
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
        const std::vector<uint64_t> path{0, 1}; // field index 1 == sub_columns[0] == "a"
        auto updated = column->update_column(path, update_vector, row_ids, 1, start_depth);
        REQUIRE_FALSE(updated.has_error());
    }
}

// ---------------------------------------------------------------------------------------------
// (7) An unnamed struct is refused at the append gate rather than thrown from a constructor.
//
// REACHABLE FROM DISK BYTES. A struct is unnamed when its ALIAS is empty; column_definition_t
// and get_types name every top-level and field struct, so the shape can only arrive nested under
// a LIST/ARRAY -- and it does: complex_logical_type::create_variant builds LIST(struct "children")
// with no alias on either.
//
// struct_column_data_t's CONSTRUCTOR has no return value to refuse through, so the precondition
// lives in column_data_t::validate_column_type instead, asked at collection_t::initialize_append
// (which already returns result_wrapper_t<bool>) before every create_column on the write path.
TEST_CASE("nested column: an unnamed nested struct is refused by initialize_append") {
    env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

    std::pmr::vector<complex_logical_type> fields(&env.resource);
    fields.emplace_back(logical_type::BIGINT, "a");

    SECTION("LIST of an unnamed struct is refused") {
        // The LIST carries the column's name; its ELEMENT struct carries none.
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

// ---------------------------------------------------------------------------------------------
// (8) A flat-vector scan asked for a non-flat result reports instead of throwing.
//
// NO SQL PATH NAMES IT: every scan that picks its own mode goes through get_vector_scan_type,
// which answers SCAN_ENTIRE_VECTOR for a non-flat result; the two places that name
// SCAN_FLAT_VECTOR outright hand it a vector they just built flat (column_data_t::fetch's
// pre-image, the LIST offset vectors). Reached here through the public fetch(), which passes the
// CALLER's vector straight down -- the whole reachable surface.
//
// The second half guards the invariant that keeps the branch unreachable in production
// (get_vector_scan_type must never answer SCAN_FLAT_VECTOR for a non-flat result); sensitivity
// verified by inverting that early return by hand.
TEST_CASE("column scan: a flat-vector scan over a non-flat result refuses on the scan state") {
    env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

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

    // The sentinel: this is the answer that keeps every production scan away from the branch.
    column_scan_state mode_state;
    mode_state.initialize(complex_logical_type{logical_type::UBIGINT});
    column->initialize_scan(mode_state);
    REQUIRE(column->get_vector_scan_type(mode_state, 8, non_flat) == scan_vector_type::SCAN_ENTIRE_VECTOR);
}

// ---------------------------------------------------------------------------------------------
// (9) An index-build scan over a column that carries updates refuses on the scan state.
//
// REACHABLE THROUGH A PUBLIC API: data_table_t::create_index_scan takes the scan type, and
// table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES asks for a snapshot with no update overlay
// (no production caller passes it today -- the one create_index_scan call site, in services/disk,
// asks for COMMITTED_ROWS). The refusal lands in the column's scan_error, which row_group_t
// already aggregates into collection_scan_state::scan_error. fetch_updates has no argument of its
// own to report on, hence `state` threaded in.
TEST_CASE("column scan: an index-build scan over a column with updates refuses") {
    env_t env;
    tstorage::transient_block_manager_t bm(env.buffer_manager, tstorage::DEFAULT_BLOCK_ALLOC_SIZE);

    std::vector<logical_value_t> rows;
    for (int64_t i = 0; i < 4; i++) {
        rows.emplace_back(&env.resource, i);
    }
    auto built = build_table(env, bm, "n", complex_logical_type{logical_type::BIGINT}, rows, "index_scan_updates");

    std::vector<storage_index_t> column_indices;
    column_indices.emplace_back(static_cast<uint64_t>(0));

    // POSITIVE CONTROL on the fixture: with no updates recorded, the very same scan mode succeeds.
    {
        table_scan_state scan_state(&env.resource);
        built.table->initialize_scan(scan_state, column_indices);
        auto types = built.table->copy_types();
        data_chunk_t out(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        built.table->create_index_scan(scan_state, out, table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES);
        REQUIRE_FALSE(scan_state.table_state.has_error());
        REQUIRE(out.size() == 4);
    }

    // Record an update overlay on the column (the in-place / replay leg).
    {
        auto row_ids = single_row_id(env, 0);
        auto types = built.table->copy_types();
        data_chunk_t upd(&env.resource, types, 1);
        upd.set_cardinality(1);
        upd.set_value(0, 0, int64_t{77});
        auto state = built.table->initialize_update({});
        REQUIRE_FALSE(built.table->update(nontransactional_update_access_t::for_test(), *state, row_ids, upd).has_error());
    }

    table_scan_state scan_state(&env.resource);
    built.table->initialize_scan(scan_state, column_indices);
    auto types = built.table->copy_types();
    data_chunk_t out(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
    built.table->create_index_scan(scan_state, out, table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES);
    REQUIRE(scan_state.table_state.has_error());
    REQUIRE(scan_state.table_state.scan_error.type == core::error_code_t::index_create_fail);
}
