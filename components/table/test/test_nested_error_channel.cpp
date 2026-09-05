// TEN REFUSALS IN THE COLUMN LAYER THAT USED TO BE THROWS.
//
// Every one of them sat below a mailbox: components/table runs inside the disk agent, whose
// handlers are actor-zeta coroutines with an EMPTY unhandled_exception(). A throw there does not
// become an error the caller can see — it unwinds out of the coroutine and the statement HANGS.
// That is strictly worse than an abort, and it is why rule 2/9 forbids it here. This file pins
// each one to the error channel its caller ALREADY reads:
//
//   * column_scan_state::scan_error  — read by column_data_t::update after fetch(), aggregated by
//                                      row_group_t into collection_scan_state::scan_error;
//   * core::result_wrapper_t<bool>   — returned by update / update_column / initialize_append all
//                                      the way up to data_table_t.
//
// Nothing new was invented: both channels predate this file (see the notes on column_state.hpp).
//
// REACHABILITY IS STATED PER CASE, not assumed. Two of the ten (LIST/ARRAY point fetch) have NO
// caller at all — column_data_t::fetch is called only from column_data_t::update and from
// struct_column_data_t::fetch, and LIST/ARRAY/STRUCT override BOTH update and update_column, so
// neither is ever entered with a nested node as `this`. They are still tested by direct call,
// because the override has to keep existing: deleting it would let the base implementation answer
// a LIST fetch with the column's raw ELEMENT OFFSETS, and an ARRAY fetch by walking a segment tree
// an ARRAY node never fills.

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
// PATH NOT NAMED: no SQL statement reaches these. The single virtual caller of a nested fetch()
// is struct_column_data_t::fetch, which itself is only reachable from column_data_t::update — and
// STRUCT overrides update and update_column both, so that call site is never entered with a struct
// as `this`. The direct call below is the whole reachable surface, and it is the contract that
// matters: the refusal lands in state.scan_error, exactly where column_data_t::update looks
// (column_data.cpp: `if (state.has_error()) return state.scan_error;`).
//
// BEFORE: `throw std::logic_error("Function is not implemented: List fetch")` — the test aborted
// with an unexpected exception instead of reaching any REQUIRE.
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
// REACHABLE FROM DATA. The offsets are this column's OWN segment payload — cumulative element
// counts, one uint64 per row (column_segment.cpp::stored_element_size pins the width at 8 for a
// LIST) — so they come off disk like any other bytes and a corrupt run is a read failure, not a
// program error. The surgery below writes one such value directly, which is the same thing a bad
// block would hand the scan.
//
// BEFORE: `throw std::runtime_error("list_column_data_t::scan_count - internal list scan offset is
// out of range")` — an unexpected exception out of scan_count.
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
// REACHABLE. data_table_t::update is the WAL REPLAY leg of an update
// (table_storage_adapter_t::update(row_ids, data)); the txn leg is delete+append and never comes
// here. So the offending length arrives from a journal on disk, on the disk agent's thread — the
// exact place a throw becomes a hang.
//
// BEFORE: `throw std::logic_error("list_column_data_t::update: in-place update cannot change a
// row's list length")` — an unexpected exception out of data_table_t::update.
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

// ---------------------------------------------------------------------------------------------
// (5) + (6) A malformed sub-column path into a STRUCT is refused on the update channel.
//
// PATH NOT NAMED, and the reason is a routing gap this case found rather than assumed:
// row_group_t::update_column — the ONLY caller of column_data_t::update_column — has no caller
// itself. collection_t::update_column (collection.cpp) hands its column_path to
// row_group_t::UPDATE instead, where the path is read as a list of top-level column ids. So
// data_table_t::update_column today behaves as a one-column update and never descends into a
// struct; the whole update_column family below it is unreached. That gap is reported, NOT patched
// here — routing is a different change from giving these two refusals a channel.
//
// The direct call below is therefore the whole reachable surface. It is still the contract that
// matters: both shapes are caller errors on a function that already returns
// result_wrapper_t<bool>, and they used to leave it by throwing across the disk agent's mailbox.
//
// BEFORE: `throw std::runtime_error("Attempting to directly update a struct column ...")` and
// `throw std::runtime_error("update column_path out of range")` — an unexpected exception instead
// of a returned error.
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
// REACHABLE FROM DISK BYTES, which is why this one mattered most. A struct type is unnamed when
// its ALIAS is empty. column_definition_t stamps the column name onto a top-level column's type
// and get_types stamps every struct FIELD, so the shape can only arrive nested under a LIST/ARRAY
// — and it does: complex_logical_type::create_variant builds LIST(struct "children") with no alias
// on either, and catalog decode_type_spec hands back a VARIANT for the literal atttypspec text
// "VARIANT". The refusal therefore has to travel, not abort.
//
// It cannot travel from where it was: struct_column_data_t's CONSTRUCTOR has no return value. The
// precondition now lives in column_data_t::validate_column_type and is asked at
// collection_t::initialize_append, which already returns result_wrapper_t<bool> and precedes every
// create_column on the write path.
//
// BEFORE: `throw std::logic_error("A table cannot be created from an unnamed struct")` out of
// data_table_t::initialize_append.
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
// NO SQL PATH NAMES IT. Every scan that picks its own mode goes through get_vector_scan_type,
// which answers SCAN_ENTIRE_VECTOR for a non-flat result and so cannot produce the mismatch; the
// two places that name SCAN_FLAT_VECTOR outright hand it a vector they just built flat
// (column_data_t::fetch's pre-image, the LIST offset vectors). The branch is reached here through
// the public fetch(), which passes the CALLER's vector straight down — the whole reachable
// surface, and enough to prove the guard reports rather than throws.
//
// The second half is the sentinel for the invariant that keeps the branch unreachable in
// production: get_vector_scan_type must never answer SCAN_FLAT_VECTOR for a non-flat result. Its
// sensitivity was proved by inverting that early return by hand — the REQUIRE below went red, and
// so did the "throw" spelling of the guard when it was restored temporarily.
//
// BEFORE: `throw std::logic_error("scan_vector called with SCAN_FLAT_VECTOR but result is not a
// flat vector")`.
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
// table_scan_type::COMMITTED_ROWS_DISALLOW_UPDATES is what asks for a snapshot with no update
// overlay. (No caller passes it today — the one create_index_scan call site in services/disk asks
// for COMMITTED_ROWS — so nothing in production reaches it either; that is stated, not assumed.)
// The refusal lands in the column's scan_error, which row_group_t already aggregates into
// collection_scan_state::scan_error, so it is readable exactly where a pin OOM would be.
//
// BEFORE: `throw std::logic_error("Cannot create index with outstanding updates")` — and
// fetch_updates had no argument it could have reported on, which is why `state` is threaded in.
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
