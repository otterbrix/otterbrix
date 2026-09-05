#include <catch2/catch_test_macros.hpp>

#include <components/table/standard_column_data.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <string>
#include <unistd.h>

// update_segment_t::update's merge leg (merge_update -> merge_update_loop ->
// merge_update_loop_internal) had never been exercised by any test in this tree before this file
// (test_column.cpp updates each column exactly once).
//
// Its tail loop:
//     for (; aidx < count; aidx++) { ...; count++; }
// uses `count` as BOTH the loop bound and the running output index, so the condition never turns
// false: indexing.get_index(aidx) walks off the indexing vector and result_values[result_offset++]
// overruns its 2048-entry stack array (EXC_BAD_ACCESS at update_segment.hpp).
//
// Triggered whenever an incoming id sorts AFTER every id already in base_info — i.e. the second
// update names a HIGHER row than the first, the shape
// agent_disk_t::update_pg_attribute_commit_id_field_inner drives once it can see the row.

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    std::string fresh_db_path(const std::string& name) {
        std::string path = "/tmp/test_otterbrix_update_merge_" + name + "_" + std::to_string(::getpid()) + ".otbx";
        std::remove(path.c_str());
        return path;
    }

    struct update_merge_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        std::string path;
        storage::single_file_block_manager_t block_manager;

        explicit update_merge_env(const std::string& name)
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , path(fresh_db_path(name))
            , block_manager(buffer_manager, fs, path) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~update_merge_env() { std::remove(path.c_str()); }
    };

    // UINTEGER on purpose: the crash was reported out of
    // merge_update_loop_internal<unsigned int, unsigned int>.
    std::unique_ptr<column_data_t> make_filled_column(update_merge_env& env, uint64_t count) {
        auto column = column_data_t::create_column(&env.resource, env.block_manager, 0, 0, logical_type::UINTEGER);
        vector_t v(&env.resource, logical_type::UINTEGER, count);
        for (uint64_t i = 0; i < count; i++) {
            v.set_value(i, static_cast<uint32_t>(i));
        }
        column_append_state state;
        REQUIRE_FALSE(column->initialize_append(state).has_error());
        REQUIRE_FALSE(column->append(state, v, count).has_error());
        return column;
    }

    // One-row update of `row_id`, through the same entry point row_group_t::update uses.
    void update_one(update_merge_env& env, column_data_t& column, int64_t row_id, uint32_t value) {
        vector_t v(&env.resource, logical_type::UINTEGER, 1);
        v.set_value(0, value);
        int64_t ids[1] = {row_id};
        auto r = column.update(0, v, ids, 1);
        REQUIRE_FALSE(r.has_error());
    }

    uint32_t read_row(update_merge_env& env, column_data_t& column, int64_t row_id) {
        vector_t v(&env.resource, logical_type::UINTEGER, 1);
        column_fetch_state state;
        column.fetch_row(state, row_id, v, 0);
        return v.value(0).value<uint32_t>();
    }

} // namespace

TEST_CASE("components::table::update_merge::a_second_update_of_a_higher_row_in_the_same_vector") {
    update_merge_env env("higher_row");
    auto column = make_filled_column(env, 64);

    update_one(env, *column, 0, 900);
    // Row 5 sorts after row 0, so the merge exhausts base_info and falls into the tail loop.
    update_one(env, *column, 5, 905);

    CHECK(read_row(env, *column, 0) == 900);
    CHECK(read_row(env, *column, 5) == 905);
    CHECK(read_row(env, *column, 6) == 6);
}

// Same defect, reached by ascending row ids across successive updates instead. Every
// earlier update stays readable.
TEST_CASE("components::table::update_merge::repeated_ascending_updates_do_not_run_off_the_merge") {
    update_merge_env env("ascending");
    auto column = make_filled_column(env, 64);

    for (int64_t row = 0; row < 8; ++row) {
        update_one(env, *column, row, static_cast<uint32_t>(1000 + row));
    }
    for (int64_t row = 0; row < 8; ++row) {
        CHECK(read_row(env, *column, row) == static_cast<uint32_t>(1000 + row));
    }
    CHECK(read_row(env, *column, 8) == 8);
}

// update_select_element_t::operation copies an incoming std::string_view into the segment's own
// heap (initialize_update_data and merge's phase-1 base branch both do it) — but phase 2's
// pick_new stored the raw view straight off the caller's update vector, a temporary at every
// caller (e.g. agent_disk_t::direct_update_sync's local data_chunk_t), leaving the merged row
// pointing at freed memory.
//
// Reads as catalog corruption, not a crash: a pg_attribute commit-id backfill left an attname
// pointing at reused memory (observed as "path 'd' was not found" / "path 'a' is ambiguous").
TEST_CASE("components::table::update_merge::a_merged_string_update_owns_its_bytes") {
    update_merge_env env("strings");
    constexpr uint64_t rows = 64;
    std::pmr::vector<complex_logical_type> string_types(&env.resource);
    string_types.emplace_back(logical_type::STRING_LITERAL);

    auto column = column_data_t::create_column(&env.resource,
                                               env.block_manager,
                                               0,
                                               0,
                                               complex_logical_type{logical_type::STRING_LITERAL});
    {
        data_chunk_t input(&env.resource, string_types, rows);
        input.set_cardinality(rows);
        for (uint64_t i = 0; i < rows; i++) {
            const std::string v = "original_row_value_" + std::to_string(i);
            input.set_value(0, i, std::string_view{v});
        }
        column_append_state state;
        REQUIRE_FALSE(column->initialize_append(state).has_error());
        REQUIRE_FALSE(column->append(state, input.data[0], rows).has_error());
    }

    auto update_string = [&](int64_t row_id, const std::string& value) {
        // Scoped exactly as every production caller scopes it: the chunk holding the
        // bytes dies the moment update() returns.
        data_chunk_t upd(&env.resource, string_types, 1);
        upd.set_cardinality(1);
        upd.set_value(0, 0, std::string_view{value});
        int64_t ids[1] = {row_id};
        auto r = column->update(0, upd.data[0], ids, 1);
        REQUIRE_FALSE(r.has_error());
    };
    auto read_string = [&](int64_t row_id) {
        data_chunk_t out(&env.resource, string_types, 1);
        out.set_cardinality(1);
        column_fetch_state state;
        column->fetch_row(state, row_id, out.data[0], 0);
        const auto cell = out.value(0, 0);
        return std::string(cell.value<std::string_view>());
    };

    update_string(0, "first_update_value_for_row_zero");
    // Row 5 sorts after row 0: the merge leg, whose pick_new stored the view uncopied.
    update_string(5, "second_update_value_for_row_five");

    // Give the resource every chance to hand those freed bytes to somebody else.
    for (int i = 0; i < 32; ++i) {
        data_chunk_t churn(&env.resource, string_types, 64);
        churn.set_cardinality(64);
        for (uint64_t j = 0; j < 64; j++) {
            const std::string v = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" + std::to_string(j);
            churn.set_value(0, j, std::string_view{v});
        }
    }

    CHECK(read_string(0) == "first_update_value_for_row_zero");
    CHECK(read_string(5) == "second_update_value_for_row_five");
    CHECK(read_string(6) == "original_row_value_6");
}

// The mirror leg: a second update naming a LOWER row runs the OTHER tail loop, bounded by
// base_info.N, which terminates fine — guards the working half against a regression.
TEST_CASE("components::table::update_merge::a_second_update_of_a_lower_row_keeps_both") {
    update_merge_env env("lower_row");
    auto column = make_filled_column(env, 64);

    update_one(env, *column, 5, 905);
    update_one(env, *column, 0, 900);

    CHECK(read_row(env, *column, 0) == 900);
    CHECK(read_row(env, *column, 5) == 905);
}

// undo_buffer_pointer_t::pin() must not swallow buffer_manager_t::pin()'s refusal in an assert
// (vanishes under -DNDEBUG, returning an undo_buffer_reference with an empty handle): every
// consumer calls update_info() = reinterpret_cast<update_info_t*>(handle.ptr() + position), so an
// empty handle silently yields `nullptr + position` in release builds. Returns
// core::result_wrapper_t<undo_buffer_reference> instead — the refusal is a value.
//
// NOT covered: the refusal leg itself. Forcing standard_buffer_manager_t::pin to fail on a
// TRANSACTION block needs it evicted and reload starved, and this tree has no seam for that
// (test_eviction_guard.cpp exhausts the pool at allocate(), not at re-pin).
TEST_CASE("components::table::update_merge::a_pinned_undo_node_is_a_real_address") {
    update_merge_env env("pin_contract");

    undo_buffer_allocator_t allocator(env.buffer_manager);
    auto allocated = allocator.allocate(update_info_t::allocation_size(sizeof(uint32_t)));
    REQUIRE_FALSE(allocated.has_error());

    auto pointer = allocated.value().buffer_pointer();
    REQUIRE(pointer.is_set());

    auto pinned = pointer.pin();
    REQUIRE_FALSE(pinned.has_error());
    // The whole point: a successful pin resolves to a real address, never nullptr + position.
    CHECK(pinned.value().ptr() != nullptr);
    CHECK(pinned.value().is_set());
}

// buffer_pointer() on a default (unset) undo_buffer_reference must answer unset: an unguarded
// `return {*entry, position};` binds a reference to *nullptr — UB that only looks harmless
// because the compiler folds &*nullptr back to nullptr.
TEST_CASE("components::table::update_merge::an_unset_undo_reference_has_no_buffer_pointer") {
    undo_buffer_reference none;
    CHECK_FALSE(none.is_set());
    CHECK_FALSE(none.buffer_pointer().is_set());

    // pin() keeps an assert here on purpose: naming no entry is a CALLER precondition (every
    // call site guards with is_set()), not a runtime refusal — and an error_t would need a
    // resource for its message that a bare pointer has none of.
    undo_buffer_pointer_t nowhere;
    CHECK_FALSE(nowhere.is_set());
}

// initialize_update_data must index the update vector by indexing.get_index(i) alone: adding
// vector_index * DEFAULT_VECTOR_CAPACITY reads past a count-element vector, and the neighbouring
// initialize_update_validity does not add it. With the addend, read_row(1500) returns heap garbage
// instead of 906.
TEST_CASE("components::table::update_merge::the_first_update_of_a_second_vector_stores_the_updates_values") {
    update_merge_env env("second_vector");
    auto column = make_filled_column(env, 1024);
    {
        // Grow the column to 2048 rows with a second append; vector capacity is 1024.
        vector_t v(&env.resource, logical_type::UINTEGER, 1024);
        for (uint64_t i = 0; i < 1024; i++) {
            v.set_value(i, static_cast<uint32_t>(1024 + i));
        }
        column_append_state state;
        REQUIRE_FALSE(column->initialize_append(state).has_error());
        REQUIRE_FALSE(column->append(state, v, 1024).has_error());
    }

    update_one(env, *column, 1500, 906);

    CHECK(read_row(env, *column, 1500) == 906);
    CHECK(read_row(env, *column, 1499) == 1499);
    CHECK(read_row(env, *column, 1501) == 1501);
}
