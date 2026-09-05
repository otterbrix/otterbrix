// WHAT THIS FILE MEASURES: the two things column_data_t's update-overlay predicate is asked
// and the one thing data_table_t must NOT offer.
//
//  * has_update_segment() answers "an update_segment_t object exists on this column", NOT
//    "this column's rows differ from their base". The two are different answers, and the
//    difference is REACHABLE, not theoretical: an update that writes back the value already
//    stored allocates the overlay and reports true forever after, on a column whose contents
//    never changed. Three consumers read it -- check_zonemap and get_vector_scan_type inside
//    column_data.cpp, and the per-segment report services/disk/manager_disk_t::
//    has_pending_update_overlay folds over -- and only the last one stated the over-report,
//    in ITS OWN header, so a reader of the declaration never saw it. The name carries it now.
//
//  * the ALTER TYPE successor constructor of data_table_t is GONE, on purpose. It never
//    assigned row_groups_ while it DID demote its parent out of root, so the first call would
//    have handed back a table whose every hot method dereferences a null collection
//    (adopt_types / initialize_scan / row_group_size / calculate_size / total_rows) on top of
//    a parent that refuses its own writes. It had zero callers, and both halves it needed --
//    collection_t::alter_type and row_group_t::alter_type -- are commented out. A compile
//    error is the loudest refusal available, so the declaration is the refusal.

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <string>
#include <type_traits>
#include <unistd.h>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    // pid-qualified, like every other fixture in this directory: two concurrent runs must not
    // share one .otbx.
    std::string overlay_predicate_db_path() {
        static std::string path = "/tmp/test_otterbrix_update_overlay_predicate_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    const std::string& overlay_predicate_fresh_db_path() {
        static const std::string path =
            (std::remove(overlay_predicate_db_path().c_str()), overlay_predicate_db_path());
        return path;
    }

    struct overlay_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        overlay_env()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, overlay_predicate_fresh_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~overlay_env() { std::remove(overlay_predicate_db_path().c_str()); }
    };

    std::unique_ptr<data_table_t> make_one_column_table(overlay_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "overlay");
    }

    void append_committed_row(data_table_t& table, overlay_env& env, int64_t value) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, 1);
        chunk.data[0].set_value(0, logical_value_t(&env.resource, value));
        chunk.set_cardinality(1);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    core::result_wrapper_t<std::pair<int64_t, uint64_t>>
    update_in_place(data_table_t& table, overlay_env& env, int64_t row_id, int64_t new_value) {
        auto types = table.copy_types();
        auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
        row_ids.set_value(0, logical_value_t(&env.resource, row_id));
        auto payload = data_chunk_t(&env.resource, types, 1);
        payload.data[0].set_value(0, logical_value_t(&env.resource, new_value));
        payload.set_cardinality(1);
        auto state = table.initialize_update({});
        return table.update(*state, row_ids, payload);
    }

    // The same answer as seen by the only production consumer: the per-segment report
    // services/disk/manager_disk_t::has_pending_update_overlay folds over.
    std::size_t segments_reporting_updates(data_table_t& table) {
        std::size_t reporting = 0;
        for (const auto& info : table.get_column_segment_info()) {
            if (info.has_updates) {
                ++reporting;
            }
        }
        return reporting;
    }

    int64_t scan_first_value(data_table_t& table, overlay_env& env) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);
        scan_state.table_state.txn = transaction_data{TRANSACTION_ID_START + 3, 30};
        auto types = table.copy_types();
        auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        table.scan(result, scan_state);
        REQUIRE(result.size() == 1);
        return result.data[0].value(0).value<int64_t>();
    }

} // anonymous namespace

// The over-report, driven without injecting a single failure: the update writes the value the
// row already holds. Nothing about the column's contents changes -- and the predicate flips
// from false to true and stays there, because what it answers is "a segment object exists".
TEST_CASE("components::table::column_data::an_update_that_changes_no_value_still_reports_an_update_segment") {
    overlay_env env;
    auto table = make_one_column_table(env);
    append_committed_row(*table, env, 7);

    REQUIRE(segments_reporting_updates(*table) == 0);
    REQUIRE(scan_first_value(*table, env) == 7);

    // The SAME value back onto the same row.
    auto updated = update_in_place(*table, env, /*row_id=*/0, /*new_value=*/7);
    REQUIRE_FALSE(updated.has_error());
    REQUIRE(updated.value().second == 1);

    const int64_t seen = scan_first_value(*table, env);
    INFO("value before " << 7 << ", value after " << seen);
    REQUIRE(seen == 7);

    // Contents identical, predicate true: that is the whole of the name.
    INFO("segments reporting an overlay: " << segments_reporting_updates(*table));
    REQUIRE(segments_reporting_updates(*table) > 0);
}

// The negative half, so the case above cannot pass on a predicate that simply answers true:
// a table that was only appended to reports NO overlay on any of its segments, and a column
// that was never written to at all answers the predicate BY NAME.
TEST_CASE("components::table::column_data::an_appended_only_column_reports_no_update_segment") {
    overlay_env env;
    auto table = make_one_column_table(env);
    append_committed_row(*table, env, 1);
    append_committed_row(*table, env, 2);
    REQUIRE(segments_reporting_updates(*table) == 0);

    // row_group_t::get_column is private, so the predicate is asked of a standalone column --
    // the same object, reached through the public factory.
    auto fresh = column_data_t::create_column(&env.resource,
                                              env.block_manager,
                                              /*column_index=*/0,
                                              /*start_row=*/0,
                                              complex_logical_type(logical_type::BIGINT));
    REQUIRE(fresh != nullptr);
    REQUIRE_FALSE(fresh->has_update_segment());
}

// The mined ALTER TYPE successor constructor must stay unbuildable. See the file header.
TEST_CASE("components::table::data_table::the_alter_type_successor_constructor_is_not_declared") {
    constexpr bool alter_type_ctor_exists = std::is_constructible_v<data_table_t,
                                                                    data_table_t&,
                                                                    uint64_t,
                                                                    const complex_logical_type&,
                                                                    const std::vector<storage_index_t>&>;
    INFO("a declared ALTER TYPE constructor leaves row_groups_ null while demoting its parent");
    REQUIRE_FALSE(alter_type_ctor_exists);

    // The two constructors that DO build a successor are unaffected -- the guard above must not
    // pass by data_table_t having become unconstructible altogether.
    REQUIRE(std::is_constructible_v<data_table_t, data_table_t&, column_definition_t&>);
    REQUIRE(std::is_constructible_v<data_table_t, data_table_t&, uint64_t>);
}
