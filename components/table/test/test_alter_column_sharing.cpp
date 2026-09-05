// ALTER structural sharing — the identity gate.
//
// row_group_t::add_column / remove_column build the successor's row group by COPYING the parent's
// column vector, so a table and its ALTER successor hold the SAME column objects. That sharing is
// load-bearing: it is why ALTER does not duplicate the table's storage, and it is the premise the
// compact() proof on data_table_t is written against.
//
// No scan, count or checksum can gate it. A deep copy of the columns reads back exactly the same
// rows through every one of them, so "the tests are green" is compatible with the sharing having
// quietly turned into a copy. Only the OBJECT can tell: its address, and how many row groups own
// it. That is what these cases assert, through row_group_t's DEV_MODE identity observers.

#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    // Two columns and enough rows to span SEVERAL row groups (row_group_size defaults to
    // DEFAULT_VECTOR_CAPACITY): sharing has to hold for every row group, not just the first.
    constexpr uint64_t COLUMN_COUNT = 2;
    constexpr uint64_t CHUNK_ROWS = 1000;
    constexpr uint64_t CHUNKS = 3;

    // The fixture runs on a real .otbx — there is no file-less block manager any more. The row
    // counts here span more than one row group, so closing one writes its segments through to
    // the file: this fixture reaches the disk path for real. Not one assertion below is about
    // the substrate.
    std::string alter_column_sharing_db_path() {
        static std::string path = "/tmp/test_otterbrix_alter_column_sharing_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    // Removes any leftover from an earlier process that died holding this pid, then names the
    // file. Called from the member-init list, so the removal precedes the manager's open.
    const std::string& alter_column_sharing_fresh_db_path() {
        static const std::string path = (std::remove(alter_column_sharing_db_path().c_str()), alter_column_sharing_db_path());
        return path;
    }

    struct alter_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        alter_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, alter_column_sharing_fresh_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~alter_env_t() { std::remove(alter_column_sharing_db_path().c_str()); }
    };

    std::unique_ptr<data_table_t> make_table(alter_env_t& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("b", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "alter_sharing");
    }

    void append_rows(data_table_t& table, alter_env_t& env, int64_t start, uint64_t count) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, count);
        for (uint64_t i = 0; i < count; i++) {
            const int64_t v = start + static_cast<int64_t>(i);
            chunk.data[0].set_value(i, v);
            chunk.data[1].set_value(i, -v);
        }
        chunk.set_cardinality(count);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    void fill(data_table_t& table, alter_env_t& env) {
        for (uint64_t c = 0; c < CHUNKS; c++) {
            append_rows(table, env, static_cast<int64_t>(c * CHUNK_ROWS), CHUNK_ROWS);
        }
    }

} // namespace

TEST_CASE("alter_sharing: ADD COLUMN hands the successor the parent's column OBJECTS") {
    alter_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    auto parent = table->row_group();
    const uint64_t groups = parent->row_group_tree()->segment_count();
    REQUIRE(groups > 1);

    column_definition_t added("c", complex_logical_type(logical_type::BIGINT));
    data_table_t successor(*table, added);

    auto child = successor.row_group();
    REQUIRE(child != parent);
    REQUIRE(child->row_group_tree()->segment_count() == groups);

    for (int64_t g = 0; g < static_cast<int64_t>(groups); g++) {
        auto* parent_group = parent->row_group(g);
        auto* child_group = child->row_group(g);
        REQUIRE(parent_group != nullptr);
        REQUIRE(child_group != nullptr);
        // Distinct row groups, one per collection...
        REQUIRE(parent_group != child_group);

        for (uint64_t c = 0; c < COLUMN_COUNT; c++) {
            INFO("row group " << g << ", column " << c);
            const column_data_t* original = parent_group->column_identity(c);
            REQUIRE(original != nullptr);
            // ...pointing at ONE column object, not at a copy of it.
            REQUIRE(child_group->column_identity(c) == original);
            // Owned by both row groups: neither may free it while the other lives.
            REQUIRE(parent_group->column_owner_count(c) == 2);
            REQUIRE(child_group->column_owner_count(c) == 2);
        }

        // The column ADD COLUMN created is the successor's alone — the parent never sees it.
        REQUIRE(child_group->column_identity(COLUMN_COUNT) != nullptr);
        REQUIRE(child_group->column_owner_count(COLUMN_COUNT) == 1);
    }
}

TEST_CASE("alter_sharing: DROP COLUMN hands the successor the surviving column OBJECTS") {
    alter_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    auto parent = table->row_group();
    const uint64_t groups = parent->row_group_tree()->segment_count();
    REQUIRE(groups > 1);

    data_table_t successor(*table, uint64_t(0)); // drop column "a"

    auto child = successor.row_group();
    REQUIRE(child != parent);
    REQUIRE(child->row_group_tree()->segment_count() == groups);

    for (int64_t g = 0; g < static_cast<int64_t>(groups); g++) {
        auto* parent_group = parent->row_group(g);
        auto* child_group = child->row_group(g);
        REQUIRE(parent_group != nullptr);
        REQUIRE(child_group != nullptr);
        REQUIRE(parent_group != child_group);

        INFO("row group " << g);
        // Column 1 of the parent IS column 0 of the successor — the same object, re-indexed.
        const column_data_t* survivor = parent_group->column_identity(1);
        REQUIRE(survivor != nullptr);
        REQUIRE(child_group->column_identity(0) == survivor);
        REQUIRE(parent_group->column_owner_count(1) == 2);

        // The dropped column stays alive in the parent alone, and is not smuggled into the
        // successor under another index.
        const column_data_t* dropped = parent_group->column_identity(0);
        REQUIRE(dropped != nullptr);
        REQUIRE(dropped != survivor);
        REQUIRE(parent_group->column_owner_count(0) == 1);
    }
}
