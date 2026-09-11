// row_group_t::add_column/remove_column share ONE row_version_manager_t between parent and
// successor row group (set_version_info(get_or_create_version_info_ptr())). A fresh manager and
// the shared one both report "every row visible" on an append-only table, so no scan/count can
// tell them apart — only the manager's address and owner count can.

#include <catch2/catch_test_macros.hpp>
#include <components/table/collection.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/row_version_manager.hpp>
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

    // Spans several row groups (row_group_size defaults to DEFAULT_VECTOR_CAPACITY) since each
    // carries its own manager.
    constexpr uint64_t CHUNK_ROWS = 1000;
    constexpr uint64_t CHUNKS = 3;

    std::string alter_version_sharing_db_path() {
        static std::string path = "/tmp/test_otterbrix_alter_version_sharing_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    // Comma operator: remove stale file before naming it, ahead of the manager's open.
    const std::string& alter_version_sharing_fresh_db_path() {
        static const std::string path =
            (std::remove(alter_version_sharing_db_path().c_str()), alter_version_sharing_db_path());
        return path;
    }

    struct version_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        version_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, alter_version_sharing_fresh_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~version_env_t() { std::remove(alter_version_sharing_db_path().c_str()); }
    };

    std::unique_ptr<data_table_t> make_table(version_env_t& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("b", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "version_sharing");
    }

    void append_rows(data_table_t& table, version_env_t& env, int64_t start, uint64_t count) {
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

    // Append creates each row group's manager (append_version_info -> get_or_create_version_info).
    void fill(data_table_t& table, version_env_t& env) {
        for (uint64_t c = 0; c < CHUNKS; c++) {
            append_rows(table, env, static_cast<int64_t>(c * CHUNK_ROWS), CHUNK_ROWS);
        }
    }

    // The atomic is a non-owning cache of the owning pointer, published by set_version_info.
    void require_representations_agree(const row_group_t* group) {
        REQUIRE(group->version_manager_identity() != nullptr);
        REQUIRE(group->version_manager_published() == group->version_manager_identity());
    }

} // namespace

TEST_CASE("alter_version_sharing: ADD COLUMN hands the successor the parent's VERSION MANAGER") {
    version_env_t env;
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

        INFO("row group " << g);
        require_representations_agree(parent_group);
        require_representations_agree(child_group);

        // ...seeing ONE version manager, not a fresh one each.
        const row_version_manager_t* shared = parent_group->version_manager_identity();
        REQUIRE(child_group->version_manager_identity() == shared);
        // Owned by both row groups: neither may free it while the other lives.
        REQUIRE(parent_group->version_manager_owner_count() == 2);
        REQUIRE(child_group->version_manager_owner_count() == 2);
    }
}

TEST_CASE("alter_version_sharing: DROP COLUMN hands the successor the parent's VERSION MANAGER") {
    version_env_t env;
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
        require_representations_agree(parent_group);
        require_representations_agree(child_group);

        const row_version_manager_t* shared = parent_group->version_manager_identity();
        REQUIRE(child_group->version_manager_identity() == shared);
        REQUIRE(parent_group->version_manager_owner_count() == 2);
        REQUIRE(child_group->version_manager_owner_count() == 2);
    }
}

TEST_CASE("alter_version_sharing: a chain of ALTERs keeps ONE manager per row group") {
    // Owner count, not just address, must prove the manager isn't duplicated on the 2nd ALTER.
    version_env_t env;
    auto table = make_table(env);
    fill(*table, env);

    auto parent = table->row_group();
    const uint64_t groups = parent->row_group_tree()->segment_count();
    REQUIRE(groups > 1);

    column_definition_t added("c", complex_logical_type(logical_type::BIGINT));
    data_table_t first(*table, added);
    data_table_t second(*table, uint64_t(1)); // drop column "b" off the SAME parent

    auto first_groups = first.row_group();
    auto second_groups = second.row_group();

    for (int64_t g = 0; g < static_cast<int64_t>(groups); g++) {
        auto* parent_group = parent->row_group(g);
        auto* first_group = first_groups->row_group(g);
        auto* second_group = second_groups->row_group(g);
        REQUIRE(parent_group != nullptr);
        REQUIRE(first_group != nullptr);
        REQUIRE(second_group != nullptr);

        INFO("row group " << g);
        const row_version_manager_t* shared = parent_group->version_manager_identity();
        REQUIRE(shared != nullptr);
        REQUIRE(first_group->version_manager_identity() == shared);
        REQUIRE(second_group->version_manager_identity() == shared);
        REQUIRE(parent_group->version_manager_owner_count() == 3);
    }
}
