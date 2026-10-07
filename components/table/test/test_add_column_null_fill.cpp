#include <catch2/catch_test_macros.hpp>

#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <memory>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    std::string table_db_path(const std::string& name) {
        std::string path = "/tmp/test_otterbrix_add_column_null_fill_" + name + "_" + std::to_string(::getpid()) + ".otbx";
        std::remove(path.c_str());
        return path;
    }

    struct table_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        std::string path;
        storage::single_file_block_manager_t block_manager;

        explicit table_env(const std::string& name)
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , path(table_db_path(name))
            , block_manager(buffer_manager, fs, path) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~table_env() { std::remove(path.c_str()); }
    };

    std::unique_ptr<data_table_t> bigint_table_with_rows(table_env& env, uint64_t count) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type(logical_type::BIGINT));
        auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");
        auto types = table->copy_types();
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        auto chunk = data_chunk_t(&env.resource, types, count);
        for (uint64_t i = 0; i < count; i++) {
            chunk.data[0].set_value(i, logical_value_t(&env.resource, static_cast<int64_t>(i)));
        }
        chunk.set_cardinality(count);
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data::committed());
        return table;
    }

    void backfills_null(const char* name, complex_logical_type type) {
        table_env env(name);
        auto table = bigint_table_with_rows(env, 3);
        column_definition_t new_column("added", std::move(type));
        auto extended = std::make_unique<data_table_t>(*table, new_column);
        INFO("error: " << (extended->has_construction_error() ? extended->construction_error().what : "none"));
        REQUIRE_FALSE(extended->has_construction_error());
        CHECK(extended->column_count() == 2);
    }

} // namespace

TEST_CASE("components::table::add_column_null_fill::fixed_array_backfills_null") {
    backfills_null("array", complex_logical_type::create_array(logical_type::BIGINT, 1));
}

TEST_CASE("components::table::add_column_null_fill::interval_backfills_null") {
    backfills_null("interval", complex_logical_type(logical_type::INTERVAL));
}

TEST_CASE("components::table::add_column_null_fill::blob_is_refused_not_thrown") {
    table_env env("blob");
    auto table = bigint_table_with_rows(env, 3);
    column_definition_t new_column("added", complex_logical_type(logical_type::BLOB));
    auto extended = std::make_unique<data_table_t>(*table, new_column);
    REQUIRE(extended->has_construction_error());
    CHECK(extended->column_count() == 1);
}
