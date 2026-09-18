// column_segment_t latches its reload-constructor failure (a corrupt big-string overflow
// list) into construction_error_, because a constructor has no return channel on the open
// path. The move constructors used to leave the error behind ("a segment is only ever moved
// after the error is read" — vacuously true: they had no callers at all), so the FIRST
// mover to appear would silently swallow a latched data_corruption and the half-built
// segment would scan as healthy. The error is state like any other: a move carries it.

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    std::string cem_db_path() {
        static std::string path = "/tmp/test_otterbrix_construction_error_move_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    struct cem_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;
        tstorage::single_file_block_manager_t block_manager;

        cem_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, (std::remove(cem_db_path().c_str()), cem_db_path())) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~cem_env_t() { std::remove(cem_db_path().c_str()); }
    };

    // The reload constructor's one failure mode: a persisted overflow list naming the same
    // block twice. Mirrors what column_data_t::initialize_column hands the constructor.
    std::unique_ptr<column_segment_t> make_latched_segment(cem_env_t& env) {
        auto overflow_state = std::make_unique<column_segment_state>();
        overflow_state->blocks = {7, 7}; // the duplicate that latches

        auto handle = env.block_manager.register_block(3);
        auto segment = std::make_unique<column_segment_t>(handle,
                                                          complex_logical_type(logical_type::STRING_LITERAL),
                                                          0,
                                                          1,
                                                          3,
                                                          0,
                                                          64,
                                                          std::move(overflow_state));
        REQUIRE(segment->has_construction_error());
        REQUIRE(segment->construction_error().type == core::error_code_t::data_corruption);
        return segment;
    }

} // namespace

TEST_CASE("construction_error: the plain move constructor carries the latched error", "[segmove]") {
    cem_env_t env;
    auto latched = make_latched_segment(env);

    column_segment_t moved(std::move(*latched));
    INFO("a latched data_corruption must survive the move, or the mover scans a corrupt segment as healthy");
    CHECK(moved.has_construction_error());
    CHECK(moved.construction_error().type == core::error_code_t::data_corruption);
}

TEST_CASE("construction_error: the re-based move constructor carries the latched error", "[segmove]") {
    cem_env_t env;
    auto latched = make_latched_segment(env);

    column_segment_t moved(std::move(*latched), /*start=*/128);
    INFO("the (other, start) constructor takes the same state; the error is state");
    CHECK(moved.has_construction_error());
    CHECK(moved.construction_error().type == core::error_code_t::data_corruption);
}
