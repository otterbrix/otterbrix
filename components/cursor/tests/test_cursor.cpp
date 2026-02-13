#include <catch2/catch.hpp>
#include <components/cursor/cursor.hpp>
#include <components/tests/generaty.hpp>
#include <core/pmr.hpp>

#include <memory>

using namespace core::pmr;

TEST_CASE("components::cursor::construction") {
    auto resource = std::pmr::synchronized_pool_resource();
    INFO("empty cursor") {
        auto cursor = components::cursor::make_cursor(&resource);
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("failed operation cursor") {
        auto cursor = components::cursor::make_cursor(&resource, components::cursor::operation_status_t::failure);
        REQUIRE_FALSE(cursor->is_success());
        REQUIRE(cursor->is_error());
    }
    INFO("successful operation cursor") {
        auto cursor = components::cursor::make_cursor(&resource, components::cursor::operation_status_t::success);
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("error cursor") {
        std::string description = "error description";
        auto cursor =
            components::cursor::make_cursor(&resource, components::cursor::error_code_t::other_error, description);
        REQUIRE_FALSE(cursor->is_success());
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == components::cursor::error_code_t::other_error);
        REQUIRE(cursor->get_error().what == description);
    }
}

TEST_CASE("components::cursor::data_chunk") {
    auto resource = std::pmr::synchronized_pool_resource();

    SECTION("cursor with data_chunk") {
        auto chunk = gen_data_chunk(100, &resource);
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 100);
    }

    SECTION("cursor with empty data_chunk") {
        auto chunk = gen_data_chunk(0, &resource);
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 0);
    }
}
