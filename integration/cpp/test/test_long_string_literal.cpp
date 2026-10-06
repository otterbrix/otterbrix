// A string literal longer than the lexer's first 1024-byte buffer grows that buffer; the text must come back whole.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <string>

namespace {

    std::string letters(std::size_t length) {
        std::string text;
        text.reserve(length);
        for (std::size_t i = 0; i < length; ++i) {
            text.push_back(static_cast<char>('a' + i % 26));
        }
        return text;
    }

    std::string first_text(const components::cursor::cursor_t_ptr& cursor) {
        REQUIRE(cursor->size() == 1);
        const auto cell = cursor->value(0, 0);
        return std::string{cell.value<std::string_view>()};
    }

} // namespace

TEST_CASE("integration::cpp::long_string_literal::round_trips_whole") {
    auto config = test_create_config(integration_fixture_path("test_long_string_literal"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE d;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE d.t (id BIGINT, s TEXT);")->is_success());

    SECTION("stored and read back") {
        std::size_t id = 0;
        for (const std::size_t length : {std::size_t{1025}, std::size_t{64} * 1024, std::size_t{200} * 1024}) {
            INFO("literal of " << length << " bytes");
            const auto text = letters(length);
            ++id;
            auto inserted = test_helpers::exec(dispatcher,
                                               "INSERT INTO d.t (id, s) VALUES (" + std::to_string(id) + ", '" +
                                                   text + "');");
            INFO((inserted->is_error() ? std::string{inserted->get_error().what} : std::string{"ok"}));
            REQUIRE(inserted->is_success());
            auto read = test_helpers::exec(dispatcher, "SELECT s FROM d.t WHERE id = " + std::to_string(id) + ";");
            REQUIRE(read->is_success());
            CHECK(first_text(read) == text);
        }
    }
    // More than one stored string may hold (262132 bytes), and enough for the overread to fault without ASAN.
    SECTION("a 1 MB literal in a query") {
        const auto text = letters(std::size_t{1024} * 1024);
        auto read = test_helpers::exec(dispatcher, "SELECT '" + text + "' AS s;");
        INFO((read->is_error() ? std::string{read->get_error().what} : std::string{"ok"}));
        REQUIRE(read->is_success());
        CHECK(first_text(read) == text);
    }
}
