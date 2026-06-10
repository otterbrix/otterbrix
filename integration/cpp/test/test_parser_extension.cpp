#include "test_config.hpp"

#include <catch2/catch.hpp>

#include <demo_extension.hpp>

using namespace components;
using namespace components::cursor;

TEST_CASE("integration::cpp::parser_extension_demo") {
    auto config = test_create_config("/tmp/test_demo_extension");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("without the extension, DEMO is rejected") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DEMO 1 + 20");
        REQUIRE_FALSE(cur->is_success());
    }

    REQUIRE_FALSE(dispatcher->add_parser_extension(make_demo_extension()).has_error());

    INFO("with the extension registered, DEMO is OK") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DEMO 1 + 20");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 21);
    }

    INFO("precedence") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DEMO 2 + 3 * 4");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 14);
    }
}

TEST_CASE("integration::cpp::parser_extension_is_per_instance") {
    auto config_a = test_create_config("/tmp/test_demo_extension_a");
    auto config_b = test_create_config("/tmp/test_demo_extension_b");
    test_clear_directory(config_a);
    test_clear_directory(config_b);
    config_a.disk.on = false;
    config_a.wal.on = false;
    config_b.disk.on = false;
    config_b.wal.on = false;
    test_spaces space_a(config_a);
    test_spaces space_b(config_b);

    // register the extension on instance A
    REQUIRE_FALSE(space_a.dispatcher()->add_parser_extension(make_demo_extension()).has_error());

    auto session = otterbrix::session_id_t();
    INFO("instance A (registered) correctly runs DEMO") {
        auto cur = space_a.dispatcher()->execute_sql(session, "DEMO 1 + 20");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().data[0].data<int64_t>()[0] == 21);
    }

    INFO("instance B (no extension) rejects") {
        auto cur = space_b.dispatcher()->execute_sql(session, "DEMO 1 + 20");
        REQUIRE_FALSE(cur->is_success());
    }
}
