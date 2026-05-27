#include "test_config.hpp"
#include <catch2/catch.hpp>

TEST_CASE("integration::cpp::test_instances") {
    auto config_works_1 = test_create_config("/tmp/test_instances/1");
    auto config_works_2 = test_create_config("/tmp/test_instances/2");
    auto config_works_3 = test_create_config("/tmp/test_instances/3");
    auto config_failes_1 = test_create_config("/tmp/test_instances/1");
    auto config_failes_2 = test_create_config("/tmp/test_instances/3");
    // Use isolated /tmp/test_instances/{1,2,3} subdirs so prior tests' artifacts in
    // the working directory (e.g. test_persistence::* WAL replays) cannot affect
    // construction of the third test_spaces.
    test_clear_directory(config_works_1);
    test_clear_directory(config_works_2);
    test_clear_directory(config_works_3);

    INFO("unique directories") {
        test_spaces space_1(config_works_1);
        test_spaces space_2(config_works_2);
        test_spaces space_3(config_works_3);

        try {
            test_spaces space(config_failes_1);
            REQUIRE(false);
        } catch (...) {
            REQUIRE(true);
        }

        try {
            test_spaces space(config_failes_2);
            REQUIRE(false);
        } catch (...) {
            REQUIRE(true);
        }
    }

    INFO("directories references deleted properly") {
        // basically run the same setup again
        test_spaces space_1(config_works_1);
        test_spaces space_2(config_works_2);
        test_spaces space_3(config_works_3);

        try {
            test_spaces space(config_failes_1);
            REQUIRE(false);
        } catch (...) {
            REQUIRE(true);
        }

        try {
            test_spaces space(config_failes_2);
            REQUIRE(false);
        } catch (...) {
            REQUIRE(true);
        }
    }
}
