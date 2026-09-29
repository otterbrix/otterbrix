#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>

using namespace test_helpers;

namespace {

    bool mentions(const core::error_t& error, const std::string& needle) {
        return std::string(error.what.c_str()).find(needle) != std::string::npos;
    }

} // namespace

TEST_CASE("integration::cpp::engine_startup_refusal::an_unwritable_disk_path_answers_an_error") {
    auto config = make_test_config(integration_fixture_path("test_engine_startup_refusal/disk_path"));
    config.log.level = log_t::level::off;
    const auto blocker = config.main_path / "not_a_directory";
    {
        std::ofstream out(blocker.string());
        out << "x";
    }
    config.disk.path = blocker / "wal";

    auto opened = otterbrix::base_otterbrix_t::open(config);
    REQUIRE(opened.has_error());
    INFO("refusal: " << opened.error().what.c_str());
    CHECK(mentions(opened.error(), "could not be created"));
}

TEST_CASE("integration::cpp::engine_startup_refusal::a_zero_executor_pool_answers_an_error") {
    auto config = make_test_config(integration_fixture_path("test_engine_startup_refusal/pool_zero"));
    config.log.level = log_t::level::off;
    config.execution.executor_pool_size = 0;

    auto opened = otterbrix::base_otterbrix_t::open(config);
    REQUIRE(opened.has_error());
    CHECK(mentions(opened.error(), "executor_pool_size"));
}

TEST_CASE("integration::cpp::engine_startup_refusal::an_executor_pool_of_any_size_reopens") {
    for (const std::size_t pool : {std::size_t{1}, std::size_t{16}}) {
        auto config =
            make_test_config(integration_fixture_path("test_engine_startup_refusal/pool_" + std::to_string(pool)));
        config.log.level = log_t::level::off;
        config.execution.executor_pool_size = pool;
        {
            test_spaces space(config);
            auto* d = space.dispatcher();
            REQUIRE(exec(d, "CREATE DATABASE p;")->is_success());
            REQUIRE(exec(d, "CREATE TABLE p.t (id BIGINT);")->is_success());
            REQUIRE(exec(d, "INSERT INTO p.t (id) VALUES (1), (2), (3);")->is_success());
        }
        test_spaces space(config);
        auto cur = exec(space.dispatcher(), "SELECT id FROM p.t;");
        INFO("executor pool " << pool);
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 3);
    }
}

TEST_CASE("integration::cpp::engine_startup_refusal::a_directory_owned_from_another_thread_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_engine_startup_refusal/two_threads"));
    config.log.level = log_t::level::off;

    test_spaces owner(config);
    bool refused = false;
    std::string refusal;
    std::thread second([&] {
        auto opened = otterbrix::base_otterbrix_t::open(config);
        refused = opened.has_error();
        if (refused) {
            refusal = opened.error().what.c_str();
        }
    });
    second.join();
    INFO("refusal: " << refusal);
    REQUIRE(refused);
    CHECK(refusal.find("unique directory") != std::string::npos);
}

// An unreadable table directory is a refused start, not an exception out of the directory walk.
TEST_CASE("integration::cpp::engine_startup_refusal::an_unreadable_namespace_directory_answers_an_error") {
    if (::geteuid() == 0) {
        SKIP("root reads a chmod 000 directory anyway");
    }
    auto config = make_test_config(integration_fixture_path("test_engine_startup_refusal/unreadable_namespace"));
    config.log.level = log_t::level::off;
    {
        test_spaces space(config);
        REQUIRE(exec(space.dispatcher(), "CREATE DATABASE u;")->is_success());
        REQUIRE(exec(space.dispatcher(), "CREATE TABLE u.t (id BIGINT);")->is_success());
        REQUIRE(exec(space.dispatcher(), "INSERT INTO u.t (id) VALUES (1);")->is_success());
    }

    const auto system_dir = std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::main_database));
    std::filesystem::path namespace_dir;
    for (const auto& entry : std::filesystem::directory_iterator(config.disk.path)) {
        const auto name = entry.path().filename().string();
        if (entry.is_directory() && name != system_dir && !name.empty() &&
            name.find_first_not_of("0123456789") == std::string::npos) {
            namespace_dir = entry.path();
        }
    }
    REQUIRE_FALSE(namespace_dir.empty());

    const auto previous = std::filesystem::status(namespace_dir).permissions();
    std::filesystem::permissions(namespace_dir, std::filesystem::perms::none);
    auto opened = otterbrix::base_otterbrix_t::open(config);
    std::filesystem::permissions(namespace_dir, previous);

    REQUIRE(opened.has_error());
    INFO("refusal: " << opened.error().what.c_str());
    CHECK(mentions(opened.error(), namespace_dir.string()));
}
