#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/log/test/test_log.hpp>

#include <spdlog/spdlog.h>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>

#ifdef __APPLE__
#include <mach/mach.h>
#endif

namespace {

    std::size_t threads_of_this_process() {
#ifdef __APPLE__
        thread_act_array_t threads = nullptr;
        mach_msg_type_number_t count = 0;
        if (task_threads(mach_task_self(), &threads, &count) != KERN_SUCCESS) {
            FAIL("task_threads failed");
        }
        for (mach_msg_type_number_t i = 0; i < count; ++i) {
            mach_port_deallocate(mach_task_self(), threads[i]);
        }
        vm_deallocate(mach_task_self(),
                      reinterpret_cast<vm_address_t>(threads),
                      static_cast<vm_size_t>(count * sizeof(thread_act_t)));
        return count;
#else
        std::size_t count = 0;
        std::error_code ec;
        for (std::filesystem::directory_iterator it{"/proc/self/task", ec}, end; !ec && it != end; it.increment(ec)) {
            ++count;
        }
        if (ec) {
            FAIL("/proc/self/task: " << ec.message());
        }
        return count;
#endif
    }

} // namespace

using namespace test_helpers;

TEST_CASE("integration::cpp::engine_logs::an_engine_leaves_the_process_default_logger_alone") {
    auto config = make_test_config(integration_fixture_path("test_engine_logs/default_logger"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    REQUIRE(spdlog::get("__default__") == nullptr);
}

TEST_CASE("integration::cpp::engine_logs::two_engines_write_to_their_own_logs") {
    auto config_a = make_test_config(integration_fixture_path("test_engine_logs/a"));
    auto config_b = make_test_config(integration_fixture_path("test_engine_logs/b"));
    config_a.log.level = log_t::level::trace;
    config_b.log.level = log_t::level::trace;
    {
        test_spaces a(config_a);
        test_spaces b(config_b);
        REQUIRE(exec(a.dispatcher(), "CREATE SCHEMA only_in_a;")->is_success());
        REQUIRE(exec(b.dispatcher(), "CREATE SCHEMA only_in_b;")->is_success());
    }
    const auto log_a = log_text(config_a.log.path);
    const auto log_b = log_text(config_b.log.path);
    CHECK(log_a.find("only_in_a") != std::string::npos);
    CHECK(log_a.find("only_in_b") == std::string::npos);
    CHECK(log_b.find("only_in_b") != std::string::npos);
    CHECK(log_b.find("only_in_a") == std::string::npos);
}

TEST_CASE("integration::cpp::engine_logs::a_log_path_under_a_file_refuses_the_engine_before_it_starts") {
    auto config = make_test_config(integration_fixture_path("test_engine_logs/refused"));
    {
        std::ofstream blocker((config.main_path / "blocker").string());
        blocker << 'x';
    }
    config.log.path = config.main_path / "blocker" / "log";
    const auto threads_before = threads_of_this_process();
    auto refused = otterbrix::base_otterbrix_t::open(config, {});
    REQUIRE(refused.has_error());
    INFO("refusal: " << refused.error().what.c_str());
    CHECK(refused.error().type == core::error_code_t::io_error);
    CHECK(std::string(refused.error().what.c_str()).find(config.log.path.string()) != std::string::npos);
    CHECK_FALSE(std::filesystem::exists(config.main_path / ".lock"));
    CHECK(threads_of_this_process() == threads_before);
}

TEST_CASE("integration::cpp::engine_logs::an_empty_log_path_refuses_the_engine_before_it_starts") {
    auto config = make_test_config(integration_fixture_path("test_engine_logs/empty"));
    config.log.path = std::filesystem::path{};
    const auto threads_before = threads_of_this_process();
    auto refused = otterbrix::base_otterbrix_t::open(config, {});
    REQUIRE(refused.has_error());
    INFO("refusal: " << refused.error().what.c_str());
    CHECK(refused.error().type == core::error_code_t::invalid_parameter);
    CHECK(std::string(refused.error().what.c_str()).find("the log directory is empty") != std::string::npos);
    CHECK_FALSE(std::filesystem::exists(config.main_path / ".lock"));
    CHECK(threads_of_this_process() == threads_before);
}
