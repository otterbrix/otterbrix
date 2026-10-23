#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/test/parse_ok.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <core/pmr.hpp>
#include <services/dev_pump.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/engine/engine.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <spdlog/sinks/sink.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <components/log/test/test_log.hpp>
#include <core/tests/skip_under_root.hpp>
#include <core/tests/wait_ready.hpp>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <sys/resource.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace services::engine;

namespace {

    std::filesystem::path test_root(const std::string& leaf) {
        auto root = std::filesystem::temp_directory_path() /
                    ("otterbrix_engine_test_" + std::to_string(static_cast<long>(::getpid()))) / leaf;
        std::filesystem::remove_all(root);
        std::filesystem::create_directories(root);
        return root;
    }

    // The host side of the contract: it owns the resource, the logger and the pools, and keeps
    // the pools alive until after the engine is gone.
    struct host_t {
        explicit host_t(const std::filesystem::path& root)
            : config(configuration::config::create_config(root)) {
            config.log.level = log_t::level::off;
            log = make_test_log("engine_test", config.log.path.string());
            log.set_level(config.log.level);
        }

        schedulers_t schedulers() { return {general.get(), exec.get(), disk.get()}; }

        core::error_t open() {
            auto opened = open_engine(&resource, schedulers(), config, log, {});
            if (opened.has_error()) {
                return opened.error();
            }
            engine.emplace(std::move(opened.value()));
            return core::error_t::no_error();
        }

        components::cursor::cursor_t_ptr execute(const std::string& sql) {
            std::pmr::monotonic_buffer_resource arena(&resource);
            auto* tree = parse_ok(&arena, sql.c_str());
            components::sql::transform::transformer transformer(&resource, sql.c_str());
            auto plan =
                transformer.transform(components::sql::transform::pg_cell_to_node_cast(linitial(tree))).finalize();
            REQUIRE_FALSE(plan.has_error());
            auto [_, future] = actor_zeta::otterbrix::send(engine->dispatcher_address(),
                                                           &services::dispatcher::manager_dispatcher_t::execute_plan,
                                                           components::session::session_id_t(),
                                                           std::move(plan.value()));
            REQUIRE(test_helpers::wait_ready(future));
            return std::move(future).take_ready();
        }

        configuration::config config;
        core::pmr::otterbrix_resource resource;
        log_t log;
        actor_zeta::scheduler_ptr general{new actor_zeta::shared_work(2, 1000)};
        actor_zeta::scheduler_ptr exec{new actor_zeta::shared_work(2, 1000)};
        actor_zeta::scheduler_ptr disk{new actor_zeta::shared_work(2, 1000)};
        std::optional<engine_t> engine;
    };

} // namespace

TEST_CASE("services::engine::directory_lock::a_second_owner_is_refused_across_threads") {
    const auto root = test_root("lock");
    std::optional<host_t> first;
    first.emplace(root);
    REQUIRE_FALSE(first->open().contains_error());

    bool refused = false;
    std::thread second([&] {
        host_t host(root);
        refused = host.open().contains_error() && !host.engine.has_value();
    });
    second.join();
    REQUIRE(refused);

    first.reset();
    host_t again(root);
    REQUIRE_FALSE(again.open().contains_error());
}

TEST_CASE("services::engine::factory::rows_survive_a_reopen_without_a_wrapper") {
    const auto root = test_root("reopen");
    {
        host_t host(root);
        REQUIRE_FALSE(host.open().contains_error());
        REQUIRE(host.execute("CREATE DATABASE e;")->is_success());
        REQUIRE(host.execute("CREATE TABLE e.t (id BIGINT);")->is_success());
        REQUIRE(host.execute("INSERT INTO e.t (id) VALUES (1), (2), (3);")->is_success());
    }
    host_t host(root);
    REQUIRE_FALSE(host.open().contains_error());
    auto cur = host.execute("SELECT id FROM e.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);
}

TEST_CASE("services::engine::factory::a_failed_bootstrap_hands_out_no_engine") {
    const auto root = test_root("bootstrap_refusal");
    host_t host(root);
    // The pg_catalog directory is a file, so bootstrap cannot lay out a single system table.
    std::filesystem::create_directories(host.config.disk.path);
    {
        std::ofstream blocker((host.config.disk.path / std::to_string(static_cast<unsigned>(
                                                           components::catalog::well_known_oid::main_database)))
                                  .string());
        blocker << "x";
    }
    auto refusal = host.open();
    REQUIRE(refusal.contains_error());
    REQUIRE_FALSE(host.engine.has_value());
}

namespace {

    // The dispatcher, disk, index and WAL loops.
    constexpr std::uint64_t kPumpLoops = 4;

    // An idle interval no test outlives: a loop that sleeps it out instead of being woken never answers.
    constexpr auto kNeverWakesByItself = std::chrono::hours(1);

    bool every_loop_waits_idle() {
        return test_helpers::wait_until([] { return services::dev_pump_idle_waiters() == kPumpLoops; });
    }

} // namespace

// A spinning loop comes out of its wait over and over; an idle one waits for work or its interval.
TEST_CASE("services::engine::pump::an_idle_engine_burns_no_cpu") {
    const auto root = test_root("idle_cpu");
    host_t host(root);
    host.config.execution.pump.idle = kNeverWakesByItself;
    REQUIRE_FALSE(host.open().contains_error());
    REQUIRE(host.execute("CREATE DATABASE idle;")->is_success());
    REQUIRE(every_loop_waits_idle());

    const auto wakeups_before = services::dev_pump_wakeups();
    for (int i = 0; i < 100000; ++i) {
        std::this_thread::yield();
    }
    INFO("loop wake-ups while every loop was idle: " << services::dev_pump_wakeups() - wakeups_before);
    CHECK(services::dev_pump_wakeups() == wakeups_before);
    CHECK(services::dev_pump_idle_waiters() == kPumpLoops);
}

TEST_CASE("services::engine::pump::a_query_to_an_idle_engine_does_not_wait_for_the_idle_interval") {
    const auto root = test_root("idle_latency");
    host_t host(root);
    host.config.execution.pump.idle = kNeverWakesByItself;
    REQUIRE_FALSE(host.open().contains_error());
    REQUIRE(host.execute("CREATE DATABASE lat;")->is_success());
    REQUIRE(host.execute("CREATE TABLE lat.t (id BIGINT);")->is_success());
    REQUIRE(host.execute("INSERT INTO lat.t (id) VALUES (1), (2);")->is_success());
    REQUIRE(every_loop_waits_idle());

    auto cur = host.execute("SELECT id FROM lat.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
}

TEST_CASE("services::engine::pump::intervals_out_of_order_are_refused_at_startup") {
    const auto root = test_root("pump_refusal");
    SECTION("in_flight is zero") {
        host_t host(root);
        host.config.execution.pump.in_flight = std::chrono::microseconds(0);
        REQUIRE(host.open().contains_error());
    }
    SECTION("idle does not exceed in_flight") {
        host_t host(root);
        host.config.execution.pump.idle = host.config.execution.pump.in_flight;
        REQUIRE(host.open().contains_error());
    }
}

TEST_CASE("services::engine::log::a_log_directory_that_cannot_be_written_answers_an_error") {
    test_helpers::skip_under_root();
    const auto root = test_root("log_refusal");
    std::filesystem::permissions(root, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
    auto refused = make_log("refused", root / "log");
    std::filesystem::permissions(root, std::filesystem::perms::owner_all);
    REQUIRE(refused.has_error());
    INFO("refusal: " << refused.error().what.c_str());
    CHECK(std::string(refused.error().what.c_str()).find((root / "log").string()) != std::string::npos);
}

TEST_CASE("services::engine::log::a_log_directory_under_a_regular_file_answers_an_error") {
    const auto root = test_root("log_under_a_file");
    {
        std::ofstream blocker((root / "blocker").string());
        blocker << 'x';
    }
    auto refused = make_log("refused", root / "blocker" / "log");
    REQUIRE(refused.has_error());
    INFO("refusal: " << refused.error().what.c_str());
    CHECK(refused.error().type == core::error_code_t::io_error);
    CHECK(std::string(refused.error().what.c_str()).find((root / "blocker" / "log").string()) != std::string::npos);
}

TEST_CASE("services::engine::log::an_empty_log_directory_answers_an_error") {
    auto refused = make_log("refused", std::filesystem::path{});
    REQUIRE(refused.has_error());
    INFO("refusal: " << refused.error().what.c_str());
    CHECK(refused.error().type == core::error_code_t::invalid_parameter);
    CHECK(std::string(refused.error().what.c_str()) ==
          "the log directory is empty: there is no directory to write the log file into");
}

TEST_CASE("services::engine::log::a_line_in_the_log_file_keeps_its_layout") {
    const auto root = test_root("log_layout");
    auto made = make_log("layout", root / "log");
    REQUIRE_FALSE(made.has_error());
    auto log = std::move(made.value());
    log.set_level(log_t::level::debug);
    info(log, "first {}", 1);
    warn(log, "second");
    error(log, "third");

    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(root / "log")) {
        files.push_back(entry.path());
    }
    REQUIRE(files.size() == 1);
    const auto file_name = files.front().filename().string();
    INFO("file: " << file_name);
    REQUIRE(file_name.rfind("layout-", 0) == 0);
    REQUIRE(file_name.size() > std::string("layout-.txt").size());
    REQUIRE(file_name.substr(file_name.size() - 4) == ".txt");
    const auto seconds = file_name.substr(7, file_name.size() - 11);
    CHECK(seconds.find_first_not_of("0123456789") == std::string::npos);

    const auto text = log_text(root / "log");
    INFO("log: " << text);
    std::vector<std::string> lines;
    for (std::size_t from = 0; from < text.size();) {
        const auto end = text.find('\n', from);
        REQUIRE(end != std::string::npos);
        lines.push_back(text.substr(from, end - from));
        from = end + 1;
    }
    REQUIRE(lines.size() == 3);
    const std::string pid = std::to_string(static_cast<long>(::getpid()));
    const std::array<std::pair<std::string, std::string>, 3> expected{
        {{"info", "first 1"}, {"warning", "second"}, {"error", "third"}}};
    for (std::size_t i = 0; i < lines.size(); ++i) {
        const auto& line = lines[i];
        INFO("line: " << line);
        REQUIRE(line.size() > 25);
        CHECK(line[0] == '[');
        for (const std::size_t digit :
             std::array<std::size_t, 17>{1, 2, 3, 4, 6, 7, 9, 10, 12, 13, 15, 16, 18, 19, 21, 22, 23}) {
            CHECK(std::isdigit(static_cast<unsigned char>(line[digit])) != 0);
        }
        CHECK(line.substr(5, 1) == "-");
        CHECK(line.substr(8, 1) == "-");
        CHECK(line.substr(11, 1) == " ");
        CHECK(line.substr(14, 1) == ":");
        CHECK(line.substr(17, 1) == ":");
        CHECK(line.substr(20, 1) == ".");
        const std::string head = "] [layout] [" + expected[i].first + "] [pid " + pid + " tid ";
        REQUIRE(line.substr(24, head.size()) == head);
        const auto tid_end = line.find(']', 24 + head.size());
        REQUIRE(tid_end != std::string::npos);
        CHECK(line.substr(24 + head.size(), tid_end - 24 - head.size()).find_first_not_of("0123456789") ==
              std::string::npos);
        CHECK(line.substr(tid_end) == "] " + expected[i].second);
    }
}

// The child stands for the engine: a sink that throws there takes the whole process down.
TEST_CASE("services::engine::log::a_write_the_log_file_has_no_room_for_does_not_throw") {
    const auto root = test_root("log_write_failure");
    auto made = make_log("full", root / "log");
    REQUIRE_FALSE(made.has_error());
    auto& sinks = made.value()->sinks();
    REQUIRE(sinks.size() == 2);
    const auto file_sink = sinks.back();
    const std::string line(1 << 20, 'x');

    // A pipe, not a file: the file size limit below would refuse the report as well.
    std::array<int, 2> report_pipe{};
    REQUIRE(::pipe(report_pipe.data()) == 0);
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        if (::dup2(report_pipe[1], STDERR_FILENO) < 0) {
            std::_Exit(2);
        }
        std::signal(SIGXFSZ, SIG_IGN);
        rlimit limit{};
        if (::getrlimit(RLIMIT_FSIZE, &limit) != 0) {
            std::_Exit(3);
        }
        limit.rlim_cur = 0;
        if (::setrlimit(RLIMIT_FSIZE, &limit) != 0) {
            std::_Exit(4);
        }
        [&]() noexcept {
            file_sink->log(spdlog::details::log_msg("full", spdlog::level::info, line));
            file_sink->flush();
        }();
        std::_Exit(0);
    }
    ::close(report_pipe[1]);
    std::string reported;
    std::array<char, 4096> chunk{};
    for (ssize_t got = 0; (got = ::read(report_pipe[0], chunk.data(), chunk.size())) > 0;) {
        reported.append(chunk.data(), static_cast<std::size_t>(got));
    }
    ::close(report_pipe[0]);
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    INFO("child stderr: " << reported);
    INFO("child status: " << status);
    REQUIRE(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
    CHECK(reported.rfind("[*** LOG ERROR #0001 ***] [", 0) == 0);
    CHECK(reported.find("] [full] Failed writing to file " + (root / "log").string()) != std::string::npos);
    CHECK(std::count(reported.begin(), reported.end(), '\n') == 1);
}

TEST_CASE("services::engine::host::an_empty_log_or_a_null_resource_is_refused_before_anything_is_written") {
    const auto root = test_root("host_log");
    host_t host(root);
    SECTION("an empty log") {
        log_t empty;
        auto opened = open_engine(&host.resource, host.schedulers(), host.config, empty, {});
        REQUIRE(opened.has_error());
        CHECK(opened.error().type == core::error_code_t::invalid_parameter);
    }
    SECTION("a null resource") {
        auto opened = open_engine(nullptr, host.schedulers(), host.config, host.log, {});
        REQUIRE(opened.has_error());
        CHECK(opened.error().type == core::error_code_t::invalid_parameter);
    }
    CHECK_FALSE(std::filesystem::exists(host.config.main_path / ".lock"));
}

TEST_CASE("services::engine::host::a_missing_idle_or_shared_pool_is_refused") {
    const auto root = test_root("host_pools");
    host_t host(root);
    actor_zeta::scheduler_ptr no_workers{new actor_zeta::shared_work(0, 1000)};
    const auto refused = [&host](schedulers_t pools) {
        auto opened = open_engine(&host.resource, pools, host.config, host.log, {});
        return opened.has_error() && opened.error().type == core::error_code_t::invalid_parameter;
    };
    CHECK(refused({nullptr, host.exec.get(), host.disk.get()}));
    CHECK(refused({host.general.get(), nullptr, host.disk.get()}));
    CHECK(refused({host.general.get(), host.exec.get(), nullptr}));
    CHECK(refused({no_workers.get(), host.exec.get(), host.disk.get()}));
    CHECK(refused({host.general.get(), host.general.get(), host.disk.get()}));
    CHECK(refused({host.general.get(), host.exec.get(), host.general.get()}));
    REQUIRE_FALSE(host.open().contains_error());
    REQUIRE(host.execute("CREATE DATABASE pools;")->is_success());
}

TEST_CASE("services::engine::factory::an_unreadable_oid_reservation_is_refused_as_an_error") {
    const auto root = test_root("oid_reservation");
    {
        host_t host(root);
        REQUIRE_FALSE(host.open().contains_error());
        REQUIRE(host.execute("CREATE DATABASE r;")->is_success());
        REQUIRE(host.execute("CREATE TABLE r.t (id BIGINT);")->is_success());
    }
    host_t host(root);
    {
        std::ofstream torn((host.config.disk.path / "oid_reservation").string(), std::ios::binary | std::ios::trunc);
        torn << 'x';
    }
    auto refusal = host.open();
    REQUIRE(refusal.contains_error());
    CHECK(refusal.type == core::error_code_t::data_corruption);
    CHECK_FALSE(host.engine.has_value());
    CHECK(services::dev_pump_idle_waiters() == 0);
}

namespace {

    class tmpdir_override_t final {
    public:
        explicit tmpdir_override_t(const std::filesystem::path& value) {
            if (const char* previous = std::getenv("TMPDIR"); previous != nullptr) {
                previous_.emplace(previous);
            }
            ::setenv("TMPDIR", value.c_str(), 1);
        }
        ~tmpdir_override_t() {
            if (previous_.has_value()) {
                ::setenv("TMPDIR", previous_->c_str(), 1);
            } else {
                ::unsetenv("TMPDIR");
            }
        }
        tmpdir_override_t(const tmpdir_override_t&) = delete;
        tmpdir_override_t& operator=(const tmpdir_override_t&) = delete;

    private:
        std::optional<std::string> previous_;
    };

    core::error_t open_with_tmpdir(host_t& host, const std::filesystem::path& tmpdir) {
        tmpdir_override_t override_tmpdir(tmpdir);
        return host.open();
    }

    bool names(const core::error_t& error, const std::filesystem::path& path) {
        return std::string(error.what.c_str()).find(path.string()) != std::string::npos;
    }

} // namespace

TEST_CASE("services::engine::host::an_unusable_temporary_directory_is_refused_at_startup") {
    const auto root = test_root("tmpdir_refusal");
    const auto not_a_directory = root / "not_a_directory";
    {
        std::ofstream blocker(not_a_directory.string());
        blocker << "x";
    }
    SECTION("a fresh directory") {
        host_t host(root / "fresh");
        const auto refusal = open_with_tmpdir(host, not_a_directory);
        INFO("refusal: " << refusal.what.c_str());
        REQUIRE(refusal.contains_error());
        CHECK(refusal.type == core::error_code_t::io_error);
        CHECK(names(refusal, not_a_directory));
        CHECK_FALSE(host.engine.has_value());
    }
    SECTION("a reopen") {
        {
            host_t host(root / "reopen");
            REQUIRE_FALSE(host.open().contains_error());
            REQUIRE(host.execute("CREATE DATABASE tmp;")->is_success());
            REQUIRE(host.execute("CREATE TABLE tmp.t (id BIGINT);")->is_success());
        }
        host_t host(root / "reopen");
        const auto refusal = open_with_tmpdir(host, not_a_directory);
        INFO("refusal: " << refusal.what.c_str());
        REQUIRE(refusal.contains_error());
        CHECK(refusal.type == core::error_code_t::io_error);
        CHECK(names(refusal, not_a_directory));
        CHECK_FALSE(host.engine.has_value());
    }
}

TEST_CASE("services::engine::host::a_wal_root_that_cannot_be_created_is_refused_at_startup") {
    const auto root = test_root("wal_root_refusal");
    const auto blocker = root / "blocker";
    {
        std::ofstream file(blocker.string());
        file << "x";
    }
    std::filesystem::path wal_root;
    SECTION("under a regular file") { wal_root = blocker / "wal"; }
    SECTION("under /dev/null") { wal_root = "/dev/null/wal"; }
    host_t host(root);
    host.config.wal.path = wal_root;
    const auto refusal = host.open();
    INFO("wal.path: " << wal_root.string() << "; refusal: " << refusal.what.c_str());
    REQUIRE(refusal.contains_error());
    CHECK(refusal.type == core::error_code_t::io_error);
    CHECK(names(refusal, wal_root));
    CHECK(std::string(refusal.what.c_str()).find("WAL root") != std::string::npos);
    CHECK(std::string(refusal.what.c_str()).find("could not be created") != std::string::npos);
    CHECK_FALSE(host.engine.has_value());
}

namespace {

#define OTTERBRIX_PUBLIC_MEMBER(name)                                                                                  \
    template<typename T>                                                                                               \
    concept public_##name = requires { &T::name; };

    OTTERBRIX_PUBLIC_MEMBER(has_storage)
    OTTERBRIX_PUBLIC_MEMBER(peek_checkpoint_wal_id_from_disk)
    OTTERBRIX_PUBLIC_MEMBER(load_storage_for_wal_replay_sync)
    OTTERBRIX_PUBLIC_MEMBER(create_storage_disk_sync)
    OTTERBRIX_PUBLIC_MEMBER(bootstrap_system_tables_sync)
    OTTERBRIX_PUBLIC_MEMBER(load_user_table_storages_sync)
    OTTERBRIX_PUBLIC_MEMBER(rehydrate_missing_user_storages_sync)
    OTTERBRIX_PUBLIC_MEMBER(reconcile_storage_with_catalog_sync)
    OTTERBRIX_PUBLIC_MEMBER(alive_user_oids_sync)
    OTTERBRIX_PUBLIC_MEMBER(relkind_for_oid_sync)
    OTTERBRIX_PUBLIC_MEMBER(relnamespace_for_oid_sync)
    OTTERBRIX_PUBLIC_MEMBER(scan_live_table_oids_sync)
    OTTERBRIX_PUBLIC_MEMBER(scan_alive_pg_index_sync)
    OTTERBRIX_PUBLIC_MEMBER(scan_storage_for_rebuild_sync)
    OTTERBRIX_PUBLIC_MEMBER(scan_dropped_oids_sync)
    OTTERBRIX_PUBLIC_MEMBER(scan_dropped_table_oids_sync)
    OTTERBRIX_PUBLIC_MEMBER(path_db)
    OTTERBRIX_PUBLIC_MEMBER(restore_oid_generator_sync)
    OTTERBRIX_PUBLIC_MEMBER(max_persisted_commit_id_sync)
    OTTERBRIX_PUBLIC_MEMBER(read_setting_sync)
    OTTERBRIX_PUBLIC_MEMBER(stored_settings_sync)
    OTTERBRIX_PUBLIC_MEMBER(append_sync)
    OTTERBRIX_PUBLIC_MEMBER(commit_append_sync)
    OTTERBRIX_PUBLIC_MEMBER(delete_sync)
    OTTERBRIX_PUBLIC_MEMBER(commit_all_deletes_sync)
    OTTERBRIX_PUBLIC_MEMBER(update_sync)
    OTTERBRIX_PUBLIC_MEMBER(direct_add_column_sync)
    OTTERBRIX_PUBLIC_MEMBER(register_dropped_storage_sync)
    OTTERBRIX_PUBLIC_MEMBER(set_manager_dispatcher_sync)
    OTTERBRIX_PUBLIC_MEMBER(set_manager_wal_sync)
    OTTERBRIX_PUBLIC_MEMBER(mark_table_dropped_sync)
    OTTERBRIX_PUBLIC_MEMBER(bootstrap_engine_sync)
    OTTERBRIX_PUBLIC_MEMBER(bootstrap_index_sync)
    OTTERBRIX_PUBLIC_MEMBER(bootstrap_dropped_sync)
    OTTERBRIX_PUBLIC_MEMBER(pending_index_rebuilds_sync)
    OTTERBRIX_PUBLIC_MEMBER(owned_bitcask_agents_sync)
    OTTERBRIX_PUBLIC_MEMBER(seed_clocks_sync)
    OTTERBRIX_PUBLIC_MEMBER(cache_settings_sync)
    OTTERBRIX_PUBLIC_MEMBER(set_disk_has_dropped_sync)
    OTTERBRIX_PUBLIC_MEMBER(set_index_has_dropped_sync)

#undef OTTERBRIX_PUBLIC_MEMBER

} // namespace

TEST_CASE("services::engine::bootstrap::no_manager_exposes_a_bootstrap_step") {
    using services::disk::manager_disk_t;
    using services::dispatcher::manager_dispatcher_t;
    using services::index::manager_index_t;
    using services::wal::manager_wal_replicate_t;

    CHECK_FALSE(public_has_storage<manager_disk_t>);
    CHECK_FALSE(public_peek_checkpoint_wal_id_from_disk<manager_disk_t>);
    CHECK_FALSE(public_load_storage_for_wal_replay_sync<manager_disk_t>);
    CHECK_FALSE(public_create_storage_disk_sync<manager_disk_t>);
    CHECK_FALSE(public_bootstrap_system_tables_sync<manager_disk_t>);
    CHECK_FALSE(public_load_user_table_storages_sync<manager_disk_t>);
    CHECK_FALSE(public_rehydrate_missing_user_storages_sync<manager_disk_t>);
    CHECK_FALSE(public_reconcile_storage_with_catalog_sync<manager_disk_t>);
    CHECK_FALSE(public_alive_user_oids_sync<manager_disk_t>);
    CHECK_FALSE(public_relkind_for_oid_sync<manager_disk_t>);
    CHECK_FALSE(public_relnamespace_for_oid_sync<manager_disk_t>);
    CHECK_FALSE(public_scan_live_table_oids_sync<manager_disk_t>);
    CHECK_FALSE(public_scan_alive_pg_index_sync<manager_disk_t>);
    CHECK_FALSE(public_scan_storage_for_rebuild_sync<manager_disk_t>);
    CHECK_FALSE(public_scan_dropped_oids_sync<manager_disk_t>);
    CHECK_FALSE(public_scan_dropped_table_oids_sync<manager_disk_t>);
    CHECK_FALSE(public_path_db<manager_disk_t>);
    CHECK_FALSE(public_restore_oid_generator_sync<manager_disk_t>);
    CHECK_FALSE(public_max_persisted_commit_id_sync<manager_disk_t>);
    CHECK_FALSE(public_read_setting_sync<manager_disk_t>);
    CHECK_FALSE(public_stored_settings_sync<manager_disk_t>);
    CHECK_FALSE(public_append_sync<manager_disk_t>);
    CHECK_FALSE(public_commit_append_sync<manager_disk_t>);
    CHECK_FALSE(public_delete_sync<manager_disk_t>);
    CHECK_FALSE(public_commit_all_deletes_sync<manager_disk_t>);
    CHECK_FALSE(public_update_sync<manager_disk_t>);
    CHECK_FALSE(public_direct_add_column_sync<manager_disk_t>);
    CHECK_FALSE(public_register_dropped_storage_sync<manager_disk_t>);
    CHECK_FALSE(public_set_manager_dispatcher_sync<manager_disk_t>);
    CHECK_FALSE(public_set_manager_wal_sync<manager_disk_t>);

    CHECK_FALSE(public_mark_table_dropped_sync<manager_index_t>);
    CHECK_FALSE(public_set_manager_dispatcher_sync<manager_index_t>);
    CHECK_FALSE(public_bootstrap_engine_sync<manager_index_t>);
    CHECK_FALSE(public_bootstrap_index_sync<manager_index_t>);
    CHECK_FALSE(public_bootstrap_dropped_sync<manager_index_t>);
    CHECK_FALSE(public_pending_index_rebuilds_sync<manager_index_t>);
    CHECK_FALSE(public_owned_bitcask_agents_sync<manager_index_t>);

    CHECK_FALSE(public_set_manager_dispatcher_sync<manager_wal_replicate_t>);

    CHECK_FALSE(public_seed_clocks_sync<manager_dispatcher_t>);
    CHECK_FALSE(public_cache_settings_sync<manager_dispatcher_t>);
    CHECK_FALSE(public_set_disk_has_dropped_sync<manager_dispatcher_t>);
    CHECK_FALSE(public_set_index_has_dropped_sync<manager_dispatcher_t>);

    CHECK(public_has_storage<manager_disk_t::bootstrap_t>);
    CHECK(public_peek_checkpoint_wal_id_from_disk<manager_disk_t::bootstrap_t>);
    CHECK(public_load_storage_for_wal_replay_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_create_storage_disk_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_bootstrap_system_tables_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_load_user_table_storages_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_rehydrate_missing_user_storages_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_reconcile_storage_with_catalog_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_alive_user_oids_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_relkind_for_oid_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_relnamespace_for_oid_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_scan_live_table_oids_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_scan_alive_pg_index_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_scan_dropped_oids_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_path_db<manager_disk_t::bootstrap_t>);
    CHECK(public_restore_oid_generator_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_max_persisted_commit_id_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_read_setting_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_stored_settings_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_append_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_commit_append_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_delete_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_commit_all_deletes_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_update_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_direct_add_column_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_register_dropped_storage_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_set_manager_dispatcher_sync<manager_disk_t::bootstrap_t>);
    CHECK(public_set_manager_wal_sync<manager_disk_t::bootstrap_t>);

    CHECK(public_mark_table_dropped_sync<manager_index_t::bootstrap_t>);
    CHECK(public_set_manager_dispatcher_sync<manager_index_t::bootstrap_t>);
    CHECK(public_bootstrap_engine_sync<manager_index_t::bootstrap_t>);
    CHECK(public_bootstrap_index_sync<manager_index_t::bootstrap_t>);
    CHECK(public_bootstrap_dropped_sync<manager_index_t::bootstrap_t>);
    CHECK(public_pending_index_rebuilds_sync<manager_index_t::bootstrap_t>);

    CHECK(public_set_manager_dispatcher_sync<manager_wal_replicate_t::bootstrap_t>);
}

namespace {

    bool includes_token_definition(const std::filesystem::path& file) {
        std::ifstream in(file);
        std::string line;
        while (std::getline(in, line)) {
            const auto hash = line.find_first_not_of(" \t");
            if (hash == std::string::npos || line[hash] != '#') {
                continue;
            }
            const auto directive = line.find_first_not_of(" \t", hash + 1);
            if (directive == std::string::npos || line.compare(directive, 7, "include") != 0) {
                continue;
            }
            if (line.find("bootstrap_token_def.hpp", directive) != std::string::npos) {
                return true;
            }
        }
        return false;
    }

    bool is_cxx_source(const std::filesystem::path& file) {
        const auto ext = file.extension();
        return ext == ".cpp" || ext == ".hpp" || ext == ".h" || ext == ".cc" || ext == ".c" || ext == ".ipp";
    }

} // namespace

TEST_CASE("services::engine::bootstrap::only_the_engine_includes_the_token_definition") {
    const std::filesystem::path root{OTTERBRIX_SOURCE_ROOT};
    const auto engine_cpp = root / "services" / "engine" / "engine.cpp";
    REQUIRE(includes_token_definition(engine_cpp));

    std::string offenders;
    for (const char* top : {"core", "components", "services", "integration", "example", "benchmark", "JSONBench"}) {
        std::error_code ec;
        std::filesystem::recursive_directory_iterator it(root / top, ec);
        REQUIRE_FALSE(ec);
        for (const std::filesystem::recursive_directory_iterator end; it != end; it.increment(ec)) {
            REQUIRE_FALSE(ec);
            const auto& path = it->path();
            if (it->is_directory(ec)) {
                if (path.filename() == "tests" || path.filename() == "test") {
                    it.disable_recursion_pending();
                }
                continue;
            }
            if (!is_cxx_source(path) || path == engine_cpp) {
                continue;
            }
            if (includes_token_definition(path)) {
                offenders += path.lexically_relative(root).generic_string();
                offenders += '\n';
            }
        }
        REQUIRE_FALSE(ec);
    }
    CHECK(offenders == std::string{});
}
