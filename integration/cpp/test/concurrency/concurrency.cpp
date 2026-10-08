#include "concurrency.hpp"

#include "../integration_fixture_path.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <tuple>

namespace concurrency {

    namespace {
        configuration::config fresh_config(const std::string& fixture) {
            auto config = test_create_config(integration_fixture_path("concurrency/" + fixture));
            test_clear_directory(config);
            config.log.level = log_t::level::off;
            return config;
        }
    } // namespace

    engine_t::engine_t(const std::string& fixture)
        : config_(fresh_config(fixture))
        , space_(config_) {}

    otterbrix::wrapper_dispatcher_t* engine_t::dispatcher() { return space_.dispatcher(); }

    int engine_t::most_calls_at_once() const { return peak_.load(); }

    void engine_t::call_started() {
        const int now = ++in_flight_;
        int peak = peak_.load();
        while (now > peak && !peak_.compare_exchange_weak(peak, now)) {
        }
    }

    void engine_t::call_finished() { --in_flight_; }

    session_t::session_t(engine_t& engine, std::string name)
        : engine_(engine)
        , name_(std::move(name))
        , worker_([this] { serve(); }) {}

    const std::string& session_t::name() const { return name_; }

    session_t::~session_t() {
        std::ignore = run("ROLLBACK;");
        {
            std::lock_guard guard(mutex_);
            stopping_ = true;
        }
        wakeup_.notify_one();
        worker_.join();
    }

    std::future<cursor_ptr> session_t::send(std::string sql) {
        auto task = std::make_shared<std::packaged_task<cursor_ptr()>>(
            [this, sql = std::move(sql)] { return engine_.dispatcher()->execute_sql(session_, sql); });
        auto answer = task->get_future();
        {
            std::lock_guard guard(mutex_);
            tasks_.emplace_back([task] { (*task)(); });
        }
        wakeup_.notify_one();
        return answer;
    }

    cursor_ptr session_t::run(const std::string& sql) {
        auto answer = send(sql);
        return await(answer, sql);
    }

    void session_t::serve() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock lock(mutex_);
                wakeup_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
                if (tasks_.empty()) {
                    return;
                }
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            engine_.call_started();
            task();
            engine_.call_finished();
        }
    }

    cursor_ptr await(std::future<cursor_ptr>& answer, const std::string& what) {
        if (answer.wait_for(STATEMENT_DEADLINE) != std::future_status::ready) {
            std::fprintf(stderr, "concurrency: '%s' got no answer, aborting\n", what.c_str());
            std::fflush(stderr);
            std::abort();
        }
        return answer.get();
    }

    bool answered_within(std::future<cursor_ptr>& answer, std::chrono::milliseconds window) {
        return answer.wait_for(window) == std::future_status::ready;
    }

    void require_ok(session_t& session, const std::string& sql) {
        INFO(session.name() << ": " << sql);
        REQUIRE(session.run(sql)->is_success());
    }

    void interleaved(std::initializer_list<step_t> steps) {
        std::size_t number = 0;
        for (const auto& step : steps) {
            ++number;
            INFO("step " << number << ", " << step.session.name() << ": " << step.sql);
            auto answer = step.session.send(step.sql);
            CHECK(answered_within(answer, ANSWER_WINDOW));
            const bool succeeded = await(answer, step.sql)->is_success();
            REQUIRE(succeeded == (step.outcome == outcome_t::succeeds));
        }
    }

    void insert_numbered_rows(session_t& session, const std::string& table, int64_t count) {
        constexpr int64_t PER_INSERT = 512;
        for (int64_t first = 0; first < count; first += PER_INSERT) {
            const int64_t last = std::min(first + PER_INSERT, count);
            std::string sql = "INSERT INTO " + table + " (id, v) VALUES ";
            for (int64_t id = first; id < last; ++id) {
                sql += "(" + std::to_string(id) + ", " + std::to_string(id) + ")" + (id + 1 == last ? ";" : ", ");
            }
            require_ok(session, sql);
        }
    }

    bool paused_statement_t::gate_t::hold(components::catalog::oid_t table_oid, uint64_t cursor_id) {
        if (components::catalog::is_catalog_table(table_oid)) {
            return false;
        }
        std::lock_guard guard(mutex);
        if (!claimed) {
            claimed = true;
            table = table_oid;
            cursor = cursor_id;
        }
        if (table_oid != table || cursor_id != cursor) {
            return false;
        }
        reached.store(true);
        return !released.load();
    }

    paused_statement_t::paused_statement_t(session_t& session, std::string sql)
        : sql_(std::move(sql)) {
        services::disk::dev_set_scan_advance_gate(&gate_);
        answer_ = session.send(sql_);
        const auto deadline = std::chrono::steady_clock::now() + ANSWER_WINDOW;
        while (!gate_.reached.load() && std::chrono::steady_clock::now() < deadline &&
               !answered_within(answer_, std::chrono::milliseconds(1))) {
        }
        paused_ = gate_.reached.load();
    }

    paused_statement_t::~paused_statement_t() {
        if (answer_.valid()) {
            std::ignore = release();
        }
        services::disk::dev_set_scan_advance_gate(nullptr);
    }

    bool paused_statement_t::paused() const { return paused_; }

    bool paused_statement_t::still_running() { return !answered_within(answer_, std::chrono::milliseconds(0)); }

    cursor_ptr paused_statement_t::release() {
        gate_.released.store(true);
        return await(answer_, sql_);
    }

} // namespace concurrency
