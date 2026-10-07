#pragma once

#include "../test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/disk/agent_disk.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <initializer_list>
#include <latch>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace concurrency {

    using cursor_ptr = components::cursor::cursor_t_ptr;

    // A statement unanswered this long is a hang: the run aborts naming it.
    constexpr auto STATEMENT_DEADLINE = std::chrono::seconds(120);
    // How long a statement may take while another transaction holds what it touches.
    constexpr auto ANSWER_WINDOW = std::chrono::seconds(5);

    // A fresh engine. Counts the calls its sessions have in flight, so a parallel test can show they overlapped.
    class engine_t {
    public:
        explicit engine_t(const std::string& fixture);

        otterbrix::wrapper_dispatcher_t* dispatcher();
        int most_calls_at_once() const;

        void call_started();
        void call_finished();

    private:
        configuration::config config_;
        test_spaces space_;
        std::atomic<int> in_flight_{0};
        std::atomic<int> peak_{0};
    };

    // One engine session, driven by its own thread.
    class session_t {
    public:
        session_t(engine_t& engine, std::string name);
        // Rolls back whatever the session still has open.
        ~session_t();

        session_t(const session_t&) = delete;
        session_t& operator=(const session_t&) = delete;

        // Returns at once; the answer arrives on the future.
        std::future<cursor_ptr> send(std::string sql);
        // Waits for the answer.
        cursor_ptr run(const std::string& sql);

        const std::string& name() const;

    private:
        void serve();

        engine_t& engine_;
        const std::string name_;
        const otterbrix::session_id_t session_{};
        std::mutex mutex_;
        std::condition_variable wakeup_;
        std::deque<std::function<void()>> tasks_;
        bool stopping_{false};
        std::thread worker_;
    };

    cursor_ptr await(std::future<cursor_ptr>& answer, const std::string& what);
    bool answered_within(std::future<cursor_ptr>& answer, std::chrono::milliseconds window);

    void require_ok(session_t& session, const std::string& sql);

    enum class outcome_t
    {
        succeeds,
        fails
    };
    constexpr auto fails = outcome_t::fails;

    struct step_t {
        session_t& session;
        std::string sql;
        outcome_t outcome{outcome_t::succeeds};
    };

    // Runs the steps one at a time, in order, each on its session: a step is sent once the previous one
    // answered, must answer within ANSWER_WINDOW, and must succeed or fail as stated.
    void interleaved(std::initializer_list<step_t> steps);

    // Column `index` of every row, in the order the statement returned them.
    template<typename T>
    std::vector<T> column(const cursor_ptr& cursor, std::size_t index = 0) {
        REQUIRE(cursor->is_success());
        std::vector<T> values;
        values.reserve(cursor->size());
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            values.push_back(cursor->value(index, row).template value<T>());
        }
        return values;
    }

    // Rows (id, v) = (i, i) for i in [0, count), in INSERTs of a bounded size.
    void insert_numbered_rows(session_t& session, const std::string& table, int64_t count);

    // Enough rows for a scan to pause in: several batches, and not a multiple of one.
    constexpr int64_t PAUSABLE_TABLE_ROWS =
        static_cast<int64_t>(static_cast<double>(components::vector::DEFAULT_VECTOR_CAPACITY) * 3.4);

    // Runs body(index, session) on `count` new sessions at once, each from its own thread, all released together.
    template<typename Body>
    void in_parallel(engine_t& engine, std::size_t count, Body body) {
        std::vector<std::unique_ptr<session_t>> sessions;
        for (std::size_t index = 0; index < count; ++index) {
            sessions.push_back(std::make_unique<session_t>(engine, "session " + std::to_string(index)));
        }
        std::latch start(static_cast<std::ptrdiff_t>(count));
        std::vector<std::thread> drivers;
        for (std::size_t index = 0; index < count; ++index) {
            drivers.emplace_back([&, index] {
                start.arrive_and_wait();
                body(index, *sessions[index]);
            });
        }
        for (auto& driver : drivers) {
            driver.join();
        }
    }

    // A statement parked between two batches of its scan of a user table, inside the engine, until release().
    class paused_statement_t {
    public:
        paused_statement_t(session_t& session, std::string sql);
        // Releases it if the test did not.
        ~paused_statement_t();

        paused_statement_t(const paused_statement_t&) = delete;
        paused_statement_t& operator=(const paused_statement_t&) = delete;

        // False when the statement finished without ever pausing: then nothing overlapped it.
        bool paused() const;
        bool still_running();
        cursor_ptr release();

    private:
        struct gate_t final : services::disk::scan_advance_gate_t {
            bool hold(components::catalog::oid_t table_oid, uint64_t cursor_id) override;

            std::mutex mutex;
            bool claimed{false};
            components::catalog::oid_t table{components::catalog::INVALID_OID};
            uint64_t cursor{0};
            std::atomic<bool> reached{false};
            std::atomic<bool> released{false};
        };

        std::string sql_;
        gate_t gate_;
        std::future<cursor_ptr> answer_;
        bool paused_{false};
    };

} // namespace concurrency
