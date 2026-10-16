#include "concurrency.hpp"

#include "../integration_fixture_path.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <span>
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

    namespace {
        cell_t child_of(const cell_t& cell, std::size_t position) {
            cell_t child = cell;
            child.path.push_back(position);
            return child;
        }

        bool is_null_cell(const cell_t& cell) {
            const auto null = cell.cursor->is_null(cell.column, cell.row, std::span<const std::size_t>{cell.path});
            return null.has_error() || null.value();
        }

        // Short strings are stored inline, long ones out of line; numbers alternate between the two.
        std::string text_of(int64_t number) {
            return (number % 2 == 0 ? "s" : "a-string-long-enough-to-leave-inline-storage-") + std::to_string(number);
        }

        std::string text_literal(int64_t number) { return "'" + text_of(number) + "'"; }

        int64_t decode_text(const cell_t& cell) {
            const auto read =
                cell.cursor->get<std::string_view>(cell.column, cell.row, std::span<const std::size_t>{cell.path});
            if (read.has_error()) {
                return MALFORMED;
            }
            const std::string text(read.value());
            const auto digits = text.find_last_not_of("0123456789");
            if (digits == std::string::npos || digits + 1 == text.size()) {
                return MALFORMED;
            }
            const int64_t number = std::stoll(text.substr(digits + 1));
            return text == text_of(number) ? number : MALFORMED;
        }

        int64_t decode_bigint(const cell_t& cell) {
            const auto read = cell.cursor->get<int64_t>(cell.column, cell.row, std::span<const std::size_t>{cell.path});
            return read.has_error() ? MALFORMED : read.value();
        }

        // Elements number, number + 1, ...: `count` of them, each read by `decode`.
        int64_t decode_run(const cell_t& cell, std::size_t count, const std::function<int64_t(const cell_t&)>& decode) {
            const auto size = cell.cursor->list_size(cell.column, cell.row);
            if (size.has_error() || size.value() != count) {
                return MALFORMED;
            }
            const int64_t number = decode(child_of(cell, 0));
            for (std::size_t position = 1; position < count; ++position) {
                if (number == MALFORMED ||
                    decode(child_of(cell, position)) != number + static_cast<int64_t>(position)) {
                    return MALFORMED;
                }
            }
            return number;
        }

        // 1 to `lengths` elements, so neighbouring versions differ in length.
        std::string run_literal(int64_t number, int64_t lengths, const std::function<std::string(int64_t)>& element) {
            std::string literal = "ARRAY[";
            for (int64_t position = 0; position <= number % lengths; ++position) {
                literal += (position == 0 ? "" : ", ") + element(number + position);
            }
            return literal + "]";
        }

        // The length a list of `lengths` lengths has for the number in its first element.
        int64_t decode_list(const cell_t& cell, int64_t lengths, const std::function<int64_t(const cell_t&)>& decode) {
            const auto size = cell.cursor->list_size(cell.column, cell.row);
            if (size.has_error() || size.value() == 0) {
                return MALFORMED;
            }
            const int64_t number = decode(child_of(cell, 0));
            if (number < 0) {
                return MALFORMED;
            }
            return decode_run(cell, static_cast<std::size_t>(number % lengths + 1), decode);
        }
    } // namespace

    const std::vector<column_type_t>& column_types() {
        static const std::vector<column_type_t> types{
            {"text", "text", "", text_literal, decode_text},
            {"fixed array bigint[3]",
             "bigint[3]",
             "",
             [](int64_t number) {
                 return "ARRAY[" + std::to_string(number) + ", " + std::to_string(number + 1) + ", " +
                        std::to_string(number + 2) + "]";
             },
             [](const cell_t& cell) { return decode_run(cell, 3, decode_bigint); }},
            {"list bigint[]",
             "bigint[]",
             "",
             [](int64_t number) {
                 return run_literal(number, 3, [](int64_t element) { return std::to_string(element); });
             },
             [](const cell_t& cell) { return decode_list(cell, 3, decode_bigint); }},
            {"list text[]",
             "text[]",
             "",
             [](int64_t number) { return run_literal(number, 2, text_literal); },
             [](const cell_t& cell) { return decode_list(cell, 2, decode_text); }},
            {"struct (bigint, text)",
             "payload_pair",
             "CREATE TYPE payload_pair AS (number bigint, label text);",
             [](int64_t number) { return "ROW(" + std::to_string(number) + ", " + text_literal(number) + ")"; },
             [](const cell_t& cell) {
                 if (is_null_cell(cell)) {
                     return MALFORMED;
                 }
                 const int64_t number = decode_bigint(child_of(cell, 0));
                 return decode_text(child_of(cell, 1)) == number ? number : MALFORMED;
             }},
        };
        return types;
    }

    std::vector<int64_t> decoded(const column_type_t& type, const cursor_ptr& cursor, std::size_t index) {
        REQUIRE(cursor->is_success());
        std::vector<int64_t> numbers;
        numbers.reserve(cursor->size());
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            numbers.push_back(type.decode(cell_t{cursor.get(), index, row, {}}));
        }
        return numbers;
    }

    void create_payload_table(session_t& session, const column_type_t& type, const std::string& table) {
        if (!type.setup.empty()) {
            require_ok(session, type.setup);
        }
        require_ok(session, "CREATE TABLE " + table + " (id bigint, v bigint, payload " + type.sql_type + ");");
    }

    void insert_payload_rows(session_t& session, const column_type_t& type, const std::string& table, int64_t count) {
        constexpr int64_t PER_INSERT = 256;
        for (int64_t first = 0; first < count; first += PER_INSERT) {
            const int64_t last = std::min(first + PER_INSERT, count);
            std::string sql = "INSERT INTO " + table + " (id, v, payload) VALUES ";
            for (int64_t id = first; id < last; ++id) {
                sql += "(" + std::to_string(id) + ", " + std::to_string(id) + ", " + type.literal(id) + ")" +
                       (id + 1 == last ? ";" : ", ");
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
