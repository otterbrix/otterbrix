#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <services/disk/agent_disk.hpp>

#include <atomic>
#include <chrono>
#include <core/tests/wait_ready.hpp>
#include <string>
#include <thread>
#include <tuple>

// A builtin is never unregistered. A UDF is, and a query already running finishes with it: every
// expression carries its own copy of the functions it calls, the filter pushed to disk included.

using namespace test_helpers;

namespace {

    constexpr std::size_t kRows = 3072; // three 1024-row batches: one flows, the unregister lands, more fetch

    struct pause_gate_t final : services::disk::scan_advance_gate_t {
        std::atomic<bool> armed{false};
        std::atomic<bool> reached{false};
        std::atomic<bool> released{false};

        bool hold(components::catalog::oid_t table_oid, uint64_t /*cursor_id*/) override {
            if (!armed.load(std::memory_order_acquire) ||
                static_cast<uint32_t>(table_oid) < static_cast<uint32_t>(components::catalog::FIRST_USER_OID)) {
                return false;
            }
            reached.store(true, std::memory_order_release);
            return !released.load(std::memory_order_acquire);
        }
    };

    struct gate_guard_t {
        pause_gate_t gate;
        gate_guard_t() { services::disk::dev_set_scan_advance_gate(&gate); }
        ~gate_guard_t() { services::disk::dev_set_scan_advance_gate(nullptr); }
        gate_guard_t(const gate_guard_t&) = delete;
        gate_guard_t& operator=(const gate_guard_t&) = delete;
    };

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE bdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE bdb.t (id bigint);")->is_success());
        for (std::size_t done = 0; done < kRows; done += 512) {
            const auto batch = static_cast<unsigned>(std::min<std::size_t>(512, kRows - done));
            REQUIRE(seed_rows(d, "bdb.t", "id", batch, [done](unsigned i) {
                        return "(" + std::to_string(done + i) + ")";
                    })->is_success());
        }
    }

} // namespace

TEST_CASE("integration::cpp::builtin_unregister::an_unregister_of_a_builtin_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_builtin_unregister/refused"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto refused =
        d->unregister_udf(otterbrix::session_id_t(), "abs", {components::types::logical_type::BIGINT});
    INFO("unregister of abs: " << refused.what.c_str());
    REQUIRE(refused.contains_error());
    CHECK(refused.type == core::error_code_t::invalid_parameter);

    for (std::size_t i = 0; i < 2 * config.execution.executor_pool_size; ++i) {
        auto cur = exec(d, "SELECT abs(id) FROM bdb.t WHERE id < 3;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 3);
    }
}

TEST_CASE("integration::cpp::builtin_unregister::a_pushed_filter_outlives_an_unregister_attempt_between_batches") {
    auto config = make_test_config(integration_fixture_path("test_builtin_unregister/between_batches"));
    config.log.level = log_t::level::off;

    // Engine first: a seam outliving the engine spins on teardown.
    test_spaces space(config);
    gate_guard_t guard;
    auto* d = space.dispatcher();
    seed(d);

    guard.gate.armed.store(true, std::memory_order_release);
    components::cursor::cursor_t_ptr reader_cursor;
    std::thread reader(
        [&] { reader_cursor = d->execute_sql(otterbrix::session_id_t(), "SELECT id FROM bdb.t WHERE abs(id) >= 0;"); });

    INFO("the reader must reach the between-batches seam");
    REQUIRE(test_helpers::wait_until([&] { return guard.gate.reached.load(); }));

    std::atomic<bool> unregister_answered{false};
    core::error_t unregistered = core::error_t::no_error();
    std::thread unregisterer([&] {
        unregistered = d->unregister_udf(otterbrix::session_id_t(), "abs", {components::types::logical_type::BIGINT});
        unregister_answered.store(true, std::memory_order_release);
    });
    // A builtin is refused by the dispatcher before any executor is asked, so the busy reader cannot delay it.
    const bool answered_while_parked = test_helpers::wait_until([&] { return unregister_answered.load(); });

    guard.gate.released.store(true, std::memory_order_release);
    reader.join();
    unregisterer.join();

    REQUIRE(answered_while_parked);
    CHECK(unregistered.contains_error());
    REQUIRE(reader_cursor != nullptr);
    INFO("reader: " << (reader_cursor->is_error() ? reader_cursor->get_error().what.c_str() : "<ok>"));
    REQUIRE(reader_cursor->is_success());
    CHECK(reader_cursor->size() == kRows);
}

namespace {

    core::error_t remainder_exec(components::compute::kernel_context&,
                                 const components::vector::data_chunk_t& in,
                                 components::vector::vector_t& out) {
        const auto* left = in.data[0].data<int64_t>();
        const auto* right = in.data[1].data<int64_t>();
        auto* destination = out.data<int64_t>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = left[row] % right[row];
        }
        return core::error_t::no_error();
    }

    components::compute::function_ptr make_remainder(std::pmr::memory_resource* resource) {
        using namespace components::compute;
        auto fn = std::make_unique<vector_function>("remainder",
                                                    arity::binary(),
                                                    function_doc{"short_doc", "full_doc", {"a", "b"}, false},
                                                    1);
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(components::types::logical_type::BIGINT),
                                parameter_type::exact(components::types::logical_type::BIGINT)},
                               {output_type::fixed(components::types::logical_type::BIGINT)});
        REQUIRE_FALSE(fn->add_kernel(resource, vector_kernel{std::move(sig), remainder_exec}).contains_error());
        return fn;
    }

} // namespace

// PostgreSQL 18: a running query that calls a function finishes with it after DROP FUNCTION, and
// the next query does not find it. The query's executor answers the unregister only once the query
// is done; the dispatcher and the other executors drop the function while it is still parked.
TEST_CASE("integration::cpp::udf_unregister::a_running_query_finishes_with_a_udf_unregistered_between_batches") {
    auto config = make_test_config(integration_fixture_path("test_builtin_unregister/udf_between_batches"));
    config.log.level = log_t::level::off;

    test_spaces space(config);
    gate_guard_t guard;
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE_FALSE(d->register_udf(otterbrix::session_id_t(), make_remainder(d->resource())).contains_error());

    const std::string sql = "SELECT remainder(id, 7) AS r FROM bdb.t WHERE remainder(id, 5) >= 0;";
    {
        auto plan = d->execute_sql(otterbrix::session_id_t(), "EXPLAIN " + sql);
        REQUIRE(plan->is_success());
        std::string text;
        for (std::size_t row = 0; row < plan->size(); ++row) {
            const auto cell = plan->value(0, row);
            text += std::string(cell.value<std::string_view>()) + '\n';
        }
        INFO(text);
        REQUIRE(text.find("Filter") == std::string::npos);
    }
    guard.gate.armed.store(true, std::memory_order_release);
    components::cursor::cursor_t_ptr reader_cursor;
    std::thread reader([&] { reader_cursor = d->execute_sql(otterbrix::session_id_t(), sql); });

    INFO("the reader must reach the between-batches seam");
    REQUIRE(test_helpers::wait_until([&] { return guard.gate.reached.load(); }));

    std::atomic<bool> unregister_answered{false};
    core::error_t unregistered = core::error_t::no_error();
    std::thread unregisterer([&] {
        unregistered =
            d->unregister_udf(otterbrix::session_id_t(),
                              "remainder",
                              {components::types::logical_type::BIGINT, components::types::logical_type::BIGINT});
        unregister_answered.store(true, std::memory_order_release);
    });
    std::ignore = test_helpers::wait_until([&] { return unregister_answered.load(); }, std::chrono::seconds(1));

    guard.gate.released.store(true, std::memory_order_release);
    reader.join();
    unregisterer.join();

    INFO("unregister: " << unregistered.what.c_str());
    CHECK_FALSE(unregistered.contains_error());
    REQUIRE(reader_cursor != nullptr);
    INFO("reader: " << (reader_cursor->is_error() ? reader_cursor->get_error().what.c_str() : "<ok>"));
    REQUIRE(reader_cursor->is_success());
    REQUIRE(reader_cursor->size() == kRows);
    int64_t expected = 0;
    int64_t got = 0;
    for (std::size_t row = 0; row < kRows; ++row) {
        expected += static_cast<int64_t>(row % 7);
        got += reader_cursor->value(0, row).value<int64_t>();
    }
    CHECK(got == expected);

    guard.gate.armed.store(false, std::memory_order_release);
    auto after = d->execute_sql(otterbrix::session_id_t(), sql);
    REQUIRE(after->is_error());
    CHECK(after->get_error().type == core::error_code_t::unrecognized_function);
}
