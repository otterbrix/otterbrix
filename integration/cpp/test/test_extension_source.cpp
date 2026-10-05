// e2e for storage SOURCE/SINK operators: the host's name resolution answers remote names with a per-statement
// storage over a simulated backend; each storage builds the operators that read and write it.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/logical_plan/table_storage.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/context_storage.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <unordered_map>

using namespace components;

namespace {

    struct rows_spec_t {
        std::string col_a;
        std::string col_b;
        std::vector<std::pair<int64_t, int64_t>> rows;
    };

    vector::data_chunk_t build_pairs(std::pmr::memory_resource* res, const rows_spec_t& spec) {
        std::pmr::vector<types::complex_logical_type> types(res);
        types.emplace_back(types::logical_type::BIGINT, spec.col_a);
        types.emplace_back(types::logical_type::BIGINT, spec.col_b);
        vector::data_chunk_t chunk(res,
                                   types,
                                   std::clamp<size_t>(spec.rows.size(), 1, vector::DEFAULT_VECTOR_CAPACITY));
        // A backend page can be wider than one vector; resize() is how a host grows a chunk past it.
        if (spec.rows.size() > chunk.capacity()) {
            chunk.resize(spec.rows.size());
        }
        chunk.set_cardinality(spec.rows.size());
        for (size_t i = 0; i < spec.rows.size(); ++i) {
            chunk.set_value(0, i, spec.rows[i].first);
            chunk.set_value(1, i, spec.rows[i].second);
        }
        return chunk;
    }

    class mock_source_op_t final : public operators::read_only_operator_t {
    public:
        mock_source_op_t(std::pmr::memory_resource* resource, log_t log, rows_spec_t spec, bool async_delivery)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , spec_(std::move(spec))
            , async_delivery_(async_delivery) {}

        [[nodiscard]] operators::pipeline_role role() const noexcept override {
            return operators::pipeline_role::source;
        }

        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
        source_next(components::pipeline::context_t*) override {
            actor_zeta::promise<core::result_wrapper_t<std::optional<vector::data_chunk_t>>> promise(resource());
            auto future = promise.get_future();
            if (drained_) {
                promise.set_value(core::result_wrapper_t<std::optional<vector::data_chunk_t>>{std::nullopt});
                return future;
            }
            drained_ = true;
            if (async_delivery_) {
                // Fulfills from a background thread after the executor has begun awaiting, modeling
                // a host backend actor answering a fetch; the promise outlives this stack frame.
                std::thread([p = std::move(promise), chunk = build_pairs(resource(), spec_)]() mutable {
                    std::this_thread::sleep_for(std::chrono::milliseconds(30));
                    p.set_value(core::result_wrapper_t<std::optional<vector::data_chunk_t>>{std::move(chunk)});
                }).detach();
            } else {
                promise.set_value(
                    core::result_wrapper_t<std::optional<vector::data_chunk_t>>{build_pairs(resource(), spec_)});
            }
            return future;
        }

        void reset_pipeline_state() noexcept override { drained_ = false; }

    private:
        rows_spec_t spec_;
        bool async_delivery_{true};
        bool drained_{false};
    };

    struct open_probe_t {
        static constexpr std::size_t kSlots = 64;
        std::atomic<std::size_t> slots_used{0};
        std::atomic<int> opens{0};
        std::atomic<int> opens_at_first_next{-1};
        std::atomic<int> fetches_done{0};
        std::atomic<int> fetches_in_flight{0};
        std::atomic<int> peak_fetches_in_flight{0};
        std::atomic<int> next_before_ready{0};
        std::atomic<int> next_on_open_ctx{0};
        std::atomic<int> destroyed_in_flight{0};
        std::array<std::atomic<bool>, kSlots> ready{};

        void reset() {
            slots_used.store(0);
            opens.store(0);
            opens_at_first_next.store(-1);
            fetches_done.store(0);
            fetches_in_flight.store(0);
            peak_fetches_in_flight.store(0);
            next_before_ready.store(0);
            next_on_open_ctx.store(0);
            destroyed_in_flight.store(0);
            for (auto& r : ready) {
                r.store(false);
            }
        }
    };
    open_probe_t& open_probe() {
        static open_probe_t probe;
        return probe;
    }

    // A backend whose fetch costs fetch_latency, paid in open() off the executor thread; source_next hands out
    // what open() fetched, so it must come after the executor awaited open().
    class fetch_on_open_source_op_t final : public operators::read_only_operator_t {
    public:
        fetch_on_open_source_op_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  rows_spec_t spec,
                                  std::chrono::milliseconds fetch_latency,
                                  bool fail_open)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , spec_(std::move(spec))
            , fetch_latency_(fetch_latency)
            , fail_open_(fail_open)
            , slot_(open_probe().slots_used.fetch_add(1) % open_probe_t::kSlots) {}

        ~fetch_on_open_source_op_t() override {
            if (opened_ && !open_probe().ready[slot_].load()) {
                open_probe().destroyed_in_flight.fetch_add(1);
            }
        }

        [[nodiscard]] operators::pipeline_role role() const noexcept override {
            return operators::pipeline_role::source;
        }

        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
        source_next(components::pipeline::context_t* ctx) override {
            int unset = -1;
            open_probe().opens_at_first_next.compare_exchange_strong(unset, open_probe().opens.load());
            if (!opened_ || !open_probe().ready[slot_].load()) {
                open_probe().next_before_ready.fetch_add(1);
            }
            if (ctx == open_ctx_) {
                open_probe().next_on_open_ctx.fetch_add(1);
            }
            actor_zeta::promise<core::result_wrapper_t<std::optional<vector::data_chunk_t>>> promise(resource());
            auto future = promise.get_future();
            if (drained_) {
                promise.set_value(core::result_wrapper_t<std::optional<vector::data_chunk_t>>{std::nullopt});
                return future;
            }
            drained_ = true;
            promise.set_value(
                core::result_wrapper_t<std::optional<vector::data_chunk_t>>{build_pairs(resource(), spec_)});
            return future;
        }

        void reset_pipeline_state() noexcept override { drained_ = false; }

    private:
        actor_zeta::unique_future<core::error_t> open_impl(components::pipeline::context_t* ctx) override {
            open_probe().opens.fetch_add(1);
            open_probe().ready[slot_].store(false);
            const int in_flight = open_probe().fetches_in_flight.fetch_add(1) + 1;
            int peak = open_probe().peak_fetches_in_flight.load();
            while (in_flight > peak && !open_probe().peak_fetches_in_flight.compare_exchange_weak(peak, in_flight)) {
            }
            opened_ = true;
            open_ctx_ = ctx;
            auto outcome = fail_open_ ? core::error_t(core::error_code_t::physical_plan_error,
                                                      std::pmr::string{"backend unavailable", resource()})
                                      : core::error_t::no_error();
            actor_zeta::promise<core::error_t> promise(resource());
            auto future = promise.get_future();
            std::thread([p = std::move(promise),
                         outcome = std::move(outcome),
                         d = fetch_latency_,
                         slot = slot_]() mutable {
                std::this_thread::sleep_for(d);
                open_probe().fetches_done.fetch_add(1);
                open_probe().fetches_in_flight.fetch_sub(1);
                open_probe().ready[slot].store(true);
                p.set_value(std::move(outcome));
            }).detach();
            return future;
        }

        rows_spec_t spec_;
        std::chrono::milliseconds fetch_latency_;
        bool fail_open_;
        std::size_t slot_;
        const components::pipeline::context_t* open_ctx_{nullptr};
        bool drained_{false};
        bool opened_{false};
    };

    class mock_sink_op_t final : public operators::read_write_operator_t {
    public:
        mock_sink_op_t(std::pmr::memory_resource* resource,
                       log_t log,
                       std::vector<std::pair<int64_t, int64_t>>* written)
            : operators::read_write_operator_t(resource, std::move(log), operators::operator_type::extension)
            , received_(written) {}

        [[nodiscard]] core::error_t
        push(components::pipeline::context_t*, vector::data_chunk_t&& input, operators::chunks_vector_t&) override {
            for (size_t i = 0; i < input.size(); ++i) {
                received_->emplace_back(input.value(0, i).value<int64_t>(), input.value(1, i).value<int64_t>());
            }
            return core::error_t::no_error();
        }

    private:
        std::vector<std::pair<int64_t, int64_t>>* received_;
    };

    // One table of the simulated backend: its rows and how its server answers.
    struct remote_table_t {
        rows_spec_t spec;
        std::pmr::vector<types::complex_logical_type> schema;
        bool async_delivery{true};
        bool fetch_on_open{false};
        std::chrono::milliseconds fetch_latency{0};
        bool fail_open{false};
        bool no_operator{false};
    };
    using remote_tables_t = std::unordered_map<std::string, remote_table_t>;

    // The simulated backend's catalog, keyed by database.name: the remote system, not host state. The host's hooks
    // and its rule read nothing else global: a rule finds its table through the storage on the read node.
    remote_tables_t& remote_server() {
        static remote_tables_t tables;
        return tables;
    }

    // What the backend received through the storages' insert sinks.
    std::vector<std::pair<int64_t, int64_t>>& remote_written() {
        static std::vector<std::pair<int64_t, int64_t>> rows;
        return rows;
    }

    // Scans the storages built, for the statement under test.
    std::atomic<int>& scans_made() {
        static std::atomic<int> made{0};
        return made;
    }

    // Declared after the engine, so the backend's types go before the resource they live on.
    struct remote_server_reset_t {
        remote_server_reset_t() { reset(); }
        ~remote_server_reset_t() { reset(); }
        static void reset() {
            remote_server().clear();
            remote_written().clear();
            scans_made().store(0);
        }
    };

    const int host_tag = 0;

    class ext_storage_t final : public logical_plan::table_storage_t {
    public:
        // The server's table outlives the statement: the backend is not changed while a statement runs.
        ext_storage_t(std::pmr::memory_resource*, const remote_table_t* table)
            : logical_plan::table_storage_t(&host_tag)
            , table_(table) {}

    private:
        logical_plan::storage_operator_t make_scan_impl(const services::context_storage_t& context) override {
            scans_made().fetch_add(1);
            if (table_->no_operator) {
                // A successful result without an operator breaks the contract.
                return operators::operator_ptr{};
            }
            if (table_->fetch_on_open) {
                return operators::operator_ptr{new fetch_on_open_source_op_t(context.resource,
                                                                             context.log.clone(),
                                                                             table_->spec,
                                                                             table_->fetch_latency,
                                                                             table_->fail_open)};
            }
            return operators::operator_ptr{
                new mock_source_op_t(context.resource, context.log.clone(), table_->spec, table_->async_delivery)};
        }

        logical_plan::storage_operator_t make_insert_impl(const services::context_storage_t& context) override {
            return operators::operator_ptr{new mock_sink_op_t(context.resource, context.log.clone(), &remote_written())};
        }

        logical_plan::storage_operator_t read_only(const services::context_storage_t& context) const {
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"this backend only reads and appends", context.resource}};
        }
        logical_plan::storage_operator_t make_update_impl(const services::context_storage_t& context) override {
            return read_only(context);
        }
        logical_plan::storage_operator_t make_delete_impl(const services::context_storage_t& context) override {
            return read_only(context);
        }

        const remote_table_t* table_;
    };

    std::string qualified(const qualified_name_t& name) { return name.database.t + "." + name.collection.t; }

    core::result_wrapper_t<std::pmr::vector<planner::table_storage_answer_t>>
    remote_decide(std::pmr::memory_resource* res,
                  std::span<const qualified_name_t> unresolved,
                  std::span<const std::pmr::vector<vector::data_chunk_t>>,
                  bool) {
        std::pmr::vector<planner::table_storage_answer_t> answers{res};
        for (const auto& name : unresolved) {
            planner::table_storage_answer_t answer{std::pmr::vector<types::complex_logical_type>{res}};
            if (auto it = remote_server().find(qualified(name)); it != remote_server().end()) {
                answer.columns.assign(it->second.schema.begin(), it->second.schema.end());
                answer.storage = core::pmr::make_polymorphic_unique<ext_storage_t>(res, &it->second);
            }
            answers.push_back(std::move(answer));
        }
        return answers;
    }

    components::planner::primitives_t remote_host(std::span<const planner::optimizer_rule_t> rules = {}) {
        return components::planner::primitives_t{rules, {&planner::no_name_reads, &remote_decide}};
    }

    // A hung executor must fail the run, not wedge it: the dispatcher wait has no deadline of its own.
    cursor::cursor_t_ptr execute_within_deadline(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        constexpr auto deadline = std::chrono::seconds(30);
        std::atomic<bool> done{false};
        cursor::cursor_t_ptr cursor;
        std::thread worker([&] {
            auto session = otterbrix::session_id_t();
            cursor = dispatcher->execute_sql(session, sql);
            done.store(true, std::memory_order_release);
        });
        const auto until = std::chrono::steady_clock::now() + deadline;
        while (!done.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() > until) {
                std::fprintf(stderr,
                             "extension_source: query exceeded %llds, executor hung\n",
                             static_cast<long long>(deadline.count()));
                std::abort();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        worker.join();
        return cursor;
    }

    cursor::cursor_t_ptr run_over_remote(otterbrix::wrapper_dispatcher_t* dispatcher,
                                         const std::string& sql,
                                         remote_tables_t tables) {
        remote_server() = std::move(tables);
        scans_made().store(0);
        return execute_within_deadline(dispatcher, sql);
    }

    std::string explain_remote_plan(otterbrix::wrapper_dispatcher_t* dispatcher,
                                    const std::string& sql,
                                    remote_tables_t tables) {
        remote_server() = std::move(tables);
        auto cursor = dispatcher->execute_sql(otterbrix::session_id_t(), "EXPLAIN " + sql);
        REQUIRE(cursor->is_success());
        std::string out;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                out += std::string(chunk.value(0, row).value<std::string_view>());
                out += '\n';
            }
        }
        return out;
    }

    std::pmr::vector<types::complex_logical_type>
    pair_schema(std::pmr::memory_resource* res, const std::string& col_a, const std::string& col_b) {
        std::pmr::vector<types::complex_logical_type> schema(res);
        schema.emplace_back(types::logical_type::BIGINT, col_a);
        schema.emplace_back(types::logical_type::BIGINT, col_b);
        return schema;
    }

    // What the passthrough rule puts in place of the aggregate's own scan: the same storage's scan, as a host node.
    struct passthrough_payload_t final : logical_plan::extension_payload_t {
        explicit passthrough_payload_t(logical_plan::table_storage_t* storage)
            : storage(storage) {}
        logical_plan::table_storage_t* storage;
    };

    logical_plan::storage_operator_t passthrough_scan(const services::context_storage_t& context,
                                                      const compute::function_registry_t&,
                                                      const logical_plan::node_extension_t& node) {
        return static_cast<const passthrough_payload_t&>(*node.payload()).storage->make_scan(context);
    }

    // A `last`-stage rule: gives a FROM aggregate over the host's own storage an explicit source child, the way a
    // host rule replaces the implicit scan late in optimization. The table is recognized by its storage's owner.
    logical_plan::node_ptr attach_passthrough_rule(std::pmr::memory_resource* res, logical_plan::node_ptr node) {
        for (auto& child : node->children()) {
            child = attach_passthrough_rule(res, child);
        }
        if (node->type() != logical_plan::node_type::aggregate_t) {
            return node;
        }
        const auto* table = node->table_metadata();
        if (table == nullptr || table->storage == nullptr || table->storage->owner() != &host_tag) {
            return node;
        }
        for (const auto& child : node->children()) {
            if (child->type() == logical_plan::node_type::extension_t) {
                return node;
            }
        }
        node->append_child(logical_plan::node_ptr{
            new logical_plan::node_extension_t(res,
                                               table->name,
                                               std::pmr::vector<types::complex_logical_type>{res},
                                               &passthrough_scan,
                                               logical_plan::extension_payload_ptr{
                                                   new passthrough_payload_t{table->storage}})});
        return node;
    }

} // namespace

static remote_tables_t one_table(std::pmr::memory_resource* res,
                                 const std::string& name,
                                 const std::string& col_a,
                                 const std::string& col_b,
                                 std::vector<std::pair<int64_t, int64_t>> rows,
                                 bool async_delivery) {
    remote_tables_t tables;
    tables.emplace(
        name,
        remote_table_t{rows_spec_t{col_a, col_b, std::move(rows)}, pair_schema(res, col_a, col_b), async_delivery});
    return tables;
}

#define EXT_TEST_BOILERPLATE(DIR)                                                                                      \
    auto config = test_create_config(DIR);                                                                             \
    test_clear_directory(config);                                                                                      \
    test_spaces space(config, remote_host()); /* the host's name resolution, given at engine start */                 \
    remote_server_reset_t remote_server_reset;                                                                         \
    auto dispatcher = space.dispatcher();                                                                              \
    auto* res = dispatcher->resource();

TEST_CASE("integration::cpp::extension_source::sync_single_leaf") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_sync/base"))
    auto tables = one_table(res, "remote.t1", "key", "val", {{7, 70}}, /*async=*/false);
    auto cursor = run_over_remote(dispatcher, "SELECT * FROM remote.t1;", tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
}

TEST_CASE("integration::cpp::extension_source::async_single_leaf") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_async/base"))
    auto tables = one_table(res, "remote.t1", "key", "val", {{1, 10}, {2, 20}, {3, 30}}, /*async=*/true);
    auto cursor = run_over_remote(dispatcher, "SELECT * FROM remote.t1;", tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 3);
}

TEST_CASE("integration::cpp::extension_source::empty_result") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_empty/base"))
    auto tables = one_table(res, "remote.t1", "key", "val", {}, /*async=*/true);
    auto cursor = run_over_remote(dispatcher, "SELECT * FROM remote.t1;", tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 0);
}

TEST_CASE("integration::cpp::extension_source::join_two_storages") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_join2/base"))
    remote_tables_t tables;
    tables.emplace("remote.t1",
                   remote_table_t{rows_spec_t{"key", "name", {{1, 11}, {2, 22}, {3, 33}}},
                                  pair_schema(res, "key", "name"),
                                  /*async_delivery=*/true});
    tables.emplace("remote.t2",
                   remote_table_t{rows_spec_t{"key", "value", {{1, 100}, {3, 300}, {9, 900}}},
                                  pair_schema(res, "key", "value"),
                                  /*async_delivery=*/true});
    auto cursor = run_over_remote(dispatcher,
                                  "SELECT l.name, r.value FROM remote.t1 AS l JOIN remote.t2 AS r ON l.key = r.key;",
                                  tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);
}

TEST_CASE("integration::cpp::extension_source::group_by") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_group/base"))
    auto tables = one_table(res, "remote.t1", "grp", "val", {{1, 10}, {1, 15}, {2, 20}, {2, 5}, {3, 1}}, true);
    auto cursor = run_over_remote(dispatcher, "SELECT grp, SUM(val) AS s FROM remote.t1 GROUP BY grp;", tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 3);
}

TEST_CASE("integration::cpp::extension_source::barrier_where_above_join") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_barrier/base"))
    remote_tables_t tables;
    tables.emplace("remote.t1",
                   remote_table_t{rows_spec_t{"key", "name", {{1, 11}, {2, 22}, {3, 33}}},
                                  pair_schema(res, "key", "name"),
                                  /*async_delivery=*/true});
    tables.emplace("remote.t2",
                   remote_table_t{rows_spec_t{"key", "value", {{1, 100}, {2, 200}, {3, 300}}},
                                  pair_schema(res, "key", "value"),
                                  /*async_delivery=*/false});
    auto cursor = run_over_remote(dispatcher,
                                  "SELECT l.name, r.value FROM remote.t1 AS l JOIN remote.t2 AS r ON l.key = r.key "
                                  "WHERE r.value > 150;",
                                  tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);
    // One scan per storage table: the pushed-down filter stays above the storage's scan.
    CHECK(scans_made().load() == 2);
}

TEST_CASE("integration::cpp::extension_source::join_with_local_table") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_local/base"))
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE extdb;")->is_success());
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE TABLE extdb.local_t (key BIGINT, amount BIGINT);")
                ->is_success());
    REQUIRE(dispatcher
                ->execute_sql(otterbrix::session_id_t(),
                              "INSERT INTO extdb.local_t (key, amount) VALUES (1, 1000), (2, 2000), (5, 5000);")
                ->is_success());
    auto tables = one_table(res, "remote.t1", "key", "name", {{1, 11}, {2, 22}, {3, 33}}, /*async=*/true);
    auto cursor = run_over_remote(dispatcher,
                                  "SELECT e.name, t.amount FROM remote.t1 AS e JOIN extdb.local_t AS t ON e.key = t.key;",
                                  tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);
}

// A storage whose scan factory builds no operator must surface a clean error, not a crash.
TEST_CASE("integration::cpp::extension_source::missing_operator_errors_not_crash") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_norule/base"))
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE extdb;")->is_success());
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE TABLE extdb.local_t (key BIGINT, amount BIGINT);")
                ->is_success());
    {
        auto tables = one_table(res, "remote.t1", "key", "name", {{1, 11}}, /*async=*/false);
        tables.at("remote.t1").no_operator = true;
        auto cursor = run_over_remote(
            dispatcher,
            "SELECT e.name, t.amount FROM remote.t1 AS e JOIN extdb.local_t AS t ON e.key = t.key;",
            tables);
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what} == "the storage of \"t1\" built no operator");
    }
    {
        auto tables = one_table(res, "remote.t2", "key", "val", {{1, 10}}, /*async=*/false);
        tables.at("remote.t2").no_operator = true;
        auto cursor = run_over_remote(dispatcher, "SELECT key, count(val) FROM remote.t2 GROUP BY key;", tables);
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what} == "the storage of \"t2\" built no operator");
    }
}

TEST_CASE("integration::cpp::extension_source::explain_shows_backend") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_explain/base"))
    remote_tables_t tables;
    tables.emplace("remote.t1",
                   remote_table_t{rows_spec_t{"key", "name", {{1, 11}}},
                                  pair_schema(res, "key", "name"),
                                  /*async_delivery=*/false});
    tables.emplace("remote.t2",
                   remote_table_t{rows_spec_t{"key", "value", {{1, 100}}},
                                  pair_schema(res, "key", "value"),
                                  /*async_delivery=*/false});
    auto text = explain_remote_plan(dispatcher,
                                    "SELECT l.name, r.value FROM remote.t1 AS l JOIN remote.t2 AS r ON l.key = r.key;",
                                    tables);
    INFO(text);
    const auto first = text.find("Extension Scan");
    REQUIRE(first != std::string::npos);
    REQUIRE(text.find("Extension Scan", first + 1) != std::string::npos);
}

// INSERT INTO a storage table: the rows of a local query reach the storage's insert sink.
TEST_CASE("integration::cpp::extension_source::sink_writes_backend") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_sink/base"))
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE sdb;")->is_success());
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE TABLE sdb.local_src (key BIGINT, val BIGINT);")
                ->is_success());
    REQUIRE(dispatcher
                ->execute_sql(otterbrix::session_id_t(),
                              "INSERT INTO sdb.local_src (key, val) VALUES (1,10),(2,20),(3,30);")
                ->is_success());

    auto tables = one_table(res, "remote.sink_target", "key", "val", {}, /*async=*/false);
    auto cursor =
        run_over_remote(dispatcher, "INSERT INTO remote.sink_target SELECT key, val FROM sdb.local_src;", tables);
    REQUIRE(cursor->is_success());

    auto written = remote_written();
    REQUIRE(written.size() == 3);
    std::sort(written.begin(), written.end());
    REQUIRE(written == std::vector<std::pair<int64_t, int64_t>>{{1, 10}, {2, 20}, {3, 30}});
}

static remote_tables_t fetch_on_open_tables(std::pmr::memory_resource* res,
                                            const std::string& prefix,
                                            const std::vector<std::chrono::milliseconds>& latencies,
                                            int failing = -1) {
    remote_tables_t tables;
    for (std::size_t i = 0; i < latencies.size(); ++i) {
        const auto n = static_cast<int64_t>(i);
        const auto value_col = "v" + std::to_string(i);
        tables.emplace("remote." + prefix + std::to_string(i),
                       remote_table_t{rows_spec_t{"key", value_col, {{1, 10 + n}, {2, 20 + n}}},
                                      pair_schema(res, "key", value_col),
                                      /*async_delivery=*/true,
                                      /*fetch_on_open=*/true,
                                      latencies[i],
                                      static_cast<int>(i) == failing});
    }
    return tables;
}

static std::vector<std::chrono::milliseconds> same_latency(int count, std::chrono::milliseconds latency) {
    return std::vector<std::chrono::milliseconds>(static_cast<std::size_t>(count), latency);
}

static std::string n_way_join(const std::string& prefix, int count) {
    std::string select = "SELECT s0.v0";
    std::string from = " FROM remote." + prefix + "0 AS s0";
    for (int i = 1; i < count; ++i) {
        const auto n = std::to_string(i);
        select += ", s" + n + ".v" + n;
        from += " JOIN remote." + prefix + n + " AS s" + n + " ON s0.key = s" + n + ".key";
    }
    return select + from + ";";
}

static std::vector<std::vector<int64_t>> sorted_rows(const cursor::cursor_t_ptr& cursor) {
    std::vector<std::vector<int64_t>> rows;
    for (const auto& chunk : cursor->chunks()) {
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            std::vector<int64_t> values;
            for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
                const auto cell = chunk.value(col, row);
                values.push_back(cell.value<int64_t>());
            }
            rows.push_back(std::move(values));
        }
    }
    std::sort(rows.begin(), rows.end());
    return rows;
}

// The executor opens every storage scan of the plan before it pumps any, so the fetches run at the same time: the
// peak of fetches in flight is the number of scans. Opened one by one, it would be 1.
TEST_CASE("integration::cpp::extension_source::sources_open_in_parallel") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_parallel_open/base"))
    constexpr int kSources = 4;
    open_probe().reset();
    auto cursor = run_over_remote(dispatcher,
                                  n_way_join("s", kSources),
                                  fetch_on_open_tables(res, "s", same_latency(kSources, std::chrono::milliseconds(50))));
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 2);
    CHECK(open_probe().opens.load() == kSources);
    CHECK(open_probe().peak_fetches_in_flight.load() == kSources);
    CHECK(open_probe().opens_at_first_next.load() == kSources);
    CHECK(open_probe().next_before_ready.load() == 0);
    CHECK(open_probe().next_on_open_ctx.load() == 0);
}

// Whichever piece the failing backend lands in, the query fails and no other open is left in flight.
TEST_CASE("integration::cpp::extension_source::failed_open_awaits_the_rest") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_failed_open/base"))
    constexpr int kSources = 4;
    for (int failing = 0; failing < kSources; ++failing) {
        std::vector<std::chrono::milliseconds> latencies = same_latency(kSources, std::chrono::milliseconds(200));
        latencies[static_cast<std::size_t>(failing)] = std::chrono::milliseconds(0);
        const std::string prefix = "f" + std::to_string(failing) + "_";
        open_probe().reset();
        auto cursor = run_over_remote(dispatcher,
                                      n_way_join(prefix, kSources),
                                      fetch_on_open_tables(res, prefix, latencies, failing));
        INFO("failing source " << failing);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().what == "backend unavailable");
        CHECK(open_probe().fetches_done.load() == open_probe().opens.load());
        CHECK(open_probe().destroyed_in_flight.load() == 0);
    }
}

TEST_CASE("integration::cpp::extension_source::uneven_open_latencies") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_uneven_open/base"))
    open_probe().reset();
    auto cursor = run_over_remote(
        dispatcher,
        n_way_join("u", 3),
        fetch_on_open_tables(
            res,
            "u",
            {std::chrono::milliseconds(10), std::chrono::milliseconds(50), std::chrono::milliseconds(200)}));
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == std::vector<std::vector<int64_t>>{{10, 11, 12}, {20, 21, 22}});
    CHECK(open_probe().opens.load() == 3);
    CHECK(open_probe().next_before_ready.load() == 0);
}

// A LATERAL inner is a private sub-plan the up-front open never reaches: run_subplan opens it on every re-drive.
TEST_CASE("integration::cpp::extension_source::lateral_inner_source_opened_per_drive") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_lateral_open/base"))
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE extdb;")->is_success());
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE TABLE extdb.outer_t (id BIGINT);")->is_success());
    REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "INSERT INTO extdb.outer_t (id) VALUES (1), (2), (3);")
                ->is_success());
    open_probe().reset();
    auto cursor = run_over_remote(dispatcher,
                                  "SELECT o.id, s.v0 FROM extdb.outer_t AS o, "
                                  "LATERAL (SELECT e.v0 FROM remote.lat0 AS e WHERE e.key = o.id) s;",
                                  fetch_on_open_tables(res, "lat", {std::chrono::milliseconds(20)}));
    REQUIRE(cursor->is_success());
    REQUIRE(sorted_rows(cursor) == std::vector<std::vector<int64_t>>{{1, 10}, {2, 20}});
    CHECK(open_probe().opens.load() == 3);
    CHECK(open_probe().fetches_done.load() == 3);
    CHECK(open_probe().next_before_ready.load() == 0);
}

// The engine takes at most DEFAULT_VECTOR_CAPACITY rows per source batch; slicing a wider backend page is the
// storage's job, so a wider chunk is refused whatever the query does with it.
TEST_CASE("integration::cpp::extension_source::chunk_over_vector_capacity") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_wide_chunk/base"))
    constexpr int64_t n = 2500;
    std::vector<std::pair<int64_t, int64_t>> rows;
    rows.reserve(n);
    for (int64_t i = 0; i < n; ++i) {
        rows.emplace_back(i, i % 3);
    }
    auto tables = one_table(res, "remote.wide", "key", "grp", rows, /*async=*/false);
    auto expect_refused = [&](const std::string& query) {
        auto cursor = run_over_remote(dispatcher, query, tables);
        INFO(query);
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_success());
        CHECK(cursor->get_error().type == core::error_code_t::invalid_parameter);
        CHECK(std::string{cursor->get_error().what}.find(std::to_string(vector::DEFAULT_VECTOR_CAPACITY)) !=
              std::string::npos);
    };

    SECTION("scan") { expect_refused("SELECT * FROM remote.wide;"); }
    SECTION("filter") { expect_refused("SELECT key FROM remote.wide WHERE key >= 2000;"); }
    SECTION("projection") { expect_refused("SELECT key + 1 AS k FROM remote.wide;"); }
    SECTION("group_by") { expect_refused("SELECT grp, COUNT(*) AS c FROM remote.wide GROUP BY grp;"); }
    SECTION("scalar_aggregate") { expect_refused("SELECT SUM(key) AS s FROM remote.wide;"); }
    auto seed_local = [&] {
        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE widedb;")->is_success());
        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE TABLE widedb.t (key BIGINT, amount BIGINT);")
                    ->is_success());
        REQUIRE(dispatcher
                    ->execute_sql(otterbrix::session_id_t(),
                                  "INSERT INTO widedb.t (key, amount) VALUES (1, 10), (1500, 20), (2499, 30);")
                    ->is_success());
    };
    SECTION("join_wide_on_the_left") {
        seed_local();
        expect_refused("SELECT w.grp, t.amount FROM remote.wide AS w JOIN widedb.t AS t ON w.key = t.key;");
    }
    SECTION("join_wide_on_the_right") {
        seed_local();
        expect_refused("SELECT w.grp, t.amount FROM widedb.t AS t JOIN remote.wide AS w ON t.key = w.key;");
    }
}

// count(*) without WHERE over a storage table counts the storage's rows: a storage table has no oid, so no
// pushdown turns the aggregate into a disk reduce.
TEST_CASE("integration::cpp::extension_source::count_star_over_a_storage_table") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_count_named/base"))
    auto tables = one_table(res, "remote.c", "key", "val", {{1, 10}, {2, 20}, {3, 30}}, /*async=*/false);
    auto cursor = run_over_remote(dispatcher, "SELECT count(*) AS c FROM remote.c;", tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    REQUIRE(cursor->value(0, 0).value<int64_t>() == 3);
}

// A host rule at the last optimizer stage gives the aggregate an explicit source child built from the same
// storage; count(*) counts that child's rows.
TEST_CASE("integration::cpp::extension_source::count_star_after_host_optimizer_rule") {
    auto config = test_create_config(integration_fixture_path("test_ext_count_pass/base"));
    test_clear_directory(config);
    const planner::optimizer_rule_t rules[] = {{planner::optimizer_stage::last, &attach_passthrough_rule}};
    test_spaces space(config, remote_host(rules));
    remote_server_reset_t remote_server_reset;
    auto dispatcher = space.dispatcher();
    auto* res = dispatcher->resource();
    auto tables = one_table(res, "remote.p", "key", "val", {{1, 10}, {2, 20}, {3, 30}}, /*async=*/false);

    auto cursor = run_over_remote(dispatcher, "SELECT count(*) AS c FROM remote.p;", tables);
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    REQUIRE(cursor->value(0, 0).value<int64_t>() == 3);
    CHECK(scans_made().load() == 1);
}
