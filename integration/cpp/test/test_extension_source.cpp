// e2e for the host-extension SOURCE/SINK operators: the host's name resolution swaps uid-qualified external
// leaves for node_extension_t leaves with declared columns; each node lowers through its own operator function.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
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

    struct mock_ext_data_t {
        rows_spec_t spec;
        bool async_delivery{true};
        std::vector<std::pair<int64_t, int64_t>>* sink_written{nullptr};
        bool fetch_on_open{false};
        std::chrono::milliseconds fetch_latency{0};
        bool fail_open{false};
        bool no_operator{false};
    };

    struct mock_payload_t final : logical_plan::extension_payload_t {
        explicit mock_payload_t(mock_ext_data_t data)
            : data(std::move(data)) {}
        mock_ext_data_t data;
    };

    class mock_source_op_t final : public operators::read_only_operator_t {
    public:
        mock_source_op_t(std::pmr::memory_resource* resource,
                         log_t log,
                         rows_spec_t spec,
                         bool async_delivery,
                         components::catalog::oid_t table_oid)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , spec_(std::move(spec))
            , async_delivery_(async_delivery)
            , table_oid_(table_oid) {}

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

        void explain_impl(const operators::explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }

    private:
        rows_spec_t spec_;
        bool async_delivery_{true};
        components::catalog::oid_t table_oid_{components::catalog::INVALID_OID};
        bool drained_{false};
    };

    struct open_probe_t {
        static constexpr std::size_t kSlots = 64;
        std::atomic<std::size_t> slots_used{0};
        std::atomic<int> opens{0};
        std::atomic<int> opens_at_first_next{-1};
        std::atomic<int> fetches_done{0};
        std::atomic<int> next_before_ready{0};
        std::atomic<int> next_on_open_ctx{0};
        std::atomic<int> destroyed_in_flight{0};
        std::array<std::atomic<bool>, kSlots> ready{};

        void reset() {
            slots_used.store(0);
            opens.store(0);
            opens_at_first_next.store(-1);
            fetches_done.store(0);
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
                                  bool fail_open,
                                  components::catalog::oid_t table_oid)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , spec_(std::move(spec))
            , fetch_latency_(fetch_latency)
            , fail_open_(fail_open)
            , table_oid_(table_oid)
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
                open_probe().ready[slot].store(true);
                p.set_value(std::move(outcome));
            }).detach();
            return future;
        }

        void explain_impl(const operators::explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }

        rows_spec_t spec_;
        std::chrono::milliseconds fetch_latency_;
        bool fail_open_;
        components::catalog::oid_t table_oid_;
        std::size_t slot_;
        const components::pipeline::context_t* open_ctx_{nullptr};
        bool drained_{false};
        bool opened_{false};
    };

    class mock_sink_op_t final : public operators::read_only_operator_t {
    public:
        mock_sink_op_t(std::pmr::memory_resource* resource,
                       log_t log,
                       std::vector<std::pair<int64_t, int64_t>>* written)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , written_(written) {}

        [[nodiscard]] operators::pipeline_role role() const noexcept override { return operators::pipeline_role::sink; }

        [[nodiscard]] core::error_t
        push(components::pipeline::context_t*, vector::data_chunk_t&& input, operators::chunks_vector_t&) override {
            for (size_t i = 0; i < input.size(); ++i) {
                written_->emplace_back(input.value(0, i).value<int64_t>(), input.value(1, i).value<int64_t>());
            }
            return core::error_t::no_error();
        }

        [[nodiscard]] core::error_t finalize(components::pipeline::context_t*, operators::chunks_vector_t&) override {
            return core::error_t::no_error();
        }

    private:
        std::vector<std::pair<int64_t, int64_t>>* written_;
    };

    // Dispatch by shape: a leaf is a source (reads a backend), a node with a child is a sink (writes one).
    services::planner::plan_result_t make_mock_extension(const services::context_storage_t& context,
                                                         const compute::function_registry_t&,
                                                         const logical_plan::node_extension_t& node) {
        const auto& data = static_cast<const mock_payload_t*>(node.payload())->data;
        if (data.no_operator) {
            // A successful result without an operator breaks the contract.
            return operators::operator_ptr{};
        }
        if (node.children().empty() && data.fetch_on_open) {
            return {new fetch_on_open_source_op_t(context.resource,
                                                  context.log.clone(),
                                                  data.spec,
                                                  data.fetch_latency,
                                                  data.fail_open,
                                                  node.table_oid())};
        }
        if (node.children().empty()) {
            return {new mock_source_op_t(context.resource,
                                         context.log.clone(),
                                         data.spec,
                                         data.async_delivery,
                                         node.table_oid())};
        }
        return {new mock_sink_op_t(context.resource, context.log.clone(), data.sink_written)};
    }

    static constexpr const char* kExtDb = "extreg";

    struct external_source_t {
        rows_spec_t spec;
        std::pmr::vector<types::complex_logical_type> schema;
        bool async_delivery{true};
        bool fetch_on_open{false};
        std::chrono::milliseconds fetch_latency{0};
        bool fail_open{false};
        bool no_operator{false};
    };
    using externals_by_uid_t = std::unordered_map<std::string, external_source_t>;

    // What the test host's name resolution swaps in for the next statement; its hooks are plain functions.
    struct host_state_t {
        externals_by_uid_t externals;
        bool named_wrapper{false};
        std::vector<logical_plan::node_extension_ptr> made;
    };
    host_state_t& host_state() {
        static host_state_t state;
        return state;
    }

    // Declared after the engine, so the host's nodes go before the resource they live on.
    struct host_state_reset_t {
        host_state_reset_t() { host_state() = host_state_t{}; }
        ~host_state_reset_t() { host_state() = host_state_t{}; }
    };

    logical_plan::node_extension_ptr
    make_extension(std::pmr::memory_resource* res, const std::string& name, const external_source_t& source) {
        auto ext = logical_plan::make_node_extension(
            res,
            name,
            std::pmr::vector<types::complex_logical_type>(source.schema, res),
            &make_mock_extension,
            logical_plan::extension_payload_ptr{new mock_payload_t{mock_ext_data_t{source.spec,
                                                                                   source.async_delivery,
                                                                                   nullptr,
                                                                                   source.fetch_on_open,
                                                                                   source.fetch_latency,
                                                                                   source.fail_open,
                                                                                   source.no_operator}}});
        REQUIRE_FALSE(ext.has_error());
        host_state().made.push_back(ext.value());
        return ext.value();
    }

    // `named_wrapper`: the host names the rebuilt aggregate after a local table (extreg.<uid>), so it resolves to
    // that table's oid like any FROM target.
    void swap_to_extension(logical_plan::node_ptr& node,
                           std::pmr::memory_resource* res,
                           const externals_by_uid_t& externals,
                           bool named_wrapper) {
        if (!node) {
            return;
        }
        if (node->type() == logical_plan::node_type::aggregate_t) {
            const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(node.get());
            const auto& uid_s = agg->target().unique_identifier.t;
            if (!uid_s.empty()) {
                auto it = externals.find(uid_s);
                if (it != externals.end()) {
                    auto ext = make_extension(res, uid_s, it->second);
                    ext->set_result_alias(agg->result_alias().empty()
                                              ? static_cast<const std::string&>(agg->target().collection)
                                              : agg->result_alias());
                    if (node->children().empty()) {
                        node = ext;
                    } else {
                        // The extension replaces only the implicit scan: rebuild as an identity aggregate
                        // whose data child is the extension leaf, keeping the uid aggregate's own stages.
                        auto wrapper = named_wrapper
                                           ? logical_plan::make_node_aggregate(
                                                 res,
                                                 qualified_name_t{core::dbname_t{kExtDb}, core::relname_t{uid_s}})
                                           : logical_plan::make_node_aggregate(res, qualified_name_t{});
                        wrapper->set_result_alias(node->result_alias());
                        wrapper->append_child(ext);
                        for (auto& child : node->children()) {
                            wrapper->append_child(child);
                        }
                        node = wrapper;
                    }
                    return;
                }
            }
        }
        for (auto& child : node->children()) {
            swap_to_extension(child, res, externals, named_wrapper);
        }
    }

    core::result_wrapper_t<logical_plan::node_ptr>
    swap_decide(std::pmr::memory_resource* res,
                logical_plan::node_ptr tree,
                std::span<const qualified_name_t>,
                std::span<const std::pmr::vector<vector::data_chunk_t>>) {
        swap_to_extension(tree, res, host_state().externals, host_state().named_wrapper);
        return tree;
    }

    services::engine::primitives_t swapping_host(std::span<const planner::optimizer_rule_t> rules = {}) {
        return services::engine::primitives_t{rules, {&planner::no_name_reads, &swap_decide}};
    }

    struct run_result_t {
        cursor::cursor_t_ptr cursor;
        std::chrono::steady_clock::duration elapsed;
    };

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

    // named_wrapper needs the local table the wrapper is named after.
    void create_named_wrapper_tables(otterbrix::wrapper_dispatcher_t* dispatcher, const externals_by_uid_t& externals) {
        dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE extreg;"); // idempotent per test dir
        for (const auto& [uid, source] : externals) {
            std::string columns;
            for (const auto& t : source.schema) {
                columns += (columns.empty() ? "" : ", ") + t.alias() + " BIGINT";
            }
            REQUIRE(
                dispatcher->execute_sql(otterbrix::session_id_t(), "CREATE TABLE extreg." + uid + " (" + columns + ");")
                    ->is_success());
        }
    }

    run_result_t run_with_extension_sources(otterbrix::wrapper_dispatcher_t* dispatcher,
                                            const std::string& sql,
                                            externals_by_uid_t externals,
                                            bool named_wrapper = false) {
        if (named_wrapper) {
            create_named_wrapper_tables(dispatcher, externals);
        }
        host_state().externals = std::move(externals);
        host_state().named_wrapper = named_wrapper;
        host_state().made.clear();
        const auto started = std::chrono::steady_clock::now();
        auto cursor = execute_within_deadline(dispatcher, sql);
        return {std::move(cursor), std::chrono::steady_clock::now() - started};
    }

    std::string explain_extension_plan(otterbrix::wrapper_dispatcher_t* dispatcher,
                                       const std::string& sql,
                                       externals_by_uid_t externals) {
        host_state().externals = std::move(externals);
        host_state().named_wrapper = false;
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

    // A `last`-stage rule: gives a FROM aggregate over a local table the host serves an extension source child,
    // the way a host rule replaces the implicit table scan late in optimization.
    logical_plan::node_ptr attach_extension_source_rule(std::pmr::memory_resource* res,
                                                        logical_plan::node_ptr node,
                                                        const planner::optimizer_rule_context_t& context) {
        if (!node) {
            return node;
        }
        for (auto& child : node->children()) {
            child = attach_extension_source_rule(res, child, context);
        }
        if (node->type() != logical_plan::node_type::aggregate_t) {
            return node;
        }
        const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(node.get());
        const auto& db = static_cast<const std::string&>(agg->target().database);
        const auto& rel = static_cast<const std::string&>(agg->target().collection);
        auto it = host_state().externals.find(rel);
        if (db != kExtDb || it == host_state().externals.end()) {
            return node;
        }
        for (const auto& child : node->children()) {
            if (child->type() == logical_plan::node_type::extension_t) {
                return node;
            }
        }
        node->append_child(make_extension(res, rel, it->second));
        return node;
    }

} // namespace

static externals_by_uid_t one_source(std::pmr::memory_resource* res,
                                     const std::string& uid,
                                     const std::string& col_a,
                                     const std::string& col_b,
                                     std::vector<std::pair<int64_t, int64_t>> rows,
                                     bool async_delivery) {
    externals_by_uid_t externals;
    externals.emplace(
        uid,
        external_source_t{rows_spec_t{col_a, col_b, std::move(rows)}, pair_schema(res, col_a, col_b), async_delivery});
    return externals;
}

#define EXT_TEST_BOILERPLATE(DIR)                                                                                      \
    auto config = test_create_config(DIR);                                                                             \
    test_clear_directory(config);                                                                                      \
    test_spaces space(config, swapping_host()); /* the host's name resolution, given at engine start */                \
    host_state_reset_t host_state_reset;                                                                               \
    auto dispatcher = space.dispatcher();                                                                              \
    auto* res = dispatcher->resource();

TEST_CASE("integration::cpp::extension_source::sync_single_leaf") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_sync/base"))
    auto externals = one_source(res, "uid_x", "key", "val", {{7, 70}}, /*async=*/false);
    auto r = run_with_extension_sources(dispatcher, "SELECT * FROM uid_x.remote.db1.t1;", externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 1);
}

TEST_CASE("integration::cpp::extension_source::async_single_leaf") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_async/base"))
    auto externals = one_source(res, "uid_x", "key", "val", {{1, 10}, {2, 20}, {3, 30}}, /*async=*/true);
    auto r = run_with_extension_sources(dispatcher, "SELECT * FROM uid_x.remote.db1.t1;", externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 3);
}

TEST_CASE("integration::cpp::extension_source::empty_result") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_empty/base"))
    auto externals = one_source(res, "uid_x", "key", "val", {}, /*async=*/true);
    auto r = run_with_extension_sources(dispatcher, "SELECT * FROM uid_x.remote.db1.t1;", externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 0);
}

TEST_CASE("integration::cpp::extension_source::join_two_extensions") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_join2/base"))
    externals_by_uid_t externals;
    externals.emplace("uid_l",
                      external_source_t{rows_spec_t{"key", "name", {{1, 11}, {2, 22}, {3, 33}}},
                                        pair_schema(res, "key", "name"),
                                        /*async_delivery=*/true});
    externals.emplace("uid_r",
                      external_source_t{rows_spec_t{"key", "value", {{1, 100}, {3, 300}, {9, 900}}},
                                        pair_schema(res, "key", "value"),
                                        /*async_delivery=*/true});
    auto r = run_with_extension_sources(dispatcher,
                                        "SELECT l.name, r.value FROM uid_l.remote.db1.t1 AS l "
                                        "JOIN uid_r.remote.db1.t2 AS r ON l.key = r.key;",
                                        externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 2);
}

TEST_CASE("integration::cpp::extension_source::group_by") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_group/base"))
    auto externals =
        one_source(res, "uid_g", "grp", "val", {{1, 10}, {1, 15}, {2, 20}, {2, 5}, {3, 1}}, /*async=*/true);
    auto r = run_with_extension_sources(dispatcher,
                                        "SELECT grp, SUM(val) AS s FROM uid_g.remote.db1.t1 GROUP BY grp;",
                                        externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 3);
}

TEST_CASE("integration::cpp::extension_source::barrier_where_above_join") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_barrier/base"))
    externals_by_uid_t externals;
    externals.emplace("uid_l",
                      external_source_t{rows_spec_t{"key", "name", {{1, 11}, {2, 22}, {3, 33}}},
                                        pair_schema(res, "key", "name"),
                                        /*async_delivery=*/true});
    externals.emplace("uid_r",
                      external_source_t{rows_spec_t{"key", "value", {{1, 100}, {2, 200}, {3, 300}}},
                                        pair_schema(res, "key", "value"),
                                        /*async_delivery=*/false});
    auto r = run_with_extension_sources(dispatcher,
                                        "SELECT l.name, r.value FROM uid_l.remote.db1.t1 AS l "
                                        "JOIN uid_r.remote.db1.t2 AS r ON l.key = r.key "
                                        "WHERE r.value > 150;",
                                        externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 2);

    // Extension leaves must survive optimize() untouched: identity intact, no predicate/limit injected.
    REQUIRE(host_state().made.size() == 2);
    const auto* ext =
        host_state().made.front()->name() == "uid_l" ? host_state().made.front().get() : host_state().made.back().get();
    REQUIRE(ext->name() == "uid_l");
    REQUIRE(ext->expressions().empty());
    REQUIRE(ext->children().empty());
}

TEST_CASE("integration::cpp::extension_source::join_with_local_table") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_local/base")) {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE extdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE extdb.local_t (key BIGINT, amount BIGINT);");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "INSERT INTO extdb.local_t (key, amount) VALUES (1, 1000), (2, 2000), (5, 5000);");
        REQUIRE(cur->is_success());
    }
    auto externals = one_source(res, "uid_l", "key", "name", {{1, 11}, {2, 22}, {3, 33}}, /*async=*/true);
    auto r = run_with_extension_sources(dispatcher,
                                        "SELECT e.name, t.amount FROM uid_l.remote.db1.t1 AS e "
                                        "JOIN extdb.local_t AS t ON e.key = t.key;",
                                        externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 2);
}

// A host operator function that builds no operator must surface a clean error, not a crash.
TEST_CASE("integration::cpp::extension_source::missing_operator_errors_not_crash") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_norule/base")) {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE extdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE extdb.local_t (key BIGINT, amount BIGINT);")->is_success());
    }
    {
        auto externals = one_source(res, "uid_l", "key", "name", {{1, 11}}, /*async=*/false);
        externals.at("uid_l").no_operator = true;
        auto r = run_with_extension_sources(dispatcher,
                                            "SELECT e.name, t.amount FROM uid_l.remote.db1.t1 AS e "
                                            "JOIN extdb.local_t AS t ON e.key = t.key;",
                                            externals);
        REQUIRE(r.cursor->is_error());
        CHECK(std::string{r.cursor->get_error().what} ==
              "the physical plan generator built no operator for $extension: uid_l");
    }
    {
        auto externals = one_source(res, "uid_g", "key", "val", {{1, 10}}, /*async=*/false);
        externals.at("uid_g").no_operator = true;
        auto r = run_with_extension_sources(dispatcher,
                                            "SELECT key, count(val) FROM uid_g.remote.db1.t1 GROUP BY key;",
                                            externals);
        REQUIRE(r.cursor->is_error());
        CHECK(std::string{r.cursor->get_error().what} ==
              "the physical plan generator built no operator for $extension: uid_g");
    }
}

TEST_CASE("integration::cpp::extension_source::explain_shows_backend") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_explain/base"))
    externals_by_uid_t externals;
    externals.emplace("uid_l",
                      external_source_t{rows_spec_t{"key", "name", {{1, 11}}},
                                        pair_schema(res, "key", "name"),
                                        /*async_delivery=*/false});
    externals.emplace("uid_r",
                      external_source_t{rows_spec_t{"key", "value", {{1, 100}}},
                                        pair_schema(res, "key", "value"),
                                        /*async_delivery=*/false});
    auto text = explain_extension_plan(dispatcher,
                                       "SELECT l.name, r.value FROM uid_l.remote.db1.t1 AS l "
                                       "JOIN uid_r.remote.db1.t2 AS r ON l.key = r.key;",
                                       externals);
    INFO(text);
    const auto first = text.find("Extension Scan");
    REQUIRE(first != std::string::npos);
    REQUIRE(text.find("Extension Scan", first + 1) != std::string::npos);
}

// Built by hand: there is no SQL syntax for INSERT INTO <backend>, so this wires a node_extension_t directly.
TEST_CASE("integration::cpp::extension_source::sink_writes_backend") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_sink/base"))

    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE DATABASE sdb;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE sdb.local_src (key BIGINT, val BIGINT);")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO sdb.local_src (key, val) VALUES (1,10),(2,20),(3,30);")
                    ->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE sdb.sink_target (key BIGINT, val BIGINT);")->is_success());
    }

    std::pmr::monotonic_buffer_resource arena(res);
    sql::transform::transformer transformer(res);
    auto* raw = raw_parser(&arena, "SELECT key, val FROM sdb.local_src;");
    REQUIRE(raw != nullptr);
    auto& ast = sql::transform::pg_cell_to_node_cast(linitial(raw));
    auto binder = transformer.transform(ast);
    REQUIRE_FALSE(binder.has_error());
    auto finalized = binder.finalize();
    REQUIRE_FALSE(finalized.has_error());
    auto exec_plan = std::move(finalized.value());
    auto child = exec_plan.sub_queries.back();
    REQUIRE(child);

    std::vector<std::pair<int64_t, int64_t>> written;
    mock_ext_data_t sink_data;
    sink_data.sink_written = &written;
    auto sink = logical_plan::make_node_extension(res,
                                                  "sink_target",
                                                  std::pmr::vector<types::complex_logical_type>{res},
                                                  &make_mock_extension,
                                                  logical_plan::extension_payload_ptr{new mock_payload_t{sink_data}});
    REQUIRE_FALSE(sink.has_error());
    sink.value()->append_child(child);

    auto session = otterbrix::session_id_t();
    exec_plan.sub_queries.back() = sink.value();
    auto cur = dispatcher->execute_plan(session, std::move(exec_plan));
    REQUIRE(cur->is_success());

    REQUIRE(written.size() == 3);
    std::sort(written.begin(), written.end());
    REQUIRE(written == std::vector<std::pair<int64_t, int64_t>>{{1, 10}, {2, 20}, {3, 30}});
}

static externals_by_uid_t fetch_on_open_sources(std::pmr::memory_resource* res,
                                                const std::string& uid_prefix,
                                                const std::vector<std::chrono::milliseconds>& latencies,
                                                int failing = -1) {
    externals_by_uid_t externals;
    for (std::size_t i = 0; i < latencies.size(); ++i) {
        const auto n = static_cast<int64_t>(i);
        const auto value_col = "v" + std::to_string(i);
        externals.emplace(uid_prefix + std::to_string(i),
                          external_source_t{rows_spec_t{"key", value_col, {{1, 10 + n}, {2, 20 + n}}},
                                            pair_schema(res, "key", value_col),
                                            /*async_delivery=*/true,
                                            /*fetch_on_open=*/true,
                                            latencies[i],
                                            static_cast<int>(i) == failing});
    }
    return externals;
}

static std::vector<std::chrono::milliseconds> same_latency(int count, std::chrono::milliseconds latency) {
    return std::vector<std::chrono::milliseconds>(static_cast<std::size_t>(count), latency);
}

static std::string n_way_join(const std::string& uid_prefix, int count) {
    std::string select = "SELECT s0.v0";
    std::string from = " FROM " + uid_prefix + "0.remote.db1.t0 AS s0";
    for (int i = 1; i < count; ++i) {
        const auto n = std::to_string(i);
        select += ", s" + n + ".v" + n;
        from += " JOIN " + uid_prefix + n + ".remote.db1.t" + n + " AS s" + n + " ON s0.key = s" + n + ".key";
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

// K backends x fetch_latency each: opened together the query pays ~one latency, pumped one by one it pays K.
TEST_CASE("integration::cpp::extension_source::sources_open_in_parallel") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_parallel_open/base"))
    constexpr int kSources = 4;
    constexpr auto kLatency = std::chrono::milliseconds(50);

    auto instant = run_with_extension_sources(
        dispatcher,
        n_way_join("uid_i", kSources),
        fetch_on_open_sources(res, "uid_i", same_latency(kSources, std::chrono::milliseconds(0))));
    REQUIRE(instant.cursor->is_success());
    REQUIRE(instant.cursor->size() == 2);

    open_probe().reset();
    auto slow = run_with_extension_sources(dispatcher,
                                           n_way_join("uid_s", kSources),
                                           fetch_on_open_sources(res, "uid_s", same_latency(kSources, kLatency)));
    REQUIRE(slow.cursor->is_success());
    REQUIRE(slow.cursor->size() == 2);

    const auto instant_ms = std::chrono::duration_cast<std::chrono::milliseconds>(instant.elapsed);
    const auto slow_ms = std::chrono::duration_cast<std::chrono::milliseconds>(slow.elapsed);
    INFO("instant " << instant_ms.count() << " ms, " << kSources << " x " << kLatency.count()
                    << " ms: " << slow_ms.count() << " ms");
    CHECK(open_probe().opens.load() == kSources);
    CHECK(open_probe().opens_at_first_next.load() == kSources);
    CHECK(open_probe().next_before_ready.load() == 0);
    CHECK(open_probe().next_on_open_ctx.load() == 0);
    REQUIRE(slow_ms - instant_ms < 2 * kLatency);
}

// Whichever piece the failing backend lands in, the query fails and no other open is left in flight.
TEST_CASE("integration::cpp::extension_source::failed_open_awaits_the_rest") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_failed_open/base"))
    constexpr int kSources = 4;
    for (int failing = 0; failing < kSources; ++failing) {
        std::vector<std::chrono::milliseconds> latencies = same_latency(kSources, std::chrono::milliseconds(200));
        latencies[static_cast<std::size_t>(failing)] = std::chrono::milliseconds(0);
        const std::string prefix = "uid_f" + std::to_string(failing) + "_";
        open_probe().reset();
        auto r = run_with_extension_sources(dispatcher,
                                            n_way_join(prefix, kSources),
                                            fetch_on_open_sources(res, prefix, latencies, failing));
        INFO("failing source " << failing);
        REQUIRE(r.cursor->is_error());
        CHECK(r.cursor->get_error().what == "backend unavailable");
        CHECK(open_probe().fetches_done.load() == open_probe().opens.load());
        CHECK(open_probe().destroyed_in_flight.load() == 0);
    }
}

TEST_CASE("integration::cpp::extension_source::uneven_open_latencies") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_uneven_open/base"))
    open_probe().reset();
    auto r = run_with_extension_sources(
        dispatcher,
        n_way_join("uid_u", 3),
        fetch_on_open_sources(
            res,
            "uid_u",
            {std::chrono::milliseconds(10), std::chrono::milliseconds(50), std::chrono::milliseconds(200)}));
    REQUIRE(r.cursor->is_success());
    REQUIRE(sorted_rows(r.cursor) == std::vector<std::vector<int64_t>>{{10, 11, 12}, {20, 21, 22}});
    CHECK(open_probe().opens.load() == 3);
    CHECK(open_probe().next_before_ready.load() == 0);
}

// A LATERAL inner is a private sub-plan the up-front open never reaches: run_subplan opens it on every re-drive.
TEST_CASE("integration::cpp::extension_source::lateral_inner_source_opened_per_drive") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_lateral_open/base")) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE extdb;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE extdb.outer_t (id BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO extdb.outer_t (id) VALUES (1), (2), (3);")->is_success());
    }
    open_probe().reset();
    auto r = run_with_extension_sources(dispatcher,
                                        "SELECT o.id, s.v0 FROM extdb.outer_t AS o, "
                                        "LATERAL (SELECT e.v0 FROM uid_lat0.remote.db1.t0 AS e WHERE e.key = o.id) s;",
                                        fetch_on_open_sources(res, "uid_lat", {std::chrono::milliseconds(20)}));
    REQUIRE(r.cursor->is_success());
    REQUIRE(sorted_rows(r.cursor) == std::vector<std::vector<int64_t>>{{1, 10}, {2, 20}});
    CHECK(open_probe().opens.load() == 3);
    CHECK(open_probe().fetches_done.load() == 3);
    CHECK(open_probe().next_before_ready.load() == 0);
}

// The engine takes at most DEFAULT_VECTOR_CAPACITY rows per source batch; slicing a wider backend page is the
// host's job, so a wider chunk is refused whatever the query does with it.
TEST_CASE("integration::cpp::extension_source::chunk_over_vector_capacity") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_wide_chunk/base"))
    constexpr int64_t n = 2500;
    std::vector<std::pair<int64_t, int64_t>> rows;
    rows.reserve(n);
    for (int64_t i = 0; i < n; ++i) {
        rows.emplace_back(i, i % 3);
    }
    auto externals = one_source(res, "uid_w", "key", "grp", rows, /*async=*/false);
    auto expect_refused = [&](const std::string& query) {
        auto r = run_with_extension_sources(dispatcher, query, externals);
        INFO(query);
        REQUIRE(r.cursor);
        REQUIRE_FALSE(r.cursor->is_success());
        CHECK(r.cursor->get_error().type == core::error_code_t::invalid_parameter);
        CHECK(std::string{r.cursor->get_error().what}.find(std::to_string(vector::DEFAULT_VECTOR_CAPACITY)) !=
              std::string::npos);
    };

    SECTION("scan") { expect_refused("SELECT * FROM uid_w.remote.db1.t1;"); }
    SECTION("filter") { expect_refused("SELECT key FROM uid_w.remote.db1.t1 WHERE key >= 2000;"); }
    SECTION("projection") { expect_refused("SELECT key + 1 AS k FROM uid_w.remote.db1.t1;"); }
    SECTION("group_by") { expect_refused("SELECT grp, COUNT(*) AS c FROM uid_w.remote.db1.t1 GROUP BY grp;"); }
    SECTION("scalar_aggregate") { expect_refused("SELECT SUM(key) AS s FROM uid_w.remote.db1.t1;"); }
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
        expect_refused("SELECT w.grp, t.amount FROM uid_w.remote.db1.t1 AS w JOIN widedb.t AS t ON w.key = t.key;");
    }
    SECTION("join_wide_on_the_right") {
        seed_local();
        expect_refused("SELECT w.grp, t.amount FROM widedb.t AS t JOIN uid_w.remote.db1.t1 AS w ON t.key = w.key;");
    }
}

// count(*) without WHERE over a host source: the host aggregate resolves to the registered (empty) catalog
// table, which must not turn the aggregate into a disk reduce of that table instead of counting the source.
TEST_CASE("integration::cpp::extension_source::count_star_over_named_host_aggregate") {
    EXT_TEST_BOILERPLATE(integration_fixture_path("test_ext_count_named/base"))
    auto externals = one_source(res, "uid_c", "key", "val", {{1, 10}, {2, 20}, {3, 30}}, /*async=*/false);
    auto r = run_with_extension_sources(dispatcher,
                                        "SELECT count(*) AS c FROM uid_c.remote.db1.t1;",
                                        externals,
                                        /*named_wrapper=*/true);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 1);
    REQUIRE(r.cursor->value(0, 0).value<int64_t>() == 3);
}

// Same bug through a host rule at the last optimizer stage, after pushdown_aggregate stamped the aggregate.
TEST_CASE("integration::cpp::extension_source::count_star_after_host_optimizer_rule") {
    auto config = test_create_config(integration_fixture_path("test_ext_count_pass/base"));
    test_clear_directory(config);
    const planner::optimizer_rule_t rules[] = {{planner::optimizer_stage::last, &attach_extension_source_rule}};
    test_spaces space(config, swapping_host(rules));
    host_state_reset_t host_state_reset;
    auto dispatcher = space.dispatcher();
    auto* res = dispatcher->resource();
    auto externals = one_source(res, "uid_p", "key", "val", {{1, 10}, {2, 20}, {3, 30}}, /*async=*/false);
    create_named_wrapper_tables(dispatcher, externals);
    host_state().externals = std::move(externals);

    auto cur = dispatcher->execute_sql(otterbrix::session_id_t(), "SELECT count(*) AS c FROM extreg.uid_p;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    REQUIRE(cur->value(0, 0).value<int64_t>() == 3);
}
