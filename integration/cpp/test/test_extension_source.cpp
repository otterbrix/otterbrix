// e2e for the host-extension SOURCE/SINK operators: uid-qualified external leaves are swapped for
// node_extension_t leaves (pure (db, rel) identity, no host state), resolved by an injected create_plan rule.

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
#include <chrono>
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
        vector::data_chunk_t chunk(res, types, spec.rows.empty() ? 1 : spec.rows.size());
        chunk.set_cardinality(spec.rows.size());
        for (size_t i = 0; i < spec.rows.size(); ++i) {
            chunk.set_value(0, i, spec.rows[i].first);
            chunk.set_value(1, i, spec.rows[i].second);
        }
        return chunk;
    }

    // Keyed by the extension node's (db, rel) identity; the node itself carries no host state.
    struct mock_ext_data_t {
        rows_spec_t spec;
        bool async_delivery{true};
        std::vector<std::pair<int64_t, int64_t>>* sink_written{nullptr};
    };
    std::unordered_map<std::string, mock_ext_data_t>& mock_ext_store() {
        static std::unordered_map<std::string, mock_ext_data_t> store;
        return store;
    }
    std::string ext_key(const std::string& db, const std::string& rel) { return db + "." + rel; }

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

        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(components::pipeline::context_t*) override {
            actor_zeta::promise<core::result_wrapper_t<vector::data_chunk_t>> promise(resource());
            auto future = promise.get_future();
            if (drained_) {
                // Drained sentinel is a 0-column chunk; a schema'd 0-row chunk is real input
                // (the empty-guard a scalar aggregate needs) per execute_pipeline's pump contract.
                vector::data_chunk_t sentinel(resource(), std::pmr::vector<types::complex_logical_type>{resource()}, 0);
                promise.set_value(core::result_wrapper_t<vector::data_chunk_t>{std::move(sentinel)});
                return future;
            }
            drained_ = true;
            if (async_delivery_) {
                // Fulfills from a background thread after the executor has begun awaiting, modeling
                // a host backend actor answering a fetch; the promise outlives this stack frame.
                std::thread([p = std::move(promise), chunk = build_pairs(resource(), spec_)]() mutable {
                    std::this_thread::sleep_for(std::chrono::milliseconds(30));
                    p.set_value(core::result_wrapper_t<vector::data_chunk_t>{std::move(chunk)});
                }).detach();
            } else {
                promise.set_value(core::result_wrapper_t<vector::data_chunk_t>{build_pairs(resource(), spec_)});
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
    operators::operator_ptr make_mock_extension(const services::context_storage_t& context,
                                                const compute::function_registry_t&,
                                                const logical_plan::node_ptr& node) {
        if (node->type() != logical_plan::node_type::extension_t) {
            return {};
        }
        const auto* ext = static_cast<const logical_plan::node_extension_t*>(node.get());
        auto it = mock_ext_store().find(ext_key(ext->dbname(), ext->relname()));
        if (it == mock_ext_store().end()) {
            return {};
        }
        if (node->children().empty()) {
            return {new mock_source_op_t(context.resource,
                                         context.log.clone(),
                                         it->second.spec,
                                         it->second.async_delivery,
                                         node->table_oid())};
        }
        return {new mock_sink_op_t(context.resource, context.log.clone(), it->second.sink_written)};
    }

    static constexpr const char* kExtDb = "extreg";

    struct external_source_t {
        rows_spec_t spec;
        std::pmr::vector<types::complex_logical_type> schema;
        bool async_delivery{true};
    };
    using externals_by_uid_t = std::unordered_map<std::string, external_source_t>;

    void swap_to_extension(logical_plan::node_ptr& node,
                           std::pmr::memory_resource* res,
                           const externals_by_uid_t& externals) {
        if (!node) {
            return;
        }
        if (node->type() == logical_plan::node_type::aggregate_t) {
            const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(node.get());
            const auto& uid_s = static_cast<const std::string&>(agg->uid());
            if (!uid_s.empty()) {
                auto it = externals.find(uid_s);
                if (it != externals.end()) {
                    mock_ext_store()[ext_key(kExtDb, uid_s)] =
                        mock_ext_data_t{it->second.spec, it->second.async_delivery, nullptr};
                    auto ext = logical_plan::make_node_extension(res, core::dbname_t{kExtDb}, core::relname_t{uid_s});
                    ext->set_result_alias(agg->result_alias().empty() ? static_cast<const std::string&>(agg->relname())
                                                                      : agg->result_alias());
                    if (node->children().empty()) {
                        node = ext;
                    } else {
                        // The extension replaces only the implicit scan: rebuild as an identity aggregate
                        // whose data child is the extension leaf, keeping the uid aggregate's own stages.
                        auto wrapper = logical_plan::make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
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
            swap_to_extension(child, res, externals);
        }
    }

    struct run_result_t {
        cursor::cursor_t_ptr cursor;
        logical_plan::node_ptr plan;
    };

    void register_externals(otterbrix::wrapper_dispatcher_t* dispatcher, const externals_by_uid_t& externals) {
        auto* res = dispatcher->resource();
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE extreg;"); // idempotent per test dir
        }
        for (const auto& [uid, source] : externals) {
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(source.schema.size());
            for (const auto& t : source.schema) {
                columns.emplace_back(t.alias(), t);
            }
            auto create_node =
                logical_plan::make_node_create_collection(res, core::relname_t{uid}, std::move(columns), {});
            auto wrapped = sql::transform::name_catalog_target(kExtDb, {}, create_node);
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                logical_plan::execution_plan_t{res, std::move(wrapped), logical_plan::make_parameter_node(res)});
            REQUIRE(cur->is_success());
        }
    }

    run_result_t run_with_extension_sources(otterbrix::wrapper_dispatcher_t* dispatcher,
                                            const std::string& sql,
                                            externals_by_uid_t externals) {
        auto* res = dispatcher->resource();
        std::pmr::monotonic_buffer_resource arena(res);
        sql::transform::transformer transformer(res);

        auto* raw = raw_parser(&arena, sql.c_str());
        REQUIRE(raw != nullptr);
        auto& ast_ref = sql::transform::pg_cell_to_node_cast(linitial(raw));
        auto binder = transformer.transform(ast_ref);
        REQUIRE_FALSE(binder.has_error());

        auto plan = binder.node_ptr();
        REQUIRE(plan);

        register_externals(dispatcher, externals);
        swap_to_extension(plan, res, externals);

        auto session = otterbrix::session_id_t();
        logical_plan::execution_plan_t exec_plan{dispatcher->resource(), plan, binder.params_ptr()};
        auto cursor = dispatcher->execute_plan(session, std::move(exec_plan));
        return {std::move(cursor), std::move(plan)};
    }

    std::string explain_extension_plan(otterbrix::wrapper_dispatcher_t* dispatcher,
                                       const std::string& sql,
                                       externals_by_uid_t externals) {
        auto* res = dispatcher->resource();
        std::pmr::monotonic_buffer_resource arena(res);
        sql::transform::transformer transformer(res);
        auto* raw = raw_parser(&arena, sql.c_str());
        REQUIRE(raw != nullptr);
        auto& ast_ref = sql::transform::pg_cell_to_node_cast(linitial(raw));
        auto binder = transformer.transform(ast_ref);
        REQUIRE_FALSE(binder.has_error());
        auto plan = binder.node_ptr();
        REQUIRE(plan);
        register_externals(dispatcher, externals);
        swap_to_extension(plan, res, externals);

        logical_plan::execution_plan_t exec_plan{res, plan, binder.params_ptr()};
        exec_plan.explain = logical_plan::explain_type::plan;
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_plan(session, std::move(exec_plan));
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

    const logical_plan::node_extension_t* find_extension(const logical_plan::node_ptr& node) {
        if (!node) {
            return nullptr;
        }
        if (node->type() == logical_plan::node_type::extension_t) {
            return static_cast<const logical_plan::node_extension_t*>(node.get());
        }
        for (const auto& child : node->children()) {
            if (const auto* found = find_extension(child)) {
                return found;
            }
        }
        return nullptr;
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
    mock_ext_store().clear();                        /* fresh host store per test (keyed by db.rel) */                 \
    test_spaces space(config, &make_mock_extension); /* host injects its create_plan rule at engine start */           \
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
    const auto* ext = find_extension(r.plan);
    REQUIRE(ext != nullptr);
    REQUIRE(ext->relname() == "uid_l");
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

// A host-extension node with no injected create_plan rule must surface a clean error, not a crash.
TEST_CASE("integration::cpp::extension_source::missing_rule_errors_not_crash") {
    auto config = test_create_config(integration_fixture_path("test_ext_norule/base"));
    test_clear_directory(config);
    mock_ext_store().clear();
    test_spaces space(config); // NO create_plan rule injected → extension lowers to null
    auto dispatcher = space.dispatcher();
    auto* res = dispatcher->resource();
    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE extdb;");
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE extdb.local_t (key BIGINT, amount BIGINT);")->is_success());
    }
    {
        auto externals = one_source(res, "uid_l", "key", "name", {{1, 11}}, /*async=*/false);
        auto r = run_with_extension_sources(dispatcher,
                                            "SELECT e.name, t.amount FROM uid_l.remote.db1.t1 AS e "
                                            "JOIN extdb.local_t AS t ON e.key = t.key;",
                                            externals);
        REQUIRE(r.cursor->is_error());
    }
    {
        auto externals = one_source(res, "uid_g", "key", "val", {{1, 10}}, /*async=*/false);
        auto r = run_with_extension_sources(dispatcher,
                                            "SELECT key, count(val) FROM uid_g.remote.db1.t1 GROUP BY key;",
                                            externals);
        REQUIRE(r.cursor->is_error());
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
    REQUIRE(text.find("Extension Scan on uid_l") != std::string::npos);
    REQUIRE(text.find("Extension Scan on uid_r") != std::string::npos);
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
    auto child = binder.node_ptr();
    REQUIRE(child);

    std::vector<std::pair<int64_t, int64_t>> written;
    mock_ext_store()[ext_key("sdb", "sink_target")] = mock_ext_data_t{rows_spec_t{}, false, &written};
    auto sink = logical_plan::make_node_extension(res, core::dbname_t{"sdb"}, core::relname_t{"sink_target"});
    sink->append_child(child);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_plan(session, logical_plan::execution_plan_t{res, sink, binder.params_ptr()});
    REQUIRE(cur->is_success());

    REQUIRE(written.size() == 3);
    std::sort(written.begin(), written.end());
    REQUIRE(written == std::vector<std::pair<int64_t, int64_t>>{{1, 10}, {2, 20}, {3, 30}});
}
