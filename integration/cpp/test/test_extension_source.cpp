// Group-A e2e for the host-extension SOURCE/SINK operators (node_extension_t):
// simulates the NEW OtterStax federation flow. Mirrors test_join_raw.cpp, which
// simulates the OLD flow (pre-fetch + node_raw_data splice): here the
// uid-qualified external leaves are swapped for node_extension_t leaves (pure
// (db, rel) identity — no host state on the node), and the injected create_plan
// RULE builds a HOST mock operator, looking its runtime data up by identity. The
// async fetch is fulfilled from a BACKGROUND thread through an actor_zeta::promise
// — the disk-actor await pattern a real host BackendActor uses.

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/temp_dir.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/context_storage.hpp>

#include <algorithm>
#include <chrono>
#include <thread>
#include <unordered_map>

using namespace components;

namespace {

    // POD row source (plain data, no function wrappers per project rules):
    // two BIGINT columns + rows; chunks are built on demand from it.
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

    // Host-side runtime store (test double), keyed by the extension node's
    // (db, rel) identity — exactly how a real host maps a registered external
    // table to its own backend data. The node carries NO host state (it is pure
    // logical plan); the injected create_plan rule looks the data up by identity.
    struct mock_ext_data_t {
        rows_spec_t spec;                                                // SOURCE rows
        bool async_delivery{true};
        std::vector<std::pair<int64_t, int64_t>>* sink_written{nullptr}; // SINK target
    };
    std::unordered_map<std::string, mock_ext_data_t>& mock_ext_store() {
        static std::unordered_map<std::string, mock_ext_data_t> store;
        return store;
    }
    std::string ext_key(const std::string& db, const std::string& rel) { return db + "." + rel; }

    // Host-side mock operator: role()==source; first source_next delivers the
    // spec's chunk (async: fulfilled from a background thread while the
    // executor cooperatively awaits), second call reports drained.
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
                // Drained sentinel: a 0-COLUMN chunk (`batch.data.empty()`), per the
                // pump contract in execute_pipeline — a schema'd 0-row chunk is REAL
                // input (the empty-guard a scalar aggregate needs), not a drain.
                vector::data_chunk_t sentinel(resource(),
                                              std::pmr::vector<types::complex_logical_type>{resource()},
                                              0);
                promise.set_value(core::result_wrapper_t<vector::data_chunk_t>{std::move(sentinel)});
                return future;
            }
            drained_ = true;
            if (async_delivery_) {
                // Fulfill from a background thread AFTER the executor has begun
                // awaiting — models a host backend actor answering a fetch.
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

        // EXPLAIN: emit the oid (uniform scan contract) — the renderer resolves it
        // to the registered catalog name, so the plan shows which source it hits.
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

    // Host-side mock SINK operator: role()==sink. push() consumes the child's
    // rows (the SELECT feeding INSERT..SELECT) and records them — a real host
    // would send them to its BackendActor to INSERT. finalize() is a no-op here
    // (a real sink flushes the remainder + reports the affected count).
    class mock_sink_op_t final : public operators::read_only_operator_t {
    public:
        mock_sink_op_t(std::pmr::memory_resource* resource,
                       log_t log,
                       std::vector<std::pair<int64_t, int64_t>>* written)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , written_(written) {}

        [[nodiscard]] operators::pipeline_role role() const noexcept override {
            return operators::pipeline_role::sink;
        }

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

    // The ONE host customization the engine calls at physgen: the injected
    // create_plan RULE (plain fn-ptr, passed to test_spaces). create_plan invokes
    // it for any node it does not lower itself; we handle only node_extension_t and
    // look the host's runtime data up by the node's (db, rel) identity — the node
    // carries no host state. Dispatch by shape: a LEAF is a source (reads a
    // backend), a node WITH a child is a sink (writes a backend).
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
            return {new mock_source_op_t(
                context.resource, context.log.clone(), it->second.spec, it->second.async_delivery, node->table_oid())};
        }
        return {new mock_sink_op_t(context.resource, context.log.clone(), it->second.sink_written)};
    }

    // Every external source is REGISTERED in the engine catalog under
    // db="extreg", rel=<uid> (register_externals), so the extension node's
    // (db, rel) resolves + types + names from the catalog like any table.
    static constexpr const char* kExtDb = "extreg";

    struct external_source_t {
        rows_spec_t spec;
        std::pmr::vector<types::complex_logical_type> schema;
        bool async_delivery{true};
    };
    using externals_by_uid_t = std::unordered_map<std::string, external_source_t>;

    // The NEW-flow splice: replace each uid-qualified external aggregate leaf with
    // a node_extension_t carrying the registered (db, rel). The old flow
    // (test_join_raw) spliced pre-fetched node_raw_data here instead.
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
                    // Host keeps its per-source runtime data keyed by the node's
                    // (db, rel) identity — NOT on the node.
                    mock_ext_store()[ext_key(kExtDb, uid_s)] =
                        mock_ext_data_t{it->second.spec, it->second.async_delivery, nullptr};
                    auto ext =
                        logical_plan::make_node_extension(res, core::dbname_t{kExtDb}, core::relname_t{uid_s});
                    ext->set_result_alias(agg->result_alias().empty()
                                              ? static_cast<const std::string&>(agg->relname())
                                              : agg->result_alias());
                    if (node->children().empty()) {
                        node = ext; // bare scan leaf — the extension IS the plan node
                    } else {
                        // The uid aggregate carries pipeline stages (group / select /
                        // match / sort / limit): keep them — the extension replaces only
                        // the implicit SCAN. Rebuild as an identity aggregate whose data
                        // child is the extension leaf (exactly how a host plan-builder
                        // wraps its source).
                        auto wrapper = logical_plan::make_node_aggregate(res, core::dbname_t{}, core::relname_t{});
                        wrapper->set_result_alias(node->result_alias());
                        wrapper->append_child(ext);
                        for (auto& child : node->children()) {
                            wrapper->append_child(child);
                        }
                        node = wrapper;
                    }
                    return; // leaf is now a host-extension source
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

    // oid is mandatory on extension nodes: register each external table in the
    // engine catalog (create_collection, columns = the source's schema) under
    // db="extreg", rel=<uid> — exactly the host flow. The extension node then
    // resolves / types / names from the catalog by that (db, rel).
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
                columns.emplace_back(t.field_name(), t);
            }
            auto create_node =
                logical_plan::make_node_create_collection(res, core::relname_t{uid}, std::move(columns), {});
            auto wrapped = sql::transform::maybe_wrap_with_catalog_resolve_namespace(res, kExtDb, create_node);
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

    // Run EXPLAIN (plan-only) over a federated plan and return the rendered text.
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

    std::pmr::vector<types::complex_logical_type> pair_schema(std::pmr::memory_resource* res,
                                                              const std::string& col_a,
                                                              const std::string& col_b) {
        std::pmr::vector<types::complex_logical_type> schema(res);
        schema.emplace_back(types::logical_type::BIGINT, col_a);
        schema.emplace_back(types::logical_type::BIGINT, col_b);
        return schema;
    }

    // Walk the executed plan and return the (single) extension leaf, if any.
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
    externals.emplace(uid,
                      external_source_t{rows_spec_t{col_a, col_b, std::move(rows)},
                                        pair_schema(res, col_a, col_b),
                                        async_delivery});
    return externals;
}

#define EXT_TEST_BOILERPLATE(DIR)                                                                                      \
    auto config = test_create_config(DIR);                                                                             \
    test_clear_directory(config);                                                                                      \
    config.disk.on = false;                                                                                            \
    config.wal.on = false;                                                                                             \
    mock_ext_store().clear(); /* fresh host store per test (keyed by db.rel) */                                       \
    test_spaces space(config, &make_mock_extension); /* host injects its create_plan rule at engine start */          \
    auto dispatcher = space.dispatcher();                                                                              \
    auto* res = dispatcher->resource();

TEST_CASE("integration::cpp::extension_source::sync_single_leaf") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_sync/base"))
    auto externals = one_source(res, "uid_x", "key", "val", {{7, 70}}, /*async=*/false);
    auto r = run_with_extension_sources(dispatcher, "SELECT * FROM uid_x.remote.db1.t1;", externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 1);
}

TEST_CASE("integration::cpp::extension_source::async_single_leaf") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_async/base"))
    auto externals = one_source(res, "uid_x", "key", "val", {{1, 10}, {2, 20}, {3, 30}}, /*async=*/true);
    auto r = run_with_extension_sources(dispatcher, "SELECT * FROM uid_x.remote.db1.t1;", externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 3);
}

TEST_CASE("integration::cpp::extension_source::empty_result") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_empty/base"))
    auto externals = one_source(res, "uid_x", "key", "val", {}, /*async=*/true);
    auto r = run_with_extension_sources(dispatcher, "SELECT * FROM uid_x.remote.db1.t1;", externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 0);
}

TEST_CASE("integration::cpp::extension_source::join_two_extensions") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_join2/base"))
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
    REQUIRE(r.cursor->size() == 2); // keys 1 and 3 match
}

TEST_CASE("integration::cpp::extension_source::group_by") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_group/base"))
    auto externals =
        one_source(res, "uid_g", "grp", "val", {{1, 10}, {1, 15}, {2, 20}, {2, 5}, {3, 1}}, /*async=*/true);
    auto r = run_with_extension_sources(dispatcher,
                                        "SELECT grp, SUM(val) AS s FROM uid_g.remote.db1.t1 GROUP BY grp;",
                                        externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 3);
}

TEST_CASE("integration::cpp::extension_source::barrier_where_above_join") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_barrier/base"))
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
    REQUIRE(r.cursor->size() == 2); // values 200, 300

    // Barrier semantics: the extension leaves survived optimize() untouched —
    // identity (db.rel) intact, no predicate/limit injected into the leaf.
    const auto* ext = find_extension(r.plan);
    REQUIRE(ext != nullptr);
    REQUIRE(ext->relname() == "uid_l");
    REQUIRE(ext->expressions().empty());
    REQUIRE(ext->children().empty());
}

TEST_CASE("integration::cpp::extension_source::join_with_local_table") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_local/base"))
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE extdb;");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE extdb.local_t (key BIGINT, amount BIGINT);");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO extdb.local_t (key, amount) VALUES (1, 1000), (2, 2000), (5, 5000);");
        REQUIRE(cur->is_success());
    }
    auto externals = one_source(res, "uid_l", "key", "name", {{1, 11}, {2, 22}, {3, 33}}, /*async=*/true);
    auto r = run_with_extension_sources(dispatcher,
                                        "SELECT e.name, t.amount FROM uid_l.remote.db1.t1 AS e "
                                        "JOIN extdb.local_t AS t ON e.key = t.key;",
                                        externals);
    REQUIRE(r.cursor->is_success());
    REQUIRE(r.cursor->size() == 2); // keys 1 and 2 match
}

// #1 regression: a host-extension node with NO injected create_plan rule (the
// default Null Object lowers it to a null operator) must surface a clean
// "invalid query plan" error — NOT a crash — in EVERY position. The null-child
// guards in create_plan_join / create_plan_aggregate convert the would-be
// null-child deref into a propagated nullptr → create_physical_plan_error.
TEST_CASE("integration::cpp::extension_source::missing_rule_errors_not_crash") {
    auto config = test_create_config(test_temp_path("test_ext_norule/base"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
    // Extension under a JOIN → create_plan_join null-child guard.
    {
        auto externals = one_source(res, "uid_l", "key", "name", {{1, 11}}, /*async=*/false);
        auto r = run_with_extension_sources(dispatcher,
                                            "SELECT e.name, t.amount FROM uid_l.remote.db1.t1 AS e "
                                            "JOIN extdb.local_t AS t ON e.key = t.key;",
                                            externals);
        REQUIRE(r.cursor->is_error());
    }
    // Extension under an AGGREGATE (GROUP BY) → create_plan_aggregate null-child guard.
    {
        auto externals = one_source(res, "uid_g", "key", "val", {{1, 10}}, /*async=*/false);
        auto r = run_with_extension_sources(dispatcher,
                                            "SELECT key, count(val) FROM uid_g.remote.db1.t1 GROUP BY key;",
                                            externals);
        REQUIRE(r.cursor->is_error());
    }
}

// The whole point of the catalog-typed design: EXPLAIN reveals WHICH backend the
// query hits. The extension scan resolves its oid to the registered catalog name,
// so the plan shows "Extension Scan on <rel>" for each federated source.
TEST_CASE("integration::cpp::extension_source::explain_shows_backend") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_explain/base"))
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
    // Both federated sources are visible as extension scans on their registered names.
    REQUIRE(text.find("Extension Scan on uid_l") != std::string::npos);
    REQUIRE(text.find("Extension Scan on uid_r") != std::string::npos);
}

// SINK via extension: a federated write. Build `INSERT INTO <backend> SELECT ...`
// by hand — a node_extension_t whose (db, rel) is the registered backend TARGET
// and whose CHILD is the SELECT feeding the rows. The host factory builds a
// role()==sink operator; physgen wires the child; execute drives child -> sink,
// and the sink "writes" the rows (records them).
TEST_CASE("integration::cpp::extension_source::sink_writes_backend") {
    EXT_TEST_BOILERPLATE(test_temp_path("test_ext_sink/base"))

    // Local source rows for the SELECT that feeds the write.
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
    // Register the federated write TARGET in the catalog (its (db, rel) identity).
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "CREATE TABLE sdb.sink_target (key BIGINT, val BIGINT);")->is_success());
    }

    // Child = the SELECT sub-plan feeding the write.
    std::pmr::monotonic_buffer_resource arena(res);
    sql::transform::transformer transformer(res);
    auto* raw = raw_parser(&arena, "SELECT key, val FROM sdb.local_src;");
    REQUIRE(raw != nullptr);
    auto& ast = sql::transform::pg_cell_to_node_cast(linitial(raw));
    auto binder = transformer.transform(ast);
    REQUIRE_FALSE(binder.has_error());
    auto child = binder.node_ptr();
    REQUIRE(child);

    // Sink extension node: target (db, rel) + a child SELECT. The host records
    // where the sink "writes" in its own store, keyed by the node's (db, rel).
    std::vector<std::pair<int64_t, int64_t>> written;
    mock_ext_store()[ext_key("sdb", "sink_target")] = mock_ext_data_t{rows_spec_t{}, false, &written};
    auto sink = logical_plan::make_node_extension(res, core::dbname_t{"sdb"}, core::relname_t{"sink_target"});
    sink->append_child(child);

    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_plan(
        session,
        logical_plan::execution_plan_t{res, sink, binder.params_ptr()});
    REQUIRE(cur->is_success());

    // The sink received + "wrote" all three source rows to the backend.
    REQUIRE(written.size() == 3);
    std::sort(written.begin(), written.end());
    REQUIRE(written == std::vector<std::pair<int64_t, int64_t>>{{1, 10}, {2, 20}, {3, 30}});
}
