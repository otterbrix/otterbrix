// A test host that knows remote tables only through its own tables in the engine: the name resolution hook reads
// otterstax.remote_columns for every name the catalog did not resolve and puts a host node with the declared
// columns in its place; the host operator serves canned backend rows.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/context_storage.hpp>

#include <algorithm>
#include <atomic>
#include <map>
#include <string>
#include <vector>

using namespace components;

namespace {

    // The "remote" data behind each name; one int64 per declared column (a TEXT column shows it as "s<n>").
    std::map<std::string, std::vector<std::vector<int64_t>>>& backend() {
        static std::map<std::string, std::vector<std::vector<int64_t>>> rows;
        return rows;
    }

    struct counters_t {
        std::atomic<int> need{0};
        std::atomic<int> reads{0};
        std::atomic<int> decide{0};
        void reset() {
            need = 0;
            reads = 0;
            decide = 0;
        }
    };
    counters_t& counters() {
        static counters_t c;
        return c;
    }

    std::string qualified(std::string_view db, std::string_view schema, std::string_view rel) {
        std::string out{db};
        if (!schema.empty()) {
            out += '.';
            out += schema;
        }
        out += '.';
        out += rel;
        return out;
    }

    struct remote_payload_t final : logical_plan::extension_payload_t {
        explicit remote_payload_t(std::string name)
            : name(std::move(name)) {}
        std::string name;
    };

    class remote_source_t final : public operators::read_only_operator_t {
    public:
        remote_source_t(std::pmr::memory_resource* resource,
                        log_t log,
                        std::pmr::vector<types::complex_logical_type> columns,
                        std::vector<std::vector<int64_t>> rows)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , columns_(std::move(columns))
            , rows_(std::move(rows)) {}

        [[nodiscard]] operators::pipeline_role role() const noexcept override {
            return operators::pipeline_role::source;
        }

        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t*) override {
            actor_zeta::promise<core::result_wrapper_t<vector::data_chunk_t>> promise(resource());
            auto future = promise.get_future();
            if (drained_) {
                vector::data_chunk_t sentinel(resource(), std::pmr::vector<types::complex_logical_type>{resource()}, 0);
                promise.set_value(core::result_wrapper_t<vector::data_chunk_t>{std::move(sentinel)});
                return future;
            }
            drained_ = true;
            vector::data_chunk_t chunk(resource(), columns_, std::max<std::size_t>(rows_.size(), 1));
            chunk.set_cardinality(rows_.size());
            for (std::size_t row = 0; row < rows_.size(); ++row) {
                for (std::size_t col = 0; col < columns_.size(); ++col) {
                    if (columns_[col].type() == types::logical_type::STRING_LITERAL) {
                        chunk.set_value(col,
                                        row,
                                        types::logical_value_t(resource(), "s" + std::to_string(rows_[row][col])));
                    } else {
                        chunk.set_value(col, row, types::logical_value_t(resource(), rows_[row][col]));
                    }
                }
            }
            promise.set_value(core::result_wrapper_t<vector::data_chunk_t>{std::move(chunk)});
            return future;
        }

        void reset_pipeline_state() noexcept override { drained_ = false; }

    private:
        std::pmr::vector<types::complex_logical_type> columns_;
        std::vector<std::vector<int64_t>> rows_;
        bool drained_{false};
    };

    // Names whose backend cannot be reached: the host's operator function refuses them with its own reason.
    std::map<std::string, std::string>& unreachable() {
        static std::map<std::string, std::string> reasons;
        return reasons;
    }

    services::planner::plan_result_t make_remote_source(const services::context_storage_t& context,
                                                        const compute::function_registry_t&,
                                                        const logical_plan::node_extension_t& node) {
        const auto* payload = static_cast<const remote_payload_t*>(node.payload());
        if (auto it = unreachable().find(payload->name); it != unreachable().end()) {
            return core::error_t{core::error_code_t::connection_closed,
                                 std::pmr::string{it->second.c_str(), context.resource}};
        }
        std::pmr::vector<types::complex_logical_type> columns(node.columns(), context.resource);
        return {
            new remote_source_t(context.resource, context.log.clone(), std::move(columns), backend()[payload->name])};
    }

    // Phase "need": one read of otterstax.remote_columns per unresolved name.
    core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>>
    need_remote_columns(std::pmr::memory_resource* resource,
                        const logical_plan::node_ptr&,
                        std::span<const planner::unresolved_table_t> unresolved) {
        counters().need.fetch_add(1);
        std::pmr::vector<logical_plan::execution_plan_t> reads{resource};
        for (const auto& name : unresolved) {
            auto agg = logical_plan::make_node_aggregate(resource,
                                                         core::dbname_t{"otterstax"},
                                                         core::relname_t{"remote_columns"});
            auto expr =
                expressions::make_compare_expression(resource,
                                                     expressions::compare_type::eq,
                                                     expressions::key_t{resource, "tbl", expressions::side_t::left},
                                                     core::parameter_id_t{1});
            agg->append_child(logical_plan::make_node_match(resource,
                                                            core::dbname_t{"otterstax"},
                                                            core::relname_t{"remote_columns"},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(resource);
            params->add_parameter(core::parameter_id_t{1},
                                  types::logical_value_t(resource, qualified(name.dbname, name.schema, name.relname)));
            reads.emplace_back(resource, std::move(agg), std::move(params));
        }
        counters().reads.fetch_add(static_cast<int>(reads.size()));
        return reads;
    }

    struct declared_t {
        std::string name;
        std::pmr::vector<types::complex_logical_type> columns;
    };

    void replace_names(logical_plan::node_ptr& node,
                       std::pmr::memory_resource* resource,
                       const std::vector<declared_t>& declared) {
        if (node->type() == logical_plan::node_type::aggregate_t) {
            const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(node.get());
            const auto name = qualified(static_cast<const std::string&>(agg->dbname()),
                                        agg->schema(),
                                        static_cast<const std::string&>(agg->relname()));
            auto it =
                std::find_if(declared.begin(), declared.end(), [&](const declared_t& d) { return d.name == name; });
            if (it != declared.end()) {
                auto ext =
                    logical_plan::make_node_extension(resource,
                                                      it->name,
                                                      it->columns,
                                                      &make_remote_source,
                                                      logical_plan::extension_payload_ptr{new remote_payload_t{name}});
                REQUIRE_FALSE(ext.has_error());
                ext.value()->set_result_alias(agg->result_alias().empty()
                                                  ? static_cast<const std::string&>(agg->relname())
                                                  : agg->result_alias());
                if (node->children().empty()) {
                    node = ext.value();
                    return;
                }
                auto wrapper = logical_plan::make_node_aggregate(resource, core::dbname_t{}, core::relname_t{});
                wrapper->set_result_alias(node->result_alias());
                wrapper->append_child(ext.value());
                for (auto& child : node->children()) {
                    wrapper->append_child(child);
                }
                node = wrapper;
                return;
            }
        }
        for (auto& child : node->children()) {
            replace_names(child, resource, declared);
        }
    }

    // Phase "decide": a name with declared columns becomes a host node; one without stays and is refused later.
    core::result_wrapper_t<logical_plan::node_ptr>
    decide_remote_nodes(std::pmr::memory_resource* resource,
                        logical_plan::node_ptr tree,
                        std::span<const planner::unresolved_table_t> unresolved,
                        std::span<const std::pmr::vector<vector::data_chunk_t>> read_results) {
        counters().decide.fetch_add(1);
        std::vector<declared_t> declared;
        for (std::size_t i = 0; i < unresolved.size(); ++i) {
            std::vector<std::pair<int64_t, types::complex_logical_type>> ordered;
            for (const auto& chunk : read_results[i]) {
                // otterstax.remote_columns (tbl TEXT, col TEXT, type TEXT, ord BIGINT)
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    const auto col_cell = chunk.value(1, row);
                    const auto type_cell = chunk.value(2, row);
                    const auto ord_cell = chunk.value(3, row);
                    const std::string col{col_cell.value<std::string_view>()};
                    const std::string type{type_cell.value<std::string_view>()};
                    ordered.emplace_back(ord_cell.value<int64_t>(),
                                         types::complex_logical_type{type == "TEXT"
                                                                         ? types::logical_type::STRING_LITERAL
                                                                         : types::logical_type::BIGINT,
                                                                     col});
                }
            }
            if (ordered.empty()) {
                continue;
            }
            std::sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
            declared_t d{qualified(unresolved[i].dbname, unresolved[i].schema, unresolved[i].relname),
                         std::pmr::vector<types::complex_logical_type>{resource}};
            for (auto& [_, type] : ordered) {
                d.columns.push_back(std::move(type));
            }
            declared.push_back(std::move(d));
        }
        replace_names(tree, resource, declared);
        return tree;
    }

    services::engine::primitives_t host_primitives() {
        return services::engine::primitives_t{{}, {&need_remote_columns, &decide_remote_nodes}};
    }

    components::cursor::cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    void create_host_tables(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(run(dispatcher, "CREATE DATABASE otterstax;")->is_success());
        REQUIRE(run(dispatcher, "CREATE TABLE otterstax.remote_columns (tbl TEXT, col TEXT, type TEXT, ord BIGINT);")
                    ->is_success());
    }

    const char* declare_orders = "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                                 "('m2.shop.orders', 'id', 'BIGINT', 1), ('m2.shop.orders', 'amount', 'BIGINT', 2);";

    std::vector<std::vector<int64_t>> sorted_int_rows(const components::cursor::cursor_t_ptr& cursor) {
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

} // namespace

#define HOST_TEST_BOILERPLATE(DIR)                                                                                     \
    auto config = test_create_config(integration_fixture_path(DIR));                                                   \
    test_clear_directory(config);                                                                                      \
    backend().clear();                                                                                                 \
    backend()["m2.shop.orders"] = {{1, 100}, {2, 200}, {3, 300}};                                                      \
    unreachable().clear();                                                                                             \
    counters().reset();                                                                                                \
    test_spaces space(config, host_primitives());                                                                      \
    auto* dispatcher = space.dispatcher();                                                                             \
    create_host_tables(dispatcher);

TEST_CASE("integration::cpp::host_names::host_table_row_decides_the_name") {
    HOST_TEST_BOILERPLATE("test_host_names/row_decides")

    auto missing = run(dispatcher, "SELECT * FROM m2.shop.orders;");
    REQUIRE(missing->is_error());
    CHECK(std::string{missing->get_error().what}.find("does not exist") != std::string::npos);

    REQUIRE(run(dispatcher, declare_orders)->is_success());
    auto found = run(dispatcher, "SELECT id, amount FROM m2.shop.orders;");
    REQUIRE(found->is_success());
    REQUIRE(sorted_int_rows(found) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});

    REQUIRE(run(dispatcher, "DELETE FROM otterstax.remote_columns WHERE tbl = 'm2.shop.orders';")->is_success());
    auto gone = run(dispatcher, "SELECT * FROM m2.shop.orders;");
    REQUIRE(gone->is_error());
    CHECK(std::string{gone->get_error().what}.find("does not exist") != std::string::npos);
}

TEST_CASE("integration::cpp::host_names::reads_run_in_the_statement_snapshot") {
    HOST_TEST_BOILERPLATE("test_host_names/snapshot")

    auto writer = otterbrix::session_id_t();
    auto other = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, writer, declare_orders)->is_success());

    auto own = run(dispatcher, writer, "SELECT id, amount FROM m2.shop.orders;");
    REQUIRE(own->is_success());
    REQUIRE(own->size() == 3);

    REQUIRE(run(dispatcher, other, "SELECT * FROM m2.shop.orders;")->is_error());

    REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
    auto after = run(dispatcher, other, "SELECT id, amount FROM m2.shop.orders;");
    REQUIRE(after->is_success());
    REQUIRE(after->size() == 3);
}

TEST_CASE("integration::cpp::host_names::join_host_node_with_local_table") {
    HOST_TEST_BOILERPLATE("test_host_names/join_local")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE shopdb;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE shopdb.customers (id BIGINT, bonus BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO shopdb.customers (id, bonus) VALUES (1, 7), (3, 9), (5, 11);")->is_success());

    auto joined = run(dispatcher,
                      "SELECT o.id, o.amount, c.bonus FROM m2.shop.orders AS o "
                      "JOIN shopdb.customers AS c ON o.id = c.id;");
    REQUIRE(joined->is_success());
    REQUIRE(sorted_int_rows(joined) == std::vector<std::vector<int64_t>>{{1, 100, 7}, {3, 300, 9}});
}

TEST_CASE("integration::cpp::host_names::declared_columns_drive_validation_and_types") {
    HOST_TEST_BOILERPLATE("test_host_names/declared")
    REQUIRE(run(dispatcher,
                "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                "('m2.shop.orders', 'id', 'BIGINT', 1), ('m2.shop.orders', 'label', 'TEXT', 2);")
                ->is_success());

    REQUIRE(run(dispatcher, "SELECT nope FROM m2.shop.orders;")->is_error());

    auto labels = run(dispatcher, "SELECT label FROM m2.shop.orders WHERE id = 2;");
    REQUIRE(labels->is_success());
    REQUIRE(labels->size() == 1);
    REQUIRE(labels->chunks().front().data[0].type().type() == types::logical_type::STRING_LITERAL);
    REQUIRE(labels->value(0, 0).value<std::string_view>() == "s200");

    auto sums = run(dispatcher, "SELECT id + 1 AS n FROM m2.shop.orders WHERE id = 3;");
    REQUIRE(sums->is_success());
    REQUIRE(sums->value(0, 0).value<int64_t>() == 4);
}

TEST_CASE("integration::cpp::host_names::count_star_counts_the_host_rows") {
    HOST_TEST_BOILERPLATE("test_host_names/count_star")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.orders;");
    REQUIRE(counted->is_success());
    REQUIRE(counted->size() == 1);
    REQUIRE(counted->value(0, 0).value<int64_t>() == 3);
}

TEST_CASE("integration::cpp::host_names::local_statements_never_reach_the_host") {
    HOST_TEST_BOILERPLATE("test_host_names/local_only")
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.a (k BIGINT, v BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.b (k BIGINT, w BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.a (k, v) VALUES (1, 10), (2, 20);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.b (k, w) VALUES (1, 5);")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.av AS SELECT k, v FROM loc.a;")->is_success());
    const char* statements[] = {
        "SELECT * FROM loc.a;",
        "SELECT count(*) FROM loc.a;",
        "SELECT a.v, b.w FROM loc.a AS a JOIN loc.b AS b ON a.k = b.k;",
        "SELECT a.v FROM loc.a AS a, loc.b AS b WHERE a.k = b.k;",
        "WITH x AS (SELECT k FROM loc.a) SELECT * FROM x;",
        "SELECT v FROM loc.a WHERE k IN (SELECT k FROM loc.b);",
        "SELECT * FROM loc.av;",
        "UPDATE loc.a SET v = 11 WHERE k = 1;",
        "DELETE FROM loc.b WHERE k = 9;",
        "INSERT INTO loc.b (k, w) SELECT k, v FROM loc.a;",
    };
    for (const auto* sql : statements) {
        INFO(sql);
        REQUIRE(run(dispatcher, sql)->is_success());
    }
    CHECK(counters().need.load() == 0);
    CHECK(counters().reads.load() == 0);
    CHECK(counters().decide.load() == 0);

    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "SELECT * FROM m2.shop.orders;")->is_success());
    CHECK(counters().need.load() == 1);
    CHECK(counters().reads.load() == 1);
    CHECK(counters().decide.load() == 1);
}

TEST_CASE("integration::cpp::host_names::dml_with_an_embedded_query") {
    HOST_TEST_BOILERPLATE("test_host_names/dml_embedded")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());

    SECTION("INSERT ... SELECT copies the host rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) SELECT id, amount FROM m2.shop.orders;")->is_success());
        auto copied = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(copied->is_success());
        REQUIRE(sorted_int_rows(copied) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});
    }
    SECTION("UPDATE ... FROM reads the host rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (1, 0), (5, 0);")->is_success());
        auto upd = run(dispatcher, "UPDATE loc.t SET amount = o.amount FROM m2.shop.orders AS o WHERE loc.t.id = o.id;");
        INFO((upd->is_error() ? std::string{upd->get_error().what} : std::string{"ok"}));
        REQUIRE(upd->is_success());
        auto updated = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(sorted_int_rows(updated) == std::vector<std::vector<int64_t>>{{1, 100}, {5, 0}});
    }
    SECTION("DELETE ... USING reads the host rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (2, 0), (7, 0);")->is_success());
        auto del = run(dispatcher, "DELETE FROM loc.t USING m2.shop.orders AS o WHERE loc.t.id = o.id;");
        INFO((del->is_error() ? std::string{del->get_error().what} : std::string{"ok"}));
        REQUIRE(del->is_success());
        auto left = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(sorted_int_rows(left) == std::vector<std::vector<int64_t>>{{7, 0}});
    }
}

TEST_CASE("integration::cpp::host_names::host_operator_error_reaches_the_cursor") {
    HOST_TEST_BOILERPLATE("test_host_names/operator_error")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    unreachable()["m2.shop.orders"] = "server m2: connection refused";
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.c (id BIGINT);")->is_success());

    for (const char* sql : {"SELECT * FROM m2.shop.orders;",
                            "SELECT count(*) FROM m2.shop.orders;",
                            "SELECT o.id FROM m2.shop.orders AS o JOIN loc.c AS c ON o.id = c.id;",
                            "INSERT INTO loc.c (id) SELECT id FROM m2.shop.orders;"}) {
        INFO(sql);
        auto cursor = run(dispatcher, sql);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::connection_closed);
        CHECK(std::string{cursor->get_error().what} == "server m2: connection refused");
    }
}

// A view body is a query too: at CREATE VIEW the host resolves the names the catalog does not, and the view
// depends on nothing it resolved (a host node has no catalog oid).
TEST_CASE("integration::cpp::host_names::a_view_over_a_host_name") {
    HOST_TEST_BOILERPLATE("test_host_names/view_created")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());

    auto created = run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;");
    INFO("error: " << (created->is_error() ? std::string{created->get_error().what} : std::string{}));
    REQUIRE(created->is_success());

    auto read = run(dispatcher, "SELECT id, amount FROM loc.ov;");
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});

    auto oid = run(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'ov';");
    REQUIRE(oid->size() == 1);
    const auto view_oid = std::to_string(oid->chunks().front().get_value<std::uint32_t>(0, 0));
    auto depends = run(dispatcher, "SELECT refclassid FROM pg_catalog.pg_depend WHERE objid = " + view_oid + ";");
    REQUIRE(depends->size() == 1);
    CHECK(depends->chunks().front().get_value<std::uint32_t>(0, 0) ==
          components::catalog::well_known_oid::pg_namespace_table);
}

// Trino 483 checkViewStaleness: the host node the view was created over declares other columns now.
TEST_CASE("integration::cpp::host_names::a_view_whose_host_node_changed_is_stale") {
    HOST_TEST_BOILERPLATE("test_host_names/view_node_changed")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;")->is_success());
    REQUIRE(run(dispatcher,
                "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                "('m2.shop.orders', 'label', 'TEXT', 3);")
                ->is_success());

    auto stale = run(dispatcher, "SELECT id FROM loc.ov;");
    REQUIRE(stale->is_error());
    CHECK(std::string{stale->get_error().what}.find("view \"ov\" is stale") != std::string::npos);
}

TEST_CASE("integration::cpp::host_names::a_view_whose_host_name_is_gone_is_stale") {
    HOST_TEST_BOILERPLATE("test_host_names/view_name_gone")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;")->is_success());
    REQUIRE(run(dispatcher, "DELETE FROM otterstax.remote_columns WHERE tbl = 'm2.shop.orders';")->is_success());

    auto stale = run(dispatcher, "SELECT id FROM loc.ov;");
    REQUIRE(stale->is_error());
    CHECK(std::string{stale->get_error().what}.find("view \"ov\" is stale") != std::string::npos);
}
