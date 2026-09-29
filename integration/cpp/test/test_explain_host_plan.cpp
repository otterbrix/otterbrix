#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <core/pmr.hpp>

#include <string>

// A host that transforms a statement itself and hands the engine the plan.

namespace {
    using test_helpers::exec;
    namespace sql = components::sql;

    // A parsed statement becomes a plan only through finalize(), which carries the EXPLAIN mode, the
    // catalog lookups and the sub-queries. A bare root + parameters would drop them all (EXPLAIN DELETE
    // then deleted), so transform_result must not hand those out.
    template<typename T>
    concept exposes_bare_root = requires(const T& result) { result.node_ptr(); } ||
                                requires(const T& result) { result.params_ptr(); };
    static_assert(!exposes_bare_root<sql::transform::transform_result>);

    template<typename D>
    bool okq(D* d, const std::string& query) {
        auto c = exec(d, query);
        return c && c->is_success();
    }

    template<typename D>
    uint64_t count_rows(D* d) {
        auto c = exec(d, "SELECT count(*) FROM m.t;");
        REQUIRE(c);
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        return c->value(0, 0).template value<uint64_t>();
    }
} // namespace

TEST_CASE("integration::cpp::explain_host_plan::explain_delete_never_deletes") {
    auto config = test_helpers::make_test_config(integration_fixture_path("explain_host_plan/delete"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id) VALUES (1),(2),(3);"));
    REQUIRE(count_rows(d) == 3);

    auto* resource = d->resource();
    std::pmr::monotonic_buffer_resource arena(resource);
    const std::string query = "EXPLAIN DELETE FROM m.t WHERE id > 0;";
    auto* raw = raw_parser(&arena, query.c_str());
    REQUIRE(raw != nullptr);
    sql::transform::transformer transformer(resource, query.c_str());
    auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(linitial(raw)));
    REQUIRE_FALSE(result.has_error());

    auto plan = result.finalize();
    REQUIRE_FALSE(plan.has_error());
    CHECK(plan.value().explain == components::logical_plan::explain_type::plan);
    auto cursor = d->execute_plan(otterbrix::session_id_t(), std::move(plan.value()));
    REQUIRE(cursor);
    REQUIRE(cursor->is_success());

    CHECK(count_rows(d) == 3);
}
