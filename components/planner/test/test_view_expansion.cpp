// View expansion, checked on tree SHAPE rather than row count — two failure modes here are invisible from
// a row count:
//  * appending the body instead of inserting it at position 0 still answers correctly but silently
//    disables filter pushdown into the body (pushdown_cte_filter reads children()[0] as the source, see
//    components/planner/optimizer/rules/pushdown_filter.cpp);
//  * leaving the reference's name/oid in place lets the next bind re-stamp the view's oid, after which
//    create_plan_match hands back a bare full_scan over a view with no storage — zero rows, no error.

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/view_expansion.hpp>

#include <core/pmr.hpp>

using namespace components;
using namespace components::planner;

namespace {

    std::pmr::memory_resource* res() {
        static core::pmr::otterbrix_resource resource;
        return &resource;
    }

    // One resolved table entry describing a view named `relname` in `dbname`.
    logical_plan::catalog_resolves_t make_view_resolves(const std::string& dbname,
                                                        const std::string& relname,
                                                        char relkind,
                                                        const std::string& view_sql) {
        logical_plan::catalog_resolves_t resolves;
        logical_plan::resolve_entry_t entry;
        entry.dbname = dbname;
        entry.relname = relname;
        logical_plan::resolved_table_metadata_t md;
        md.table_oid = 4242;
        md.namespace_oid = 7;
        md.relkind = relkind;
        md.name = relname;
        md.view_sql = view_sql;
        entry.table_md = std::move(md);
        resolves.ensure(res(), logical_plan::resolve_kind::table).add(std::move(entry));
        return resolves;
    }

    logical_plan::node_aggregate_ptr make_view_ref(const std::string& dbname, const std::string& relname) {
        auto agg =
            logical_plan::make_node_aggregate(res(),
                                              qualified_name_t{core::dbname_t{dbname}, core::relname_t{relname}});
        agg->set_table_oid(4242);
        return agg;
    }

} // namespace

TEST_CASE("planner::view_expansion::collects only aggregate references to a plain view") {
    auto resolves = make_view_resolves("db", "v", components::catalog::relkind::view, "SELECT a FROM db.t");

    SECTION("an aggregate naming the view is a reference") {
        auto ref = make_view_ref("db", "v");
        auto refs = collect_view_references(res(), resolves, ref.get());
        REQUIRE(refs.size() == 1);
        CHECK(refs.front().node == ref.get());
    }

    SECTION("a match node carrying the same name is NOT a splice site") {
        // A clause node knows the relation it filters; hanging the body under it
        // would put the body below the filter instead of below the consumer.
        auto match =
            logical_plan::make_node_match(res(), qualified_name_t{core::dbname_t{"db"}, core::relname_t{"v"}}, nullptr);
        auto refs = collect_view_references(res(), resolves, match.get());
        CHECK(refs.empty());
    }

    SECTION("a materialized view is not expanded — it is a real heap") {
        auto mv_resolves =
            make_view_resolves("db", "v", components::catalog::relkind::materialized_view, "SELECT a FROM db.t");
        auto ref = make_view_ref("db", "v");
        auto refs = collect_view_references(res(), mv_resolves, ref.get());
        CHECK(refs.empty());
    }

    SECTION("a relkind='v' entry with an empty body is not expandable") {
        auto empty_resolves = make_view_resolves("db", "v", components::catalog::relkind::view, "");
        auto ref = make_view_ref("db", "v");
        auto refs = collect_view_references(res(), empty_resolves, ref.get());
        CHECK(refs.empty());
    }
}

TEST_CASE("planner::view_expansion::splice puts the body in the source slot and clears the identity") {
    auto resolves = make_view_resolves("db", "v", components::catalog::relkind::view, "SELECT a FROM db.t");
    auto ref = make_view_ref("db", "v");
    // A clause already hanging on the reference — the body must land BEFORE it.
    auto existing_clause = logical_plan::make_node_match(res(), qualified_name_t{}, nullptr);
    ref->append_child(existing_clause);

    auto body = logical_plan::make_node_aggregate(res(), qualified_name_t{core::dbname_t{"db"}, core::relname_t{"t"}});
    auto err = splice_view_body(ref.get(), body);
    REQUIRE_FALSE(err.contains_error());

    INFO("the body is children()[0] — the slot pushdown_cte_filter reads as the source");
    REQUIRE(ref->children().size() == 2);
    CHECK(ref->children()[0].get() == body.get());
    CHECK(ref->children()[1].get() == existing_clause.get());

    INFO("the body answers to the name the outer query addresses it by");
    CHECK(body->result_alias() == "v");

    INFO("the reference stopped being a source: no name, no oid, no metadata");
    CHECK(ref->target().database.t.empty());
    CHECK(ref->target().collection.t.empty());
    CHECK(ref->table_oid() == components::catalog::INVALID_OID);
    CHECK(ref->table_metadata() == nullptr);

    INFO("and so it is no longer found as a reference — this is what terminates the loop");
    CHECK(collect_view_references(res(), resolves, ref.get()).empty());
}

TEST_CASE("planner::view_expansion::an aliased reference keeps the alias") {
    auto ref = make_view_ref("db", "v");
    ref->set_result_alias("x");
    auto body = logical_plan::make_node_aggregate(res(), qualified_name_t{core::dbname_t{"db"}, core::relname_t{"t"}});
    REQUIRE_FALSE(splice_view_body(ref.get(), body).contains_error());
    CHECK(body->result_alias() == "x");
}

TEST_CASE("planner::view_expansion::a correlated join in the body is refused") {
    // node_join_t::correlations() is const-only, so those parameter ids cannot be
    // renumbered against the outer plan's — a silent collision. Refuse instead.
    auto ref = make_view_ref("db", "v");
    auto body = logical_plan::make_node_aggregate(res(), qualified_name_t{core::dbname_t{"db"}, core::relname_t{"t"}});
    auto join = logical_plan::make_node_join(res(), logical_plan::join_type::inner);
    join->set_lateral(true);
    join->add_correlation(core::parameter_id_t{0}, expressions::key_t{res(), "col_a"});
    body->append_child(join);

    auto err = splice_view_body(ref.get(), body);
    CHECK(err.contains_error());
    CHECK(ref->children().empty());
}

TEST_CASE("planner::view_expansion::body parameters are renumbered into the outer plan") {
    // Both plans number from zero. Without renumbering the outer #0 and the body
    // #0 are the same slot and the outer constant wins.
    auto outer_params = logical_plan::make_parameter_node(res());
    const auto outer_id = outer_params->add_parameter(types::logical_value_t{res(), int64_t{18}});
    CHECK(outer_id == core::parameter_id_t{0});

    auto body_params = logical_plan::make_parameter_node(res());
    const auto body_id = body_params->add_parameter(types::logical_value_t{res(), int64_t{10}});
    CHECK(body_id == core::parameter_id_t{0});

    auto body = logical_plan::make_node_aggregate(res(), qualified_name_t{core::dbname_t{"db"}, core::relname_t{"t"}});
    auto predicate =
        expressions::make_compare_expression(res(),
                                             expressions::compare_type::gt,
                                             expressions::param_storage{expressions::key_t{res(), "col_b"}},
                                             expressions::param_storage{body_id});
    body->append_expression(predicate);

    renumber_body_parameters(res(), body.get(), body_params, outer_params);

    INFO("the body's operand now points at a fresh id");
    const auto& moved =
        static_cast<const expressions::compare_expression_t*>(body->expressions().front().get())->right();
    REQUIRE(expressions::is_parameter(moved));
    const auto new_id = expressions::as_parameter(moved);
    CHECK(new_id != outer_id);

    INFO("and that id holds the body's own constant, not the outer query's");
    CHECK(outer_params->parameter(new_id).value<int64_t>() == 10);
    CHECK(outer_params->parameter(outer_id).value<int64_t>() == 18);
}

namespace {
    logical_plan::resolved_table_metadata_t view_bound_to_t() {
        logical_plan::resolved_table_metadata_t view;
        view.name = "v";
        view.view_bindings.push_back({components::catalog::view_refkind::relation,
                                      core::dbname_t{"db"},
                                      core::schema_t{},
                                      core::relname_t{"t"},
                                      16500});
        return view;
    }

    logical_plan::catalog_resolves_t body_naming(const std::string& relname) {
        logical_plan::catalog_resolves_t body;
        logical_plan::resolve_entry_t entry;
        entry.dbname = "db";
        entry.relname = relname;
        body.ensure(res(), logical_plan::resolve_kind::table).add(std::move(entry));
        return body;
    }
} // namespace

TEST_CASE("planner::view_expansion::a body name is pinned to the oid it was bound to") {
    auto body = body_naming("t");

    REQUIRE_FALSE(pin_view_body_names(res(), body, view_bound_to_t()).contains_error());
    CHECK(body.tables->entries().front().pin.oid == 16500);
}

TEST_CASE("planner::view_expansion::a body name without a binding is refused as stale") {
    auto body = body_naming("other");

    auto err = pin_view_body_names(res(), body, view_bound_to_t());
    REQUIRE(err.contains_error());
    CHECK(std::string(err.what).find("view \"v\" is stale") != std::string::npos);
}

// The statement resolved the same spelling to another relation: the view's own relation is never swapped for it.
TEST_CASE("planner::view_expansion::a pin the statement disagrees with is refused as stale") {
    auto statement = body_naming("t");
    logical_plan::resolved_table_metadata_t other;
    other.table_oid = 16600;
    statement.tables->entries().front().table_md = std::move(other);
    auto body = body_naming("t");
    REQUIRE_FALSE(pin_view_body_names(res(), body, view_bound_to_t()).contains_error());

    auto err = merge_view_body_resolves(res(), statement, body);
    REQUIRE(err.contains_error());
    CHECK(std::string(err.what).find("view \"v\" is stale") != std::string::npos);
}

// REFRESH MATERIALIZED VIEW is an INSERT into the matview over its stored body (PostgreSQL 18 matview.c runs the
// stored query; Trino 483 analyzes an INSERT into the storage table with the parsed body as its source), not a read
// of the matview that expands into the body.
TEST_CASE("planner::view_expansion::refresh is an insert into the matview over its pinned body") {
    logical_plan::resolved_table_metadata_t matview;
    matview.name = "mv";
    matview.table_oid = 4243;
    matview.relkind = components::catalog::relkind::materialized_view;
    matview.view_sql = "SELECT a FROM db.t";
    matview.view_bindings.push_back({components::catalog::view_refkind::relation,
                                     core::dbname_t{"db"},
                                     core::schema_t{},
                                     core::relname_t{"t"},
                                     16500});

    auto refresh = refresh_matview_plan(res(), matview, core::dbname_t{"db"});
    REQUIRE_FALSE(refresh.has_error());
    auto& plan = refresh.value().plan;

    const auto* root = plan.sub_queries.back().get();
    REQUIRE(root->type() == logical_plan::node_type::insert_t);
    const auto* insert = static_cast<const logical_plan::node_insert_t*>(root);
    CHECK(insert->target().database.t == "db");
    CHECK(insert->target().collection.t == "mv");
    REQUIRE(insert->children().size() == 1);

    INFO("the source is the reference the body is spliced into, returned for the read's staleness check");
    CHECK(refresh.value().reference.get() == insert->children().front().get());
    const auto* body = insert->children().front()->children().front().get();
    REQUIRE(body->type() == logical_plan::node_type::aggregate_t);
    CHECK(static_cast<const logical_plan::node_aggregate_t*>(body)->target().collection.t == "t");

    INFO("the body's name is pinned; mv is only the write target, never read");
    const auto* t = plan.catalog_resolves.table_entry(
        qualified_name_t{core::dbname_t{std::string{"db"}}, core::relname_t{std::string{"t"}}});
    REQUIRE(t != nullptr);
    CHECK(t->pin.oid == 16500);
    CHECK(plan.catalog_resolves.table_entry(
              qualified_name_t{core::dbname_t{std::string{"db"}}, core::relname_t{std::string{"mv"}}}) != nullptr);
    CHECK(collect_view_references(res(), plan.catalog_resolves, plan.sub_queries.back().get()).empty());
}
