// View expansion, checked on the SHAPE of the tree rather than on a row count.
//
// Two of the failure modes here are invisible from the outside:
//
//  * appending the body instead of inserting it at position 0 answers correctly
//    and silently switches filter pushdown into the body off, because
//    pushdown_cte_filter reads the source as children()[0] and scans clauses from
//    index 1 (optimizer/rules/pushdown_filter.cpp);
//  * leaving the reference node's name / oid in place makes bind_catalog_data
//    re-stamp the view's oid on the next bind, after which create_plan_match
//    hands back a bare full_scan over a view that has no storage — zero rows, no
//    error.
//
// Neither shows up in "how many rows came back" on the fixture that exercises it,
// so they are pinned here, where the pass is a plain function over a tree.

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
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
        auto agg = logical_plan::make_node_aggregate(res(), core::dbname_t{dbname}, core::relname_t{relname});
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
        auto match = logical_plan::make_node_match(res(), core::dbname_t{"db"}, core::relname_t{"v"}, nullptr);
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
    auto existing_clause = logical_plan::make_node_match(res(), core::dbname_t{}, core::relname_t{}, nullptr);
    ref->append_child(existing_clause);

    auto body = logical_plan::make_node_aggregate(res(), core::dbname_t{"db"}, core::relname_t{"t"});
    auto err = splice_view_body(ref.get(), body);
    REQUIRE_FALSE(err.contains_error());

    INFO("the body is children()[0] — the slot pushdown_cte_filter reads as the source");
    REQUIRE(ref->children().size() == 2);
    CHECK(ref->children()[0].get() == body.get());
    CHECK(ref->children()[1].get() == existing_clause.get());

    INFO("the body answers to the name the outer query addresses it by");
    CHECK(body->result_alias() == "v");

    INFO("the reference stopped being a source: no name, no oid, no metadata");
    CHECK(ref->dbname().t.empty());
    CHECK(ref->relname().t.empty());
    CHECK(ref->table_oid() == components::catalog::INVALID_OID);
    CHECK(ref->table_metadata() == nullptr);

    INFO("and so it is no longer found as a reference — this is what terminates the loop");
    CHECK(collect_view_references(res(), resolves, ref.get()).empty());
}

TEST_CASE("planner::view_expansion::an aliased reference keeps the alias") {
    auto ref = make_view_ref("db", "v");
    ref->set_result_alias("x");
    auto body = logical_plan::make_node_aggregate(res(), core::dbname_t{"db"}, core::relname_t{"t"});
    REQUIRE_FALSE(splice_view_body(ref.get(), body).contains_error());
    CHECK(body->result_alias() == "x");
}

TEST_CASE("planner::view_expansion::a correlated join in the body is refused") {
    // node_join_t::correlations() is const-only, so those parameter ids cannot be
    // renumbered against the outer plan's — a silent collision. Refuse instead.
    auto ref = make_view_ref("db", "v");
    auto body = logical_plan::make_node_aggregate(res(), core::dbname_t{"db"}, core::relname_t{"t"});
    auto join = logical_plan::make_node_join(res(),
                                             core::dbname_t{"db"},
                                             core::relname_t{"t"},
                                             logical_plan::join_type::inner);
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

    auto body = logical_plan::make_node_aggregate(res(), core::dbname_t{"db"}, core::relname_t{"t"});
    auto predicate = expressions::make_compare_expression(res(),
                                                          expressions::compare_type::gt,
                                                          expressions::param_storage{expressions::key_t{res(), "col_b"}},
                                                          expressions::param_storage{body_id});
    body->append_expression(predicate);

    renumber_body_parameters(res(), body.get(), body_params, outer_params);

    INFO("the body's operand now points at a fresh id");
    const auto& moved = static_cast<const expressions::compare_expression_t*>(body->expressions().front().get())->right();
    REQUIRE(expressions::is_parameter(moved));
    const auto new_id = expressions::as_parameter(moved);
    CHECK(new_id != outer_id);

    INFO("and that id holds the body's own constant, not the outer query's");
    CHECK(outer_params->parameter(new_id).value<int64_t>() == 10);
    CHECK(outer_params->parameter(outer_id).value<int64_t>() == 18);
}
