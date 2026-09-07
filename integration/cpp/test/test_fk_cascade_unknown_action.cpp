// Root cause: the del_action switch used to fall through `default: break;` before
// mark_executed(), letting an unhandled action skip the cascade yet report success and
// orphan children.
// Unreachable via SQL (both ALTER TABLE routes normalize del_action to one of five values),
// so the test writes confdeltype directly through the production node/build path instead.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace test_helpers;

namespace {

    std::vector<int64_t> column_i64(const components::cursor::cursor_t_ptr& cur, uint64_t col) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t row = 0; row < cur->size(); ++row) {
            out.push_back(cur->value(col, row).value<int64_t>());
        }
        return out;
    }

    // Builds the same ADD CONSTRAINT FOREIGN KEY plan transform_alter_table would, with
    // del_action as the only variable across the cases below.
    components::cursor::cursor_t_ptr add_fk_constraint(otterbrix::wrapper_dispatcher_t* d,
                                                       const std::string& db,
                                                       const std::string& child_rel,
                                                       const std::string& parent_rel,
                                                       const std::string& con_name,
                                                       char del_action) {
        auto* resource = d->resource();
        auto node =
            components::logical_plan::make_node_create_constraint(resource,
                                                                  db,
                                                                  child_rel,
                                                                  core::constraint_name_t{con_name},
                                                                  components::logical_plan::constraint_kind::foreign_key,
                                                                  db);
        node->set_local_col_names({"pid"});
        node->set_ref_relname(parent_rel);
        node->set_ref_col_names({"id"});
        node->set_del_action(del_action);
        components::logical_plan::execution_plan_t plan{resource,
                                                        components::logical_plan::node_ptr{node},
                                                        components::logical_plan::make_parameter_node(resource)};
        std::vector<std::pair<std::string, std::string>> targets;
        targets.emplace_back(db, child_rel);
        targets.emplace_back(db, parent_rel);
        components::sql::transform::register_catalog_resolve_tables(resource, &plan.catalog_resolves, targets);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.parent (id bigint, v bigint);")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.child (cid bigint, pid bigint);")->is_success());
        REQUIRE(exec(d, "INSERT INTO cur.parent (id, v) VALUES (1, 10), (2, 20);")->is_success());
        REQUIRE(exec(d, "INSERT INTO cur.child (cid, pid) VALUES (100, 1);")->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::fk_cascade_unknown_action::restrict_blocks_the_parent_delete") {
    auto config = make_test_config(integration_fixture_path("test_fk_cascade_unknown_action/restrict"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);
    auto ddl = add_fk_constraint(d, "cur", "child", "parent", "fk_child_parent", 'r');
    INFO("ADD CONSTRAINT FOREIGN KEY ON DELETE RESTRICT: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));
    REQUIRE(ddl->is_success());

    auto del = exec(d, "DELETE FROM cur.parent WHERE id = 1;");
    INFO("DELETE of a referenced parent row: " << (del->is_error() ? del->get_error().what : "accepted"));
    REQUIRE(del->is_error());

    auto parents = exec(d, "SELECT id FROM cur.parent ORDER BY id;");
    REQUIRE(parents->is_success());
    REQUIRE(column_i64(parents, 0) == std::vector<int64_t>{1, 2});
    auto children = exec(d, "SELECT cid FROM cur.child ORDER BY cid;");
    REQUIRE(children->is_success());
    REQUIRE(column_i64(children, 0) == std::vector<int64_t>{100});
}

// Distinguishes a cascade that ran from one that silently did nothing.
TEST_CASE("integration::cpp::fk_cascade_unknown_action::cascade_removes_the_child_row") {
    auto config = make_test_config(integration_fixture_path("test_fk_cascade_unknown_action/cascade"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);
    auto ddl = add_fk_constraint(d, "cur", "child", "parent", "fk_child_parent", 'c');
    INFO("ADD CONSTRAINT FOREIGN KEY ON DELETE CASCADE: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));
    REQUIRE(ddl->is_success());

    auto del = exec(d, "DELETE FROM cur.parent WHERE id = 1;");
    INFO("DELETE of a referenced parent row: " << (del->is_error() ? del->get_error().what : "accepted"));
    REQUIRE(del->is_success());

    auto parents = exec(d, "SELECT id FROM cur.parent ORDER BY id;");
    REQUIRE(parents->is_success());
    REQUIRE(column_i64(parents, 0) == std::vector<int64_t>{2});
    auto children = exec(d, "SELECT cid FROM cur.child ORDER BY cid;");
    REQUIRE(children->is_success());
    INFO("the cascade ran: the child row that referenced the deleted parent is gone");
    REQUIRE(column_i64(children, 0).empty());
}

TEST_CASE("integration::cpp::fk_cascade_unknown_action::an_unknown_action_does_not_orphan_the_child") {
    auto config = make_test_config(integration_fixture_path("test_fk_cascade_unknown_action/unknown"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);
    auto ddl = add_fk_constraint(d, "cur", "child", "parent", "fk_child_parent", 'z');
    INFO("ADD CONSTRAINT FOREIGN KEY ON DELETE <unknown>: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));

    auto del = exec(d, "DELETE FROM cur.parent WHERE id = 1;");
    INFO("DELETE of a referenced parent row: " << (del->is_error() ? del->get_error().what : "accepted"));

    auto parents = exec(d, "SELECT id FROM cur.parent ORDER BY id;");
    REQUIRE(parents->is_success());
    const auto parent_ids = column_i64(parents, 0);
    auto children = exec(d, "SELECT pid FROM cur.child ORDER BY pid;");
    REQUIRE(children->is_success());
    const auto child_refs = column_i64(children, 0);

    const bool parent_gone =
        std::find(parent_ids.begin(), parent_ids.end(), int64_t{1}) == parent_ids.end();
    const bool child_still_points_at_it =
        std::find(child_refs.begin(), child_refs.end(), int64_t{1}) != child_refs.end();

    INFO("parent rows left: " << parent_ids.size() << ", child rows still referencing id = 1: "
                              << (child_still_points_at_it ? 1 : 0));
    INFO("a cascade that cannot be evaluated must refuse, not report that nothing referenced the parent");
    const bool orphaned = del->is_success() && parent_gone && child_still_points_at_it;
    REQUIRE_FALSE(orphaned);
}
