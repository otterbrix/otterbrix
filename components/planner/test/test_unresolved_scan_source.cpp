#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_union.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/context_storage.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <memory_resource>
#include <string>

// A named table that never resolved arrives at plan generation carrying INVALID_OID --
// the exact value a no-FROM SELECT carries. create_plan_match already tells the two
// apart through the node's own source declaration and refuses the named case with a
// null plan; these tests pin the CALLERS of that refusal, where the null used to be
// swallowed instead of propagated:
//   - create_plan_aggregate folded a missing scan child into the no-table sentinel
//     transfer_scan, which FABRICATES one synthetic row -- a SELECT over a table that
//     does not exist would answer a row instead of an error;
//   - create_plan_delete / create_plan_update pushed the null through set_children;
//     the streaming executor admits a childless DML sink as a sourceless sink, so the
//     statement commits nothing and reports SUCCESS;
//   - create_plan_union pushed a refused arm through set_children the same way.
// The refusal contract is a null plan ROOT (the executor maps it to
// create_physical_plan_error); a swallowed null child surfaces as nothing at all.

namespace {

    namespace lp = components::logical_plan;
    namespace ops = components::operators;
    namespace expr = components::expressions;

    constexpr components::catalog::oid_t known_oid{16400};

    lp::node_aggregate_ptr make_named_aggregate(std::pmr::memory_resource* res) {
        // The table is NAMED but no resolved oid ever arrived (table_oid() stays
        // INVALID_OID): the shape a lost upstream refusal hands to the planner.
        return lp::make_node_aggregate(res, core::dbname_t{std::string{"edb"}}, core::relname_t{std::string{"ghost"}});
    }

    // Count the rows the plan's source answers without any pipeline: the no-table
    // sentinel branch of transfer_scan::source_next never awaits, so its future is
    // ready synchronously. Returns SIZE_MAX when the root is not a synchronous source.
    size_t fabricated_rows(const ops::operator_ptr& plan, components::pipeline::context_t* ctx) {
        if (plan->type() != ops::operator_type::transfer_scan) {
            return SIZE_MAX;
        }
        auto fut = plan->source_next(ctx);
        if (!fut.is_ready()) {
            return SIZE_MAX;
        }
        auto result = std::move(fut).take_ready();
        if (result.has_error()) {
            return SIZE_MAX;
        }
        return result.value().size();
    }

    struct harness_t {
        std::pmr::monotonic_buffer_resource arena;
        services::context_storage_t context;
        components::compute::function_registry_t registry;
        lp::storage_parameters pipeline_params;
        components::pipeline::context_t pipeline_ctx;

        harness_t()
            : context(&arena, log_t{}, core::date::timezone_offset_t{})
            , registry(&arena)
            , pipeline_params(&arena)
            , pipeline_ctx(pipeline_params,
                           actor_zeta::address_t::empty_address(),
                           actor_zeta::address_t::empty_address(),
                           actor_zeta::address_t::empty_address()) {}
    };

} // namespace

TEST_CASE("physical_plan_generator::unresolved_source::no_from_select_keeps_the_synthetic_row") {
    harness_t h;

    // No FROM at all: an empty relname declares the synthetic one-row source, which
    // is the correct plan for `SELECT 1`-style statements. Pinned so the named-table
    // refusal below cannot over-reach into this shape.
    auto agg = lp::make_node_aggregate(&h.arena, core::dbname_t{std::string{}}, core::relname_t{std::string{}});
    auto plan = services::planner::create_plan(h.context, h.registry, agg, lp::limit_t::unlimit(), nullptr);

    REQUIRE(plan);
    CHECK(fabricated_rows(plan, &h.pipeline_ctx) == 1);
}

TEST_CASE("physical_plan_generator::unresolved_source::aggregate_over_a_resolved_table_still_lowers") {
    harness_t h;
    h.context.known_oids.insert(known_oid);

    auto agg = make_named_aggregate(&h.arena);
    agg->set_table_oid(known_oid);
    auto plan = services::planner::create_plan(h.context, h.registry, agg, lp::limit_t::unlimit(), nullptr);

    INFO("a resolved, known table keeps its scan; the refusal below is only for the unresolved case");
    REQUIRE(plan);
    CHECK(plan->type() == ops::operator_type::transfer_scan);
}

TEST_CASE("physical_plan_generator::unresolved_source::aggregate_over_an_unresolved_table_refuses") {
    harness_t h;

    auto agg = make_named_aggregate(&h.arena);
    auto plan = services::planner::create_plan(h.context, h.registry, agg, lp::limit_t::unlimit(), nullptr);

    if (plan) {
        // The defective lowering: the named-but-unresolved table fell into the
        // no-FROM sentinel scan, which answers a fabricated row for a table that
        // does not exist. Show the row count the caller would receive.
        CHECK(fabricated_rows(plan, &h.pipeline_ctx) == 0);
    }
    INFO("a table that never resolved must refuse with a null root, not scan a synthetic row");
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::unresolved_source::aggregate_with_an_unlowerable_match_child_refuses") {
    harness_t h;

    // The match child NAMES the table and create_plan_match refuses it (null); the
    // aggregate used to swallow that null and degrade into the sentinel scan.
    auto agg = make_named_aggregate(&h.arena);
    agg->append_child(
        lp::make_node_match(&h.arena, core::dbname_t{std::string{"edb"}}, core::relname_t{std::string{"ghost"}}, nullptr));
    auto plan = services::planner::create_plan(h.context, h.registry, agg, lp::limit_t::unlimit(), nullptr);

    if (plan) {
        CHECK(fabricated_rows(plan, &h.pipeline_ctx) == 0);
    }
    INFO("a refused scan child must refuse the aggregate, not degrade into an unfiltered scan");
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::unresolved_source::union_arm_refusal_reaches_the_root") {
    harness_t h;
    h.context.known_oids.insert(known_oid);

    auto good_arm = make_named_aggregate(&h.arena);
    good_arm->set_table_oid(known_oid);
    auto ghost_arm = make_named_aggregate(&h.arena); // stays unresolved
    auto union_node = lp::make_node_union(&h.arena, good_arm, ghost_arm, true);

    auto plan = services::planner::create_plan(h.context, h.registry, union_node, lp::limit_t::unlimit(), nullptr);

    INFO("one refused arm must refuse the whole union: a null arm swallowed by set_children "
         "would execute as a half-union answering partial data");
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::unresolved_source::delete_over_an_unresolved_table_refuses") {
    harness_t h;

    // The exact shape `DELETE FROM edb.ghost` hands over when the upstream refusal is
    // lost: a named target with INVALID_OID and an all_true predicate.
    auto match = lp::make_node_match(&h.arena,
                                     core::dbname_t{std::string{"edb"}},
                                     core::relname_t{std::string{"ghost"}},
                                     expr::make_compare_expression(&h.arena, expr::compare_type::all_true));
    auto limit =
        lp::make_node_limit(&h.arena, core::dbname_t{std::string{}}, core::relname_t{std::string{}}, lp::limit_t::unlimit());
    auto del = lp::make_node_delete(&h.arena, match, limit);
    del->set_dbname(std::string{"edb"});
    del->set_relname(std::string{"ghost"});

    auto plan = services::planner::create_plan(h.context, h.registry, del, lp::limit_t::unlimit(), nullptr);

    INFO("a DELETE whose target never resolved must refuse with a null root; lowering it "
         "produces a sink with no table behind it, which executes as a no-op reporting success");
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::unresolved_source::delete_with_an_unlowerable_match_child_refuses") {
    harness_t h;

    // A match child create_plan_match refuses outright (named, no predicate, no
    // resolved table): the null child used to go straight through set_children,
    // leaving a childless DML sink -- the streaming executor admits that shape as a
    // sourceless sink, so the DELETE commits nothing and reports SUCCESS.
    auto match = lp::make_node_match(&h.arena,
                                     core::dbname_t{std::string{"edb"}},
                                     core::relname_t{std::string{"ghost"}},
                                     nullptr);
    auto limit =
        lp::make_node_limit(&h.arena, core::dbname_t{std::string{}}, core::relname_t{std::string{}}, lp::limit_t::unlimit());
    auto del = lp::make_node_delete(&h.arena, match, limit);
    del->set_dbname(std::string{"edb"});
    del->set_relname(std::string{"ghost"});

    auto plan = services::planner::create_plan(h.context, h.registry, del, lp::limit_t::unlimit(), nullptr);

    if (plan) {
        CHECK(plan->left() != nullptr); // the silent no-op shape: a DML sink with no scan child
    }
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::unresolved_source::delete_with_an_unlowerable_using_source_refuses") {
    harness_t h;
    h.context.known_oids.insert(known_oid);

    // The target IS resolved; the USING source is the child that fails to lower
    // (node_type::drop_t has no arm in create_plan's dispatch). The null source used
    // to go through set_children as the semi-join's missing right side.
    auto match = lp::make_node_match(&h.arena,
                                     core::dbname_t{std::string{"edb"}},
                                     core::relname_t{std::string{"t"}},
                                     expr::make_compare_expression(&h.arena, expr::compare_type::all_true));
    auto limit =
        lp::make_node_limit(&h.arena, core::dbname_t{std::string{}}, core::relname_t{std::string{}}, lp::limit_t::unlimit());
    auto del = lp::make_node_delete(&h.arena, match, limit);
    del->set_table_oid(known_oid);
    del->append_child(lp::make_node_drop(&h.arena, lp::drop_target_kind::collection));

    auto plan = services::planner::create_plan(h.context, h.registry, del, lp::limit_t::unlimit(), nullptr);

    INFO("a USING source that failed to lower must refuse the DELETE, not join against a missing side");
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::unresolved_source::update_over_an_unresolved_table_refuses") {
    harness_t h;

    auto match = lp::make_node_match(&h.arena,
                                     core::dbname_t{std::string{"edb"}},
                                     core::relname_t{std::string{"ghost"}},
                                     expr::make_compare_expression(&h.arena, expr::compare_type::all_true));
    auto limit =
        lp::make_node_limit(&h.arena, core::dbname_t{std::string{}}, core::relname_t{std::string{}}, lp::limit_t::unlimit());
    std::pmr::vector<expr::expression_ptr> updates{&h.arena};
    auto upd = lp::make_node_update(&h.arena, match, limit, updates);

    auto plan = services::planner::create_plan(h.context, h.registry, upd, lp::limit_t::unlimit(), nullptr);

    INFO("an UPDATE whose target never resolved must refuse with a null root, same as DELETE");
    REQUIRE(plan == nullptr);
}
