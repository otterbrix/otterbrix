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

// A named table that never resolved carries INVALID_OID, same as a no-FROM SELECT; create_plan_match
// refuses the named case with a null root, which these tests' CALLERS must propagate, not swallow.

namespace {

    namespace lp = components::logical_plan;
    namespace ops = components::operators;
    namespace expr = components::expressions;

    constexpr components::catalog::oid_t known_oid{16400};

    lp::node_aggregate_ptr make_named_aggregate(std::pmr::memory_resource* res) {
        return lp::make_node_aggregate(res, core::dbname_t{std::string{"edb"}}, core::relname_t{std::string{"ghost"}});
    }

    // The sentinel scan never awaits, so this reads synchronously; SIZE_MAX means the root isn't that scan.
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

    // An empty relname is the no-FROM shape (`SELECT 1`); pinned so the refusal tests below can't over-reach here.
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
        CHECK(fabricated_rows(plan, &h.pipeline_ctx) == 0);
    }
    INFO("a table that never resolved must refuse with a null root, not scan a synthetic row");
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::unresolved_source::aggregate_with_an_unlowerable_match_child_refuses") {
    harness_t h;

    // The match child refuses (named, no predicate); the aggregate must propagate that null, not the sentinel scan.
    auto agg = make_named_aggregate(&h.arena);
    agg->append_child(lp::make_node_match(&h.arena,
                                          core::dbname_t{std::string{"edb"}},
                                          core::relname_t{std::string{"ghost"}},
                                          nullptr));
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

    auto match = lp::make_node_match(&h.arena,
                                     core::dbname_t{std::string{"edb"}},
                                     core::relname_t{std::string{"ghost"}},
                                     expr::make_compare_expression(&h.arena, expr::compare_type::all_true));
    auto limit = lp::make_node_limit(&h.arena,
                                     core::dbname_t{std::string{}},
                                     core::relname_t{std::string{}},
                                     lp::limit_t::unlimit());
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

    // A refused match child, swallowed, would leave a childless DML sink that runs as a no-op reporting SUCCESS.
    auto match = lp::make_node_match(&h.arena,
                                     core::dbname_t{std::string{"edb"}},
                                     core::relname_t{std::string{"ghost"}},
                                     nullptr);
    auto limit = lp::make_node_limit(&h.arena,
                                     core::dbname_t{std::string{}},
                                     core::relname_t{std::string{}},
                                     lp::limit_t::unlimit());
    auto del = lp::make_node_delete(&h.arena, match, limit);
    del->set_dbname(std::string{"edb"});
    del->set_relname(std::string{"ghost"});

    auto plan = services::planner::create_plan(h.context, h.registry, del, lp::limit_t::unlimit(), nullptr);

    if (plan) {
        CHECK(plan->left() != nullptr);
    }
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::unresolved_source::delete_with_an_unlowerable_using_source_refuses") {
    harness_t h;
    h.context.known_oids.insert(known_oid);

    // node_type::drop_t has no arm in create_plan's dispatch; swallowed, it'd stand as the semi-join's missing side.
    auto match = lp::make_node_match(&h.arena,
                                     core::dbname_t{std::string{"edb"}},
                                     core::relname_t{std::string{"t"}},
                                     expr::make_compare_expression(&h.arena, expr::compare_type::all_true));
    auto limit = lp::make_node_limit(&h.arena,
                                     core::dbname_t{std::string{}},
                                     core::relname_t{std::string{}},
                                     lp::limit_t::unlimit());
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
    auto limit = lp::make_node_limit(&h.arena,
                                     core::dbname_t{std::string{}},
                                     core::relname_t{std::string{}},
                                     lp::limit_t::unlimit());
    std::pmr::vector<expr::expression_ptr> updates{&h.arena};
    auto upd = lp::make_node_update(&h.arena, match, limit, updates);

    auto plan = services::planner::create_plan(h.context, h.registry, upd, lp::limit_t::unlimit(), nullptr);

    INFO("an UPDATE whose target never resolved must refuse with a null root, same as DELETE");
    REQUIRE(plan == nullptr);
}
