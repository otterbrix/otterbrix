#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <services/collection/context_storage.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <memory_resource>

// A sequence child's unlowerable operator must null the WHOLE sequence: an unchecked null FIRST child
// is silently dropped (truncated statement reports success), a null LATER child is dereferenced (crash).

namespace {

    namespace lp = components::logical_plan;

    lp::node_ptr make_lowerable_alter_add(std::pmr::memory_resource* res, const char* col_name) {
        auto add = lp::make_node_alter_column(res, lp::alter_column_op::add);
        add->set_table_oid(components::catalog::oid_t{16400});
        add->set_column(components::table::column_definition_t(
            col_name,
            components::types::complex_logical_type(components::types::logical_type::BIGINT)));
        return add;
    }

    lp::node_ptr make_unlowerable_leaf(std::pmr::memory_resource* res) {
        return lp::make_node_drop(res, lp::drop_target_kind::collection);
    }

} // namespace

TEST_CASE("physical_plan_generator::sequence::an_unlowerable_first_child_refuses_the_sequence") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, components::catalog::session_catalog_t{});
    components::compute::function_registry_t registry(res);

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_unlowerable_leaf(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);

    INFO("the first child failed to lower: the whole sequence must refuse (null root), "
         "not silently run without its first step");
    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::sequence::an_unlowerable_later_child_refuses_instead_of_dereferencing_null") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, components::catalog::session_catalog_t{});
    components::compute::function_registry_t registry(res);

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    seq->append_child(make_unlowerable_leaf(res));

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);

    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::sequence::all_lowerable_children_still_chain") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, components::catalog::session_catalog_t{});
    components::compute::function_registry_t registry(res);

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    seq->append_child(make_lowerable_alter_add(res, "c2"));

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);

    REQUIRE(plan != nullptr);
    REQUIRE(plan->left() != nullptr);
    REQUIRE(plan->left()->left() == nullptr);
}

TEST_CASE("physical_plan_generator::sequence::the_first_written_clause_executes_first_in_the_alter_chain") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, components::catalog::session_catalog_t{});
    components::compute::function_registry_t registry(res);

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    auto rename = lp::make_node_alter_column(res, lp::alter_column_op::rename);
    rename->set_table_oid(components::catalog::oid_t{16400});
    rename->set_old_name(core::columnname_t{std::string{"a"}});
    rename->set_new_name(core::columnname_t{std::string{"b"}});
    seq->append_child(rename);

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);
    REQUIRE(plan != nullptr);

    // The executor runs bottom-up (deepest-left first); children[0] must sit deepest, or attnum order runs backwards.
    REQUIRE(plan->type() == components::operators::operator_type::alter_column_rename);
    REQUIRE(plan->left() != nullptr);
    REQUIRE(plan->left()->type() == components::operators::operator_type::alter_column_add);
}
