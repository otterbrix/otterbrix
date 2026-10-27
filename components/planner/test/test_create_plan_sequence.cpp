#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_sequence.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <services/collection/context_storage.hpp>
#include <string>

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

    // The table the ALTERs name, as the plan resolved it: the planner takes its storage from the metadata.
    const lp::resolved_table_metadata_t* altered_table() {
        static lp::resolved_table_metadata_t table = [] {
            lp::resolved_table_metadata_t resolved;
            resolved.table_oid = components::catalog::oid_t{16400};
            return resolved;
        }();
        return &table;
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
    context.table_metadata[components::catalog::oid_t{16400}] = altered_table();

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_unlowerable_leaf(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::all(), nullptr);

    INFO("the first child failed to lower: the whole sequence must refuse, not silently run without its first "
         "step");
    REQUIRE(plan.has_error());
    CHECK(std::string{plan.error().what}.starts_with("no physical plan for a logical node of this kind: "));
}

TEST_CASE("physical_plan_generator::sequence::an_unlowerable_later_child_refuses_instead_of_dereferencing_null") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, components::catalog::session_catalog_t{});
    components::compute::function_registry_t registry(res);
    context.table_metadata[components::catalog::oid_t{16400}] = altered_table();

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    seq->append_child(make_unlowerable_leaf(res));

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::all(), nullptr);

    REQUIRE(plan.has_error());
}

TEST_CASE("physical_plan_generator::sequence::the_children_are_steps_not_a_chain") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, components::catalog::session_catalog_t{});
    components::compute::function_registry_t registry(res);
    context.table_metadata[components::catalog::oid_t{16400}] = altered_table();

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    seq->append_child(make_lowerable_alter_add(res, "c2"));

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::all(), nullptr);

    REQUIRE_FALSE(plan.has_error());
    const auto& root = plan.value();
    REQUIRE(root->type() == components::operators::operator_type::sequence);
    const auto& steps = static_cast<const components::operators::operator_sequence_t&>(*root).steps();
    REQUIRE(steps.size() == 2);
    for (const auto& step : steps) {
        CHECK(step->left() == nullptr);
        CHECK(step->right() == nullptr);
    }
}

TEST_CASE("physical_plan_generator::sequence::ten_thousand_steps_stay_flat") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, components::catalog::session_catalog_t{});
    components::compute::function_registry_t registry(res);
    context.table_metadata[components::catalog::oid_t{16400}] = altered_table();

    constexpr std::size_t count = 10000;
    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    for (std::size_t i = 0; i < count; ++i) {
        seq->append_child(make_lowerable_alter_add(res, "c"));
    }

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::all(), nullptr);
    REQUIRE_FALSE(plan.has_error());
    std::size_t depth = 0;
    for (auto* op = plan.value().get(); op != nullptr; op = op->left().get()) {
        ++depth;
    }
    CHECK(depth == 1);
    REQUIRE(plan.value()->type() == components::operators::operator_type::sequence);
    CHECK(static_cast<const components::operators::operator_sequence_t&>(*plan.value()).steps().size() == count);
}

TEST_CASE("physical_plan_generator::sequence::the_first_written_clause_is_the_first_step") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, components::catalog::session_catalog_t{});
    components::compute::function_registry_t registry(res);
    context.table_metadata[components::catalog::oid_t{16400}] = altered_table();

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    auto rename = lp::make_node_alter_column(res, lp::alter_column_op::rename);
    rename->set_table_oid(components::catalog::oid_t{16400});
    rename->set_old_name(core::columnname_t{std::string{"a"}});
    rename->set_new_name(core::columnname_t{std::string{"b"}});
    seq->append_child(rename);

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::all(), nullptr);
    REQUIRE_FALSE(plan.has_error());
    const auto& root = plan.value();

    // The executor runs the steps in order; children[0] must run first, or attnum order runs backwards.
    REQUIRE(root->type() == components::operators::operator_type::sequence);
    const auto& steps = static_cast<const components::operators::operator_sequence_t&>(*root).steps();
    REQUIRE(steps.size() == 2);
    CHECK(steps[0]->type() == components::operators::operator_type::alter_column_add);
    CHECK(steps[1]->type() == components::operators::operator_type::alter_column_rename);
}
