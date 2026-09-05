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

// A SEQUENCE CHILD THAT FAILS TO LOWER REFUSES THE WHOLE STATEMENT.
//
// create_plan's contract for "cannot lower this node" is a null operator_ptr: the executor maps a null
// physical root to create_physical_plan_error ("invalid query plan"). Consuming each child's operator
// UNCHECKED in create_plan_sequence's generic child loop breaks that contract twice:
//
//   * a FIRST child that lowered to null is silently dropped from the chain — the statement then executes
//     with one of its steps missing and reports success on the remainder;
//   * a LATER null child is dereferenced (`op->left()` / `op->set_children`) — a crash, not a refusal.
//
// These cases pin the propagation: one unlowerable child nulls the whole sequence, so the executor refuses
// the statement instead of running a truncated one. A bare drop_t is the unlowerable probe — it has no case
// in create_plan's dispatch (DROP lowers only through its own rewritten shapes).

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
        // node_type::drop_t has no arm in create_plan's dispatch: it reaches the
        // default and lowers to a null operator.
        return lp::make_node_drop(res, lp::drop_target_kind::collection);
    }

} // namespace

TEST_CASE("physical_plan_generator::sequence::an_unlowerable_first_child_refuses_the_sequence") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
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
    services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(res);

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    seq->append_child(make_unlowerable_leaf(res));

    // Without the guard this dereferences the null second operator (op->left()).
    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);

    REQUIRE(plan == nullptr);
}

TEST_CASE("physical_plan_generator::sequence::all_lowerable_children_still_chain") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(res);

    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    seq->append_child(make_lowerable_alter_add(res, "c2"));

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);

    // Two ADD COLUMN clauses chain: one root with one left child, no orphans.
    REQUIRE(plan != nullptr);
    REQUIRE(plan->left() != nullptr);
    REQUIRE(plan->left()->left() == nullptr);
}

TEST_CASE("physical_plan_generator::sequence::the_first_written_clause_executes_first_in_the_alter_chain") {
    std::pmr::monotonic_buffer_resource arena;
    auto* res = &arena;
    services::context_storage_t context(res, log_t{}, core::date::timezone_offset_t{});
    components::compute::function_registry_t registry(res);

    // ALTER TABLE t ADD COLUMN c1 ..., RENAME COLUMN a TO b — two chainable
    // clauses of DISTINCT operator types, so the chain's shape is observable.
    auto seq = boost::intrusive_ptr(new lp::node_sequence_t(res));
    seq->append_child(make_lowerable_alter_add(res, "c1"));
    auto rename = lp::make_node_alter_column(res, lp::alter_column_op::rename);
    rename->set_table_oid(components::catalog::oid_t{16400});
    rename->set_old_name(core::columnname_t{std::string{"a"}});
    rename->set_new_name(core::columnname_t{std::string{"b"}});
    seq->append_child(rename);

    auto plan = services::planner::create_plan(context, registry, seq, lp::limit_t::unlimit(), nullptr);
    REQUIRE(plan != nullptr);

    // The executor drives the chain bottom-up: the DEEPEST-left operator runs first. Clause order is
    // user-visible (two ADD COLUMNs decide attnum order), so children[0] — the first user-written clause —
    // must sit at the deepest level, exactly as the generic sequence loop already arranges it. A reversed
    // walk puts children[0] at the ROOT instead, running the clauses back to front.
    REQUIRE(plan->type() == components::operators::operator_type::alter_column_rename);
    REQUIRE(plan->left() != nullptr);
    REQUIRE(plan->left()->type() == components::operators::operator_type::alter_column_add);
}
