#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_resolve_constraint.hpp>

#include <memory_resource>
#include <string>

using namespace components;

// ===========================================================================
// AN UNRESOLVED TABLE OID DOES NOT REPEAL THE TABLE'S CONSTRAINTS.
//
// operator_resolve_constraint_t opened its per-entry loop with
//
//     if (!target_md.has_value() || target_md->table_oid == INVALID_OID) {
//         continue;
//     }
//
// and the two halves of that condition are not the same fact.
//
//   * NO table_md is "the table was not found". operator_resolve_table_t stamps
//     the field only when pg_class answered, and documents the empty optional as
//     exactly that signal; a gather for a table that is not there has nothing to
//     gather, and the missing table is reported by the layer that looked for it.
//
//   * A table_md that IS there with table_oid == INVALID_OID is a table whose
//     NAME resolved and whose identity did not. Skipping it leaves the entry's
//     fks / check_exprs / unique_constraints / pk_columns EMPTY, and empty is
//     indistinguishable from "this table declares no constraints": enrich stamps
//     nothing on the DML node, the planner splices no constraint operator, and
//     every declared key, foreign key and CHECK on that table stops existing
//     while the statement reports success. That is WIDER than the layer-2 skip in
//     operator_unique_constraint_t, which the same predicate used to switch off
//     and which is now a refusal there — this one repeals ALL of the table's
//     constraints, not one check inside one of them.
//
// PATH NOT NAMED FROM SQL, deliberately: table_md is stamped only from a pg_class
// row whose oid column was read non-null, so a zero there is a catalog that says
// a relation exists with no identity. Sensitivity proven by injection — restore
// `|| target_md->table_oid == INVALID_OID` to the skip and the REQUIRE below goes
// red, the operator reporting success over a constraint set it never read.
// ===========================================================================

namespace {

    // The constraint-resolve pair: a tables node carrying ONE entry (the table
    // the constraint entry targets) and a constraint node targeting it.
    struct resolve_pair_t {
        logical_plan::node_catalog_resolve_ptr tables;
        logical_plan::node_catalog_resolve_ptr constraints;
    };

    resolve_pair_t make_pair(std::pmr::memory_resource* resource,
                             bool stamp_table_md,
                             catalog::oid_t table_oid,
                             std::size_t override_target = 0,
                             bool use_override = false) {
        resolve_pair_t pair{
            logical_plan::make_node_catalog_resolve(resource, logical_plan::resolve_kind::table),
            logical_plan::make_node_catalog_resolve(resource, logical_plan::resolve_kind::constraint)};

        logical_plan::resolve_entry_t table_entry;
        table_entry.dbname = "db";
        table_entry.relname = "t";
        if (stamp_table_md) {
            logical_plan::resolved_table_metadata_t md;
            md.table_oid = table_oid;
            md.name = "t";
            table_entry.table_md = std::move(md);
        }
        const auto target = pair.tables->add(std::move(table_entry));

        logical_plan::resolve_entry_t constraint_entry;
        constraint_entry.target = use_override ? override_target : target;
        constraint_entry.direction = logical_plan::resolve_direction::outgoing;
        pair.constraints->add(std::move(constraint_entry));
        return pair;
    }

    // Drive the operator to completion over `pair`. A disk actor IS wired up
    // (address_t compares the pointee, so any non-null pointer is "not the empty
    // address"); nothing is ever enqueued on it, because every case below is
    // decided before the first send.
    bool run_resolve(std::pmr::memory_resource* resource, resolve_pair_t& pair, std::string* err_out = nullptr) {
        operators::operator_ptr op(
            new operators::operator_resolve_constraint_t(resource, log_t{}, pair.constraints.get(), pair.tables.get()));

        pipeline::context_t ctx(logical_plan::storage_parameters{resource});
        int disk_actor_stand_in = 0;
        ctx.disk_address = actor_zeta::address_t{resource, &disk_actor_stand_in};

        auto fut = op->await_async_and_resume(&ctx);
        REQUIRE(fut.is_ready());
        std::move(fut).take_ready();

        if (err_out && op->has_error()) {
            *err_out = std::string(op->get_error().what);
        }
        return op->has_error();
    }

} // namespace

TEST_CASE("resolve constraint: a table whose oid did not resolve is refused, not skipped", "[resolve_constraint]") {
    auto resource = core::pmr::otterbrix_resource();
    auto pair = make_pair(&resource, /*stamp_table_md=*/true, catalog::INVALID_OID);

    std::string err;
    INFO("a constraint set that was never read must not be handed on as an EMPTY constraint set");
    REQUIRE(run_resolve(&resource, pair, &err));
    INFO("error: " << err);
    // The message has to say WHICH fact is missing, not merely that something is.
    REQUIRE(err.find("table") != std::string::npos);
}

TEST_CASE("resolve constraint: a table that was not found is not an error", "[resolve_constraint]") {
    // The other half of the old condition, kept as it was and pinned here so the
    // refusal above cannot spread onto it: an absent table_md is how
    // operator_resolve_table_t says "no such relation", and the statement that
    // named it is refused by the layer that looked for it, not by this one.
    auto resource = core::pmr::otterbrix_resource();
    auto pair = make_pair(&resource, /*stamp_table_md=*/false, catalog::INVALID_OID);

    std::string err;
    const bool errored = run_resolve(&resource, pair, &err);
    INFO("error: " << err);
    REQUIRE_FALSE(errored);
}

// ===========================================================================
// AND A TARGET THAT IS NOT AN INDEX AT ALL IS THE SAME DEFECT ONE STEP EARLIER.
//
// `entry.target >= tables_node_->entries().size()` shared a `continue` with the two
// TOPOLOGY facts on the same line — an empty disk address and a null tables node — and it
// is not one of them: those are shapes of the world this operator runs in, while an index
// outside the tables node is a CORRUPT PLAN.
//
// resolve_entry_t::no_target (size_t(-1)) is the DEFAULT, so an entry nobody filled in
// lands here. It is never a legitimate marker on a constraint entry:
//
//   * every constraint entry in this engine is created in one place,
//     register_catalog_resolve_table (components/sql/transformer/utils.cpp), which sets
//     target from the value node_catalog_resolve_t::add just returned for the TABLE entry
//     — by construction < entries().size();
//   * entries() only ever grows (add() push_backs; no erase / clear / resize exists
//     anywhere), so a valid index cannot go stale;
//   * merge_catalog_resolves copies constraint entries without rebasing target, but no
//     view body can carry one: constraint entries come only from INSERT / UPDATE / DELETE
//     / CREATE TABLE / ALTER TABLE, never from a SELECT;
//   * resolve_entry_t is never serialized, so nothing reconstructs a target.
//
// The consequence of the skip is the WIDEST in this operator: fks, check_exprs,
// unique_constraints and pk_columns all left empty at once, which is exactly what "this
// table declares no constraints" looks like.
// ===========================================================================

TEST_CASE("resolve constraint: an entry whose target is not an index is refused", "[resolve_constraint]") {
    auto resource = core::pmr::otterbrix_resource();

    INFO("the default target — an entry that never named its table");
    {
        auto pair = make_pair(&resource,
                              /*stamp_table_md=*/true,
                              /*table_oid=*/42,
                              logical_plan::resolve_entry_t::no_target,
                              /*use_override=*/true);
        std::string err;
        REQUIRE(run_resolve(&resource, pair, &err));
        INFO("error: " << err);
        REQUIRE(err.find("table") != std::string::npos);
    }

    INFO("and an index past the end of the tables node — one entry, target 1");
    {
        auto pair = make_pair(&resource, /*stamp_table_md=*/true, /*table_oid=*/42, 1, /*use_override=*/true);
        std::string err;
        REQUIRE(run_resolve(&resource, pair, &err));
        INFO("error: " << err);
        REQUIRE(err.find("table") != std::string::npos);
    }
}
