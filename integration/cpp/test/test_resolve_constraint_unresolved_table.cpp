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

// table_md present but table_oid == INVALID_OID means the NAME resolved but not the identity --
// distinct from "table not found" (no table_md, already handled by operator_resolve_table_t).
// Skipping it used to report success while silently dropping all of the table's constraints.
// Proven by injection: reinstating that skip turns the REQUIRE below red.

namespace {

    // tables node holds ONE entry: the table the constraint entry targets.
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

    // A disk actor is wired up only so address_t sees a non-empty address; nothing
    // is ever enqueued on it, since every case below is decided before the first send.
    bool run_resolve(std::pmr::memory_resource* resource, resolve_pair_t& pair, std::string* err_out = nullptr) {
        operators::operator_ptr op(
            new operators::operator_resolve_constraint_t(resource, log_t{}, pair.constraints.get(), pair.tables.get()));

        int disk_actor_stand_in = 0;
        pipeline::context_t ctx(logical_plan::storage_parameters{resource},
                                actor_zeta::address_t{resource, &disk_actor_stand_in},
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox());

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
    // The other half of the old condition: absent table_md means operator_resolve_table_t
    // already refused the statement for "no such relation" -- this layer must not error too.
    auto resource = core::pmr::otterbrix_resource();
    auto pair = make_pair(&resource, /*stamp_table_md=*/false, catalog::INVALID_OID);

    std::string err;
    const bool errored = run_resolve(&resource, pair, &err);
    INFO("error: " << err);
    REQUIRE_FALSE(errored);
}

// entry.target >= entries().size() is a CORRUPT PLAN, not a topology fact: every real entry
// gets a valid index from register_catalog_resolve_table (sql/transformer/utils.cpp), and
// entries() only grows. no_target (size_t(-1)) reaching here means something never filled it in.

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
