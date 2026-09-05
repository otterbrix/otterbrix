#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/oid_batch.hpp>
#include <core/result_wrapper.hpp>

#ifdef DEV_MODE
#include <services/collection/executor.hpp>
#endif

#include <cstddef>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    // THE REFUSAL AN OPERATOR THAT MINTS EXACTLY ONE CATALOG IDENTITY OWES ITS CALLER.
    //
    // Three operators do not take their identity from the planner's DDL allocation round:
    // operator_register_udf_t (pg_proc), operator_register_cast_t (pg_cast) and
    // operator_alter_column_add_t (pg_attribute) each run their OWN one-OID round against
    // the disk actor at execute time. manager_disk_t::allocate_oids_batch carries no error
    // channel — an EMPTY vector is the only shape a round that did not deliver has — and all
    // three used to write
    //     batch.oids = co_await ...; const oid_t id = batch.allocate();
    // and carry straight on into the catalog write.
    //
    // oid_batch_t::allocate() on an exhausted batch answers INVALID_OID and latches a STICKY
    // overrun() flag, so the read is defined; nothing here looked at that flag, so the row
    // still went into the catalog stamped with INVALID_OID. That is a DURABLE "this object
    // has no identity" in pg_proc / pg_cast / pg_attribute — rule 16 broken in the loudest
    // available way — reported to the caller as SUCCESS.
    //
    // This is planner_t::create_plan's shape (rewrite, then check overrun(), then throw the
    // half-stamped tree away with an error) reduced to the one-identity case: consume, check
    // the flag, and refuse BEFORE anything is built out of the answer. It lives in one place
    // because the three call sites are the same site three times — one wording of the
    // refusal, one consultation of the fault seam below.
    //
    // ERROR CODE. io_error, the name components::catalog::oid_batch_t::make() already gives
    // this exact accident ("the allocation round delivered fewer OIDs than it was asked
    // for"). create_physical_plan_error is the planner's name for a DIFFERENT one — the
    // demand and the rewrite disagreeing at plan time — and none of already_exists /
    // do_not_exists / invalid_parameter / unrecognized_function describes a store that could
    // not mint an identity.
    //
    // `refusal_prefix` names the statement, so the caller can tell WHICH catalog was spared.
    // Returns no_error() and writes `out` on success; on refusal `out` is left untouched and
    // the caller must not build anything.
    [[nodiscard]] inline core::error_t single_oid_from_round(std::pmr::memory_resource* resource,
                                                             std::vector<components::catalog::oid_t> oids,
                                                             const char* refusal_prefix,
                                                             components::catalog::oid_t& out) {
#ifdef DEV_MODE
        // The OID-allocation fault seam (services/collection/executor.hpp) is consulted here
        // as well as in executor_t::allocate_oids_inline: these three rounds never pass
        // through that function, so without this the seam cannot reach them and no test could
        // put CREATE FUNCTION / CREATE CAST / ALTER TABLE ADD COLUMN in front of a round that
        // did not deliver. Same seam object, same substitution, exactly one consultation per
        // round.
        if (auto* interposer = services::collection::executor::dev_oid_alloc_interposer(); interposer != nullptr) {
            oids = interposer->substitute(std::size_t{1}, std::move(oids));
        }
#endif
        components::catalog::oid_batch_t batch;
        batch.oids = std::move(oids);
        const components::catalog::oid_t id = batch.allocate();
        if (batch.overrun()) {
            const std::string msg = std::string{refusal_prefix} + ": the OID allocation round delivered " +
                                    std::to_string(batch.oids.size()) +
                                    " of the 1 OID this statement needs; the statement is refused rather than "
                                    "written into the catalog with an invalid identity";
            return core::error_t{core::error_code_t::io_error, std::pmr::string{msg.c_str(), resource}};
        }
        out = id;
        return core::error_t::no_error();
    }

} // namespace components::operators
