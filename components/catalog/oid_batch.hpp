#pragma once

#include "catalog_oids.hpp"

#include <core/result_wrapper.hpp>

#include <cstddef>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace components::catalog {

    // Pre-allocated batch of OIDs handed to the planner before DDL logical rewrite. The DDL
    // allocation round (executor_t::allocate_oids_inline -> node_allocate_oids_t ->
    // manager_disk_t::allocate_oids_batch) fills it; planner_t::create_plan then consumes it while
    // building pg_class / pg_attribute / pg_depend rows without async disk access in the rewrite.
    //
    // Exhaustion is not guarded by assert: the round collapses both of its failures into an
    // EMPTY vector with nothing comparing it against compute_oid_demand's count, so under
    // NDEBUG an assert-only guard would let allocate()/peek() read PAST THE END and stamp
    // pg_class/pg_attribute/pg_depend with garbage that survives restart. Two halves answer it
    // instead, no fallback: make() checks the batch against the demand at construction
    // (a short round is a refusal, not a smaller batch); allocate()/peek() on an exhausted batch
    // answer INVALID_OID and latch a STICKY overrun flag, which planner_t::create_plan turns
    // into a refused statement that throws the half-stamped tree away. The second half covers a
    // rewrite that consumes MORE than compute_oid_demand predicted -- the two counts live in
    // different files (planner.cpp vs the rewrite_*/ddl_metadata_builder.cpp functions), kept
    // equal by hand.
    //
    // Every consumer of allocate()/peek() must read overrun(): pg_proc
    // (operator_register_udf_t), pg_cast (operator_register_cast_t) and pg_attribute
    // (operator_alter_column_add_t) mint a single identity outside the planner's round entirely
    // and refuse through the shared reader, single_oid_round.hpp. A new caller that skips
    // overrun() reopens this hole.
    struct oid_batch_t {
        // Checked construction. `need` is compute_oid_demand's answer for the node about to be
        // rewritten; `oids` is what the allocation round delivered.
        //
        // need == 0 IS LEGAL AND IS A SUCCESS: DROP (except DROP INDEX's own rewrite), ALTER TABLE
        // and CREATE MATERIALIZED VIEW whose schema derivation inferred no columns all rewrite
        // without consuming an OID, and never run an allocation round at all.
        [[nodiscard]] static core::result_wrapper_t<oid_batch_t>
        make(std::pmr::memory_resource* resource, std::vector<oid_t> oids, std::size_t need) {
            if (oids.size() < need) {
                const std::string msg = "DDL OID allocation round delivered " + std::to_string(oids.size()) +
                                        " of " + std::to_string(need) +
                                        " OIDs; the statement is refused rather than written with "
                                        "an invalid catalog identity";
                return core::error_t{core::error_code_t::io_error, std::pmr::string{msg.c_str(), resource}};
            }
            oid_batch_t batch;
            batch.oids = std::move(oids);
            return batch;
        }

        std::vector<oid_t> oids;
        std::size_t next = 0;

        bool empty() const noexcept { return next >= oids.size(); }

        // Consume the next OID. An exhausted batch answers INVALID_OID and latches
        // overrun() -- see the class comment; the caller of the rewrite is required to
        // check overrun() and discard everything it built.
        oid_t allocate() noexcept {
            if (empty()) {
                overrun_ = true;
                return INVALID_OID;
            }
            return oids[next++];
        }

        // Inspect the next OID without consuming it. Used by the planner to mirror the
        // about-to-be-allocated table_oid onto the cc node for the physical plan generator.
        // Non-const because exhaustion latches overrun() here exactly as in allocate():
        // a peek past the end is the same out-of-bounds read.
        oid_t peek() noexcept {
            if (empty()) {
                overrun_ = true;
                return INVALID_OID;
            }
            return oids[next];
        }

        // Sticky: true once allocate()/peek() has been asked for an OID this batch did not
        // have. Never cleared -- a batch that overran is spent, whatever happened after.
        bool overrun() const noexcept { return overrun_; }

    private:
        bool overrun_ = false;
    };

} // namespace components::catalog
