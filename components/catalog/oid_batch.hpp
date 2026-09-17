#pragma once

#include "catalog_oids.hpp"

#include <core/result_wrapper.hpp>

#include <cstddef>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace components::catalog {

    // Not assert-guarded: under NDEBUG that would let allocate()/peek() read past the end and stamp
    // catalog rows with garbage that survives restart. make() refuses a short round at construction;
    // exhaustion instead latches overrun() for planner_t::create_plan to refuse the statement.
    //
    // Every consumer of allocate()/peek() must check overrun(): pg_proc, pg_cast and pg_attribute mint
    // an identity outside the planner's round via single_oid_round.hpp. Skipping the check reopens this hole.
    //
    // The demand and the consumption live in different files (planner.cpp against the rewrite_* /
    // ddl_metadata_builder.cpp functions) and are kept equal BY HAND.
    struct oid_batch_t {
        // need == 0 is a legal success: DROP (except DROP INDEX's own rewrite), ALTER TABLE and
        // CREATE MATERIALIZED VIEW inferring no columns never run a round at all.
        [[nodiscard]] static core::result_wrapper_t<oid_batch_t>
        make(std::pmr::memory_resource* resource, std::vector<oid_t> oids, std::size_t need) {
            if (oids.size() < need) {
                const std::string msg = "DDL OID allocation round delivered " + std::to_string(oids.size()) + " of " +
                                        std::to_string(need) +
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

        // An exhausted batch answers INVALID_OID and latches overrun() (see the class comment).
        oid_t allocate() noexcept {
            if (empty()) {
                overrun_ = true;
                return INVALID_OID;
            }
            return oids[next++];
        }

        // Non-const: exhaustion latches overrun() here exactly as in allocate(), and a peek past
        // the end is the same out-of-bounds read.
        oid_t peek() noexcept {
            if (empty()) {
                overrun_ = true;
                return INVALID_OID;
            }
            return oids[next];
        }

        // Sticky and never cleared: once allocate()/peek() asked for an OID the batch didn't have,
        // a spent batch stays spent regardless of what happens after.
        bool overrun() const noexcept { return overrun_; }

    private:
        bool overrun_ = false;
    };

} // namespace components::catalog
