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

    // Shared by operators that mint one OID at execute time outside the planner's DDL allocation round
    // (register_udf/pg_proc, register_cast/pg_cast, alter_column_add/pg_attribute). allocate_oids_batch has
    // no error channel — an exhausted batch answers INVALID_OID and only latches overrun() — so this checks
    // that flag before use. Error code is io_error (oid_batch_t::make()'s name for this exact
    // shortfall), distinct from create_physical_plan_error (a demand/rewrite mismatch, not an exhausted round).
    [[nodiscard]] inline core::error_t single_oid_from_round(std::pmr::memory_resource* resource,
                                                             std::vector<components::catalog::oid_t> oids,
                                                             const char* refusal_prefix,
                                                             components::catalog::oid_t& out) {
#ifdef DEV_MODE
        // Consults the same fault seam as executor_t::allocate_oids_inline (services/collection/executor.hpp):
        // these three rounds bypass that function, so without this a test could not simulate a shortfall for
        // CREATE FUNCTION / CREATE CAST / ALTER TABLE ADD COLUMN.
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
