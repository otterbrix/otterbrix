#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <core/result_wrapper.hpp>
#include <services/disk/disk_contract.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string>

namespace components::operators {
    inline std::pmr::vector<services::disk::pg_catalog_delete_spec_t>
    stage_function_deletes(std::pmr::memory_resource* resource,
                           pipeline::context_t* ctx,
                           const std::pmr::vector<catalog::oid_t>& function_oids,
                           std::pmr::vector<std::size_t>& pg_proc_specs) {
        constexpr catalog::oid_t pg_proc_coll = catalog::well_known_oid::pg_proc_table;
        constexpr catalog::oid_t pg_depend_coll = catalog::well_known_oid::pg_depend_table;
        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource);
        specs.reserve(function_oids.size() * 3);
        pg_proc_specs.reserve(function_oids.size());
        for (const auto oid : function_oids) {
            pg_proc_specs.push_back(specs.size());
            specs.push_back({pg_proc_coll, std::int64_t{0}, oid});
            specs.push_back({pg_depend_coll, std::int64_t{1}, oid});
            specs.push_back({pg_depend_coll, std::int64_t{3}, oid});
            if (ctx->txn.transaction_id != 0) {
                ctx->pg_catalog_delete_tables.insert(pg_proc_coll);
                ctx->pg_catalog_delete_tables.insert(pg_depend_coll);
            }
        }
        return specs;
    }

    inline core::error_t confirm_function_deletes(std::pmr::memory_resource* resource,
                                                  const std::pmr::vector<std::uint64_t>& deleted,
                                                  const std::pmr::vector<std::size_t>& pg_proc_specs,
                                                  const std::string& statement,
                                                  const std::string& function_name) {
        for (const auto i : pg_proc_specs) {
            if (i < deleted.size() && deleted[i] == 0) {
                return core::error_t{core::error_code_t::other_error,
                                     std::pmr::string{statement + ": no pg_proc row was deleted for '" + function_name +
                                                          "' — the function is still in the catalog",
                                                      resource}};
            }
        }
        return core::error_t::no_error();
    }
} // namespace components::operators
