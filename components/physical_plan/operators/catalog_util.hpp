#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/proc_signature.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <core/result_wrapper.hpp>
#include <services/disk/disk_contract.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::operators {

    // One per kernel signature of the function; a function without one still gets a row, with an empty signature.
    inline std::pmr::vector<catalog::proc_signature_t> proc_signatures(std::pmr::memory_resource* resource,
                                                                       const components::compute::function& function) {
        std::pmr::vector<catalog::proc_signature_t> out{resource};
        for (const auto& signature : function.get_signatures()) {
            catalog::proc_signature_t row;
            row.pronargs = static_cast<std::int32_t>(signature.input_types.size());
            row.proargmatchers = catalog::encode_proargmatchers(signature.input_types);
            row.prorettype = catalog::encode_prorettype(signature.output_types);
            out.push_back(std::move(row));
        }
        if (out.empty()) {
            out.emplace_back();
        }
        return out;
    }
} // namespace components::operators
