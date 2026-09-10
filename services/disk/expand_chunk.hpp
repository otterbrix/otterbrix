#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace services::disk::detail {

    // Rewrites data.data so slot t holds the column named table_columns[t], matched by alias
    [[nodiscard]] inline core::error_t
    expand_chunk_to_columns(std::pmr::memory_resource* resource,
                            components::catalog::oid_t table_oid,
                            const std::vector<components::table::column_definition_t>& table_columns,
                            components::vector::data_chunk_t& data,
                            bool is_computed) {
        std::pmr::vector<components::types::complex_logical_type> full_types(resource);
        full_types.reserve(table_columns.size());
        for (const auto& col_def : table_columns) {
            full_types.push_back(col_def.type());
        }

        std::vector<components::vector::vector_t> expanded_data;
        expanded_data.reserve(table_columns.size());
        for (std::size_t t = 0; t < table_columns.size(); t++) {
            bool found = false;
            for (uint64_t col = 0; col < data.column_count(); col++) {
                if (!data.data[col].type().has_alias() ||
                    data.data[col].type().alias() != table_columns[t].name() ||
                    (is_computed && data.data[col].type().type() != table_columns[t].type().type())) {
                    continue;
                }
                const auto& incoming_type = data.data[col].type();
                const auto& stored_type = table_columns[t].type();
                if (incoming_type != stored_type) {
                    const auto spell = [](const components::types::complex_logical_type& t_) {
                        auto spec = components::catalog::encode_type_spec(t_);
                        return spec.empty() ? std::to_string(static_cast<int>(t_.type())) : spec;
                    };
                    std::pmr::string what{"storage append: column '", resource};
                    what.append(table_columns[t].name().c_str());
                    what.append("' of table oid ");
                    what.append(std::to_string(static_cast<unsigned>(table_oid)).c_str());
                    what.append(" stores type ");
                    what.append(spell(stored_type).c_str());
                    what.append(", the incoming chunk carries ");
                    what.append(spell(incoming_type).c_str());
                    what.append("; nothing was appended");
                    return core::error_t{core::error_code_t::schema_error, std::move(what)};
                }
                expanded_data.push_back(std::move(data.data[col]));
                found = true;
                break;
            }
            if (!found) {
                expanded_data.emplace_back(resource, full_types[t], data.size());
                expanded_data.back().validity().set_all_invalid(data.size());
            }
        }
        data.data = std::move(expanded_data);
        return core::error_t::no_error();
    }

} // namespace services::disk::detail
