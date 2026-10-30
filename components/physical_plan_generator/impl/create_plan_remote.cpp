#include "create_plan_remote.hpp"
#include "create_plan_select.hpp"

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_remote.hpp>
#include <components/physical_plan/operators/operator_remote.hpp>

namespace services::planner::impl {

    plan_result_t create_plan_remote(const context_storage_t& context, const components::logical_plan::node_ptr& node) {
        const auto& remote = static_cast<const components::logical_plan::node_remote_t&>(*node);
        std::pmr::vector<std::pmr::string> explain{remote.explain().begin(), remote.explain().end(), context.resource};
        std::pmr::vector<components::catalog::oid_t> tables{remote.tables().begin(),
                                                            remote.tables().end(),
                                                            context.resource};
        if (remote.kind() == components::logical_plan::node_remote_t::kind_t::read) {
            return components::operators::operator_ptr{new components::operators::operator_remote_scan(
                context.resource,
                context.log.clone(),
                remote.address(),
                std::pmr::string{remote.request(), context.resource},
                std::move(explain),
                std::pmr::vector<components::types::complex_logical_type>{remote.output_types().begin(),
                                                                          remote.output_types().end(),
                                                                          context.resource},
                std::move(tables))};
        }
        // A write's RETURNING reads the written table's columns, those of a DELETE's scan.
        std::pmr::vector<components::types::complex_logical_type> table_types{context.resource};
        if (!remote.tables().empty()) {
            if (const auto* table = context.table_metadata_for(remote.tables().front())) {
                auto add = [&](const auto& column) {
                    auto type = column.type;
                    type.set_alias(column.attname);
                    table_types.push_back(std::move(type));
                };
                if (remote.scan_columns().empty()) {
                    for (const auto& column : table->columns) {
                        add(column);
                    }
                } else {
                    for (const auto position : remote.scan_columns()) {
                        add(table->columns[position]);
                    }
                }
            }
        }
        return components::operators::operator_ptr{new components::operators::operator_remote_write(
            context.resource,
            context.log.clone(),
            remote.address(),
            std::pmr::string{remote.request(), context.resource},
            std::move(explain),
            build_returning_columns(context.resource, remote.returning()),
            std::move(table_types),
            remote.scan_columns(),
            std::move(tables))};
    }

} // namespace services::planner::impl
