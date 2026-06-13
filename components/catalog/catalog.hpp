#pragma once

#include <components/catalog/table_id.hpp>
#include <components/catalog/table_metadata.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <stdexcept>
#include <unordered_map>

namespace components::catalog {

    class catalog {
    public:
        explicit catalog(std::pmr::memory_resource* resource)
            : resource_(resource) {}

        void create_namespace(const table_namespace_t& /*namespace_name*/) {}

        [[nodiscard]] core::error_t create_table(const table_id& id, table_metadata meta) {
            auto [_, inserted] = tables_.emplace(id.to_pmr_string(), std::move(meta));
            if (!inserted) {
                return core::error_t(core::error_code_t::other_error,
                                     std::pmr::string{"table already exists", resource_});
            }
            return core::error_t::no_error();
        }

        [[nodiscard]] bool table_exists(const table_id& id) const {
            return tables_.find(id.to_pmr_string()) != tables_.end();
        }

        [[nodiscard]] const schema& get_table_schema(const table_id& id) const {
            auto it = tables_.find(id.to_pmr_string());
            if (it == tables_.end()) {
                throw std::out_of_range("table schema not found");
            }
            return it->second.get_schema();
        }

    private:
        std::pmr::memory_resource* resource_;
        std::unordered_map<std::pmr::string, table_metadata> tables_;
    };

} // namespace components::catalog
