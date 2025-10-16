#pragma once

#include <components/base/collection_full_name.hpp>

#include <memory_resource>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace components::catalog {
    using table_namespace_t = std::pmr::vector<std::pmr::string>;

    class table_id {
    public:
        table_id(std::pmr::memory_resource* resource, std::pmr::vector<std::pmr::string> full_name);
        table_id(std::pmr::memory_resource* resource, table_namespace_t ns, std::pmr::string name);
        table_id(std::pmr::memory_resource* resource, const collection_full_name_t& full_name);

        bool operator==(const table_id& other) const;

        [[nodiscard]] static table_id parse(const std::string& identifier_str, std::pmr::memory_resource* resource);
        [[nodiscard]] collection_full_name_t collection_full_name() const;
        [[nodiscard]] const table_namespace_t& get_namespace() const;
        [[nodiscard]] const std::pmr::string& table_name() const;
        [[nodiscard]] std::pmr::string to_pmr_string() const;
        [[nodiscard]] std::string to_string() const;

    private:
        table_namespace_t namespace_parts_;
        std::pmr::string name_;
        std::pmr::memory_resource* resource_;
    };
} // namespace components::catalog

namespace std {
    template<>
    struct hash<components::catalog::table_id> {
        std::size_t operator()(const components::catalog::table_id& id) const noexcept {
            return std::hash<std::pmr::string>{}(id.to_pmr_string());
        }
    };
} // namespace std
