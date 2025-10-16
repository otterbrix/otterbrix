#pragma once

#include <components/catalog/schema.hpp>

#include <bitset>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace components::catalog {
    class schema_diff {
    public:
        schema_diff(std::pmr::memory_resource* resource);

        void add_column(const std::string& name,
                        const types::complex_logical_type& type,
                        bool required = false,
                        const std::pmr::string& doc = "");
        void delete_column(const std::string& name);
        void rename_column(const std::string& name, const std::string& new_name);
        void update_column_type(const std::string& name, const types::complex_logical_type& new_type);
        void update_column_doc(const std::string& name, const std::pmr::string& doc);
        void make_optional(const std::string& name);
        void make_required(const std::string& name);
        void update_primary_key(const std::pmr::vector<field_id_t>& primary_key);

        [[nodiscard]] bool has_changes() const;
        [[nodiscard]] schema apply(const schema& base_schema) const;

    private:
        // enum ordering is important: type must be updated before name
        enum diff_info_type : uint8_t
        {
            UPDATE_TYPE = 1,     // 00001
            UPDATE_NAME = 2,     // 00010
            UPDATE_DOC = 3,      // 00100
            UPDATE_OPTIONAL = 4, // 01000
            COUNT                // sentinel
        };

        struct struct_entry {
            types::complex_logical_type type;
            types::field_description desc;
        };

        struct diff_info {
            diff_info(diff_info_type info_type,
                      types::field_description desc = types::field_description(),
                      types::complex_logical_type type = types::complex_logical_type());

            std::bitset<diff_info_type::COUNT> info;
            struct_entry entry;
        };

        bool do_update(std::vector<types::complex_logical_type>& new_columns,
                       std::vector<types::field_description>& new_desc,
                       const std::string& name) const;

        // aliases are using std::string, not pmr, avoid copy hell
        std::pmr::vector<struct_entry> added_columns_;
        std::pmr::unordered_map<std::string, diff_info> updates_;
        std::pmr::unordered_map<std::string, std::string> renames_;
        std::pmr::unordered_set<std::string> deleted_columns_;
        std::optional<std::pmr::vector<field_id_t>> new_primary_key_;
        std::pmr::memory_resource* resource_;
    };
} // namespace components::catalog
