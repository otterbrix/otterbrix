#include "schema.hpp"

#include <memory_resource>
#include <unordered_set>

using namespace components::types;

// todo: use result, monad interface of it will make this code MUCH cleaner
namespace components::catalog {
    schema::schema(std::pmr::memory_resource* resource,
                   const std::vector<table::column_definition_t>& columns,
                   const std::vector<field_description>& descriptions,
                   const std::pmr::vector<field_id_t>& primary_key)
        : columns_(columns)
        , descriptions_(descriptions)
        , primary_key_field_ids_(primary_key, resource)
        , id_to_struct_idx_(resource)
        , resource_(resource) {
        {
            std::pmr::unordered_set<std::pmr::string> names(resource);
            for (const auto& column : columns_) {
                std::pmr::string alias(column.name(), resource);
                if (names.count(alias)) {
                    error_ = catalog_error(catalog_mistake_t::DUPLICATE_COLUMN,
                                           "Duplicate column with name \"" + column.type().alias() +
                                               "\", names must be unique");
                }

                names.emplace(std::move(alias));
            }
        }

        {
            field_id_t max = 0;
            size_t idx = 0;
            for (const auto& desc : descriptions_) {
                if (id_to_struct_idx_.find(desc.field_id) != id_to_struct_idx_.end()) {
                    error_ = catalog_error(catalog_mistake_t::DUPLICATE_COLUMN,
                                           "Duplicate id in schema: " + std::to_string(desc.field_id) +
                                               ", ids must be unique");
                }

                id_to_struct_idx_.emplace(desc.field_id, idx++);
                max = std::max(max, desc.field_id);
            }

            for (field_id_t key : primary_key_field_ids_) {
                if (id_to_struct_idx_.find(key) == id_to_struct_idx_.end()) {
                    error_ = catalog_error(catalog_mistake_t::MISSING_PRIMARY_KEY_ID,
                                           "No field with id from primary key: " + std::to_string(key));
                }
            }

            highest_ = max;
        }
    }

    cursor::cursor_t_ptr schema::find_field(field_id_t id) const {
        size_t idx = find_idx_by_id(id);

        if (static_cast<bool>(error_)) {
            return cursor::make_cursor(resource_, cursor::error_code_t::schema_error, error_.what());
        }

        return cursor::make_cursor(resource_, {columns_[idx].type()});
    }

    cursor::cursor_t_ptr schema::find_field(const std::pmr::string& name) const {
        size_t idx = find_idx_by_name(name);

        if (static_cast<bool>(error_)) {
            return cursor::make_cursor(resource_, cursor::error_code_t::schema_error, error_.what());
        }

        return cursor::make_cursor(resource_, {columns_[idx].type()});
    }

    std::optional<schema::field_description_cref>
    schema::get_field_description(components::catalog::field_id_t id) const {
        size_t idx = find_idx_by_id(id);
        if (static_cast<bool>(error_)) {
            return {};
        }

        return descriptions_[idx];
    }

    std::optional<schema::field_description_cref> schema::get_field_description(const std::pmr::string& name) const {
        size_t idx = find_idx_by_name(name);
        if (static_cast<bool>(error_)) {
            return {};
        }

        return descriptions_[idx];
    }

    const std::pmr::vector<field_id_t>& schema::primary_key() const { return primary_key_field_ids_; }

    const std::vector<table::column_definition_t>& schema::columns() const { return columns_; }

    const std::vector<field_description>& schema::descriptions() const { return descriptions_; }

    field_id_t schema::highest_field_id() const { return highest_; }

    const catalog_error& schema::error() const { return error_; }

    std::vector<complex_logical_type> schema::types() const {
        std::vector<complex_logical_type> result;
        result.reserve(columns_.size());
        for (const auto& column : columns_) {
            result.emplace_back(column.type());
        }
        return result;
    }

    size_t schema::find_idx_by_id(field_id_t id) const {
        if (auto it = id_to_struct_idx_.find(id); it != id_to_struct_idx_.end()) {
            return it->second;
        }

        error_ = catalog_error(catalog_mistake_t::FIELD_MISSING, "No field with such id: " + std::to_string(id));
        return {};
    }

    size_t schema::find_idx_by_name(const std::pmr::string& name) const {
        auto it =
            std::find_if(columns_.cbegin(), columns_.cend(), [&name](const table::column_definition_t& column) -> bool {
                return column.name() == name.c_str();
            });

        if (it != columns_.cend()) {
            return static_cast<size_t>(it - columns_.cbegin());
        }

        error_ =
            catalog_error(catalog_mistake_t::FIELD_MISSING, "No field with such name: \"" + std::string(name) + "\"");
        return {};
    }
} // namespace components::catalog
