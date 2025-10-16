#include "schema.hpp"

using namespace components::types;

// todo: use result, monad interface of it will make this code MUCH cleaner
namespace components::catalog {
    schema::schema(std::pmr::memory_resource* resource,
                   const components::types::complex_logical_type& schema_struct,
                   const std::pmr::vector<field_id_t>& primary_key)
        : schema_struct_(schema_struct)
        , primary_key_field_ids_(primary_key, resource)
        , id_to_struct_idx_(resource)
        , resource_(resource) {
        auto& detailed_struct = to_struct(schema_struct);
        {
            std::pmr::unordered_set<std::pmr::string> names(resource);
            for (const auto& type : detailed_struct.child_types()) {
                if (names.find(type.alias().c_str()) != names.end()) {
                    error_ = catalog_error(catalog_mistake_t::DUPLICATE_COLUMN,
                                           "Duplicate column with name \"" + type.alias() + "\", names must be unique");
                }

                names.emplace(type.alias());
            }
        }

        {
            field_id_t max = 0;
            size_t idx = 0;
            for (const auto& desc : detailed_struct.descriptions()) {
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

        return cursor::make_cursor(resource_, {schema_struct_.child_types()[idx]});
    }

    cursor::cursor_t_ptr schema::find_field(const std::pmr::string& name) const {
        size_t idx = find_idx_by_name(name);

        if (static_cast<bool>(error_)) {
            return cursor::make_cursor(resource_, cursor::error_code_t::schema_error, error_.what());
        }

        return cursor::make_cursor(resource_, {schema_struct_.child_types()[idx]});
    }

    std::optional<schema::field_description_cref>
    schema::get_field_description(components::catalog::field_id_t id) const {
        size_t idx = find_idx_by_id(id);
        if (static_cast<bool>(error_)) {
            return {};
        }

        return to_struct(schema_struct_).descriptions()[idx];
    }

    std::optional<schema::field_description_cref> schema::get_field_description(const std::pmr::string& name) const {
        size_t idx = find_idx_by_name(name);
        if (static_cast<bool>(error_)) {
            return {};
        }

        return to_struct(schema_struct_).descriptions()[find_idx_by_name(name)];
    }

    const std::pmr::vector<field_id_t>& schema::primary_key() const { return primary_key_field_ids_; }

    const std::vector<types::complex_logical_type>& schema::columns() const { return schema_struct_.child_types(); }

    const std::vector<types::field_description>& schema::descriptions() const {
        return to_struct(schema_struct_).descriptions();
    }

    field_id_t schema::highest_field_id() const { return highest_; }

    const catalog_error& schema::error() const { return error_; }

    types::complex_logical_type schema::schema_struct() const { return schema_struct_; }

    size_t schema::find_idx_by_id(field_id_t id) const {
        if (auto it = id_to_struct_idx_.find(id); it != id_to_struct_idx_.end()) {
            return it->second;
        }

        error_ = catalog_error(catalog_mistake_t::FIELD_MISSING, "No field with such id: " + std::to_string(id));
        return {};
    }

    size_t schema::find_idx_by_name(const std::pmr::string& name) const {
        const auto& fields = schema_struct_.child_types();

        auto it = std::find_if(fields.cbegin(), fields.cend(), [&name](const complex_logical_type& type) -> bool {
            return type.alias() == name.c_str();
        });

        if (it != fields.cend()) {
            return static_cast<size_t>(it - fields.cbegin());
        }

        error_ =
            catalog_error(catalog_mistake_t::FIELD_MISSING, "No field with such name: \"" + std::string(name) + "\"");
        return {};
    }
} // namespace components::catalog
