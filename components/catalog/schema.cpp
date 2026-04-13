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
        , error_(core::error_t::no_error())
        , resource_(resource) {
        {
            std::pmr::unordered_set<std::pmr::string> names(resource);
            for (const auto& column : columns_) {
                std::pmr::string alias(column.name(), resource);
                if (names.count(alias)) {
                    error_ = core::error_t(core::error_code_t::duplicate_field,
                                           std::pmr::string{"Duplicate column with name \"" + column.type().alias() +
                                                                "\", names must be unique",
                                                            resource_});
                }

                names.emplace(std::move(alias));
            }
        }

        {
            field_id_t max = 0;
            size_t idx = 0;
            for (const auto& desc : descriptions_) {
                if (id_to_struct_idx_.find(desc.field_id) != id_to_struct_idx_.end()) {
                    error_ = core::error_t(core::error_code_t::duplicate_field,
                                           std::pmr::string{"Duplicate id in schema: " + std::to_string(desc.field_id) +
                                                                ", ids must be unique",
                                                            resource_});
                }

                id_to_struct_idx_.emplace(desc.field_id, idx++);
                max = std::max(max, desc.field_id);
            }

            for (field_id_t key : primary_key_field_ids_) {
                if (id_to_struct_idx_.find(key) == id_to_struct_idx_.end()) {
                    error_ = core::error_t(
                        core::error_code_t::missing_primary_key_id,
                        std::pmr::string{"No field with id from primary key: " + std::to_string(key), resource_});
                }
            }

            highest_ = max;
        }
    }

    core::result_wrapper_t<complex_logical_type> schema::find_field(field_id_t id) const {
        size_t idx = find_idx_by_id(id);

        if (error_.contains_error()) {
            return error_;
        }

        return columns_[idx].type();
    }

    core::result_wrapper_t<complex_logical_type> schema::find_field(const std::pmr::string& name) const {
        size_t idx = find_idx_by_name(name);

        if (error_.contains_error()) {
            return error_;
        }

        return columns_[idx].type();
    }

    core::result_wrapper_t<schema::field_description_cref>
    schema::get_field_description(components::catalog::field_id_t id) const {
        size_t idx = find_idx_by_id(id);
        if (error_.contains_error()) {
            return error_;
        }

        return descriptions_[idx];
    }

    core::result_wrapper_t<schema::field_description_cref>
    schema::get_field_description(const std::pmr::string& name) const {
        size_t idx = find_idx_by_name(name);
        if (error_.contains_error()) {
            return error_;
        }

        return descriptions_[idx];
    }

    const std::pmr::vector<field_id_t>& schema::primary_key() const { return primary_key_field_ids_; }

    const std::vector<table::column_definition_t>& schema::columns() const { return columns_; }

    const std::vector<field_description>& schema::descriptions() const { return descriptions_; }

    field_id_t schema::highest_field_id() const { return highest_; }

    const core::error_t& schema::error() const { return error_; }

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

        error_ = core::error_t(core::error_code_t::missing_field,

                               std::pmr::string{"No field with such id: " + std::to_string(id), resource_});
        return std::numeric_limits<size_t>::max();
    }

    size_t schema::find_idx_by_name(const std::pmr::string& name) const {
        auto it =
            std::find_if(columns_.cbegin(), columns_.cend(), [&name](const table::column_definition_t& column) -> bool {
                return column.name() == name.c_str();
            });

        if (it != columns_.cend()) {
            return static_cast<size_t>(it - columns_.cbegin());
        }

        error_ = core::error_t(core::error_code_t::missing_field,

                               std::pmr::string{"No field with such name: \"" + std::string(name) + "\"", resource_});
        return std::numeric_limits<size_t>::max();
    }
} // namespace components::catalog
