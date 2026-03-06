#include "schema_diff.hpp"

using namespace components::types;

namespace components::catalog {
    schema_diff::schema_diff(std::pmr::memory_resource* resource)
        : updates_(resource)
        , renames_(resource)
        , deleted_columns_(resource)
        , new_primary_key_()
        , resource_(resource) {}

    schema_diff::diff_info::diff_info(schema_diff::diff_info_type info_type,
                                      table::column_definition_t column,
                                      types::field_description desc)
        : info(1ULL << info_type)
        , entry{std::move(column), std::move(desc)} {}

    void schema_diff::add_column(const table::column_definition_t& column, bool required, const std::pmr::string& doc) {
        added_columns_.emplace_back(struct_entry{column, field_description(0, required, doc.c_str())});
    }

    void schema_diff::delete_column(const std::string& name) { deleted_columns_.emplace(name); }

    void schema_diff::rename_column(const std::string& name, const std::string& new_name) {
        if (auto it = updates_.find(name); it != updates_.end()) {
            it->second.info.set(diff_info_type::UPDATE_NAME);
            it->second.entry.column.set_name(new_name);
            return;
        }

        auto type = complex_logical_type();
        type.set_alias(new_name);
        renames_.emplace(name, new_name);
        updates_.emplace(new_name,
                         diff_info(diff_info_type::UPDATE_NAME,
                                   table::column_definition_t{new_name, std::move(type)},
                                   field_description()));
    }

    void schema_diff::update_column_type(const std::string& name, const types::complex_logical_type& new_type) {
        if (auto it = updates_.find(name); it != updates_.end()) {
            const auto& old_alias = it->second.entry.column.type().alias();
            it->second.info.set(diff_info_type::UPDATE_TYPE);
            it->second.entry.column.type() = new_type;
            it->second.entry.column.type().set_alias(old_alias);
            return;
        }

        updates_.emplace(
            name,
            diff_info(diff_info_type::UPDATE_TYPE, table::column_definition_t{name, new_type}, field_description()));
    }

    void schema_diff::update_column_doc(const std::string& name, const std::pmr::string& doc) {
        if (auto it = updates_.find(name); it != updates_.end()) {
            it->second.info.set(diff_info_type::UPDATE_DOC);
            it->second.entry.desc.doc = doc;
            return;
        }

        updates_.emplace(name,
                         diff_info(diff_info_type::UPDATE_DOC,
                                   table::column_definition_t{name, complex_logical_type{}},
                                   field_description(0, false, doc.c_str())));
    }

    void schema_diff::make_optional(const std::string& name) {
        if (auto it = updates_.find(name); it != updates_.end()) {
            it->second.info.set(diff_info_type::UPDATE_OPTIONAL);
            it->second.entry.desc.required = false;
            return;
        }

        updates_.emplace(name,
                         diff_info(diff_info_type::UPDATE_OPTIONAL,
                                   table::column_definition_t{name, complex_logical_type{}},
                                   field_description(0, false)));
    }

    void schema_diff::make_required(const std::string& name) {
        if (auto it = updates_.find(name); it != updates_.end()) {
            it->second.info.set(diff_info_type::UPDATE_OPTIONAL);
            it->second.entry.desc.required = true;
            return;
        }

        updates_.emplace(name,
                         diff_info(diff_info_type::UPDATE_OPTIONAL,
                                   table::column_definition_t{name, complex_logical_type{}},
                                   field_description(0, true)));
    }

    void schema_diff::update_primary_key(const std::pmr::vector<field_id_t>& primary_key) {
        new_primary_key_.emplace(primary_key);
    }

    bool schema_diff::has_changes() const {
        return !updates_.empty() || !added_columns_.empty() || !deleted_columns_.empty() || new_primary_key_;
    }

    schema schema_diff::apply(const schema& base_schema) const {
        size_t sz = base_schema.columns().size();

        std::vector<components::table::column_definition_t> new_columns;
        std::vector<types::field_description> new_desc;
        new_columns.reserve(sz + added_columns_.size());
        new_desc.reserve(sz + added_columns_.size());

        for (size_t i = 0; i < sz; ++i) {
            const auto& column = base_schema.columns()[i];
            const auto& desc = base_schema.descriptions()[i];
            if (deleted_columns_.find(column.name()) != deleted_columns_.end()) {
                continue;
            }

            new_columns.push_back(column);
            new_desc.push_back(desc);

            if (!do_update(new_columns, new_desc, column.name())) {
                if (const auto& rename = renames_.find(column.name()); rename != renames_.end()) {
                    do_update(new_columns, new_desc, rename->second);
                }
            }
        }

        for (const auto& entry : added_columns_) {
            new_columns.push_back(entry.column);
            new_desc.push_back(entry.desc);
        }

        return schema(resource_, new_columns, new_desc, new_primary_key_.value_or(base_schema.primary_key()));
    }

    bool schema_diff::do_update(std::vector<components::table::column_definition_t>& new_columns,
                                std::vector<types::field_description>& new_desc,
                                const std::string& name) const {
        if (const auto& it = updates_.find(name); it != updates_.end()) {
            auto& info = it->second;
            for (uint8_t i = diff_info_type::UPDATE_TYPE; i < diff_info_type::COUNT; ++i) {
                auto diff_type = static_cast<diff_info_type>(i);

                if (!info.info.test(diff_type)) {
                    continue;
                }

                switch (diff_type) {
                    case diff_info_type::UPDATE_TYPE:
                        new_columns.back() = info.entry.column;
                        break;
                    case diff_info_type::UPDATE_NAME: {
                        // if type is being updated - do nothing, correct alias is already set
                        if (!info.info.test(UPDATE_TYPE)) {
                            new_columns.back().set_name(info.entry.column.name());
                        }
                        break;
                    }
                    case diff_info_type::UPDATE_DOC:
                        new_desc.back().doc = std::move(info.entry.desc.doc);
                        break;
                    case diff_info_type::UPDATE_OPTIONAL:
                        new_desc.back().required = info.entry.desc.required;
                        break;
                    default:
                        break;
                }
            }

            return true;
        }

        return false;
    }
} // namespace components::catalog
