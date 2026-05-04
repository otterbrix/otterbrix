#include "node_alter_table.hpp"

#include <sstream>

namespace components::logical_plan {

    namespace {
        std::string kind_to_string(alter_table_kind k) {
            switch (k) {
                case alter_table_kind::add_column:
                    return "ADD COLUMN";
                case alter_table_kind::drop_column:
                    return "DROP COLUMN";
                case alter_table_kind::rename_column:
                    return "RENAME COLUMN";
            }
            return "?";
        }
    } // namespace

    node_alter_table_t::node_alter_table_t(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             components::table::column_definition_t column)
        : node_t(resource, node_type::alter_table_t, collection) {
        alter_table_subcommand_t sub;
        sub.kind = alter_table_kind::add_column;
        sub.column_name = column.name();
        sub.column = std::move(column);
        subcommands_.push_back(std::move(sub));
    }

    node_alter_table_t::node_alter_table_t(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             alter_table_kind kind,
                                             std::string column_name)
        : node_t(resource, node_type::alter_table_t, collection) {
        alter_table_subcommand_t sub;
        sub.kind = kind;
        sub.column_name = std::move(column_name);
        subcommands_.push_back(std::move(sub));
    }

    node_alter_table_t::node_alter_table_t(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             std::string old_name,
                                             std::string new_name)
        : node_t(resource, node_type::alter_table_t, collection) {
        alter_table_subcommand_t sub;
        sub.kind = alter_table_kind::rename_column;
        sub.column_name = std::move(old_name);
        sub.new_column_name = std::move(new_name);
        subcommands_.push_back(std::move(sub));
    }

    node_alter_table_t::node_alter_table_t(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             std::vector<alter_table_subcommand_t> subcommands)
        : node_t(resource, node_type::alter_table_t, collection)
        , subcommands_(std::move(subcommands)) {
        if (subcommands_.empty()) {
            subcommands_.emplace_back();
        }
    }

    hash_t node_alter_table_t::hash_impl() const { return 0; }

    std::string node_alter_table_t::to_string_impl() const {
        std::stringstream s;
        s << "$alter_table: " << database_name() << "." << collection_name();
        for (size_t i = 0; i < subcommands_.size(); ++i) {
            const auto& sub = subcommands_[i];
            if (i > 0) {
                s << ",";
            }
            s << " " << kind_to_string(sub.kind) << " " << sub.column_name;
            if (sub.kind == alter_table_kind::rename_column) {
                s << " -> " << sub.new_column_name;
            }
        }
        return s.str();
    }

    node_alter_table_ptr make_node_alter_table_add_column(std::pmr::memory_resource* resource,
                                                            const collection_full_name_t& collection,
                                                            components::table::column_definition_t column) {
        return {new node_alter_table_t{resource, collection, std::move(column)}};
    }

    node_alter_table_ptr make_node_alter_table_drop_column(std::pmr::memory_resource* resource,
                                                             const collection_full_name_t& collection,
                                                             std::string column_name) {
        return {new node_alter_table_t{resource, collection, alter_table_kind::drop_column,
                                         std::move(column_name)}};
    }

    node_alter_table_ptr make_node_alter_table_rename_column(std::pmr::memory_resource* resource,
                                                               const collection_full_name_t& collection,
                                                               std::string old_name,
                                                               std::string new_name) {
        return {new node_alter_table_t{resource, collection, std::move(old_name),
                                         std::move(new_name)}};
    }

    node_alter_table_ptr make_node_alter_table_multi(std::pmr::memory_resource* resource,
                                                      const collection_full_name_t& collection,
                                                      std::vector<alter_table_subcommand_t> subcommands) {
        return {new node_alter_table_t{resource, collection, std::move(subcommands)}};
    }

} // namespace components::logical_plan
