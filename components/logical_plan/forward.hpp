#pragma once

#include <cstdint>
#include <magic_enum.hpp>
#include <string>

namespace components::logical_plan {
    enum class node_type : uint8_t
    {
        aggregate_t,
        alias_t,
        create_collection_t,
        create_database_t,
        create_index_t,
        create_type_t,
        data_t,
        delete_t,
        drop_collection_t,
        drop_database_t,
        drop_index_t,
        drop_type_t,
        function_t,
        insert_t,
        join_t,
        intersect_t,
        limit_t,
        match_t,
        group_t,
        sort_t,
        update_t,
        union_t,
        unused
    };

    inline std::string to_string(node_type type) {
        switch (type) {
            case node_type::aggregate_t:
                return "aggregate_t";
            case node_type::alias_t:
                return "alias_t";
            case node_type::create_collection_t:
                return "create_collection_t";
            case node_type::create_database_t:
                return "create_database_t";
            case node_type::create_index_t:
                return "create_index_t";
            case node_type::create_type_t:
                return "create_type_t";
            case node_type::data_t:
                return "data_t";
            case node_type::delete_t:
                return "delete_t";
            case node_type::drop_collection_t:
                return "drop_collection_t";
            case node_type::drop_database_t:
                return "drop_database_t";
            case node_type::drop_index_t:
                return "drop_index_t";
            case node_type::drop_type_t:
                return "drop_type_t";
            case node_type::function_t:
                return "function_t";
            case node_type::insert_t:
                return "insert_t";
            case node_type::join_t:
                return "join_t";
            case node_type::intersect_t:
                return "intersect_t";
            case node_type::limit_t:
                return "limit_t";
            case node_type::match_t:
                return "match_t";
            case node_type::group_t:
                return "group_t";
            case node_type::sort_t:
                return "sort_t";
            case node_type::update_t:
                return "update_t";
            case node_type::union_t:
                return "union_t";
            default:
                return "unused";
        }
    }

#define node_type_from_string(STR)                                                                                     \
    do {                                                                                                               \
        return node_type::STR;                                                                                         \
    } while (false);

    enum class visitation : uint8_t
    {
        visit_inputs,
        do_not_visit_inputs
    };

    enum class input_side : uint8_t
    {
        left,
        right
    };

    enum class expression_iteration : uint8_t
    {
        continue_t,
        break_t
    };

    namespace aggregate {
        enum class operator_type : int16_t
        {
            invalid = 1,
            count, ///group + project
            group,
            limit,
            match,
            merge,
            out,
            project,
            skip,
            sort,
            unset,
            unwind,
            finish
        };

        inline operator_type get_aggregate_type(const std::string& key) {
            auto type = magic_enum::enum_cast<operator_type>(key);

            if (type.has_value()) {
                return type.value();
            }

            return aggregate::operator_type::invalid;
        }
    } // namespace aggregate

} // namespace components::logical_plan
