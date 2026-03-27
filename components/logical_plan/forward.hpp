#pragma once

#include <cstdint>
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
        create_sequence_t,
        drop_sequence_t,
        create_view_t,
        drop_view_t,
        create_macro_t,
        drop_macro_t,
        checkpoint_t,
        vacuum_t,
        having_t,
        unused
    };

    std::string to_string(node_type type);

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

        operator_type get_aggregate_type(const std::string& key);

    } // namespace aggregate

} // namespace components::logical_plan
