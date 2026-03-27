#include "forward.hpp"

namespace components::logical_plan {

    std::string to_string(node_type type) {
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
            case node_type::create_sequence_t:
                return "create_sequence_t";
            case node_type::drop_sequence_t:
                return "drop_sequence_t";
            case node_type::create_view_t:
                return "create_view_t";
            case node_type::drop_view_t:
                return "drop_view_t";
            case node_type::create_macro_t:
                return "create_macro_t";
            case node_type::drop_macro_t:
                return "drop_macro_t";
            case node_type::checkpoint_t:
                return "checkpoint_t";
            case node_type::vacuum_t:
                return "vacuum_t";
            case node_type::having_t:
                return "having_t";
            default:
                return "unused";
        }
    }

    namespace aggregate {

        operator_type get_aggregate_type(const std::string& key) {
            if (key == "count") {
                return operator_type::count;
            } else if (key == "group") {
                return operator_type::group;
            } else if (key == "limit") {
                return operator_type::limit;
            } else if (key == "match") {
                return operator_type::match;
            } else if (key == "merge") {
                return operator_type::merge;
            } else if (key == "out") {
                return operator_type::out;
            } else if (key == "project") {
                return operator_type::project;
            } else if (key == "skip") {
                return operator_type::skip;
            } else if (key == "sort") {
                return operator_type::sort;
            } else if (key == "unset") {
                return operator_type::unset;
            } else if (key == "unwind") {
                return operator_type::unwind;
            } else if (key == "finish") {
                return operator_type::finish;
            } else {
                return aggregate::operator_type::invalid;
            }
        }

    } // namespace aggregate

} // namespace components::logical_plan