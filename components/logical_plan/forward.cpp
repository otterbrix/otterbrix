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
            case node_type::create_matview_t:
                return "create_matview_t";
            case node_type::refresh_matview_t:
                return "refresh_matview_t";
            case node_type::checkpoint_t:
                return "checkpoint_t";
            case node_type::vacuum_t:
                return "vacuum_t";
            case node_type::having_t:
                return "having_t";
            case node_type::alter_table_t:
                return "alter_table_t";
            case node_type::create_constraint_t:
                return "create_constraint_t";
            case node_type::check_constraint_t:
                return "check_constraint_t";
            case node_type::fk_check_t:
                return "fk_check_t";
            case node_type::fk_cascade_t:
                return "fk_cascade_t";
            case node_type::sequence_t:
                return "sequence_t";
            case node_type::primitive_write_t:
                return "primitive_write_t";
            case node_type::primitive_delete_t:
                return "primitive_delete_t";
            case node_type::alter_column_add_t:
                return "alter_column_add_t";
            case node_type::alter_column_rename_t:
                return "alter_column_rename_t";
            case node_type::alter_column_drop_t:
                return "alter_column_drop_t";
            case node_type::dynamic_cascade_delete_t:
                return "dynamic_cascade_delete_t";
            case node_type::register_udf_t:
                return "register_udf_t";
            case node_type::unregister_udf_t:
                return "unregister_udf_t";
            case node_type::commit_transaction_t:
                return "commit_transaction_t";
            case node_type::abort_transaction_t:
                return "abort_transaction_t";
            case node_type::begin_transaction_t:
                return "begin_transaction_t";
            case node_type::computed_field_register_t:
                return "computed_field_register_t";
            case node_type::computed_field_unregister_t:
                return "computed_field_unregister_t";
            case node_type::catalog_resolve_table_t:
                return "catalog_resolve_table_t";
            case node_type::catalog_resolve_namespace_t:
                return "catalog_resolve_namespace_t";
            case node_type::catalog_resolve_database_t:
                return "catalog_resolve_database_t";
            case node_type::catalog_resolve_type_t:
                return "catalog_resolve_type_t";
            case node_type::catalog_resolve_function_t:
                return "catalog_resolve_function_t";
            case node_type::catalog_resolve_constraint_t:
                return "catalog_resolve_constraint_t";
            case node_type::allocate_oids_t:
                return "allocate_oids_t";
            case node_type::set_timezone_t:
                return "set_timezone_t";
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