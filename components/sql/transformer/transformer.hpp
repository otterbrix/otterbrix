#pragma once

#include "expressions/function_expression.hpp"
#include "transform_result.hpp"
#include "utils.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/nodes/parsenodes.h>

namespace components::sql::transform {
    class transformer {
    public:
        transformer(std::pmr::memory_resource* resource)
            : resource_(resource)
            , parameter_map_(resource_)
            , parameter_insert_rows_(resource_, {}) {}

        transform_result transform(Node& node);

    private:
        logical_plan::node_ptr transform_create_database(CreatedbStmt& node);
        logical_plan::node_ptr transform_drop_database(DropdbStmt& node);
        logical_plan::node_ptr transform_create_table(CreateStmt& node);
        logical_plan::node_ptr transform_drop(DropStmt& node);
        logical_plan::node_ptr transform_select(SelectStmt& node, logical_plan::parameter_node_t* params);
        logical_plan::node_ptr transform_update(UpdateStmt& node, logical_plan::parameter_node_t* params);
        logical_plan::node_ptr transform_insert(InsertStmt& node, logical_plan::parameter_node_t* params);
        logical_plan::node_ptr transform_delete(DeleteStmt& node, logical_plan::parameter_node_t* params);
        logical_plan::node_ptr transform_create_index(IndexStmt& node);
        logical_plan::node_ptr transform_create_type(CompositeTypeStmt& node);
        logical_plan::node_ptr transform_create_enum_type(CreateEnumStmt& node);

    private:
        using insert_location_t = std::pair<size_t, std::string>; // position in vector + string key

        expressions::expression_ptr
        transform_a_expr(A_Expr* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        expressions::expression_ptr
        transform_a_expr_func(FuncCall* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        expressions::expression_ptr transform_a_indirection(A_Indirection* node,
                                                            const name_collection_t& names,
                                                            logical_plan::parameter_node_t* params);

        logical_plan::node_ptr
        transform_function(RangeFunction& node, const name_collection_t& names, logical_plan::parameter_node_t* params);
        logical_plan::node_ptr
        transform_function(FuncCall& node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        void join_dfs(std::pmr::memory_resource* resource,
                      JoinExpr* join,
                      logical_plan::node_join_ptr& node_join,
                      const name_collection_t& names,
                      logical_plan::parameter_node_t* params);

        expressions::update_expr_ptr
        transform_update_expr(Node* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        std::string get_str_value(Node* node);
        types::logical_value_t get_value(Node* node);

        core::parameter_id_t add_param_value(Node* node, logical_plan::parameter_node_t* params);

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<size_t, core::parameter_id_t> parameter_map_;
        std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>> parameter_insert_map_;
        vector::data_chunk_t parameter_insert_rows_;
    };
} // namespace components::sql::transform