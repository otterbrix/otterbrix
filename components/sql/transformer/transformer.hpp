#pragma once

#include "transform_result.hpp"
#include "utils.hpp"

#include <components/expressions/compare_expression.hpp>
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

        // Arithmetic expression: returns scalar_expression_t
        expressions::expression_ptr
        transform_a_expr_arithmetic(A_Expr* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        // Resolve any node to param_storage for arithmetic operand
        expressions::param_storage
        transform_a_expr_operand(Node* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        // Handle T_A_Expr in SELECT target list (may contain aggregates)
        void transform_select_a_expr(A_Expr* node,
                                     const char* alias,
                                     const name_collection_t& names,
                                     logical_plan::parameter_node_t* params,
                                     logical_plan::node_ptr& group);

        // Resolve SELECT operand — aggregates become separate group expressions
        expressions::param_storage
        resolve_select_operand(Node* node,
                               const name_collection_t& names,
                               logical_plan::parameter_node_t* params,
                               logical_plan::node_ptr& group);

        expressions::expression_ptr
        transform_a_expr_func(FuncCall* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        // HAVING clause: resolve aggregate references to aliases from group node
        expressions::expression_ptr
        transform_having_expr(Node* node,
                              const name_collection_t& names,
                              logical_plan::parameter_node_t* params,
                              const logical_plan::node_ptr& group);

        // Handle T_CaseExpr in SELECT target list
        void transform_select_case_expr(CaseExpr* node,
                                        const char* alias,
                                        const name_collection_t& names,
                                        logical_plan::parameter_node_t* params,
                                        logical_plan::node_ptr& group);

        // Resolve a HAVING operand: FuncCall → aggregate alias key
        expressions::param_storage
        resolve_having_operand(Node* node,
                               const name_collection_t& names,
                               logical_plan::parameter_node_t* params,
                               const logical_plan::node_ptr& group);

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
                      name_collection_t& names,
                      logical_plan::parameter_node_t* params);

        expressions::update_expr_ptr
        transform_update_expr(Node* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        std::string get_str_value(Node* node);

        core::parameter_id_t add_param_value(Node* node, logical_plan::parameter_node_t* params);

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<size_t, core::parameter_id_t> parameter_map_;
        std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>> parameter_insert_map_;
        vector::data_chunk_t parameter_insert_rows_;
        size_t aggregate_counter_{0};
    };
} // namespace components::sql::transform