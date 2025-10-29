#pragma once

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
            , parameter_map_(resource_) {}

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

    private:
        using insert_location_t = std::pair<size_t, std::string>; // position in vector + string key

        expressions::compare_expression_ptr transform_a_expr(logical_plan::parameter_node_t* params,
                                                             A_Expr* node,
                                                             logical_plan::node_ptr* func_node = nullptr);

        components::expressions::compare_expression_ptr transform_a_indirection(logical_plan::parameter_node_t* params,
                                                                                A_Indirection* node);

        logical_plan::node_ptr transform_function(RangeFunction& node, logical_plan::parameter_node_t* params);
        logical_plan::node_ptr transform_function(FuncCall& node, logical_plan::parameter_node_t* params);

        void join_dfs(std::pmr::memory_resource* resource,
                      JoinExpr* join,
                      logical_plan::node_join_ptr& node_join,
                      logical_plan::parameter_node_t* params);

        expressions::update_expr_ptr transform_update_expr(Node* node,
                                                           const collection_full_name_t& to,
                                                           const collection_full_name_t& from,
                                                           logical_plan::parameter_node_t* params);

        std::string get_str_value(Node* node);
        document::value_t get_value(Node* node, document::impl::base_document* tape);

        core::parameter_id_t add_param_value(Node* node, logical_plan::parameter_node_t* params);

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<size_t, core::parameter_id_t> parameter_map_;
        std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>> parameter_insert_map_;
        std::pmr::vector<components::document::document_ptr> parameter_insert_docs_;
    };
} // namespace components::sql::transform