#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <sql/parser/pg_functions.h>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_insert(InsertStmt& node, logical_plan::parameter_node_t* params) {
        auto fields = pg_ptr_cast<List>(node.cols)->lst;
        std::pmr::vector<expressions::key_t> key_translation(resource_);
        for (const auto& field : fields) {
            auto target = pg_ptr_cast<ResTarget>(field.data);
            if (target->indirection->lst.empty()) {
                key_translation.emplace_back(resource_, target->name);
            } else {
                auto key = expressions::key_t{
                    std::pmr::vector<std::pmr::string>{{std::pmr::string{target->name, resource_},
                                                        pmrStrVal(target->indirection->lst.back().data, resource_)},
                                                       resource_}};
                key_translation.emplace_back(std::move(key));
            }
        }
        if (!node.selectStmt) {
            return logical_plan::make_node_insert(resource_,
                                                  rangevar_to_collection(node.relation),
                                                  std::move(vector::data_chunk_t{resource_, {}, 0}),
                                                  std::move(key_translation));
        }
        if (pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists) {
            auto vals = pg_ptr_cast<List>(pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists)->lst;

            vector::data_chunk_t chunk(resource_, {}, vals.size());
            chunk.set_cardinality(vals.size());
            size_t row_index = 0;
            bool has_params = false;

            for (auto row : vals) {
                auto values = pg_ptr_cast<List>(row.data)->lst;
                if (values.size() != fields.size()) {
                    error_ =
                        core::error_t(core::error_code_t::sql_parse_error,

                                      std::pmr::string{"INSERT has more expressions than target columns", resource_});
                    return nullptr;
                }

                auto it_field = key_translation.begin();
                for (auto it_value = values.begin(); it_value != values.end(); ++it_field, ++it_value) {
                    if (nodeTag(it_value->data) == T_ParamRef) {
                        has_params = true;
                        auto ref = pg_ptr_cast<ParamRef>(it_value->data);
                        auto loc = std::make_pair(row_index, it_field->as_string());

                        if (auto it = parameter_insert_map_.find(ref->number); it != parameter_insert_map_.end()) {
                            it->second.emplace_back(std::move(loc));
                        } else {
                            std::pmr::vector<insert_location_t> par(resource_);
                            par.emplace_back(std::move(loc));
                            parameter_insert_map_.emplace(ref->number, std::move(par));
                        }
                    } else if (nodeTag(it_value->data) == T_A_Expr) {
                        // Evaluate constant arithmetic at parse time
                        // TODO: move column matching to validation/optimizer phase for complex path resolution
                        auto value = evaluate_const_a_expr(resource_, pg_ptr_cast<A_Expr>(it_value->data));
                        if (value.has_error()) {
                            error_ = value.error();
                            return nullptr;
                        }
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == it_field->as_string();
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.value().set_alias(it_field->as_string());
                            chunk.data.emplace_back(resource_, value.value().type(), chunk.capacity());
                        }
                        chunk.set_value(column_index, row_index, std::move(value.value()));
                    } else {
                        auto value = get_value(resource_, pg_ptr_cast<Node>(it_value->data));
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == it_field->as_string();
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.value().set_alias(it_field->as_string());
                            chunk.data.emplace_back(resource_, value.value().type(), chunk.capacity());
                        }
                        chunk.set_value(column_index, row_index, std::move(value.value()));
                    }
                }
                row_index++;
            }

            if (has_params) {
                parameter_insert_rows_ = std::move(chunk);
                return logical_plan::make_node_insert(resource_,
                                                      rangevar_to_collection(node.relation),
                                                      vector::data_chunk_t(resource_, {}, 0),
                                                      std::move(key_translation));
            } else {
                return logical_plan::make_node_insert(resource_,
                                                      rangevar_to_collection(node.relation),
                                                      std::move(chunk),
                                                      std::move(key_translation));
            }
        } else {
            auto res = logical_plan::make_node_insert(resource_, rangevar_to_collection(node.relation));
            res->append_child(transform_select(*pg_ptr_cast<SelectStmt>(node.selectStmt), params));
            res->key_translation() = key_translation;
            return res;
        }
    }
} // namespace components::sql::transform
