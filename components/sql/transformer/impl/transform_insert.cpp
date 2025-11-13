#include <components/expressions/aggregate_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <sql/parser/pg_functions.h>

using namespace components::expressions;

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_insert(InsertStmt& node, logical_plan::parameter_node_t* params) {
        auto fields = pg_ptr_cast<List>(node.cols)->lst;
        std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>> key_translation(resource_);
        for (const auto& field : fields) {
            auto name = pg_ptr_cast<ResTarget>(field.data)->name;
            key_translation.emplace_back(name, name);
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
                    throw parser_exception_t{"INSERT has more expressions than target columns", {}};
                }

                auto it_field = key_translation.begin();
                for (auto it_value = values.begin(); it_value != values.end(); ++it_field, ++it_value) {
                    if (nodeTag(it_value->data) == T_ParamRef) {
                        has_params = true;
                        auto ref = pg_ptr_cast<ParamRef>(it_value->data);
                        auto loc = std::make_pair(row_index, it_field->first.as_string());

                        if (auto it = parameter_insert_map_.find(ref->number); it != parameter_insert_map_.end()) {
                            it->second.emplace_back(std::move(loc));
                        } else {
                            std::pmr::vector<insert_location_t> par(resource_);
                            par.emplace_back(std::move(loc));
                            parameter_insert_map_.emplace(ref->number, std::move(par));
                        }
                    } else {
                        auto value = get_value(pg_ptr_cast<Node>(it_value->data));
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == it_field->first.as_string();
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.set_alias(it_field->first.as_string());
                            chunk.data.emplace_back(resource_, value.type(), chunk.capacity());
                        }
                        chunk.set_value(column_index, row_index, std::move(value));
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
            for (const auto& aggregate_child : res->children().back()->children()) {
                if (aggregate_child->type() != logical_plan::node_type::group_t) {
                    continue;
                }

                std::pmr::vector<expressions::key_t> select_fields;
                select_fields.reserve(key_translation.size());
                for (const auto& expr : aggregate_child->expressions()) {
                    if (expr->group() == expression_group::scalar) {
                        select_fields.emplace_back(reinterpret_cast<const scalar_expression_ptr&>(expr)->key());
                    }
                }
                if (key_translation.size() != select_fields.size()) {
                    throw parser_exception_t("Insert column count has to be equal to Select column count",
                                             "Or use SELSECT * FROM");
                }
                assert(key_translation.size() == select_fields.size());
                for (size_t i = 0; i < key_translation.size(); i++) {
                    key_translation[i].second = select_fields[i];
                }
            }
            res->key_translation() = key_translation;
            return res;
        }
    }
} // namespace components::sql::transform
