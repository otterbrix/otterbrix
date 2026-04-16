#include "json_insert.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <sql/parser/pg_functions.h>

#include <list>
#include <optional>
#include <unordered_map>

using namespace components::expressions;

namespace components::sql::transform {

    namespace {
        // Detect `json('<literal>')` FuncCall. Returns the JSON string if matched.
        // Returns empty optional otherwise (not a json() call).
        std::optional<std::string_view> match_json_call(Node* node) {
            if (nodeTag(node) != T_FuncCall) return std::nullopt;
            auto* fc = pg_ptr_cast<FuncCall>(node);
            if (!fc->funcname || fc->funcname->lst.empty()) return std::nullopt;
            std::string_view fname = strVal(fc->funcname->lst.back().data);
            if (fname != "json") return std::nullopt;
            if (!fc->args || fc->args->lst.size() != 1) {
                throw parser_exception_t{"json() expects exactly one string literal argument", {}};
            }
            auto* arg = pg_ptr_cast<Node>(fc->args->lst.front().data);
            if (nodeTag(arg) != T_A_Const) {
                throw parser_exception_t{"json() argument must be a string literal", {}};
            }
            auto* aconst = pg_ptr_cast<A_Const>(arg);
            if (nodeTag(&aconst->val) != T_String) {
                throw parser_exception_t{"json() argument must be a string literal", {}};
            }
            return std::string_view{strVal(&aconst->val)};
        }

        // Build a node_insert_t from a list of `json(...)` rows. Each row is expected to
        // contain exactly one element, a FuncCall to json() wrapping a string literal.
        //
        // Output chunk:
        //   - one column per distinct flattened JSON path across all rows
        //   - column alias = dotted path (e.g. "a.b.c"), which the computed-schema
        //     dispatcher uses as the field_name when evolving the schema
        //   - column type = type of the first non-null value seen for that path
        //   - rows missing a field leave it NULL (validity bit unset)
        template<typename Lst>
        logical_plan::node_ptr build_json_insert(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& coll_name,
                                                 Lst& vals) {
            // Phase 1: parse & flatten every row; discover column layout.
            std::vector<std::vector<json_field_t>> parsed;
            parsed.reserve(vals.size());

            std::unordered_map<std::string, size_t> path_to_idx;
            std::vector<std::string> path_names;
            std::vector<types::complex_logical_type> col_types;

            for (auto& row : vals) {
                auto row_values = pg_ptr_cast<List>(row.data)->lst;
                if (row_values.size() != 1) {
                    throw parser_exception_t{
                        "json() INSERT row must contain exactly one json('...') value", {}};
                }
                auto maybe_json = match_json_call(pg_ptr_cast<Node>(row_values.front().data));
                if (!maybe_json.has_value()) {
                    throw parser_exception_t{
                        "mixing json() rows with regular VALUES rows is not supported", {}};
                }

                std::vector<json_field_t> fields;
                try {
                    fields = flatten_json_object(resource, *maybe_json);
                } catch (const std::exception& e) {
                    throw parser_exception_t{std::string{"json() parse error: "} + e.what(), {}};
                }

                for (const auto& f : fields) {
                    auto it = path_to_idx.find(std::string{f.path.c_str(), f.path.size()});
                    if (it == path_to_idx.end()) {
                        size_t idx = col_types.size();
                        std::string key{f.path.c_str(), f.path.size()};
                        path_to_idx.emplace(key, idx);
                        path_names.push_back(key);
                        col_types.push_back(f.value.type());
                    } else {
                        // Require consistent types across rows for the same path.
                        if (col_types[it->second].type() != f.value.type().type()) {
                            throw parser_exception_t{
                                "json() field '" + std::string{f.path.c_str(), f.path.size()} +
                                    "' has inconsistent types across rows",
                                {}};
                        }
                    }
                }
                parsed.push_back(std::move(fields));
            }

            // Phase 2: build chunk with NULL-initialized columns.
            vector::data_chunk_t chunk(resource, {}, vals.size());
            chunk.set_cardinality(vals.size());
            chunk.data.reserve(col_types.size());
            for (size_t i = 0; i < col_types.size(); ++i) {
                auto typ = col_types[i];
                typ.set_alias(path_names[i]);
                chunk.data.emplace_back(resource, typ, chunk.capacity());
                chunk.data.back().validity().set_all_invalid(vals.size());
            }

            // Phase 3: fill values; set_value flips the validity bit on its own.
            for (size_t row_idx = 0; row_idx < parsed.size(); ++row_idx) {
                for (auto& f : parsed[row_idx]) {
                    auto it = path_to_idx.find(std::string{f.path.c_str(), f.path.size()});
                    chunk.set_value(it->second, row_idx, f.value);
                }
            }

            // key_translation stays empty: the computed-schema dispatcher treats each
            // column's alias as the target field_name directly.
            std::pmr::vector<expressions::key_t> key_translation(resource);
            return logical_plan::make_node_insert(resource,
                                                  coll_name,
                                                  std::move(chunk),
                                                  std::move(key_translation));
        }

    } // namespace

    logical_plan::node_ptr transformer::transform_insert(InsertStmt& node, logical_plan::parameter_node_t* params) {
        // Intercept `INSERT INTO t VALUES (json('...')), ...` before the usual column-aligned
        // path. A json() VALUES form has no INSERT column list and each row holds a single
        // FuncCall to json(). The JSON builder derives columns from the union of keys.
        if (pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists && (!node.cols || node.cols->lst.empty())) {
            auto vals = pg_ptr_cast<List>(pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists)->lst;
            bool all_json = !vals.empty();
            for (auto& row : vals) {
                auto row_values = pg_ptr_cast<List>(row.data)->lst;
                if (row_values.size() != 1) { all_json = false; break; }
                if (!match_json_call(pg_ptr_cast<Node>(row_values.front().data)).has_value()) {
                    all_json = false;
                    break;
                }
            }
            if (all_json) {
                return build_json_insert(resource_, rangevar_to_collection(node.relation), vals);
            }
        }

        auto fields = pg_ptr_cast<List>(node.cols)->lst;
        std::pmr::vector<expressions::key_t> key_translation(resource_);
        for (const auto& field : fields) {
            auto target = pg_ptr_cast<ResTarget>(field.data);
            if (target->indirection->lst.empty()) {
                key_translation.emplace_back(resource_, target->name);
            } else {
                auto key = expressions::key_t{
                    std::pmr::vector<std::pmr::string>{{target->name, resource_},
                                                       pmrStrVal(target->indirection->lst.back().data, resource_)}};
                key_translation.emplace_back(std::move(key));
            }
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
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == it_field->as_string();
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.set_alias(it_field->as_string());
                            chunk.data.emplace_back(resource_, value.type(), chunk.capacity());
                        }
                        chunk.set_value(column_index, row_index, std::move(value));
                    } else {
                        auto value = get_value(resource_, pg_ptr_cast<Node>(it_value->data));
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == it_field->as_string();
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.set_alias(it_field->as_string());
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
            res->key_translation() = key_translation;
            return res;
        }
    }
} // namespace components::sql::transform
