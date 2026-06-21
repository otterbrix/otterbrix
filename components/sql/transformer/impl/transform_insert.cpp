#include <algorithm>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <sql/parser/pg_functions.h>

using namespace components::expressions;

namespace {

    // Converts a numeric logical_value_t to a different numeric type without cast_as (no timezone needed).
    // Both val.type().type() and target must satisfy is_numeric().
    components::types::logical_value_t numeric_widen(std::pmr::memory_resource* resource,
                                                     const components::types::logical_value_t& val,
                                                     components::types::logical_type target) {
        using LT = components::types::logical_type;
        if (val.type().type() == target) {
            return val;
        }
        bool target_is_float = (target == LT::DOUBLE || target == LT::FLOAT);
        if (target_is_float) {
            double dbl;
            switch (val.type().type()) {
                case LT::BOOLEAN:
                    dbl = static_cast<double>(val.value<bool>());
                    break;
                case LT::TINYINT:
                    dbl = static_cast<double>(val.value<int8_t>());
                    break;
                case LT::SMALLINT:
                    dbl = static_cast<double>(val.value<int16_t>());
                    break;
                case LT::INTEGER:
                    dbl = static_cast<double>(val.value<int32_t>());
                    break;
                case LT::BIGINT:
                    dbl = static_cast<double>(val.value<int64_t>());
                    break;
                case LT::UTINYINT:
                    dbl = static_cast<double>(val.value<uint8_t>());
                    break;
                case LT::USMALLINT:
                    dbl = static_cast<double>(val.value<uint16_t>());
                    break;
                case LT::UINTEGER:
                    dbl = static_cast<double>(val.value<uint32_t>());
                    break;
                case LT::UBIGINT:
                    dbl = static_cast<double>(val.value<uint64_t>());
                    break;
                case LT::FLOAT:
                    dbl = static_cast<double>(val.value<float>());
                    break;
                default:
                    assert(false && "numeric_widen: unsupported source for float target");
                    return val;
            }
            if (target == LT::DOUBLE) {
                return components::types::logical_value_t(resource, dbl);
            }
            return components::types::logical_value_t(resource, static_cast<float>(dbl));
        } else if (components::types::is_unsigned(target)) {
            uint64_t uval;
            switch (val.type().type()) {
                case LT::BOOLEAN:
                    uval = static_cast<uint64_t>(val.value<bool>());
                    break;
                case LT::TINYINT:
                    uval = static_cast<uint64_t>(val.value<int8_t>());
                    break;
                case LT::SMALLINT:
                    uval = static_cast<uint64_t>(val.value<int16_t>());
                    break;
                case LT::INTEGER:
                    uval = static_cast<uint64_t>(val.value<int32_t>());
                    break;
                case LT::BIGINT:
                    uval = static_cast<uint64_t>(val.value<int64_t>());
                    break;
                case LT::UTINYINT:
                    uval = static_cast<uint64_t>(val.value<uint8_t>());
                    break;
                case LT::USMALLINT:
                    uval = static_cast<uint64_t>(val.value<uint16_t>());
                    break;
                case LT::UINTEGER:
                    uval = static_cast<uint64_t>(val.value<uint32_t>());
                    break;
                case LT::UBIGINT:
                    uval = val.value<uint64_t>();
                    break;
                default:
                    assert(false && "numeric_widen: unsupported source for unsigned target");
                    return val;
            }
            switch (target) {
                case LT::UTINYINT:
                    return components::types::logical_value_t(resource, static_cast<uint8_t>(uval));
                case LT::USMALLINT:
                    return components::types::logical_value_t(resource, static_cast<uint16_t>(uval));
                case LT::UINTEGER:
                    return components::types::logical_value_t(resource, static_cast<uint32_t>(uval));
                case LT::UBIGINT:
                    return components::types::logical_value_t(resource, uval);
                default:
                    assert(false && "numeric_widen: unsupported unsigned target");
                    return val;
            }
        } else {
            // Signed integer target
            int64_t ival;
            switch (val.type().type()) {
                case LT::BOOLEAN:
                    ival = static_cast<int64_t>(val.value<bool>());
                    break;
                case LT::TINYINT:
                    ival = static_cast<int64_t>(val.value<int8_t>());
                    break;
                case LT::SMALLINT:
                    ival = static_cast<int64_t>(val.value<int16_t>());
                    break;
                case LT::INTEGER:
                    ival = static_cast<int64_t>(val.value<int32_t>());
                    break;
                case LT::BIGINT:
                    ival = val.value<int64_t>();
                    break;
                case LT::UTINYINT:
                    ival = static_cast<int64_t>(val.value<uint8_t>());
                    break;
                case LT::USMALLINT:
                    ival = static_cast<int64_t>(val.value<uint16_t>());
                    break;
                case LT::UINTEGER:
                    ival = static_cast<int64_t>(val.value<uint32_t>());
                    break;
                case LT::UBIGINT:
                    ival = static_cast<int64_t>(val.value<uint64_t>());
                    break;
                case LT::FLOAT:
                    ival = static_cast<int64_t>(val.value<float>());
                    break;
                case LT::DOUBLE:
                    ival = static_cast<int64_t>(val.value<double>());
                    break;
                default:
                    assert(false && "numeric_widen: unsupported source for signed target");
                    return val;
            }
            switch (target) {
                case LT::BOOLEAN:
                    return components::types::logical_value_t(resource, static_cast<bool>(ival));
                case LT::TINYINT:
                    return components::types::logical_value_t(resource, static_cast<int8_t>(ival));
                case LT::SMALLINT:
                    return components::types::logical_value_t(resource, static_cast<int16_t>(ival));
                case LT::INTEGER:
                    return components::types::logical_value_t(resource, static_cast<int32_t>(ival));
                case LT::BIGINT:
                    return components::types::logical_value_t(resource, ival);
                default:
                    assert(false && "numeric_widen: unsupported signed target");
                    return val;
            }
        }
    }

    // Promotes an existing column vector to a wider numeric type, converting all stored values.
    // Caller must ensure col.type().type() and promoted are both is_numeric(), and promoted != col.type().type().
    components::vector::vector_t promote_column(std::pmr::memory_resource* resource,
                                                const components::vector::vector_t& col,
                                                size_t num_rows,
                                                components::types::logical_type promoted,
                                                uint64_t capacity) {
        components::vector::vector_t new_col(
            resource,
            components::types::complex_logical_type{promoted, std::string(col.type().alias())},
            capacity);
        for (size_t row = 0; row < num_rows; ++row) {
            if (col.is_null(row)) {
                new_col.set_null(row, true);
            } else {
                new_col.set_value(row, numeric_widen(resource, col.value(row), promoted));
            }
        }
        return new_col;
    }

} // anonymous namespace

namespace components::sql::transform {
    logical_plan::node_ptr transformer::transform_insert(InsertStmt& node, logical_plan::execution_plan_t* plan) {
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
        // RETURNING projection (references the target table's columns). Parsed
        // once and attached to whichever insert node this statement lowers to.
        std::pmr::vector<expressions::expression_ptr> returning(resource_);
        if (node.returningList) {
            name_collection_t rnames;
            rnames.left_name = rangevar_to_qualified_name(node.relation);
            rnames.left_alias = construct_alias(node.relation->alias);
            returning = transform_returning(node.returningList, rnames, plan);
            if (error_.contains_error()) {
                return nullptr;
            }
        }

        if (!node.selectStmt) {
            auto qn = rangevar_to_qualified_name(node.relation);
            auto res = logical_plan::make_node_insert(resource_);
            res->append_child(transform_select(*pg_ptr_cast<SelectStmt>(node.selectStmt), plan));
            res->key_translation() = key_translation;
            res->returning() = returning;
            return maybe_wrap_with_catalog_resolve_table(resource_,
                                                         qn.dbname,
                                                         qn.relname,
                                                         std::move(res),
                                                         constraint_resolve_kind::outgoing);
            return logical_plan::make_node_insert(resource_,
                                                  std::move(vector::data_chunk_t{resource_, {}, 0}),
                                                  std::move(key_translation));
        }
        if (pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists) {
            auto vals = pg_ptr_cast<List>(pg_ptr_cast<SelectStmt>(node.selectStmt)->valuesLists)->lst;

            // A parameterised INSERT binds rows by absolute index in transform_result,
            // which materialises them into a single working chunk — so keep one chunk for
            // that path. A literal INSERT splits into ≤DEFAULT_VECTOR_CAPACITY chunks so no
            // oversized data_chunk_t is ever built. Detect the case up front.
            bool has_params = false;
            for (auto row : vals) {
                for (auto value : pg_ptr_cast<List>(row.data)->lst) {
                    if (nodeTag(value.data) == T_ParamRef) {
                        has_params = true;
                        break;
                    }
                }
                if (has_params) {
                    break;
                }
            }

            // Fills one row of `chunk` at chunk-local index `chunk_row` from the value list
            // of global row `global_row`. Discovers/promotes columns in `chunk` as it goes
            // and records ParamRef slots (keyed by global row) in parameter_insert_map_.
            // Returns false (and sets error_) on a malformed row.
            auto fill_row =
                [&](vector::data_chunk_t& chunk, size_t chunk_row, size_t global_row, List* values_list) -> bool {
                auto values = values_list->lst;
                if (values.size() != fields.size()) {
                    error_ =
                        core::error_t(core::error_code_t::sql_parse_error,
                                      std::pmr::string{"INSERT has more expressions than target columns", resource_});
                    return false;
                }

                auto it_field = key_translation.begin();
                for (auto it_value = values.begin(); it_value != values.end(); ++it_field, ++it_value) {
                    if (nodeTag(it_value->data) == T_ParamRef) {
                        auto ref = pg_ptr_cast<ParamRef>(it_value->data);
                        auto loc = std::make_pair(global_row, it_field->as_string());

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
                            return false;
                        }
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == it_field->as_string();
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.value().set_alias(it_field->as_string());
                            chunk.data.emplace_back(resource_, value.value().type(), chunk.capacity());
                            chunk.set_value(column_index, chunk_row, std::move(value.value()));
                        } else {
                            auto col_type = it->type().type();
                            auto val_type = value.value().type().type();
                            if (types::is_numeric(col_type) && types::is_numeric(val_type) && col_type != val_type) {
                                auto promoted = types::promote_type(col_type, val_type);
                                if (promoted != col_type) {
                                    chunk.data[column_index] =
                                        promote_column(resource_, *it, chunk_row, promoted, chunk.capacity());
                                }
                                chunk.set_value(column_index,
                                                chunk_row,
                                                numeric_widen(resource_, value.value(), promoted));
                            } else {
                                chunk.set_value(column_index, chunk_row, std::move(value.value()));
                            }
                        }
                    } else {
                        auto value = get_value(resource_, pg_ptr_cast<Node>(it_value->data));
                        if (value.has_error()) {
                            error_ = value.error();
                            return false;
                        }
                        auto it =
                            std::find_if(chunk.data.begin(), chunk.data.end(), [&](const vector::vector_t& column) {
                                return column.type().alias() == it_field->as_string();
                            });
                        size_t column_index = it - chunk.data.begin();
                        if (it == chunk.data.end()) {
                            value.value().set_alias(it_field->as_string());
                            chunk.data.emplace_back(resource_, value.value().type(), chunk.capacity());
                            chunk.set_value(column_index, chunk_row, std::move(value.value()));
                        } else {
                            auto col_type = it->type().type();
                            auto val_type = value.value().type().type();
                            if (types::is_numeric(col_type) && types::is_numeric(val_type) && col_type != val_type) {
                                auto promoted = types::promote_type(col_type, val_type);
                                if (promoted != col_type) {
                                    chunk.data[column_index] =
                                        promote_column(resource_, *it, chunk_row, promoted, chunk.capacity());
                                }
                                chunk.set_value(column_index,
                                                chunk_row,
                                                numeric_widen(resource_, value.value(), promoted));
                            } else {
                                chunk.set_value(column_index, chunk_row, std::move(value.value()));
                            }
                        }
                    }
                }
                return true;
            };

            auto qn = rangevar_to_qualified_name(node.relation);
            // Identity travels via the catalog-resolve wrap; the insert node
            // itself carries only payload + table_oid() (stamped at enrich
            // time from the sibling resolve_table).
            logical_plan::node_ptr ins;
            if (has_params) {
                vector::data_chunk_t chunk(resource_, {}, vals.size() == 0 ? 1 : vals.size());
                chunk.set_cardinality(vals.size());
                size_t row_index = 0;
                for (auto row : vals) {
                    if (!fill_row(chunk, row_index, row_index, pg_ptr_cast<List>(row.data))) {
                        return nullptr;
                    }
                    row_index++;
                }
                parameter_insert_rows_ = std::move(chunk);
                ins = logical_plan::make_node_insert(resource_,
                                                     vector::data_chunk_t(resource_, {}, 0),
                                                     std::move(key_translation));
            } else {
                // Split the literal rows into uniform ≤CAP chunks (only the last is smaller).
                const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
                const uint64_t total = vals.size();
                std::pmr::vector<vector::data_chunk_t> chunks(resource_);
                auto row_it = vals.begin();
                uint64_t global_row = 0;
                while (global_row < total) {
                    const uint64_t batch = std::min<uint64_t>(cap, total - global_row);
                    vector::data_chunk_t chunk(resource_, {}, batch);
                    chunk.set_cardinality(batch);
                    for (uint64_t chunk_row = 0; chunk_row < batch; ++chunk_row, ++row_it, ++global_row) {
                        if (!fill_row(chunk, chunk_row, global_row, pg_ptr_cast<List>(row_it->data))) {
                            return nullptr;
                        }
                    }
                    chunks.emplace_back(std::move(chunk));
                }
                ins = logical_plan::make_node_insert(resource_, std::move(chunks), std::move(key_translation));
            }
            static_cast<logical_plan::node_insert_t*>(ins.get())->returning() = returning;
            return maybe_wrap_with_catalog_resolve_table(resource_,
                                                         qn.dbname,
                                                         qn.relname,
                                                         std::move(ins),
                                                         constraint_resolve_kind::outgoing);
        } else {
            auto qn = rangevar_to_qualified_name(node.relation);
            auto res = logical_plan::make_node_insert(resource_);
            res->append_child(transform_select(*pg_ptr_cast<SelectStmt>(node.selectStmt), plan));
            res->key_translation() = key_translation;
            res->returning() = returning;
            return maybe_wrap_with_catalog_resolve_table(resource_,
                                                         qn.dbname,
                                                         qn.relname,
                                                         std::move(res),
                                                         constraint_resolve_kind::outgoing);
        }
    }
} // namespace components::sql::transform
