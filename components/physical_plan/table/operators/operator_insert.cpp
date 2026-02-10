#include "operator_insert.hpp"
#include <components/table/data_table.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/collection/collection.hpp>
#include <unordered_set>

namespace components::table::operators {

    operator_insert::operator_insert(services::collection::context_collection_t* context)
        : read_write_operator_t(context, operator_type::insert) {}

    void operator_insert::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            auto& incoming = left_->output()->data_chunk();
            if (context_->table_storage().table().columns().empty() &&
                incoming.column_count() > 0) {
                context_->table_storage().table().adopt_schema(incoming.types());
            }

            auto& table_columns = context_->table_storage().table().columns();
            if (!table_columns.empty() && incoming.column_count() < table_columns.size()) {
                auto* resource = context_->resource();
                std::pmr::vector<types::complex_logical_type> full_types(resource);
                for (auto& col_def : table_columns) {
                    full_types.push_back(col_def.type());
                }

                std::vector<vector::vector_t> expanded_data;
                expanded_data.reserve(table_columns.size());
                for (size_t t = 0; t < table_columns.size(); t++) {
                    bool found = false;
                    for (uint64_t s = 0; s < incoming.column_count(); s++) {
                        if (incoming.data[s].type().has_alias() &&
                            incoming.data[s].type().alias() == table_columns[t].name()) {
                            expanded_data.push_back(std::move(incoming.data[s]));
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        expanded_data.emplace_back(resource, full_types[t], incoming.size());
                        expanded_data.back().validity().set_all_invalid(incoming.size());
                    }
                }
                incoming.data = std::move(expanded_data);
            }

            int64_t id_col = -1;
            for (uint64_t i = 0; i < incoming.column_count(); i++) {
                if (incoming.data[i].type().has_alias() && incoming.data[i].type().alias() == "_id") {
                    id_col = static_cast<int64_t>(i);
                    break;
                }
            }

            std::unordered_set<std::string> existing_ids;
            auto& table = context_->table_storage().table();
            auto total_rows = table.row_group()->total_rows();
            if (id_col >= 0 && total_rows > 0) {
                table.scan_table_segment(0, total_rows, [&](vector::data_chunk_t& chunk) {
                    int64_t table_id_col = -1;
                    for (uint64_t i = 0; i < chunk.column_count(); i++) {
                        if (chunk.data[i].type().has_alias() && chunk.data[i].type().alias() == "_id") {
                            table_id_col = static_cast<int64_t>(i);
                            break;
                        }
                    }
                    if (table_id_col >= 0) {
                        for (uint64_t row = 0; row < chunk.size(); row++) {
                            auto val = chunk.value(static_cast<uint64_t>(table_id_col), row);
                            if (val.type().type() == types::logical_type::STRING_LITERAL) {
                                existing_ids.insert(std::string(val.value<std::string_view>()));
                            }
                        }
                    }
                });
            }

            std::vector<uint64_t> keep_indices;
            if (id_col >= 0 && !existing_ids.empty()) {
                for (uint64_t row = 0; row < incoming.size(); row++) {
                    auto val = incoming.value(static_cast<uint64_t>(id_col), row);
                    if (val.type().type() == types::logical_type::STRING_LITERAL) {
                        std::string id_str(val.value<std::string_view>());
                        if (existing_ids.count(id_str) == 0) {
                            keep_indices.push_back(row);
                            existing_ids.insert(std::move(id_str));
                        }
                    } else {
                        keep_indices.push_back(row);
                    }
                }
            } else {
                for (uint64_t row = 0; row < incoming.size(); row++) {
                    keep_indices.push_back(row);
                }
            }

            if (keep_indices.empty()) {
                modified_ = base::operators::make_operator_write_data(context_->resource());
                output_ = base::operators::make_operator_data(context_->resource(),
                                                              incoming.types(), 0);
                return;
            }

            if (keep_indices.size() == incoming.size()) {
                modified_ = base::operators::make_operator_write_data(context_->resource());
                output_ = base::operators::make_operator_data(context_->resource(),
                                                              incoming.types(), incoming.size());
                table::table_append_state state(context_->resource());
                table.append_lock(state);
                table.initialize_append(state);
                for (size_t id = 0; id < incoming.size(); id++) {
                    modified_->append(id + static_cast<size_t>(state.row_start));
                    context_->index_engine()->insert_row(incoming,
                                                         id + static_cast<size_t>(state.row_start),
                                                         pipeline_context);
                }
                table.append(incoming, state);
                table.finalize_append(state);
                incoming.copy(output_->data_chunk(), 0);
            } else {
                auto* resource = context_->resource();
                vector::indexing_vector_t indexing(resource, keep_indices.size());
                for (size_t i = 0; i < keep_indices.size(); i++) {
                    indexing.set_index(i, keep_indices[i]);
                }
                vector::data_chunk_t filtered(resource, incoming.types(), keep_indices.size());
                incoming.copy(filtered, indexing, keep_indices.size(), 0);

                modified_ = base::operators::make_operator_write_data(resource);
                output_ = base::operators::make_operator_data(resource,
                                                              filtered.types(), filtered.size());
                table::table_append_state state(resource);
                table.append_lock(state);
                table.initialize_append(state);
                for (size_t id = 0; id < filtered.size(); id++) {
                    modified_->append(id + static_cast<size_t>(state.row_start));
                    context_->index_engine()->insert_row(filtered,
                                                         id + static_cast<size_t>(state.row_start),
                                                         pipeline_context);
                }
                table.append(filtered, state);
                table.finalize_append(state);
                filtered.copy(output_->data_chunk(), 0);
            }
        }
    }

} // namespace components::table::operators