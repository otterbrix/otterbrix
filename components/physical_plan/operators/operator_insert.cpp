#include "operator_insert.hpp"
#include <components/table/data_table.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/collection/collection.hpp>
#include <unordered_set>

namespace components::operators {

    operator_insert::operator_insert(services::collection::context_collection_t* context)
        : read_write_operator_t(context, operator_type::insert) {}

    void operator_insert::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            auto& incoming = left_->output()->data_chunk();
            if (context_->table_storage().table().columns().empty() && incoming.column_count() > 0) {
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

            auto& table = context_->table_storage().table();
            modified_ = operators::make_operator_write_data(context_->resource());
            output_ = operators::make_operator_data(context_->resource(), incoming.types(), incoming.size());
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
        }
    }

} // namespace components::operators