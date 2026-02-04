#include "operator_insert.hpp"
#include <services/collection/collection.hpp>

namespace components::table::operators {

    operator_insert::operator_insert(services::collection::context_collection_t* context)
        : read_write_operator_t(context, operator_type::insert) {}

    void operator_insert::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            modified_ = base::operators::make_operator_write_data(context_->resource());
            output_ = base::operators::make_operator_data(context_->resource(),
                                                          left_->output()->data_chunk().types(),
                                                          left_->output()->data_chunk().size());
            table::table_append_state state(context_->resource());
            context_->table_storage().table().append_lock(state);
            context_->table_storage().table().initialize_append(state);
            for (size_t id = 0; id < left_->output()->data_chunk().size(); id++) {
                modified_->append(id + static_cast<size_t>(state.row_start));
                context_->index_engine()->insert_row(left_->output()->data_chunk(),
                                                     id + static_cast<size_t>(state.row_start),
                                                     pipeline_context);
            }
            context_->table_storage().table().append(left_->output()->data_chunk(), state);
            context_->table_storage().table().finalize_append(state);
            left_->output()->data_chunk().copy(output_->data_chunk(), 0);
        }
    }

} // namespace components::table::operators
