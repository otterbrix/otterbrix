#include "transfer_scan.hpp"

#include <services/collection/collection.hpp>

namespace components::operators {

    transfer_scan::transfer_scan(services::collection::context_collection_t* context, logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , limit_(limit) {}

    void transfer_scan::on_execute_impl(pipeline::context_t*) {
        trace(context_->log(), "transfer_scan");
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }

        auto types = context_->table_storage().table().copy_types();
        output_ = operators::make_operator_data(context_->resource(), types);
        std::vector<table::storage_index_t> column_indices;
        column_indices.reserve(context_->table_storage().table().column_count());
        for (size_t i = 0; i < context_->table_storage().table().column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table::table_scan_state state(context_->resource());
        context_->table_storage().table().initialize_scan(state, column_indices);
        // TODO: check limit inside scan
        context_->table_storage().table().scan(output_->data_chunk(), state);
        if (limit_.limit() >= 0) {
            output_->data_chunk().set_cardinality(
                std::min(output_->data_chunk().size(), static_cast<uint64_t>(limit_.limit())));
        }
    }

} // namespace components::operators
