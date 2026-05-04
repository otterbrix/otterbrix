#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;


    uint64_t manager_disk_t::direct_append_sync(const collection_full_name_t& name,
                                                 components::vector::data_chunk_t& data) {
        // Default: committed-at-txn=0 (WAL replay / bootstrap path).
        return direct_append_sync(name, data, components::table::transaction_data{0, 0});
    }

    uint64_t manager_disk_t::direct_append_sync(const collection_full_name_t& name,
                                                 components::vector::data_chunk_t& data,
                                                 const components::table::transaction_data& txn) {
        auto* s = get_storage(name);
        if (!s || data.size() == 0)
            return 0;

        // Rebuild data with storage-compatible resource
        auto local = rebuild_chunk(resource(), data);

        // Schema adoption for computing tables
        if (!s->has_schema() && local.column_count() > 0) {
            s->adopt_schema(local.types());
        }

        // Column expansion
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && local.column_count() < table_columns.size()) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < local.column_count(); col++) {
                    if (local.data[col].type().has_alias() &&
                        local.data[col].type().alias() == table_columns[t].name()) {
                        expanded_data.push_back(std::move(local.data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    expanded_data.emplace_back(resource(), full_types[t], local.size());
                    expanded_data.back().validity().set_all_invalid(local.size());
                }
            }
            local.data = std::move(expanded_data);
        }

        // Type promotion for numeric columns
        if (s->has_schema() && !table_columns.empty()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            for (size_t i = 0; i < table_columns.size() && i < local.column_count(); i++) {
                auto src_type = local.data[i].type().type();
                auto tgt_type = table_columns[i].type().type();
                if (src_type != tgt_type && (is_numeric(src_type) || src_type == logical_type::STRING_LITERAL) &&
                    (is_numeric(tgt_type) || tgt_type == logical_type::STRING_LITERAL)) {
                    auto& src_vec = local.data[i];
                    auto target_type = table_columns[i].type();
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    components::vector::vector_t casted(resource(), target_type, local.size());
                    for (uint64_t row = 0; row < local.size(); row++) {
                        if (src_vec.validity().row_is_valid(row)) {
                            casted.set_value(row, src_vec.value(row).cast_as(target_type));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    local.data[i] = std::move(casted);
                }
            }
        }

        // Direct append — no dedup, no NOT NULL enforcement. txn={0,0} is committed-at-0
        // (replay/bootstrap); a non-zero txn makes the append MVCC-aware (ddl_* path).
        return s->append(local, txn);
    }

    void manager_disk_t::direct_delete_sync(const collection_full_name_t& name,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count) {
        direct_delete_sync(name, row_ids, count, components::table::transaction_data{0, 0});
    }

    void manager_disk_t::direct_delete_sync(const collection_full_name_t& name,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count,
                                            const components::table::transaction_data& txn) {
        auto* s = get_storage(name);
        if (!s || row_ids.empty())
            return;

        components::vector::vector_t ids_vec(
            resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count && i < row_ids.size(); i++) {
            ids_vec.set_value(i, components::types::logical_value_t(resource(), row_ids[i]));
        }
        s->delete_rows(ids_vec, count, txn.transaction_id);
    }

    void manager_disk_t::direct_update_sync(const collection_full_name_t& name,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            components::vector::data_chunk_t& new_data) {
        auto* s = get_storage(name);
        if (!s || row_ids.empty())
            return;

        // Build update data matching storage columns (by name).
        // The WAL update chunk may have extra columns from update expressions.
        const auto& table_columns = s->columns();
        auto rows = new_data.size();
        std::pmr::vector<components::types::complex_logical_type> matched_types(resource());
        matched_types.reserve(table_columns.size());
        for (const auto& col_def : table_columns) {
            matched_types.push_back(col_def.type());
        }
        components::vector::data_chunk_t local(resource(), matched_types, rows);
        local.set_cardinality(rows);
        for (size_t t = 0; t < table_columns.size(); t++) {
            bool found = false;
            for (uint64_t c = 0; c < new_data.column_count(); c++) {
                if (new_data.data[c].type().has_alias() && new_data.data[c].type().alias() == table_columns[t].name()) {
                    // Copy values from source to local
                    for (uint64_t row = 0; row < rows; row++) {
                        local.data[t].set_value(row, new_data.data[c].value(row));
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                // Column not in update data — mark all rows as null
                local.data[t].validity().set_all_invalid(rows);
            }
        }

        auto count = static_cast<uint64_t>(row_ids.size());
        components::vector::vector_t ids_vec(
            resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count; i++) {
            ids_vec.set_value(i, components::types::logical_value_t(resource(), row_ids[i]));
        }
        s->update(ids_vec, local);
    }

    // --- Storage management ---

    components::storage::storage_t* manager_disk_t::get_storage(const collection_full_name_t& name) {
        auto it = storages_.find(name);
        if (it == storages_.end()) {
            error(log_, "manager_disk: storage not found for {}", name.to_string());
            return nullptr;
        }
        return it->second->storage.get();
    }

    manager_disk_t::unique_future<void> manager_disk_t::create_storage(session_id_t session,
                                                                       collection_full_name_t name) {
        trace(log_, "manager_disk_t::create_storage , session : {} , name : {}", session.data(), name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource()));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_with_columns(session_id_t session,
                                                collection_full_name_t name,
                                                std::vector<components::table::column_definition_t> columns) {
        trace(log_,
              "manager_disk_t::create_storage_with_columns , session : {} , name : {}",
              session.data(),
              name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_disk(session_id_t session,
                                        collection_full_name_t name,
                                        std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_disk , session : {} , name : {}", session.data(), name.to_string());
        auto otbx_path = config_.path / name.database / "main" / name.collection / "table.otbx";
        std::filesystem::create_directories(otbx_path.parent_path());
        storages_.emplace(name,
                          std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::drop_storage(session_id_t session,
                                                                     collection_full_name_t name) {
        trace(log_, "manager_disk_t::drop_storage , session : {} , name : {}", session.data(), name.to_string());
        storages_.erase(name);
        co_return;
    }

    // --- Storage queries ---

    manager_disk_t::unique_future<std::pmr::vector<components::types::complex_logical_type>>
    manager_disk_t::storage_types(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return std::pmr::vector<components::types::complex_logical_type>(resource());
        }
        co_return s->types();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_total_rows(session_id_t /*session*/,
                                                                               collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return 0;
        }
        co_return s->total_rows();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_calculate_size(session_id_t /*session*/,
                                                                                   collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return 0;
        }
        co_return s->calculate_size();
    }

    // --- Storage data operations ---

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan(session_id_t /*session*/,
                                 collection_full_name_t name,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int limit,
                                 components::table::transaction_data txn) {
        auto* s = get_storage(name);
        if (!s) {
            co_return nullptr;
        }
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan(*result, filter.get(), limit, txn);
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_fetch(session_id_t /*session*/,
                                  collection_full_name_t name,
                                  components::vector::vector_t row_ids,
                                  uint64_t count) {
        auto* s = get_storage(name);
        if (!s) {
            co_return nullptr;
        }
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types, count);
        s->fetch(*result, row_ids, count);
        std::memcpy(result->row_ids.data(), row_ids.data(), count * sizeof(int64_t));
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan_segment(session_id_t /*session*/,
                                         collection_full_name_t name,
                                         int64_t start,
                                         uint64_t count) {
        auto* s = get_storage(name);
        if (!s) {
            co_return nullptr;
        }
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan_segment(start, count, [&result](components::vector::data_chunk_t& chunk) { result->append(chunk); });
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::pair<uint64_t, uint64_t>>
    manager_disk_t::storage_append(execution_context_t ctx, std::unique_ptr<components::vector::data_chunk_t> data) {
        auto& name = ctx.name;
        auto& txn = ctx.txn;
        auto* s = get_storage(name);
        if (!s || !data || data->size() == 0) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }

        // 1. Schema adoption
        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        // 2. Column expansion — reorder/expand incoming data to match storage columns
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && data->column_count() > 0) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            // Positional fallback: when the chunk has the same column count as the table
            // but column names don't match (e.g. INSERT with renamed alias), align by position.
            const bool positional_fallback = (data->column_count() == table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < data->column_count(); col++) {
                    if (data->data[col].type().has_alias() &&
                        data->data[col].type().alias() == table_columns[t].name()) {
                        expanded_data.push_back(std::move(data->data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found && positional_fallback && t < data->column_count()) {
                    expanded_data.push_back(std::move(data->data[t]));
                    found = true;
                }
                if (!found) {
                    if (table_columns[t].has_default_value()) {
                        // Apply DEFAULT value for missing column
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        for (uint64_t row = 0; row < data->size(); row++) {
                            expanded_data.back().set_value(row, table_columns[t].default_value());
                        }
                    } else {
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        expanded_data.back().validity().set_all_invalid(data->size());
                    }
                }
            }
            data->data = std::move(expanded_data);
        }

        // 2b. NOT NULL enforcement
        if (!table_columns.empty()) {
            for (size_t col = 0; col < table_columns.size() && col < data->column_count(); col++) {
                if (table_columns[col].is_not_null()) {
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (!data->data[col].validity().row_is_valid(row)) {
                            trace(log_, "storage_append: NOT NULL violation on column '{}'", table_columns[col].name());
                            co_return std::make_pair(uint64_t{0}, uint64_t{0});
                        }
                    }
                }
            }
        }

        // 3. Dedup — filter out rows with _id values that already exist in the table
        if (s->total_rows() > 0) {
            int64_t id_col = -1;
            for (uint64_t col = 0; col < data->column_count(); col++) {
                if (data->data[col].type().has_alias() && data->data[col].type().alias() == "_id") {
                    id_col = static_cast<int64_t>(col);
                    break;
                }
            }
            if (id_col >= 0) {
                auto existing = std::make_unique<components::vector::data_chunk_t>(resource(), s->types(), 0);
                s->scan(*existing, nullptr, -1);

                int64_t existing_id_col = -1;
                for (uint64_t col = 0; col < existing->column_count(); col++) {
                    if (existing->data[col].type().has_alias() && existing->data[col].type().alias() == "_id") {
                        existing_id_col = static_cast<int64_t>(col);
                        break;
                    }
                }

                if (existing_id_col >= 0 && existing->size() > 0) {
                    std::unordered_set<std::string> existing_ids;
                    for (uint64_t i = 0; i < existing->size(); i++) {
                        auto val = existing->data[static_cast<size_t>(existing_id_col)].value(i);
                        if (!val.is_null()) {
                            existing_ids.emplace(val.value<std::string_view>());
                        }
                    }

                    std::vector<uint64_t> keep_rows;
                    keep_rows.reserve(data->size());
                    for (uint64_t i = 0; i < data->size(); i++) {
                        auto val = data->data[static_cast<size_t>(id_col)].value(i);
                        if (val.is_null() ||
                            existing_ids.find(std::string(val.value<std::string_view>())) == existing_ids.end()) {
                            keep_rows.push_back(i);
                        }
                    }

                    if (keep_rows.empty()) {
                        co_return std::make_pair(uint64_t{0}, uint64_t{0});
                    }

                    if (keep_rows.size() < data->size()) {
                        auto filtered = std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                                           data->types(),
                                                                                           keep_rows.size());
                        for (uint64_t col = 0; col < data->column_count(); col++) {
                            for (uint64_t i = 0; i < keep_rows.size(); i++) {
                                auto val = data->data[col].value(keep_rows[i]);
                                filtered->data[col].set_value(i, val);
                            }
                        }
                        data = std::move(filtered);
                    }
                }
            }
        }

        // 4. Type promotion/conversion (numeric↔numeric, numeric↔string)
        if (s->has_schema() && !table_columns.empty()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            for (size_t i = 0; i < table_columns.size() && i < data->column_count(); i++) {
                auto src_type = data->data[i].type();
                auto tgt_type = table_columns[i].type();
                if (src_type != tgt_type && src_type.is_convertable_to(tgt_type)) {
                    auto& src_vec = data->data[i];
                    auto target_type = table_columns[i].type();
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    components::vector::vector_t casted(resource(), target_type, data->size());
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (src_vec.validity().row_is_valid(row)) {
                            casted.set_value(row, src_vec.value(row).cast_as(target_type));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    data->data[i] = std::move(casted);
                }
            }
        }

        // 5. Append
        auto actual_count = data->size();
        uint64_t start_row;
        if (txn.transaction_id != 0) {
            start_row = s->append(*data, txn);
        } else {
            start_row = s->append(*data);
        }
        co_return std::make_pair(start_row, actual_count);
    }

    manager_disk_t::unique_future<std::pair<int64_t, uint64_t>>
    manager_disk_t::storage_update(execution_context_t ctx,
                                   components::vector::vector_t row_ids,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(ctx.name);
        if (!s) {
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        co_return s->update(row_ids, *data, ctx.txn);
    }

    manager_disk_t::unique_future<uint64_t>
    manager_disk_t::storage_delete_rows(execution_context_t ctx, components::vector::vector_t row_ids, uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (!s) {
            co_return 0;
        }
        if (ctx.txn.transaction_id != 0) {
            co_return s->delete_rows(row_ids, count, ctx.txn.transaction_id);
        }
        co_return s->delete_rows(row_ids, count);
    }

    // MVCC commit/revert methods

    manager_disk_t::unique_future<void> manager_disk_t::storage_commit_append(execution_context_t ctx,
                                                                              uint64_t commit_id,
                                                                              int64_t row_start,
                                                                              uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (s)
            s->commit_append(commit_id, row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_revert_append(execution_context_t ctx, int64_t row_start, uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (s)
            s->revert_append(row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_commit_delete(execution_context_t ctx,
                                                                              uint64_t commit_id) {
        auto* s = get_storage(ctx.name);
        if (s)
            s->commit_all_deletes(ctx.txn.transaction_id, commit_id);
        co_return;
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t { return agents_[0]->address(); }


} //namespace services::disk} // namespace services::disk
