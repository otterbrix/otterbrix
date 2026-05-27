#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    uint64_t manager_disk_t::direct_append_sync(catalog::oid_t table_oid,
                                                components::vector::data_chunk_t& data,
                                                core::date::timezone_offset_t session_tz) {
        return direct_append_sync(table_oid, data, session_tz, components::table::transaction_data{0, 0});
    }

    uint64_t manager_disk_t::direct_append_sync(catalog::oid_t table_oid,
                                                components::vector::data_chunk_t& data,
                                                core::date::timezone_offset_t session_tz,
                                                const components::table::transaction_data& txn) {
        auto* s = get_storage(table_oid);
        if (!s || data.size() == 0)
            return 0;

        auto local = rebuild_chunk(resource(), data);

        if (!s->has_schema() && local.column_count() > 0) {
            s->adopt_schema(local.types());
        }

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
                            casted.set_value(row, src_vec.value(row).cast_as(target_type, session_tz));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    local.data[i] = std::move(casted);
                }
            }
        }

        return s->append(local, txn);
    }

    void manager_disk_t::direct_delete_sync(catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count) {
        direct_delete_sync(table_oid, row_ids, count, components::table::transaction_data{0, 0});
    }

    void manager_disk_t::direct_delete_sync(catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            uint64_t count,
                                            const components::table::transaction_data& txn) {
        auto* s = get_storage(table_oid);
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

    void manager_disk_t::direct_update_sync(catalog::oid_t table_oid,
                                            const std::pmr::vector<int64_t>& row_ids,
                                            components::vector::data_chunk_t& new_data) {
        auto* s = get_storage(table_oid);
        if (!s || row_ids.empty())
            return;

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
                    for (uint64_t row = 0; row < rows; row++) {
                        local.data[t].set_value(row, new_data.data[c].value(row));
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
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

    components::storage::storage_t* manager_disk_t::get_storage(catalog::oid_t table_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            error(log_, "manager_disk: storage not found for oid={}", static_cast<unsigned>(table_oid));
            return nullptr;
        }
        return it->second->storage.get();
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage(session_id_t session, catalog::oid_t table_oid, catalog::oid_t /*database_oid*/) {
        trace(log_,
              "manager_disk_t::create_storage , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        storages_.emplace(table_oid, std::make_unique<collection_storage_entry_t>(resource()));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_with_columns(session_id_t session,
                                                catalog::oid_t table_oid,
                                                catalog::oid_t /*database_oid*/,
                                                std::vector<components::table::column_definition_t> columns) {
        trace(log_,
              "manager_disk_t::create_storage_with_columns , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        storages_.emplace(table_oid, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_disk(session_id_t session,
                                        catalog::oid_t table_oid,
                                        catalog::oid_t database_oid,
                                        std::vector<components::table::column_definition_t> columns) {
        trace(log_,
              "manager_disk_t::create_storage_disk , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        auto otbx_path = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                         std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
        std::filesystem::create_directories(otbx_path.parent_path());
        storages_.emplace(table_oid,
                          std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::drop_storage(session_id_t session, catalog::oid_t table_oid) {
        trace(log_,
              "manager_disk_t::drop_storage , session : {} , oid : {}",
              session.data(),
              static_cast<unsigned>(table_oid));
        // Physically remove the .otbx file (and its sidecar + per-oid directory)
        // when dropping a DISK-backed storage. Otherwise a restart would see the
        // surviving .otbx, WAL replay would synthesise a phantom storage, and
        // re-CREATE TABLE could collide with the recycled oid.
        if (auto it = storages_.find(table_oid); it != storages_.end()) {
            auto otbx_path = it->second->otbx_path;
            storages_.erase(it);
            if (!otbx_path.empty()) {
                std::error_code ec;
                std::filesystem::remove(otbx_path, ec);
                auto sidecar = otbx_path;
                sidecar += ".wal_id";
                std::filesystem::remove(sidecar, ec);
                auto prev = otbx_path;
                prev += ".prev";
                std::filesystem::remove(prev, ec);
                std::filesystem::remove(otbx_path.parent_path(), ec);
            }
        }
        co_return;
    }

    // --- Storage queries ---

    manager_disk_t::unique_future<std::pmr::vector<components::types::complex_logical_type>>
    manager_disk_t::storage_types(session_id_t /*session*/, catalog::oid_t table_oid) {
        auto* s = get_storage(table_oid);
        if (!s) {
            co_return std::pmr::vector<components::types::complex_logical_type>(resource());
        }
        co_return s->types();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_total_rows(session_id_t /*session*/,
                                                                               catalog::oid_t table_oid) {
        auto* s = get_storage(table_oid);
        if (!s) {
            co_return 0;
        }
        co_return s->total_rows();
    }

    // --- Storage data operations ---

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan(session_id_t /*session*/,
                                 catalog::oid_t table_oid,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int limit,
                                 components::table::transaction_data txn) {
        auto* s = get_storage(table_oid);
        if (!s) {
            co_return nullptr;
        }
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan(*result, filter.get(), limit, txn);
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan_batched(session_id_t /*session*/,
                                         catalog::oid_t table_oid,
                                         std::unique_ptr<components::table::table_filter_t> filter,
                                         int64_t limit,
                                         std::vector<size_t> projected_cols,
                                         components::table::transaction_data txn) {
        std::pmr::vector<components::vector::data_chunk_t> batches{resource()};
        auto* s = get_storage(table_oid);
        if (!s) {
            co_return std::move(batches);
        }
        const std::vector<size_t>* projected_ptr = projected_cols.empty() ? nullptr : &projected_cols;
        s->scan_batched(batches, filter.get(), limit, projected_ptr, txn);
        co_return std::move(batches);
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_fetch(session_id_t /*session*/,
                                  catalog::oid_t table_oid,
                                  components::vector::vector_t row_ids,
                                  uint64_t count) {
        auto* s = get_storage(table_oid);
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
                                         catalog::oid_t table_oid,
                                         int64_t start,
                                         uint64_t count) {
        auto* s = get_storage(table_oid);
        if (!s) {
            co_return nullptr;
        }
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan_segment(start, count, [&result](components::vector::data_chunk_t& chunk) { chunk.copy(*result, 0); });
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::pair<uint64_t, uint64_t>>
    manager_disk_t::storage_append(execution_context_t ctx,
                                   catalog::oid_t table_oid,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        auto& txn = ctx.txn;
        auto* s = get_storage(table_oid);
        if (!s || !data || data->size() == 0) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }

        // Computing (relkind='g') tables may hold several columns with the same
        // name but different types (multi-type fields): schema growth and column
        // matching key on (name, type) instead of name alone, and no coercion.
        bool is_computed_table = false;
        if (auto it = storages_.find(table_oid); it != storages_.end()) {
            is_computed_table = it->second->is_computed;
        }

        // 1. Schema adoption
        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        // 1b. Dynamic schema growth for IN_MEMORY storages.
        //
        // Trigger only when chunk and table differ in width — that covers
        // both "fewer columns than the table, with a new alias" (relkind='g'
        // partial INSERT introducing a fresh column) and "more columns than
        // the table" (truly new attribute). When `chunk.column_count() ==
        // table.column_count()`, an alias mismatch is the "rename / type
        // conversion" pattern (e.g. INSERT with column "count_but_integer"
        // into a table whose column is "count") and the column-expansion
        // loop's positional fallback handles it. Without this gate, the
        // renamed column would be dynamic-added, the table would grow to
        // N+1 columns, positional_fallback would then evaluate
        // `data.count != table.count` and refuse to fill the original
        // (now-unfilled) column — producing a NOT NULL violation on the
        // original. test_collection::insert::"insert with conversions"
        // exercises this exact path.
        // Computing tables grow per (name,type), regardless of chunk width, so a
        // same-name different-type field becomes a separate physical column.
        // Non-computing tables keep the original width-difference guard (an
        // alias mismatch at equal width is a rename/type-conversion, not growth).
        if (s->has_schema() && data->column_count() > 0 &&
            (is_computed_table || data->column_count() != s->columns().size())) {
            auto it = storages_.find(table_oid);
            if (it != storages_.end() && it->second->table_storage.mode() == storage_mode_t::IN_MEMORY) {
                std::vector<components::table::column_definition_t> new_columns;
                for (uint64_t col = 0; col < data->column_count(); col++) {
                    if (!data->data[col].type().has_alias()) {
                        continue;
                    }
                    const auto alias = data->data[col].type().alias();
                    const auto ctype = data->data[col].type().type();
                    bool present = false;
                    for (const auto& tc : s->columns()) {
                        if (tc.name() == alias && (!is_computed_table || tc.type().type() == ctype)) {
                            present = true;
                            break;
                        }
                    }
                    if (!present) {
                        auto ct = data->data[col].type();
                        ct.set_alias(alias);
                        new_columns.emplace_back(alias, ct);
                    }
                }
                if (!new_columns.empty()) {
                    for (auto& col : new_columns) {
                        it->second->add_column(col, resource());
                    }
                    s = get_storage(table_oid);
                    if (!s) {
                        co_return std::make_pair(uint64_t{0}, uint64_t{0});
                    }
                }
            }
        }

        // 2. Column expansion
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && data->column_count() > 0) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            // Computing tables match by (name, type) so each type-variant lands in
            // its own physical column; unmatched variants get NULL. Positional
            // fallback is disabled there (it assumes one column per name).
            const bool positional_fallback = !is_computed_table && (data->column_count() == table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < data->column_count(); col++) {
                    if (data->data[col].type().has_alias() &&
                        data->data[col].type().alias() == table_columns[t].name() &&
                        (!is_computed_table || data->data[col].type().type() == table_columns[t].type().type())) {
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

        // 3. Dedup
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

        // 4. Type promotion
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
                            casted.set_value(row, src_vec.value(row).cast_as(target_type, ctx.session_tz));
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
                                   catalog::oid_t table_oid,
                                   components::vector::vector_t row_ids,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(table_oid);
        if (!s) {
            co_return std::pair<int64_t, uint64_t>{0, 0};
        }
        co_return s->update(row_ids, *data, ctx.txn);
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_delete_rows(execution_context_t ctx,
                                                                                catalog::oid_t table_oid,
                                                                                components::vector::vector_t row_ids,
                                                                                uint64_t count) {
        auto* s = get_storage(table_oid);
        if (!s) {
            co_return 0;
        }
        if (ctx.txn.transaction_id != 0) {
            co_return s->delete_rows(row_ids, count, ctx.txn.transaction_id);
        }
        co_return s->delete_rows(row_ids, count);
    }

    // MVCC commit/revert methods

    manager_disk_t::unique_future<void> manager_disk_t::storage_commit_append(execution_context_t /*ctx*/,
                                                                              catalog::oid_t table_oid,
                                                                              uint64_t commit_id,
                                                                              int64_t row_start,
                                                                              uint64_t count) {
        auto* s = get_storage(table_oid);
        if (s)
            s->commit_append(commit_id, row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_revert_append(execution_context_t /*ctx*/,
                                                                              catalog::oid_t table_oid,
                                                                              int64_t row_start,
                                                                              uint64_t count) {
        auto* s = get_storage(table_oid);
        if (s)
            s->revert_append(row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_commit_delete(execution_context_t ctx, catalog::oid_t table_oid, uint64_t commit_id) {
        auto* s = get_storage(table_oid);
        if (s) {
            s->commit_all_deletes(ctx.txn.transaction_id, commit_id);
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_commit_appends(execution_context_t /*ctx*/,
                                           uint64_t commit_id,
                                           std::vector<components::pg_catalog_append_range_t> ranges) {
        for (const auto& r : ranges) {
            if (r.count == 0)
                continue;
            auto* s = get_storage(r.table_oid);
            if (s)
                s->commit_append(commit_id, r.start_row, r.count);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_commit_deletes(execution_context_t ctx,
                                                                               uint64_t commit_id,
                                                                               std::set<catalog::oid_t> tables) {
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0)
            co_return;
        for (const auto& tbl_oid : tables) {
            auto* s = get_storage(tbl_oid);
            if (s)
                s->commit_all_deletes(txn_id, commit_id);
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_revert_appends(execution_context_t /*ctx*/,
                                           std::vector<components::pg_catalog_append_range_t> ranges) {
        for (auto it = ranges.rbegin(); it != ranges.rend(); ++it) {
            if (it->count == 0)
                continue;
            auto* s = get_storage(it->table_oid);
            if (s)
                s->revert_append(it->start_row, it->count);
        }
        co_return;
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t { return agents_[0]->address(); }

} //namespace services::disk
