#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Crash-safe pg_catalog row append: WAL is written first so a crash before the
    // storage update can be replayed on restart, then storage is updated.
    manager_disk_t::unique_future<components::pg_catalog_append_range_t>
    manager_disk_t::append_pg_catalog_row(execution_context_t ctx,
                                          components::catalog::oid_t table_oid,
                                          components::vector::data_chunk_t row) {
        const bool wal_available = (manager_wal_ != actor_zeta::address_t::empty_address());
        if (wal_available) {
            auto wal_chunk = std::make_unique<components::vector::data_chunk_t>(resource(), row.types(), row.size());
            wal_chunk->set_cardinality(row.size());
            for (uint64_t col = 0; col < row.column_count(); col++) {
                for (uint64_t r = 0; r < row.size(); r++) {
                    wal_chunk->data[col].set_value(r, row.data[col].value(r));
                }
            }
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                             &wal::manager_wal_replicate_t::write_physical_insert,
                                             ctx.session,
                                             table_oid,
                                             std::move(wal_chunk),
                                             std::uint64_t{0},
                                             static_cast<std::uint64_t>(row.size()),
                                             ctx.txn.transaction_id);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "pg_catalog insert WAL write returned zero id for oid={}",
                      static_cast<unsigned>(table_oid));
            }
        }
        const auto count = static_cast<std::uint64_t>(row.size());
        const auto start_row = direct_append_sync(table_oid, row, ctx.session_tz, ctx.txn);
        if (ctx.txn.transaction_id == 0 || count == 0) {
            co_return components::pg_catalog_append_range_t{table_oid, static_cast<int64_t>(start_row), 0};
        }
        co_return components::pg_catalog_append_range_t{table_oid, static_cast<int64_t>(start_row), count};
    }

    manager_disk_t::unique_future<void> manager_disk_t::delete_pg_catalog_rows(execution_context_t ctx,
                                                                               components::catalog::oid_t table_oid,
                                                                               std::int64_t oid_col_idx,
                                                                               components::catalog::oid_t target_oid) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            co_return;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        inline_scan(it->second->table_storage.table(),
                    {oid_col_idx},
                    &scan_resource,
                    [&, oid_col_idx](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(static_cast<uint64_t>(oid_col_idx), i);
                        if (v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) == target_oid) {
                            row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                        }
                        return true;
                    });
        if (row_ids.empty()) {
            co_return;
        }
        if (manager_wal_ != actor_zeta::address_t::empty_address()) {
            std::pmr::vector<std::int64_t> wal_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                             &wal::manager_wal_replicate_t::write_physical_delete,
                                             ctx.session,
                                             table_oid,
                                             std::move(wal_ids),
                                             static_cast<std::uint64_t>(row_ids.size()),
                                             ctx.txn.transaction_id);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "pg_catalog delete WAL write returned zero id for oid={}",
                      static_cast<unsigned>(table_oid));
            }
        }
        direct_delete_sync(table_oid, row_ids, static_cast<std::uint64_t>(row_ids.size()), ctx.txn);
        co_return;
    }

    manager_disk_t::unique_future<std::uint64_t>
    manager_disk_t::compact_relkind_g_storage(execution_context_t /*ctx*/,
                                              components::catalog::oid_t table_oid,
                                              std::set<std::string> live_attnames) {
        auto it = storages_.find(table_oid);
        if (it == storages_.end()) {
            co_return 0;
        }
        if (it->second->table_storage.mode() != storage_mode_t::IN_MEMORY) {
            trace(log_,
                  "compact_relkind_g_storage: skip DISK-backed oid={} (out of scope)",
                  static_cast<unsigned>(table_oid));
            co_return 0;
        }

        std::vector<std::string> to_drop;
        {
            const auto& cols = it->second->table_storage.table().columns();
            to_drop.reserve(cols.size());
            for (const auto& c : cols) {
                if (live_attnames.find(c.name()) == live_attnames.end()) {
                    to_drop.push_back(c.name());
                }
            }
        }

        std::uint64_t dropped = 0;
        for (const auto& attname : to_drop) {
            if (it->second->drop_column(attname, resource())) {
                ++dropped;
            }
        }
        co_return dropped;
    }

} // namespace services::disk
